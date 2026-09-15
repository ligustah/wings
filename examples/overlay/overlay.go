// Package overlay is a wings program for testing the p2p overlay: it fans work
// out across the cluster and reports which host ran each piece, and it passes a
// channel between two forked threads, so a run over -p2p -p2p-overlay exercises
// both replicated dispatch and a cross-node channel over the tailnet the
// coordinator hosts.
//
// Build it once, then run it against each target. The coordinator runs on your
// machine and hosts the overlay; -p2p-overlay is the address workers reach it at,
// so on the cloud it must be one your workers can dial (your public IP:port, or a
// tailnet address), not 127.0.0.1.
//
//	go run github.com/ligustah/wings/cmd/wings build -pkg ./examples/overlay -o overlaytest
//
//	# Local: coordinator and worker child processes on one machine.
//	./overlaytest -target local -workers 3 -p2p -p2p-overlay 127.0.0.1:8443 -input '{"jobs":12}'
//
//	# One cloud: coordinator here, workers on EC2, all on the overlay.
//	WINGS_OVERLAY_AWS_REGION=eu-west-1 \
//	  ./overlaytest -target remote -workers 3 -p2p -p2p-overlay <your-public-ip>:8443 -input '{"jobs":12}'
//
//	# Both clouds in one cluster: workers split across GCP and EC2, coordinator
//	# local — three networks on one overlay, the real cross-NAT test.
//	WINGS_OVERLAY_GCP_PROJECT=my-project WINGS_OVERLAY_GCP_ZONE=europe-west1-b \
//	WINGS_OVERLAY_AWS_REGION=eu-west-1 \
//	  ./overlaytest -target remote -workers 4 -p2p -p2p-overlay <your-public-ip>:8443 -input '{"jobs":16}'
package overlay

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/ligustah/wings"
	"github.com/ligustah/wings/aws"
	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/gcp"
)

// Params is the workflow's input, given as JSON via -input.
type Params struct {
	// Jobs is how many ping jobs to fan out. Default 12.
	Jobs int `json:"jobs"`
	// ChannelValues is how many values the producer sends over the cross-node
	// channel. Default 100.
	ChannelValues int `json:"channelValues"`
}

// Ping reports the host and process it ran on, so a fan-out shows work landing on
// different machines — different clouds, over the overlay. A package-scope
// flow.Define, so every worker that links this package has it.
var Ping = flow.Define(func(ctx flow.Context, seq int) (string, error) {
	host, err := ctx.Effect(os.Hostname)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s#%d", host, os.Getpid()), nil
}, flow.WithName("ping"))

// produce sends seq values on a channel and closes it. Spawned as its own thread,
// so its channel reaches the consumer's thread across whatever nodes they land on.
var produce = flow.Define(func(ctx flow.Context, in produceIn) (flow.None, error) {
	for i := range in.Count {
		if err := in.Out.Send(ctx, i); err != nil {
			return flow.None{}, err
		}
	}
	return flow.None{}, in.Out.Close(ctx)
}, flow.WithName("overlay.produce"))

type produceIn struct {
	Count int
	Out   flow.Writer[int]
}

// Result is what the workflow returns and prints.
type Result struct {
	// Hosts is every distinct host#pid a ping ran on — one per node that did work.
	Hosts []string `json:"hosts"`
	// Received and Sum are the count and sum of the values that crossed the channel.
	Received int `json:"received"`
	Sum      int `json:"sum"`
	Elapsed  string `json:"elapsed"`
}

// Main fans Ping out across the cluster, then runs a producer thread whose values
// it consumes over a channel — so a p2p-overlay run proves both dispatch and a
// channel cross the tailnet. It prints the distinct hosts that participated.
var Main = flow.Define(func(ctx flow.Context, in Params) (Result, error) {
	start := time.Now()
	jobs := cmp.Or(in.Jobs, 12)
	values := cmp.Or(in.ChannelValues, 100)

	seqs := make([]int, jobs)
	for i := range seqs {
		seqs[i] = i
	}
	pings, err := ctx.Map(Ping, seqs)
	if err != nil {
		return Result{}, err
	}
	hosts := slices.Clone(pings)
	slices.Sort(hosts)
	hosts = slices.Compact(hosts)

	// A producer thread sends over a channel the workflow consumes; when the
	// producer lands on another node, the values cross it over the overlay.
	r, w := ctx.NewChannel[int](flow.WithCapacity(8))
	prod := ctx.Go(produce, produceIn{Count: values, Out: w})

	var received, sum int
	for {
		v, ok, err := r.Recv(ctx)
		if err != nil {
			return Result{}, err
		}
		if !ok {
			break
		}
		received++
		sum += v
	}
	if _, err := prod.Await(ctx); err != nil {
		return Result{}, err
	}

	return Result{
		Hosts:    hosts,
		Received: received,
		Sum:      sum,
		Elapsed:  time.Since(start).Round(time.Millisecond).String(),
	}, nil
}, flow.WithName("overlay"))

var _ = flow.Main(Main)

// Provisioner builds the remote target from the environment, so one -target
// remote cluster can span more than one cloud: set WINGS_OVERLAY_GCP_PROJECT (and
// _ZONE) for GCP, WINGS_OVERLAY_AWS_REGION for EC2, or both to split workers
// across the two. wings build bakes this in; with neither set, use -target local.
func Provisioner() wings.Provisioner {
	var provs []wings.Provisioner
	if project := os.Getenv("WINGS_OVERLAY_GCP_PROJECT"); project != "" {
		provs = append(provs, gcp.New(gcp.Config{Project: project, Zone: os.Getenv("WINGS_OVERLAY_GCP_ZONE")}))
	}
	if region := os.Getenv("WINGS_OVERLAY_AWS_REGION"); region != "" {
		provs = append(provs, aws.New(aws.Config{Region: region}))
	}
	switch len(provs) {
	case 0:
		return nil
	case 1:
		return provs[0]
	default:
		return &composite{provs: provs}
	}
}

// composite spreads a cluster's workers across several provisioners, so workers
// on different clouds join one overlay. It splits leases round-robin, and asks
// every sub-provisioner to recover on reattach — each finds only its own.
type composite struct{ provs []wings.Provisioner }

func (c *composite) Provision(ctx context.Context, leases []string) ([]wings.Machine, error) {
	buckets := make([][]string, len(c.provs))
	for i, lease := range leases {
		buckets[i%len(c.provs)] = append(buckets[i%len(c.provs)], lease)
	}
	var all []wings.Machine
	for i, p := range c.provs {
		if len(buckets[i]) == 0 {
			continue
		}
		machines, err := p.Provision(ctx, buckets[i])
		if err != nil {
			for _, m := range all {
				_ = m.Close(ctx)
			}
			return nil, err
		}
		all = append(all, machines...)
	}
	return all, nil
}

func (c *composite) Reattach(ctx context.Context, leases []string) ([]wings.Machine, error) {
	var all []wings.Machine
	for _, p := range c.provs {
		r, ok := p.(wings.Reattacher)
		if !ok {
			continue
		}
		machines, err := r.Reattach(ctx, leases)
		if err != nil {
			continue
		}
		all = append(all, machines...)
	}
	return all, nil
}

// Package gcpchaos is a wings program for chaos-testing a p2p cluster on real
// cloud nodes: it fans a batch of checkpointed, slow jobs across the workers, so
// killing a node mid-run shows work redispatch onto a survivor and the durable
// tier surviving the loss. Each job reports the host it ran on, so the result
// names every node that participated.
//
//	go run github.com/ligustah/wings/cmd/wings build -pkg ./examples/gcpchaos -o gcpchaos.exe
//	GOOGLE_APPLICATION_CREDENTIALS=sa.json ./gcpchaos.exe -target remote -provider gcp \
//	  -gcp.project P -gcp.zone europe-west1-b -workers 4 -concurrency 2 -p2p \
//	  -input '{"jobs":48,"steps":15}'
package gcpchaos

import (
	"cmp"
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/ligustah/wings/flow"
)

// Params is the workflow input, given as JSON via -input.
type Params struct {
	// Jobs is how many jobs to fan out. Default 48.
	Jobs int `json:"jobs"`
	// Steps is how many one-second checkpointed steps each job runs. Default 15.
	Steps int `json:"steps"`
}

// Result is what the workflow returns and prints.
type Result struct {
	// Hosts is every distinct host#pid a job ran on.
	Hosts []string `json:"hosts"`
	// Done is how many jobs completed.
	Done int `json:"done"`
	// Elapsed is the wall-clock time the fan-out took.
	Elapsed string `json:"elapsed"`
}

// Work sleeps for Steps seconds, checkpointing after each so a redispatch after a
// node is killed resumes rather than restarts, and returns the host it ran on.
var Work = flow.Define(func(ctx flow.Context, steps int) (string, error) {
	host, err := ctx.Effect(os.Hostname)
	if err != nil {
		return "", err
	}
	start, _, err := ctx.Checkpoint[int]()
	if err != nil {
		return "", err
	}
	for i := start; i < steps; i++ {
		time.Sleep(time.Second)
		if err := ctx.Heartbeat(i + 1); err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%s#%d", host, os.Getpid()), nil
}, flow.WithName("gcpchaos.work"))

// Main fans Work out across the cluster and reports the distinct hosts that ran
// it, so a kill mid-run shows the survivors finishing the dead node's share.
var Main = flow.Define(func(ctx flow.Context, in Params) (Result, error) {
	start := time.Now()
	jobs := cmp.Or(in.Jobs, 48)
	steps := cmp.Or(in.Steps, 15)

	each := make([]int, jobs)
	for i := range each {
		each[i] = steps
	}
	ran, err := ctx.Map(Work, each)
	if err != nil {
		return Result{}, err
	}

	hosts := slices.Clone(ran)
	slices.Sort(hosts)
	hosts = slices.Compact(hosts)
	return Result{
		Hosts:   hosts,
		Done:    len(ran),
		Elapsed: time.Since(start).Round(time.Millisecond).String(),
	}, nil
}, flow.WithName("gcpchaos"))

var _ = flow.Main(Main)

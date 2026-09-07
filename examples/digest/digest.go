// Package digest is a complete wings program — work functions and workflows in
// a library package, with no cloud, Target, or provisioner in the code; where
// the work runs is chosen on the command line:
//
//	go run github.com/ligustah/wings/cmd/wings build -pkg ./examples/digest -o digest
//
//	./digest -target inprocess -input '{"jobs":32}'
//	./digest -target local -workers 4 -input '{"jobs":32}'
//	./digest -target remote -workers 4 -input @params.json -gcp.project my-project -gcp.zone europe-west1-b
package digest

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ligustah/wings/flow"
)

// Params is the workflow's input, supplied as JSON via -input.
type Params struct {
	// Jobs is how many seeds to digest. Default 32.
	Jobs int `json:"jobs"`
	// Rounds is how many times each seed is hashed. Default 2,000,000.
	Rounds int `json:"rounds"`
}

// Work is one unit of input.
type Work struct {
	Seed   string `json:"seed"`
	Rounds int    `json:"rounds"`
}

// Result is what a worker sends back; Host shows which machine did the work.
type Result struct {
	Seed   string `json:"seed"`
	Digest string `json:"digest"`
	Host   string `json:"host"`
}

// Digest hashes a seed repeatedly. A package-scope flow.Define, so every process
// that links this package — including a worker — has it in its registry.
var Digest = flow.Define(func(ctx flow.Context, in Work) (Result, error) {

	host, err := ctx.Effect(os.Hostname)
	if err != nil {
		return Result{}, err
	}

	sum := sha256.Sum256([]byte(in.Seed))
	for range in.Rounds {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		sum = sha256.Sum256(sum[:])
	}
	return Result{Seed: in.Seed, Digest: hex.EncodeToString(sum[:]), Host: host}, nil
}, flow.WithName("digest"))

// Main is the workflow: it maps Digest over a batch of seeds and prints where
// each ran. With several roots defined, -workflow picks one.
var Main = flow.Define(func(ctx flow.Context, in Params) (flow.None, error) {
	jobs, rounds := cmp.Or(in.Jobs, 32), cmp.Or(in.Rounds, 2_000_000)
	work := make([]Work, jobs)
	for i := range work {
		work[i] = Work{Seed: fmt.Sprintf("job-%03d", i), Rounds: rounds}
	}

	start := time.Now()
	results, err := ctx.Map(Digest, work)
	elapsed := time.Since(start)
	if err != nil {
		return flow.None{}, err
	}

	hosts := map[string]int{}
	for _, r := range results {
		hosts[r.Host]++
	}

	fmt.Printf("\n%d jobs in %s\n", len(results), elapsed.Round(time.Millisecond))
	for host, n := range hosts {
		fmt.Printf("  %-24s %d jobs\n", host, n)
	}
	fmt.Printf("\nfirst result: %s -> %s\n", results[0].Seed, results[0].Digest[:16])
	fmt.Println(strings.Repeat("-", 40))
	return flow.None{}, nil
}, flow.WithName("digest"))

var _ = flow.Main(Main)

// Batch digests seeds through Digest and reports each result on the channel it
// was handed as it arrives. Its own Digest calls fan out across the fleet.
type Batch struct {
	Work    []Work                `json:"work"`
	Results *flow.Channel[Result] `json:"results"`
}

var DigestBatch = flow.Define(func(ctx flow.Context, in Batch) (int, error) {
	results, err := ctx.Map(Digest, in.Work)
	if err != nil {
		return 0, err
	}
	for _, r := range results {
		if err := in.Results.Send(ctx, r); err != nil {
			return 0, err
		}
	}
	return len(results), nil
}, flow.WithName("digestBatch"))

// Fanout (-workflow fanout) splits the work into batches, each reporting on a
// shared channel the workflow reads on the coordinator while the batches write
// it on their workers. Even batches are dispatched by function name (Go); odd
// ones are run-code closures shipped as a lineage (Spawn) — both fan out.
var Fanout = flow.Define(func(ctx flow.Context, in Params) (flow.None, error) {
	jobs, rounds := cmp.Or(in.Jobs, 32), cmp.Or(in.Rounds, 2_000_000)
	const batches = 4
	results := ctx.NewChannel[Result]()

	var futures []*flow.Future[int]
	for b := range batches {
		var work []Work
		for i := b; i < jobs; i += batches {
			work = append(work, Work{Seed: fmt.Sprintf("job-%03d", i), Rounds: rounds})
		}
		batch := Batch{Work: work, Results: results}
		if b%2 == 0 {
			futures = append(futures, ctx.Go(DigestBatch, batch))
		} else {
			futures = append(futures, ctx.Spawn(func(ctx flow.Context) (int, error) {
				return DigestBatch(ctx, batch)
			}))
		}
	}

	start := time.Now()
	hosts := map[string]int{}
	for range jobs {
		r, ok, err := results.Recv(ctx)
		if err != nil {
			return flow.None{}, err
		}
		if !ok {
			return flow.None{}, fmt.Errorf("the results channel closed after %d of %d results", len(hosts), jobs)
		}
		hosts[r.Host]++
		fmt.Printf("  %s on %s\n", r.Seed, r.Host)
	}
	for _, f := range futures {
		if _, err := f.Await(ctx); err != nil {
			return flow.None{}, err
		}
	}

	fmt.Printf("\n%d results in %s, from %d batches\n", jobs, time.Since(start).Round(time.Millisecond), batches)
	for host, n := range hosts {
		fmt.Printf("  %-24s %d jobs\n", host, n)
	}
	fmt.Println(strings.Repeat("-", 40))
	return flow.None{}, nil
}, flow.WithName("fanout"))

var _ = flow.Main(Fanout)

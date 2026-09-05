// Package digest is a complete wings program: work functions, a workflow,
// and an optional provisioner.
//
// It is a LIBRARY, not a main. `wings build` generates both mains — one for the
// coordinator, one for the worker — and imports this package into each:
//
//	go run github.com/ligustah/wings/cmd/wings build -pkg ./examples/digest -o digest
//
//	./digest -target inprocess -input '{"jobs":32}'
//	./digest -target local -workers 4 -input '{"jobs":32}'
//	./digest -target remote -workers 4 -input @params.json -gcp.project my-project -gcp.zone europe-west1-b
//
// Note what is NOT in this file: no cloud, no SDK, no Target, no provisioner.
// Where the work runs is chosen entirely on the command line, and this package
// cannot tell the difference.
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

// Params is the workflow's input. It arrives as JSON from -input, and is part
// of the run: a coordinator restarted over the same -dir is given what the
// first start recorded, not whatever is on the new command line.
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

// Result is what a worker sends back. Host is here so the output shows which
// machine actually did the work.
type Result struct {
	Seed   string `json:"seed"`
	Digest string `json:"digest"`
	Host   string `json:"host"`
}

// Digest is the work: hash a seed repeatedly, so the machine it runs on is
// doing something a network round trip cannot hide.
//
// Defined at PACKAGE SCOPE, which is what puts it in the registry of every
// process that links this package — including a worker, which never runs
// the workflow. Nothing here mentions wings: a work function is a flow function,
// and wings is one place it can be sent to run.
var Digest = flow.Define("digest", func(ctx flow.Context, in Work) (Result, error) {
	// The host is an answer from outside the run, different on every machine,
	// so it is asked for as an effect: recorded once, replayed on a retry.
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
})

// Main is the workflow: what this program is about. wings runs it as a flow
// once the cluster is up, and tears the cluster down when it returns. Because
// it is a flow, a coordinator restarted over the same -dir replays what this
// already did rather than doing it again. It is the only workflow defined, so
// the binary runs it without being told; a program that defines several takes
// -workflow.
var Main = flow.DefineWorkflow("digest", func(ctx flow.Context, in Params) error {
	jobs, rounds := cmp.Or(in.Jobs, 32), cmp.Or(in.Rounds, 2_000_000)
	work := make([]Work, jobs)
	for i := range work {
		work[i] = Work{Seed: fmt.Sprintf("job-%03d", i), Rounds: rounds}
	}

	start := time.Now()
	results, err := ctx.Map(Digest, work)
	elapsed := time.Since(start)
	if err != nil {
		return err
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
	return nil
})

// Batch is a work function that does its work by calling another: it digests
// a batch of seeds through Digest — calls the cluster places like any other,
// so a batch on one worker fans out across the fleet — and reports each
// result on the channel it was handed as it comes, rather than all at once
// when it returns.
type Batch struct {
	Work    []Work                `json:"work"`
	Results *flow.Channel[Result] `json:"results"`
}

var DigestBatch = flow.Define("digestBatch", func(ctx flow.Context, in Batch) (int, error) {
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
})

// Fanout is a second workflow, chosen with -workflow fanout. It splits the
// work into batches, hands each batch a channel to report on, and prints
// results as they arrive from wherever they were computed. The channel
// crosses machines: the workflow reads it on the coordinator, the batches
// write it on their workers.
var Fanout = flow.DefineWorkflow("fanout", func(ctx flow.Context, in Params) error {
	jobs, rounds := cmp.Or(in.Jobs, 32), cmp.Or(in.Rounds, 2_000_000)
	const batches = 4
	results := ctx.NewChannel[Result]()

	var futures []*flow.Future[int]
	for b := range batches {
		var work []Work
		for i := b; i < jobs; i += batches {
			work = append(work, Work{Seed: fmt.Sprintf("job-%03d", i), Rounds: rounds})
		}
		futures = append(futures, ctx.Go(DigestBatch, Batch{Work: work, Results: results}))
	}

	start := time.Now()
	hosts := map[string]int{}
	for range jobs {
		r, ok, err := results.Recv(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("the results channel closed after %d of %d results", len(hosts), jobs)
		}
		hosts[r.Host]++
		fmt.Printf("  %s on %s\n", r.Seed, r.Host)
	}
	for _, f := range futures {
		if _, err := f.Await(ctx); err != nil {
			return err
		}
	}

	fmt.Printf("\n%d results in %s, from %d batches\n", jobs, time.Since(start).Round(time.Millisecond), batches)
	for host, n := range hosts {
		fmt.Printf("  %-24s %d jobs\n", host, n)
	}
	fmt.Println(strings.Repeat("-", 40))
	return nil
})

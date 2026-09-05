// Package digest is a complete wings program: work functions, a coordinator
// body, and an optional provisioner.
//
// It is a LIBRARY, not a main. `wings build` generates both mains — one for the
// coordinator, one for the worker — and imports this package into each:
//
//	go run github.com/ligustah/wings/cmd/wings build -pkg ./examples/digest -o digest
//
//	./digest -target inprocess -jobs 32
//	./digest -target local -workers 4
//	./digest -target remote -workers 4 -gcp.project my-project -gcp.zone europe-west1-b
//
// Note what is NOT in this file: no cloud, no SDK, no Target, no provisioner.
// Where the work runs is chosen entirely on the command line, and this package
// cannot tell the difference.
package digest

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ligustah/wings/flow"
)

// Flags registered here are parsed too: wings.CoordinatorMain calls
// flag.Parse() on the default flag set, so a package can add its own without
// wings knowing about them.
var (
	jobs   = flag.Int("jobs", 32, "number of jobs to run")
	rounds = flag.Int("rounds", 2_000_000, "hash rounds per job")
)

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
// Coordinate. Nothing here mentions wings: a work function is a flow function,
// and wings is one place it can be sent to run.
var Digest = flow.Define("digest", func(ctx flow.Context, in Work) (Result, error) {
	host, _ := os.Hostname()

	sum := sha256.Sum256([]byte(in.Seed))
	for range in.Rounds {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		sum = sha256.Sum256(sum[:])
	}
	return Result{Seed: in.Seed, Digest: hex.EncodeToString(sum[:]), Host: host}, nil
})

// Coordinate is the coordinator body. wings runs it as a flow once the cluster
// is up, and tears the cluster down when it returns. Because it is a flow, a
// coordinator restarted over the same -dir replays what this already did
// rather than doing it again.
func Coordinate(ctx flow.Context) error {
	work := make([]Work, *jobs)
	for i := range work {
		work[i] = Work{Seed: fmt.Sprintf("job-%03d", i), Rounds: *rounds}
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
}

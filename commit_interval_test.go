package wings

import (
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// THE POINT: a CommitInterval holds the coordinator's plain history writes open
// to coalesce their commits, but a channel send still commits at once on its own
// path — so a worker receiving from the coordinator gets each value without
// waiting the interval, and nothing recorded is lost: the run's result is right.
func TestACommitIntervalHoldsHistoryButNotChannelSends(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	const interval = 10 * time.Second
	const n = 5
	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 1, CommitInterval: interval})

	var got int
	started := time.Now()
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int]()
		consumer := ctx.Go(sums, feed{Values: r})
		for i := 1; i <= n; i++ {
			// A plain history event between sends: its commit coalesces under the
			// interval, riding home on the following send's commit.
			if _, err := ctx.Effect(func() (int, error) { return i, nil }); err != nil {
				return err
			}
			if err := w.Send(ctx, i); err != nil {
				return err
			}
		}
		if err := w.Close(ctx); err != nil {
			return err
		}
		var err error
		got, err = consumer.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 15 {
		t.Fatalf("the consumer summed %d, want 15", got)
	}
	// Were a send coalesced like plain history, the worker would wait for the open
	// transaction to age past CommitInterval before the value arrived; delivering
	// all n takes well under one interval, so the sends committed at once.
	if took := time.Since(started); took > interval/2 {
		t.Fatalf("the run took %v with a %v commit interval; channel sends were held by coalescing", took, interval)
	}
}

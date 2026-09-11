package wings

import (
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// THE POINT: under a CommitInterval the coordinator coalesces its commits —
// history and channel sends alike — yet a thread flushes what it has written as
// it parks (here on the await), so the worker still gets every value and the run
// returns the right total. Were the flush-at-park missing, the coalesced sends
// would never reach the worker and the run would hang.
func TestACommitIntervalHoldsHistoryButFlushesAtParks(t *testing.T) {
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
			// interval, to be flushed when the thread next parks.
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
	// Were the values only flushed once the transaction aged past CommitInterval,
	// the run would take about one interval; the await's flush-at-park delivers them
	// at once, so it finishes well under that.
	if took := time.Since(started); took > interval/2 {
		t.Fatalf("the run took %v with a %v commit interval; the flush-at-park did not deliver the sends", took, interval)
	}
}

// THE POINT: a CommitInterval coalesces commits, yet a bounded channel keeps
// flowing. The coordinator sends past the capacity; each side flushes what it has
// coalesced as it parks — the sender on a full buffer, the reader when it runs out
// — so a value reaches the reader and a consume report frees the sender's place
// without waiting out the interval. Were the boundary flush missing, the run
// would either deadlock or crawl at one interval per exchange.
func TestACommitIntervalDoesNotStallABoundedChannel(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	const interval = 10 * time.Second
	const n = 20
	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 1, CommitInterval: interval})

	var got int
	started := time.Now()
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int](flow.WithCapacity(1))
		consumer := ctx.Go(sums, feed{Values: r})
		for i := 1; i <= n; i++ {
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
	if want := n * (n + 1) / 2; got != want {
		t.Fatalf("the consumer summed %d, want %d", got, want)
	}
	if took := time.Since(started); took > interval/2 {
		t.Fatalf("the run took %v with a %v commit interval; the flush-at-park did not keep the channel flowing", took, interval)
	}
}

// THE POINT: the CommitInterval reaches the WORKERS, the fsync path that matters
// most — a worker runs the bulk of the jobs. A forked producer on a worker sends
// under a long interval, so its channel sends coalesce into far fewer commits
// rather than one fsync each; yet the values still reach the coordinator's reader,
// flushed as the producer's thread ends (and by its heartbeats), so the reader
// sums them all well within the interval. Were the worker sink to ignore the
// interval it would fsync per send; were it to coalesce without flushing at the
// boundaries, the values would never arrive.
func TestACommitIntervalCoalescesAWorkersSendsYetDelivers(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	const interval = 10 * time.Second
	const n = 50
	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 1, CommitInterval: interval})

	var got int
	started := time.Now()
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int]()
		producer := ctx.Go(counts, writeFeed{Values: w, Count: n})
		for {
			v, ok, err := r.Recv(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			got += v
		}
		_, err := producer.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := n * (n + 1) / 2; got != want {
		t.Fatalf("the reader summed %d, want %d", got, want)
	}
	if took := time.Since(started); took > interval/2 {
		t.Fatalf("the run took %v with a %v commit interval; the worker's boundary flush did not deliver its sends", took, interval)
	}
}

package wings

import (
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// THE POINT: a job's history is dropped when it returns, mid-run, not only when the
// whole run ends — so a long run of short activities does not accumulate their
// histories. fanSum and the threads it forks record events, so they leave history
// streams; those are gone while the run is still held open on other work.
func TestASettledJobsHistoryIsDropped(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 4})

	hold := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
			if _, err := ctx.Go(fanSum, struct{}{}).Await(ctx); err != nil {
				return err
			}
			// Hold the run open in-process (no job of its own), so fanSum's histories
			// are the only ones and they should be reclaimed while this waits — proof
			// the drop is per-activity, not only per-run.
			_, err := ctx.Blocking(func() (struct{}, error) {
				<-hold
				return struct{}{}, nil
			})
			return err
		})
	}()

	deadline := time.Now().Add(30 * time.Second)
	dropped := false
	for time.Now().Before(deadline) {
		if streamsWithPrefix(t, c, historyPrefix) == 0 {
			dropped = true
			break
		}
		select {
		case err := <-done:
			close(hold)
			t.Fatalf("run finished before its history was seen dropped mid-run: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	close(hold)
	if !dropped {
		t.Fatal("the settled job's history streams were not dropped while the run was still open")
	}
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// THE POINT: RetainHistory keeps a returned job's history — the anchor that proves
// the history above is real and normally created, and that the flag turns the drop
// off so a finished thread tree stays inspectable.
func TestRetainHistoryKeepsAReturnedJobsHistory(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 4, RetainHistory: true})

	var got int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		var err error
		got, err = ctx.Go(fanSum, struct{}{}).Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 15 {
		t.Fatalf("fanSum returned %d, want 15", got)
	}
	if streamsWithPrefix(t, c, historyPrefix) == 0 {
		t.Fatal("RetainHistory was set but the returned job's history was dropped anyway")
	}
}

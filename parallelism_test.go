package wings

import (
	"testing"

	"github.com/ligustah/wings/flow"
)

var reportsParallelism = flow.Define(func(ctx flow.Context, _ int) (int, error) {
	return ctx.MaxParallelism()
}, flow.WithName("test.reportsParallelism"))

// THE POINT: a run body — and a thread it forks to a worker — reads the
// cluster's parallelism, the fleet ceiling times per-worker concurrency,
// through flow.Context.MaxParallelism. The coordinator stamps it on every job,
// so a fan-out on a worker sizes itself to the whole fleet and not to the one
// machine it happens to run on.
func TestMaxParallelismIsTheFleetsCapacity(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 3, Concurrency: 2})

	var here, onWorker int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		n, err := ctx.MaxParallelism()
		if err != nil {
			return err
		}
		here = n
		w, err := ctx.Go(reportsParallelism, 0).Await(ctx)
		if err != nil {
			return err
		}
		onWorker = w
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if here != 6 {
		t.Errorf("the body read MaxParallelism = %d, want 3 workers × 2 = 6", here)
	}
	if onWorker != 6 {
		t.Errorf("a forked thread read MaxParallelism = %d, want the fleet's 6, not the worker's own", onWorker)
	}
}

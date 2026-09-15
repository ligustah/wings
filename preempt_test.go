package wings

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

var preemptCounter struct {
	starts atomic.Int32
	steps  atomic.Int32
}

// preemptWork counts n steps, checkpointing after each so a reload resumes rather
// than restarts. It records how many times it was started, to show preemption
// reloaded it, and sleeps per step so a preempt lands mid-run.
var preemptWork = flow.Define(func(ctx flow.Context, n int) (int, error) {
	preemptCounter.starts.Add(1)
	start, _, err := ctx.Checkpoint[int]()
	if err != nil {
		return 0, err
	}
	for i := start; i < n; i++ {
		time.Sleep(15 * time.Millisecond)
		preemptCounter.steps.Add(1)
		if err := ctx.Heartbeat(i + 1); err != nil {
			return 0, err
		}
	}
	return n, nil
}, flow.WithName("test.preemptWork"))

// THE POINT: a running thread is preempted at its next checkpoint, gives up its
// slot, and is reloaded in place from that checkpoint — making progress across
// several preemptions and finishing with the right answer, without the
// coordinator redispatching it.
func TestARunningThreadIsPreemptedAndReloaded(t *testing.T) {
	if testing.Short() {
		t.Skip("preempt/reload timing")
	}
	old := preemptAfter
	preemptAfter = 30 * time.Millisecond
	t.Cleanup(func() { preemptAfter = old })
	preemptCounter.starts.Store(0)
	preemptCounter.steps.Store(0)

	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 1})

	got, err := preemptWork(c.Bind(t.Context()), 10)
	if err != nil {
		t.Fatalf("preemptWork: %v", err)
	}
	if got != 10 {
		t.Fatalf("got %d, want 10", got)
	}
	if n := preemptCounter.starts.Load(); n < 2 {
		t.Fatalf("preemptWork started %d times; want at least 2 — it must have been preempted and reloaded", n)
	}
}

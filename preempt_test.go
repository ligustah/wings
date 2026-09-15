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

// THE POINT: two threads oversubscribe one slot; each is preempted at a
// checkpoint to give the other a turn, reloaded in place from that checkpoint,
// and both finish with the right answer — time-slicing, not running one to
// completion before the other starts. Preemption never reaches the coordinator.
func TestOversubscribedThreadsArePreemptedAndTimeSlice(t *testing.T) {
	if testing.Short() {
		t.Skip("preempt/reload timing")
	}
	preemptCounter.starts.Store(0)
	preemptCounter.steps.Store(0)

	// One slot, two jobs: the second waits, so the first is preempted to share it.
	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 1, PreemptAfter: 30 * time.Millisecond})

	var sum int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		a := ctx.Go(preemptWork, 6)
		b := ctx.Go(preemptWork, 6)
		ra, err := a.Await(ctx)
		if err != nil {
			return err
		}
		rb, err := b.Await(ctx)
		if err != nil {
			return err
		}
		sum = ra + rb
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sum != 12 {
		t.Fatalf("sum %d, want 12", sum)
	}
	// Two jobs sharing one slot: with preemption both are reloaded, so the work
	// function starts more than twice.
	if n := preemptCounter.starts.Load(); n <= 2 {
		t.Fatalf("preemptWork started %d times; want > 2 — the two must have preempted each other", n)
	}
	// Worker-owned: preemption never reached the coordinator.
	entries := awaitJournal(t, c, func(es []journalEntry) bool {
		n := 0
		for _, e := range es {
			if e.Func == "test.preemptWork" && e.Kind == journalCompleted {
				n++
			}
		}
		return n >= 2
	})
	for _, e := range entries {
		if e.Func == "test.preemptWork" && (e.Kind == journalYielded || e.Kind == journalRedispatch) {
			t.Fatalf("preemptWork's %s reached the coordinator; preemption must be worker-owned", e.Kind)
		}
	}
}

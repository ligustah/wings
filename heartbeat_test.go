package wings

import (
	"context"
	"errors"

	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// resumable counts how much work each attempt actually did, which is the only
// way to check that a retry resumed rather than started over.
var resumable struct {
	mu   sync.Mutex
	done []int // items processed, per attempt
}

// wedge lets a test hold the first attempt of a job still while the second one
// runs.
var wedge struct {
	attempts atomic.Int64
	release  chan struct{}
	once     sync.Once
}

// chunks processes items one at a time, heartbeating its position, and can be
// told to go quiet part-way through. That combination is the whole feature: a
// job that stops reporting is moved, and the move is cheap because it resumes.
var chunks = Define("test.chunks", func(ctx context.Context, total int) (int, error) {
	from, _, err := Checkpoint[int](ctx)
	if err != nil {
		return 0, err
	}

	attempt := int(wedge.attempts.Add(1))
	did := 0
	for i := from; i < total; i++ {
		if attempt == 1 && i == from+2 {
			// Go quiet: still running, still holding the job, reporting
			// nothing. This is a wedged worker as the coordinator sees one.
			select {
			case <-wedge.release:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			return 0, errors.New("test.chunks: first attempt was abandoned")
		}
		did++
		if err := Heartbeat(ctx, i+1); err != nil {
			return 0, err
		}
		time.Sleep(10 * time.Millisecond)
	}

	resumable.mu.Lock()
	resumable.done = append(resumable.done, did)
	resumable.mu.Unlock()
	return total, nil
}, WithHeartbeatTimeout(300*time.Millisecond))

var forever = Define("test.forever", func(ctx context.Context, _ int) (int, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}, WithTimeout(200*time.Millisecond))

// THE POINT: a job that goes quiet is presumed stuck on its machine rather than
// slow, and is moved — but moving it must not throw away what it had already
// done, or a long job on a flaky fleet never finishes at all.
func TestAStuckJobMovesAndResumesFromItsCheckpoint(t *testing.T) {
	wedge.release = make(chan struct{})
	wedge.attempts.Store(0)
	resumable.mu.Lock()
	resumable.done = nil
	resumable.mu.Unlock()
	t.Cleanup(func() { wedge.once.Do(func() { close(wedge.release) }) })

	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 2})

	got, err := chunks(c.Bind(t.Context()), 8)
	if err != nil {
		t.Fatalf("chunks: %v", err)
	}
	if got != 8 {
		t.Fatalf("got %d, want 8", got)
	}

	resumable.mu.Lock()
	done := append([]int(nil), resumable.done...)
	resumable.mu.Unlock()

	if len(done) != 1 {
		t.Fatalf("%d attempts finished, want 1", len(done))
	}
	// The first attempt heartbeated twice before going quiet, so the second
	// starts at 2 and has six left. Doing all eight would mean the checkpoint
	// was ignored.
	if done[0] != 6 {
		t.Errorf("the retry processed %d items, want 6 — it must resume from the checkpoint, not start over", done[0])
	}

	entries := awaitJournal(t, c, func(es []journalEntry) bool {
		return countKind(es, journalRedispatch) >= 1
	})
	var why string
	for _, e := range entries {
		if e.Kind == journalRedispatch {
			why = e.Err
		}
	}
	if !strings.Contains(why, "heartbeat") {
		t.Errorf("the redispatch was recorded as %q; the record must say why a job was moved", why)
	}
}

// A job that blows its total bound is over. Moving it would spend the same time
// again to reach the same answer, so it fails instead.
func TestAJobThatRunsTooLongFailsRatherThanMoving(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2})

	start := time.Now()
	_, err := forever(c.Bind(t.Context()), 0)
	if err == nil {
		t.Fatal("want an error from a job that exceeded its timeout")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("got %v, want a timeout", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the job took %s to give up; a total bound must not be retried", took)
	}

	entries := awaitJournal(t, c, func(es []journalEntry) bool {
		return countKind(es, journalFailed) >= 1 || countKind(es, journalCompleted) >= 1
	})
	if n := countKind(entries, journalRedispatch); n != 0 {
		t.Errorf("the job was moved %d times; exceeding a total bound is not a reason to retry", n)
	}
}

// A per-function bound must beat the cluster-wide default, or the number
// declared beside the code means nothing.
func TestAFunctionsOwnTimeoutWinsOverTheClusterDefault(t *testing.T) {
	c := start(t, Config{Target: InProcess(), JobTimeout: time.Hour})

	start := time.Now()
	if _, err := forever(c.Bind(t.Context()), 0); err == nil {
		t.Fatal("want an error")
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("the job ran for %s; the function declared 200ms", took)
	}
}

// Heartbeat outside a work function has no job to report on, and says so rather
// than quietly doing nothing.
func TestHeartbeatOutsideAJobIsAnError(t *testing.T) {
	if err := Heartbeat(context.Background(), 1); err == nil {
		t.Fatal("want an error from a heartbeat with no job")
	}
	v, ok, err := Checkpoint[int](context.Background())
	if err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if ok || v != 0 {
		t.Fatalf("got (%v, %v), want the zero value and false", v, ok)
	}
}

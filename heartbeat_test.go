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

// quick is a short job with short bounds. It runs in a fraction of either, so
// the only way it can fail is by being charged for time it spent waiting.
var quick = Define("test.quick", func(ctx context.Context, _ int) (string, error) {
	select {
	case <-time.After(20 * time.Millisecond):
		return "finished", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}, WithTimeout(300*time.Millisecond), WithHeartbeatTimeout(300*time.Millisecond))

// THE POINT: a bound is on the work, not on the queue in front of it. One worker
// with one slot is given a slow job, and then a quick one that waits behind it
// for longer than its own bounds before it so much as starts. The coordinator
// used to run both clocks from dispatch, so the quick job was moved as "stuck" —
// to the back of the same queue, there being nowhere else — until it ran out of
// attempts without ever having run.
func TestAQueuedJobIsNotChargedForItsWait(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 1})
	ctx := c.Bind(t.Context())

	blocker := make(chan error, 1)
	go func() {
		_, err := slow(ctx, 900*time.Millisecond)
		blocker <- err
	}()
	// Let it take the only slot before the quick one is sent.
	time.Sleep(100 * time.Millisecond)

	got, err := quick(ctx, 0)
	if err != nil {
		t.Fatalf("a job that waited in a queue was charged for the wait: %v", err)
	}
	if got != "finished" {
		t.Fatalf("got %q", got)
	}
	if err := <-blocker; err != nil {
		t.Fatalf("slow: %v", err)
	}

	entries := awaitJournal(t, c, func(es []journalEntry) bool {
		return countKind(es, journalCompleted) >= 2
	})
	if n := countKind(entries, journalRedispatch); n != 0 {
		t.Errorf("a queued job was moved %d times; waiting is not being stuck", n)
	}
}

// The clocks, in isolation: what each bound measures from, and what a job that
// has not started is measured against at all.
func TestBoundsRunFromWhenTheJobStarted(t *testing.T) {
	t0 := time.Now()
	p := &pendingJob{
		opts:  defOptions{timeout: time.Minute, beat: 10 * time.Second},
		since: t0,
	}

	// Queued for an hour: neither bound has begun.
	if stuck, slow := p.overdue(t0.Add(time.Hour)); stuck || slow {
		t.Fatalf("a job that has not started is overdue (stuck=%v slow=%v); nothing has been measured yet", stuck, slow)
	}

	started := t0.Add(time.Hour)
	p.started = started

	// The total bound counts from the start, not from dispatch.
	if _, slow := p.overdue(started.Add(30 * time.Second)); slow {
		t.Fatal("a job thirty seconds into a one-minute bound is too slow; the hour it queued was counted")
	}
	if _, slow := p.overdue(started.Add(2 * time.Minute)); !slow {
		t.Fatal("a job two minutes into a one-minute bound is not too slow")
	}

	// So does the heartbeat bound, with the start standing in for a beat until
	// there is one.
	if stuck, _ := p.overdue(started.Add(5 * time.Second)); stuck {
		t.Fatal("a job five seconds into a ten-second heartbeat bound is stuck")
	}
	if stuck, _ := p.overdue(started.Add(11 * time.Second)); !stuck {
		t.Fatal("a job that has been silent for eleven seconds of a ten-second bound is not stuck")
	}
	p.beat = started.Add(10 * time.Second)
	if stuck, _ := p.overdue(started.Add(15 * time.Second)); stuck {
		t.Fatal("a job that beat five seconds ago is stuck")
	}

	// The start bound is the one thing that applies before the job runs.
	q := &pendingJob{opts: defOptions{start: time.Minute}, since: t0}
	if stuck, _ := q.overdue(t0.Add(30 * time.Second)); stuck {
		t.Fatal("a job queued for thirty seconds of a one-minute start bound is overdue")
	}
	if stuck, _ := q.overdue(t0.Add(2 * time.Minute)); !stuck {
		t.Fatal("a job queued for two minutes of a one-minute start bound is not overdue")
	}
	q.started = t0.Add(2 * time.Minute)
	if stuck, _ := q.overdue(t0.Add(time.Hour)); stuck {
		t.Fatal("the start bound still applies to a job that has started")
	}
}

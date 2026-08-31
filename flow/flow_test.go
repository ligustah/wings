package flow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings"
	"github.com/ligustah/wings/flow"
)

// calls counts how many times each work function actually ran. Replay is the
// claim that a retry does NOT run what already succeeded, so counting is the
// only way to check it.
var calls struct {
	double atomic.Int64
	slow   atomic.Int64
}

var double = wings.Define("flow.double", func(ctx context.Context, in int) (int, error) {
	calls.double.Add(1)
	return in * 2, nil
})

var boom = wings.Define("flow.boom", func(ctx context.Context, in string) (string, error) {
	return "", errors.New("deliberate failure: " + in)
})

var slow = wings.Define("flow.slow", func(ctx context.Context, d time.Duration) (string, error) {
	calls.slow.Add(1)
	select {
	case <-time.After(d):
		return "finished", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
})

func cluster(t *testing.T) context.Context {
	t.Helper()

	c, err := wings.Start(t.Context(), wings.Config{
		Target: wings.InProcess(),
		Dir:    t.TempDir(),
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return c.Bind(t.Context())
}

func TestWorkflowRunsAndReturnsItsResult(t *testing.T) {
	ctx := cluster(t)

	wf := flow.Define("t.simple", func(ctx context.Context, in int) (int, error) {
		a, err := double(ctx, in)
		if err != nil {
			return 0, err
		}
		return double(ctx, a)
	})

	got, err := flow.Run(ctx, wf, flow.NewInstance(), 5, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 20 {
		t.Fatalf("got %d, want 20", got)
	}
}

// THE POINT OF THE WHOLE PACKAGE: a workflow that fails after doing real work
// must not do that work a second time.
func TestRetryReplaysCompletedWorkInsteadOfRepeatingIt(t *testing.T) {
	ctx := cluster(t)

	before := calls.double.Load()
	var attempts atomic.Int64

	wf := flow.Define("t.replay", func(ctx context.Context, in int) (int, error) {
		a, err := double(ctx, in)
		if err != nil {
			return 0, err
		}
		b, err := double(ctx, a)
		if err != nil {
			return 0, err
		}
		// Fail the first two attempts AFTER the calls above have succeeded.
		if attempts.Add(1) <= 2 {
			return 0, errors.New("not yet")
		}
		return b, nil
	}, flow.Backoff(time.Millisecond, time.Millisecond))

	got, err := flow.Run(ctx, wf, flow.NewInstance(), 3, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 12 {
		t.Fatalf("got %d, want 12", got)
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("the workflow body ran %d times, want 3", n)
	}
	// Three attempts, two calls each if nothing replayed. Two total is the claim.
	if n := calls.double.Load() - before; n != 2 {
		t.Fatalf("the work function ran %d times across 3 attempts; replay should have held it to 2", n)
	}
}

func TestPermanentErrorIsNotRetried(t *testing.T) {
	ctx := cluster(t)

	var attempts atomic.Int64
	wf := flow.Define("t.permanent", func(ctx context.Context, in int) (int, error) {
		attempts.Add(1)
		return 0, flow.Permanent(errors.New("malformed input"))
	}, flow.Backoff(time.Millisecond, time.Millisecond))

	_, err := flow.Run(ctx, wf, flow.NewInstance(), 1, flow.WithStore(flow.NewMemStore()))
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "malformed input") {
		t.Errorf("the cause should survive: %v", err)
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("ran %d times; a permanent error must not be retried", n)
	}
}

func TestDeclaredPermanentErrorIsNotRetried(t *testing.T) {
	ctx := cluster(t)

	sentinel := errors.New("no such account")
	var attempts atomic.Int64

	wf := flow.Define("t.declared", func(ctx context.Context, in int) (int, error) {
		attempts.Add(1)
		return 0, fmt.Errorf("looking up %d: %w", in, sentinel)
	}, flow.PermanentErrors(sentinel), flow.Backoff(time.Millisecond, time.Millisecond))

	if _, err := flow.Run(ctx, wf, flow.NewInstance(), 1, flow.WithStore(flow.NewMemStore())); err == nil {
		t.Fatal("want an error")
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("ran %d times; a declared permanent error must not be retried", n)
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	ctx := cluster(t)

	var attempts atomic.Int64
	wf := flow.Define("t.giveup", func(ctx context.Context, in int) (int, error) {
		attempts.Add(1)
		return 0, errors.New("always fails")
	}, flow.MaxAttempts(3), flow.Backoff(time.Millisecond, time.Millisecond))

	_, err := flow.Run(ctx, wf, flow.NewInstance(), 1, flow.WithStore(flow.NewMemStore()))
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "after 3 attempts") {
		t.Errorf("the error should say how many attempts it made: %v", err)
	}
	if n := attempts.Load(); n != 3 {
		t.Errorf("ran %d times, want 3", n)
	}
}

// A failing work function is a normal outcome that the workflow can handle,
// not something that kills the run.
func TestWorkFunctionErrorReachesTheWorkflow(t *testing.T) {
	ctx := cluster(t)

	wf := flow.Define("t.handled", func(ctx context.Context, in string) (string, error) {
		_, err := boom(ctx, in)
		if err == nil {
			return "", errors.New("expected the work function to fail")
		}
		return "handled: " + err.Error(), nil
	})

	got, err := flow.Run(ctx, wf, flow.NewInstance(), "x", flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(got, "deliberate failure: x") {
		t.Fatalf("got %q; the work function's message should have reached the workflow", got)
	}
}

// Map must work unchanged inside a workflow — that is the whole "same code"
// claim — and it must survive a replay, which a bare goroutine fan-out would
// not.
func TestMapWorksInsideAWorkflowAndReplays(t *testing.T) {
	ctx := cluster(t)

	before := calls.double.Load()
	var attempts atomic.Int64

	wf := flow.Define("t.map", func(ctx context.Context, in []int) ([]int, error) {
		outs, err := wings.Map(ctx, double, in)
		if err != nil {
			return nil, err
		}
		if attempts.Add(1) == 1 {
			return nil, errors.New("fail once, after the fan-out")
		}
		return outs, nil
	}, flow.Backoff(time.Millisecond, time.Millisecond))

	got, err := flow.Run(ctx, wf, flow.NewInstance(), []int{1, 2, 3, 4}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := []int{2, 4, 6, 8}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if n := calls.double.Load() - before; n != 4 {
		t.Fatalf("the work function ran %d times across 2 attempts; replay should have held it to 4", n)
	}
}

func TestFuturesRunConcurrentlyAndReplay(t *testing.T) {
	ctx := cluster(t)

	before := calls.slow.Load()
	var attempts atomic.Int64

	wf := flow.Define("t.futures", func(ctx context.Context, d time.Duration) (string, error) {
		a := flow.Go(ctx, slow, d)
		b := flow.Go(ctx, slow, d)

		x, err := a.Await(ctx)
		if err != nil {
			return "", err
		}
		y, err := b.Await(ctx)
		if err != nil {
			return "", err
		}
		if attempts.Add(1) == 1 {
			return "", errors.New("fail once, after both futures")
		}
		return x + "+" + y, nil
	}, flow.Backoff(time.Millisecond, time.Millisecond))

	start := time.Now()
	got, err := flow.Run(ctx, wf, flow.NewInstance(), 300*time.Millisecond, flow.WithStore(flow.NewMemStore()))
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != "finished+finished" {
		t.Fatalf("got %q", got)
	}
	if n := calls.slow.Load() - before; n != 2 {
		t.Fatalf("the work function ran %d times across 2 attempts; replay should have held it to 2", n)
	}
	// Two 300ms calls concurrently, then a replayed attempt that does neither.
	if elapsed > 900*time.Millisecond {
		t.Errorf("took %s; the two futures should have overlapped", elapsed)
	}
}

// Awaiting twice would record a second join that the next attempt never
// produces, so it is refused rather than quietly served from the cache.
func TestAwaitingAFutureTwiceIsRefused(t *testing.T) {
	ctx := cluster(t)

	wf := flow.Define("t.double-await", func(ctx context.Context, in int) (int, error) {
		f := flow.Go(ctx, double, in)
		if _, err := f.Await(ctx); err != nil {
			return 0, flow.Permanent(err)
		}
		_, err := f.Await(ctx)
		if err == nil {
			return 0, flow.Permanent(errors.New("second Await should have failed"))
		}
		return 1, nil
	})

	if _, err := flow.Run(ctx, wf, flow.NewInstance(), 3, flow.WithStore(flow.NewMemStore())); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// Now must be stable across attempts, or every retry decides something
// different from the run it is supposed to be continuing.
func TestNowIsRecordedAndReplayed(t *testing.T) {
	ctx := cluster(t)

	var attempts atomic.Int64
	var seen []time.Time

	wf := flow.Define("t.now", func(ctx context.Context, in int) (int, error) {
		now, err := flow.Now(ctx)
		if err != nil {
			return 0, err
		}
		seen = append(seen, now)
		if attempts.Add(1) == 1 {
			return 0, errors.New("fail once")
		}
		return 1, nil
	}, flow.Backoff(20*time.Millisecond, 20*time.Millisecond))

	if _, err := flow.Run(ctx, wf, flow.NewInstance(), 0, flow.WithStore(flow.NewMemStore())); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("saw %d times, want 2", len(seen))
	}
	if !seen[0].Equal(seen[1]) {
		t.Fatalf("Now returned %s then %s; it must replay the recorded instant", seen[0], seen[1])
	}
}

func TestSleepIsNotServedTwice(t *testing.T) {
	ctx := cluster(t)

	var attempts atomic.Int64
	wf := flow.Define("t.sleep", func(ctx context.Context, d time.Duration) (int, error) {
		if err := flow.Sleep(ctx, d); err != nil {
			return 0, err
		}
		if attempts.Add(1) == 1 {
			return 0, errors.New("fail once, after the sleep")
		}
		return 1, nil
	}, flow.Backoff(time.Millisecond, time.Millisecond))

	start := time.Now()
	if _, err := flow.Run(ctx, wf, flow.NewInstance(), 400*time.Millisecond, flow.WithStore(flow.NewMemStore())); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// One 400ms sleep, not two: the second attempt replays a sleep that is
	// already over.
	if elapsed := time.Since(start); elapsed > 750*time.Millisecond {
		t.Errorf("took %s; the sleep was served twice", elapsed)
	}
}

// A completed instance must not run again, whatever the caller does — its work
// functions already had their effects.
func TestACompletedInstanceReturnsItsStoredResult(t *testing.T) {
	ctx := cluster(t)

	store := flow.NewMemStore()
	instance := flow.NewInstance()
	var attempts atomic.Int64

	wf := flow.Define("t.once", func(ctx context.Context, in int) (int, error) {
		attempts.Add(1)
		return double(ctx, in)
	})

	first, err := flow.Run(ctx, wf, instance, 21, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	second, err := flow.Run(ctx, wf, instance, 21, flow.WithStore(store))
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}

	if first != 42 || second != 42 {
		t.Fatalf("got %d then %d, want 42 both times", first, second)
	}
	if n := attempts.Load(); n != 1 {
		t.Fatalf("the workflow body ran %d times; a completed instance must not run again", n)
	}
}

// Editing a workflow while a run of it is in flight is the failure this
// machinery is most likely to meet in practice, so it must be named clearly
// rather than producing a wrong answer.
func TestChangedWorkflowIsReportedAsAContinuityError(t *testing.T) {
	ctx := cluster(t)

	store := flow.NewMemStore()
	instance := flow.NewInstance()

	// First shape: one call, then a RETRYABLE failure — so the run gives up
	// without reaching a terminal state and its history is left mid-flight,
	// which is exactly the situation a redeploy creates.
	v1 := flow.Define("t.changed", func(ctx context.Context, in int) (int, error) {
		if _, err := double(ctx, in); err != nil {
			return 0, err
		}
		return 0, errors.New("stop here")
	}, flow.MaxAttempts(1), flow.Backoff(time.Millisecond, time.Millisecond))

	if _, err := flow.Run(ctx, v1, instance, 2, flow.WithStore(store)); err == nil {
		t.Fatal("want the first run to fail")
	}

	// Second shape: sleeps where the first called. Same instance, so it replays
	// into the history the first one left.
	v2 := flow.Define("t.changed", func(ctx context.Context, in int) (int, error) {
		if err := flow.Sleep(ctx, time.Millisecond); err != nil {
			return 0, err
		}
		return 1, nil
	}, flow.MaxAttempts(2), flow.Backoff(time.Millisecond, time.Millisecond))

	_, err := flow.Run(ctx, v2, instance, 2, flow.WithStore(store))
	if err == nil {
		t.Fatal("want a continuity error")
	}
	if !flow.IsContinuity(err) {
		t.Fatalf("want a continuity error, got %v", err)
	}
	// The message has to say what changed, or it is useless at 3am.
	for _, want := range []string{"CallEvent", "SleepEvent"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error should name the mismatched event types; %q missing from: %v", want, err)
		}
	}
}

func TestRunRefusesAnUnboundContext(t *testing.T) {
	wf := flow.Define("t.unbound", func(ctx context.Context, in int) (int, error) { return in, nil })

	_, err := flow.Run(t.Context(), wf, flow.NewInstance(), 1, flow.WithStore(flow.NewMemStore()))
	if err == nil {
		t.Fatal("want an error for a context with no cluster")
	}
	if !strings.Contains(err.Error(), "not bound to a cluster") {
		t.Errorf("the error should say what is missing: %v", err)
	}
}

func TestRunRequiresAStore(t *testing.T) {
	ctx := cluster(t)
	wf := flow.Define("t.nostore", func(ctx context.Context, in int) (int, error) { return in, nil })

	if _, err := flow.Run(ctx, wf, flow.NewInstance(), 1); err == nil {
		t.Fatal("want an error when no Store is given")
	}
}

// A replayed fork must be RECOGNISED, not recorded again. Nothing about the
// result goes wrong if it is — the cursor still advances one per operation
// either way — but the history grows by a fork and a join per parallel call per
// attempt, and it then claims the workflow forked more threads than it did.
// A long-lived workflow that retries is exactly where that compounds.
func TestReplayDoesNotDuplicateForkAndJoinEvents(t *testing.T) {
	ctx := cluster(t)
	store := flow.NewMemStore()
	instance := flow.NewInstance()

	var attempts atomic.Int64
	wf := flow.Define("t.forkdup", func(ctx context.Context, in []int) ([]int, error) {
		outs, err := wings.Map(ctx, double, in)
		if err != nil {
			return nil, err
		}
		if attempts.Add(1) < 3 {
			return nil, errors.New("fail twice, after the fan-out")
		}
		return outs, nil
	}, flow.Backoff(time.Millisecond, time.Millisecond))

	if _, err := flow.Run(ctx, wf, instance, []int{1, 2, 3}, flow.WithStore(store)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	events, err := store.Events(context.Background(), "t.forkdup", instance)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	var forks, joins int
	for _, ev := range events {
		if ev.GetFork() != nil {
			forks++
		}
		if ev.GetJoin() != nil {
			joins++
		}
	}
	if forks != 3 || joins != 3 {
		t.Fatalf("history records %d forks and %d joins after 3 attempts over 3 inputs; "+
			"want 3 of each — a replayed fork must be recognised, not appended again", forks, joins)
	}
}

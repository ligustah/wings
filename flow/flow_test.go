package flow_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ligustah/wings/flow"
)

// calls counts how many times each function actually ran. Replay is the claim
// that a retry does NOT run what already succeeded, so counting is the only
// way to check it.
var calls struct {
	double atomic.Int64
	slow   atomic.Int64
}

var double = flow.Define(func(ctx flow.Context, in int) (int, error) {
	calls.double.Add(1)
	return in * 2, nil
}, flow.WithName("flow.double"))

var boom = flow.Define(func(ctx flow.Context, in string) (string, error) {
	return "", errors.New("deliberate failure: " + in)
}, flow.WithName("flow.boom"))

var slow = flow.Define(func(ctx flow.Context, d time.Duration) (string, error) {
	calls.slow.Add(1)
	select {
	case <-time.After(d):
		return "finished", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}, flow.WithName("flow.slow"))

// gated finishes when the test lets it, and counts how often it was started.
var gate struct {
	starts atomic.Int64
	open   chan struct{}
}

var gated = flow.Define(func(ctx flow.Context, in int) (int, error) {
	gate.starts.Add(1)
	select {
	case <-gate.open:
		return in * 2, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}, flow.WithName("flow.gated"))

// quick is a retry policy that does not make a test wait.
var quick = flow.Backoff(time.Millisecond, time.Millisecond)

func TestARunExecutesItsBodyOnce(t *testing.T) {
	var got int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		a, err := double(ctx, 5)
		if err != nil {
			return err
		}
		got, err = double(ctx, a)
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 20 {
		t.Fatalf("got %d, want 20", got)
	}
}

// THE POINT OF THE WHOLE PACKAGE: a run that fails after doing real work must
// not do that work a second time.
func TestRetryReplaysCompletedWorkInsteadOfRepeatingIt(t *testing.T) {
	before := calls.double.Load()
	var attempts atomic.Int64
	var got int

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		a, err := double(ctx, 3)
		if err != nil {
			return err
		}
		b, err := double(ctx, a)
		if err != nil {
			return err
		}
		// Fail the first two attempts AFTER the calls above have succeeded.
		if attempts.Add(1) <= 2 {
			return errors.New("not yet")
		}
		got = b
		return nil
	}, flow.WithStore(flow.NewMemStore()), quick)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 12 {
		t.Fatalf("got %d, want 12", got)
	}
	if n := attempts.Load(); n != 3 {
		t.Fatalf("the body ran %d times, want 3", n)
	}
	// Three attempts, two calls each if nothing replayed. Two total is the claim.
	if n := calls.double.Load() - before; n != 2 {
		t.Fatalf("the function ran %d times across 3 attempts; replay should have held it to 2", n)
	}
}

func TestPermanentErrorIsNotRetried(t *testing.T) {
	var attempts atomic.Int64
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		attempts.Add(1)
		return flow.Permanent(errors.New("malformed input"))
	}, flow.WithStore(flow.NewMemStore()), quick)
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
	sentinel := errors.New("no such account")
	var attempts atomic.Int64

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		attempts.Add(1)
		return fmt.Errorf("looking up %d: %w", 1, sentinel)
	}, flow.WithStore(flow.NewMemStore()), flow.PermanentErrors(sentinel), quick)
	if err == nil {
		t.Fatal("want an error")
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("ran %d times; a declared permanent error must not be retried", n)
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	var attempts atomic.Int64
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		attempts.Add(1)
		return errors.New("always fails")
	}, flow.WithStore(flow.NewMemStore()), flow.MaxAttempts(3), quick)
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

// A failing function is a normal outcome that the body can handle, not
// something that kills the run.
func TestACallsFailureReachesTheBody(t *testing.T) {
	var got string
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		_, err := boom(ctx, "x")
		if err == nil {
			return errors.New("expected the function to fail")
		}
		if !flow.IsCallFailure(err) {
			return fmt.Errorf("the body should be able to tell a call's failure apart: %v", err)
		}
		got = "handled: " + err.Error()
		return nil
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.Contains(got, "deliberate failure: x") {
		t.Fatalf("got %q; the function's message should have reached the body", got)
	}
}

// THE POINT: a call's failure is recorded, so a retry of the run replays it
// and the body fails the same way. Retrying that used to be ten attempts and
// minutes of backoff to reach the answer the first attempt had. A body that
// lets a call's failure through is done.
func TestARunThatFailsBecauseACallFailedIsNotRetried(t *testing.T) {
	var attempts atomic.Int64
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		attempts.Add(1)
		_, err := boom(ctx, "y")
		return err
	}, flow.WithStore(flow.NewMemStore()), quick)
	if err == nil {
		t.Fatal("want an error")
	}
	if !strings.Contains(err.Error(), "deliberate failure: y") {
		t.Errorf("the call's own message should be the run's: %v", err)
	}
	if n := attempts.Load(); n != 1 {
		t.Errorf("the body ran %d times; a call's recorded failure must not be retried", n)
	}
}

// A call the caller interrupted has no answer yet, and must not be given one:
// the interruption is nearly always a shutdown, and the restart that follows
// would otherwise replay a failure that never happened.
func TestAnInterruptedCallIsNotRecordedAsFailed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := flow.NewMemStore()
		name := flow.NewName()
		gate.open = make(chan struct{})
		gate.starts.Store(0)

		var got int
		body := func(ctx flow.Context) error {
			var err error
			got, err = gated(ctx, 21)
			return err
		}

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- flow.Run(ctx, name, body, flow.WithStore(store)) }()
		// Everything in the bubble is blocked: the call is waiting on the gate.
		synctest.Wait()
		if n := gate.starts.Load(); n != 1 {
			t.Fatalf("the call was started %d times before the interruption, want 1", n)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want the cancellation", err)
		}

		// Resumed: the call is made again, and this time it is let through.
		close(gate.open)
		if err := flow.Run(t.Context(), name, body, flow.WithStore(store)); err != nil {
			t.Fatalf("resumed Run: %v", err)
		}
		if got != 42 {
			t.Fatalf("got %d, want 42", got)
		}
		if n := gate.starts.Load(); n != 2 {
			t.Fatalf("the call was started %d times, want 2: once interrupted, once through", n)
		}
	})
}

// Map must work inside a run — it is the fan-out — and it must survive a
// replay, which a bare goroutine fan-out would not.
func TestMapReplays(t *testing.T) {
	before := calls.double.Load()
	var attempts atomic.Int64
	var got []int

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		outs, err := ctx.Map(double, []int{1, 2, 3, 4})
		if err != nil {
			return err
		}
		if attempts.Add(1) == 1 {
			return errors.New("fail once, after the fan-out")
		}
		got = outs
		return nil
	}, flow.WithStore(flow.NewMemStore()), quick)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := []int{2, 4, 6, 8}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if n := calls.double.Load() - before; n != 4 {
		t.Fatalf("the function ran %d times across 2 attempts; replay should have held it to 4", n)
	}
}

// Map keeps every successful output and names each failure by its index.
func TestMapReportsEachFailureInItsPlace(t *testing.T) {
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		outs, err := ctx.Map(boom, []string{"a", "b"})
		if err == nil {
			return errors.New("want an error")
		}
		if len(outs) != 2 {
			return fmt.Errorf("got %d outputs, want a place for each input", len(outs))
		}
		for _, want := range []string{"input 0", "input 1", "deliberate failure: a", "deliberate failure: b"} {
			if !strings.Contains(err.Error(), want) {
				return fmt.Errorf("the error should mention %q: %v", want, err)
			}
		}
		return nil
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatal(err)
	}
}

// Under synctest's clock the timing is exact rather than bounded: two 300ms
// calls that overlap take 300ms, the retry's backoff is the 1ms it was told,
// and the replayed attempt runs neither call.
func TestFuturesRunConcurrentlyAndReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		before := calls.slow.Load()
		var attempts atomic.Int64
		var got string

		start := time.Now()
		err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
			a := ctx.Go(slow, 300*time.Millisecond)
			b := ctx.Go(slow, 300*time.Millisecond)

			x, err := a.Await(ctx)
			if err != nil {
				return err
			}
			y, err := b.Await(ctx)
			if err != nil {
				return err
			}
			if attempts.Add(1) == 1 {
				return errors.New("fail once, after both futures")
			}
			got = x + "+" + y
			return nil
		}, flow.WithStore(flow.NewMemStore()), quick)
		elapsed := time.Since(start)

		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if got != "finished+finished" {
			t.Fatalf("got %q", got)
		}
		if n := calls.slow.Load() - before; n != 2 {
			t.Fatalf("the function ran %d times across 2 attempts; replay should have held it to 2", n)
		}
		if want := 300*time.Millisecond + time.Millisecond; elapsed != want {
			t.Errorf("took %s, want %s: the two calls overlapping, one backoff, and a replay that runs nothing", elapsed, want)
		}
	})
}

// Awaiting twice would record a second join that the next attempt never
// produces, so it is refused rather than quietly served from the cache.
func TestAwaitingAFutureTwiceIsRefused(t *testing.T) {
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		f := ctx.Go(double, 3)
		if _, err := f.Await(ctx); err != nil {
			return flow.Permanent(err)
		}
		if _, err := f.Await(ctx); err == nil {
			return flow.Permanent(errors.New("second Await should have failed"))
		}
		return nil
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// Now must be stable across attempts, or every retry decides something
// different from the run it is supposed to be continuing.
func TestNowIsRecordedAndReplayed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts atomic.Int64
		var seen []time.Time

		// A backoff long enough that the clock has visibly moved on by the
		// retry, so a Now that re-read it would show.
		err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
			now, err := ctx.Now()
			if err != nil {
				return err
			}
			seen = append(seen, now)
			if attempts.Add(1) == 1 {
				return errors.New("fail once")
			}
			return nil
		}, flow.WithStore(flow.NewMemStore()), flow.Backoff(time.Second, time.Second))
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if len(seen) != 2 {
			t.Fatalf("saw %d times, want 2", len(seen))
		}
		if !seen[0].Equal(seen[1]) {
			t.Fatalf("Now returned %s then %s; it must replay the recorded instant", seen[0], seen[1])
		}
		if time.Since(seen[1]) < time.Second {
			t.Fatalf("the retry ran %s after the recorded instant; the clock should have moved a second",
				time.Since(seen[1]))
		}
	})
}

func TestSleepIsNotServedTwice(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts atomic.Int64

		start := time.Now()
		err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
			if err := ctx.Sleep(400 * time.Millisecond); err != nil {
				return err
			}
			if attempts.Add(1) == 1 {
				return errors.New("fail once, after the sleep")
			}
			return nil
		}, flow.WithStore(flow.NewMemStore()), quick)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		// One 400ms sleep and one 1ms backoff, not two sleeps: the second
		// attempt replays a sleep that is already over.
		if elapsed, want := time.Since(start), 400*time.Millisecond+time.Millisecond; elapsed != want {
			t.Errorf("took %s, want %s", elapsed, want)
		}
	})
}

// A completed run must not run again, whatever the caller does — its calls
// already had their effects.
func TestACompletedRunDoesNotRunAgain(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()
	var attempts atomic.Int64

	body := func(ctx flow.Context) error {
		attempts.Add(1)
		_, err := double(ctx, 21)
		return err
	}
	if err := flow.Run(t.Context(), name, body, flow.WithStore(store)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := flow.Run(t.Context(), name, body, flow.WithStore(store)); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if n := attempts.Load(); n != 1 {
		t.Fatalf("the body ran %d times; a completed run must not run again", n)
	}
}

// Editing a run's body while a run of it is in flight is the failure this
// machinery is most likely to meet in practice, so it must be named clearly
// rather than producing a wrong answer.
func TestAChangedBodyIsReportedAsAContinuityError(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()

	// First shape: one call, then a RETRYABLE failure — so the run gives up
	// without reaching a terminal state and its history is left mid-flight,
	// which is exactly the situation a redeploy creates.
	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		if _, err := double(ctx, 2); err != nil {
			return err
		}
		return errors.New("stop here")
	}, flow.WithStore(store), flow.MaxAttempts(1), quick)
	if err == nil {
		t.Fatal("want the first run to fail")
	}

	// Second shape: sleeps where the first called. Same name, so it replays
	// into the history the first one left.
	err = flow.Run(t.Context(), name, func(ctx flow.Context) error {
		return ctx.Sleep(time.Millisecond)
	}, flow.WithStore(store), flow.MaxAttempts(2), quick)
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

func TestRunRequiresAStore(t *testing.T) {
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error { return nil })
	if err == nil {
		t.Fatal("want an error when no Store is given")
	}
}

// A call needs somewhere to go. Inside a run that is the run's executor;
// outside one it has to be said, and a call with neither is refused rather
// than quietly run in place.
func TestACallOutsideARunNeedsAnExecutor(t *testing.T) {
	if _, err := double(flow.From(context.Background()), 1); err == nil {
		t.Fatal("want an error for a call with nowhere to go")
	} else if !strings.Contains(err.Error(), "not inside a Run") {
		t.Fatalf("the error should say what is missing: %v", err)
	}

	got, err := double(flow.Bind(context.Background(), flow.Local()), 21)
	if err != nil {
		t.Fatalf("a bare call on a bound context: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

// A fan-out forks threads, and a thread belongs to a run; outside one there
// is nothing to fork from.
func TestAFanOutOutsideARunIsRefused(t *testing.T) {
	ctx := flow.Bind(context.Background(), flow.Local())

	if _, err := ctx.Map(double, []int{1}); err == nil {
		t.Fatal("want an error from Map outside a run")
	} else if !strings.Contains(err.Error(), "outside a Run") {
		t.Fatalf("got %v", err)
	}
	if _, err := ctx.Go(double, 1).Await(ctx); err == nil {
		t.Fatal("want an error from Go outside a run")
	}
}

// A replayed fork must be RECOGNISED, not recorded again. Nothing about the
// result goes wrong if it is — the cursor still advances one per operation
// either way — but the history grows by a fork and a join per parallel call per
// attempt, and it then claims the run forked more threads than it did. A
// long-lived run that retries is exactly where that compounds.
func TestReplayDoesNotDuplicateForkAndJoinEvents(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()

	var attempts atomic.Int64
	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		if _, err := ctx.Map(double, []int{1, 2, 3}); err != nil {
			return err
		}
		if attempts.Add(1) < 3 {
			return errors.New("fail twice, after the fan-out")
		}
		return nil
	}, flow.WithStore(store), quick)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	events, err := store.Events(context.Background(), name, "main")
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

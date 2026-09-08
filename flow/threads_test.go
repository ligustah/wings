package flow_test

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: a thread that was joined is over. Its result is in the parent's
// history, and a replay of the parent must not start it again — the thread
// may have run on a machine that no longer has it, and a body of run code
// that ran once has had its effects.
func TestAJoinedThreadIsNotRunAgainOnReplay(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()

	var bodyRuns, childRuns atomic.Int64
	var got int
	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		fut := ctx.Spawn(func(ctx flow.Context) (int, error) {
			childRuns.Add(1)
			return 21, nil
		})
		v, err := fut.Await(ctx)
		if err != nil {
			return err
		}
		if bodyRuns.Add(1) < 3 {
			return errors.New("fail after the join")
		}
		got = v * 2
		return nil
	}, flow.WithStore(store), quick)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	if n := bodyRuns.Load(); n != 3 {
		t.Fatalf("the body ran %d times, want 3", n)
	}
	if n := childRuns.Load(); n != 1 {
		t.Fatalf("the spawned thread ran %d times, want 1: a joined thread's result is in the join", n)
	}

	// And nothing of the child is left: joined, its history is dropped, and
	// only main's remains.
	if threads := store.Threads(name); !slices.Equal(threads, []string{"main"}) {
		t.Fatalf("the run's history holds threads %v, want only main once the child is joined", threads)
	}
}

// THE POINT: RetainHistory keeps a joined thread's history instead of dropping
// it, so a finished run's whole thread tree stays inspectable.
func TestRetainHistoryKeepsJoinedThreads(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()

	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		fut := ctx.Spawn(func(ctx flow.Context) (int, error) { return 21, nil })
		_, err := fut.Await(ctx)
		return err
	}, flow.WithStore(store), flow.RetainHistory())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	threads := store.Threads(name)
	if !slices.Contains(threads, "main") || len(threads) < 2 {
		t.Fatalf("with RetainHistory the joined child should survive; threads = %v", threads)
	}
}

// THE POINT: each thread has a history of its own. A thread in flight when
// the parent fails is started again by the retry and replays what IT did —
// which is what would let it run on another machine.
func TestAThreadStillRunningReplaysItsOwnHistory(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()

	var attempts atomic.Int64
	release := make(chan struct{})
	var got int
	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		fut := ctx.Spawn(func(ctx flow.Context) (int, error) {
			// Recorded on this thread's stream, and replayed from it.
			v, err := double(ctx, 10)
			if err != nil {
				return 0, err
			}
			select {
			case <-release:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			return v + 1, nil
		})
		if attempts.Add(1) == 1 {
			// Walk away while the thread is mid-flight: its call is on record
			// and its result is not.
			return errors.New("abandoned")
		}
		close(release)
		var err error
		got, err = fut.Await(ctx)
		return err
	}, flow.WithStore(store), quick)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 21 {
		t.Fatalf("got %d, want 21", got)
	}
	if n := calls.double.Load(); n < 1 {
		t.Fatalf("double ran %d times", n)
	}
}

var flaky struct{ failures atomic.Int64 }

// THE POINT: the thread is the unit of retry. A thread of run code that
// fails for a transient reason is tried again on its own, from its history,
// and its parent sees only what it finally produced.
func TestAThreadIsRetriedOnItsOwn(t *testing.T) {
	flaky.failures.Store(0)
	var parentRuns atomic.Int64
	var got int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		parentRuns.Add(1)
		fut := ctx.Spawn(func(ctx flow.Context) (int, error) {
			v, err := double(ctx, 4)
			if err != nil {
				return 0, err
			}
			if flaky.failures.Add(1) <= 2 {
				return 0, errors.New("transient")
			}
			return v, nil
		})
		var err error
		got, err = fut.Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()), quick)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 8 {
		t.Fatalf("got %d, want 8", got)
	}
	if n := parentRuns.Load(); n != 1 {
		t.Fatalf("the parent ran %d times, want 1: the thread's retries are its own", n)
	}
	if n := flaky.failures.Load(); n != 3 {
		t.Fatalf("the thread ran %d times, want 3", n)
	}
}

// THE POINT: a thread's failure, once it has exhausted its retries, is on
// record like a call's. The parent is given it and is not retried for it.
func TestAThreadsFinalFailureIsARecordedFailure(t *testing.T) {
	var parentRuns atomic.Int64
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		parentRuns.Add(1)
		_, err := ctx.Spawn(func(ctx flow.Context) (int, error) {
			return 0, errors.New("never")
		}).Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()), flow.MaxAttempts(2), quick)
	if err == nil || !strings.Contains(err.Error(), "never") {
		t.Fatalf("got %v, want the thread's error", err)
	}
	if !flow.IsCallFailure(err) {
		t.Fatalf("the thread's failure is not reported as recorded: %v", err)
	}
	if n := parentRuns.Load(); n != 1 {
		t.Fatalf("the parent ran %d times, want 1: a recorded failure is not retried", n)
	}
}

var resumed struct{ release chan struct{} }

// doubleThenWait calls a function, then waits to be released — or for its
// context to end, which is how its first attempt is interrupted.
var doubleThenWait = flow.Define(func(ctx flow.Context, in int) (int, error) {
	v, err := double(ctx, in)
	if err != nil {
		return 0, err
	}
	select {
	case <-resumed.release:
		return v, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}, flow.WithName("flow.doubleThenWait"))

// THE POINT: a thread handed to another process runs from its own history
// there. RunThread on a store holding the thread's first attempt replays it
// and carries on, the way a worker resumes a thread it was given.
func TestRunThreadResumesAThreadFromItsHistory(t *testing.T) {
	store := flow.NewMemStore()
	before := calls.double.Load()
	resumed.release = make(chan struct{})

	// First attempt: the function's call is recorded, then the thread is
	// interrupted by its context while it waits.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		for calls.double.Load() == before {
			runtime.Gosched()
		}
		cancel()
	}()
	_, err := flow.RunThread(ctx, "r1", "main.0", "flow.doubleThenWait", encodeInt(t, 5),
		flow.WithStore(store), flow.Once())
	if err == nil {
		t.Fatal("want the first attempt to be interrupted")
	}

	// Second: the same thread, resumed. The call is replayed, not made.
	close(resumed.release)
	out, err := flow.RunThread(context.Background(), "r1", "main.0", "flow.doubleThenWait", encodeInt(t, 5),
		flow.WithStore(store), flow.Once())
	if err != nil {
		t.Fatalf("RunThread: %v", err)
	}
	if got := decodeInt(t, out); got != 10 {
		t.Fatalf("got %d, want 10", got)
	}
	if n := calls.double.Load() - before; n != 1 {
		t.Fatalf("double ran %d times across the two attempts, want 1", n)
	}
}

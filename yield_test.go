package wings

import (
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

var napping struct{ attempts atomic.Int32 }

// naps sleeps for longer than a short sleep, as the test has set it, and
// says how many attempts it took.
var naps = flow.Define("test.naps", func(ctx flow.Context, in int) (int, error) {
	napping.attempts.Add(1)
	if err := ctx.Sleep(300 * time.Millisecond); err != nil {
		return 0, err
	}
	return in * 2, nil
})

// THE POINT: a thread that sleeps past the short-sleep threshold does not
// hold a worker for the duration. The worker hands the attempt back with the
// wake-up time, the coordinator keeps the job off every worker until then,
// and dispatches it afresh — to which the thread replays a sleep that is
// over and carries on.
func TestALongSleepIsScheduledByTheCoordinator(t *testing.T) {
	old := flow.ShortSleep
	flow.ShortSleep = 50 * time.Millisecond
	t.Cleanup(func() { flow.ShortSleep = old })
	napping.attempts.Store(0)

	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 1})
	name := "test-nap-" + strconv.FormatUint(runSeq.Add(1), 36)

	var got int
	var other int
	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		sleeper := ctx.Go(naps, 21)
		// The one slot is free while the sleeper is off the worker.
		v, err := ctx.Go(double, 5).Await(ctx)
		if err != nil {
			return err
		}
		other = v
		got, err = sleeper.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 42 || other != 10 {
		t.Fatalf("got %d and %d, want 42 and 10", got, other)
	}
	if n := napping.attempts.Load(); n != 2 {
		t.Fatalf("the sleeper ran %d attempts, want 2: one that yielded, one that woke", n)
	}
	entries := awaitJournal(t, c, func(es []journalEntry) bool {
		return countKind(es, journalYielded) >= 1 && countKind(es, journalRedispatch) >= 1
	})
	if countKind(entries, journalYielded) != 1 {
		t.Errorf("journal: %d yields, want 1", countKind(entries, journalYielded))
	}
	if countKind(entries, journalRedispatch) != 1 {
		t.Errorf("journal: %d redispatches, want 1 — the wake-up", countKind(entries, journalRedispatch))
	}
}

var receiving struct{ attempts atomic.Int32 }

// receivesOnce takes one value off the channel it was handed and reports
// how many attempts it took to get it.
var receivesOnce = flow.Define("test.receivesOnce", func(ctx flow.Context, in feed) (int, error) {
	receiving.attempts.Add(1)
	v, ok, err := in.Values.Recv(ctx)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, nil
	}
	return v, nil
})

// THE POINT: a thread that waits for long enough is unloaded — its attempt
// ended where it stood and handed back with what it was waiting for — and
// dispatched afresh when that arrives. The worker keeps nothing of it in
// between, and the retry replays to the wait and finds the value there.
func TestALongWaitIsUnloadedAndWokenByWhatItWaitsFor(t *testing.T) {
	oldUnload, oldReport := unloadAfter, parkReport
	unloadAfter, parkReport = 150*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { unloadAfter, parkReport = oldUnload, oldReport })
	receiving.attempts.Store(0)

	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 1})
	name := "test-unload-" + strconv.FormatUint(runSeq.Add(1), 36)

	var got int
	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		receiver := ctx.Go(receivesOnce, feed{Values: ch})
		// Long enough for the receiver to be unloaded, and the slot it
		// held is free meanwhile.
		if _, err := ctx.Go(double, 1).Await(ctx); err != nil {
			return err
		}
		if err := ctx.Sleep(600 * time.Millisecond); err != nil {
			return err
		}
		if err := ch.Send(ctx, 7); err != nil {
			return err
		}
		var err error
		got, err = receiver.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 7 {
		t.Fatalf("got %d, want 7", got)
	}
	if n := receiving.attempts.Load(); n != 2 {
		t.Fatalf("the receiver ran %d attempts, want 2: one unloaded, one woken by the value", n)
	}
	entries := awaitJournal(t, c, func(es []journalEntry) bool {
		return countKind(es, journalYielded) >= 1 && countKind(es, journalRedispatch) >= 1
	})
	var why string
	for _, e := range entries {
		if e.Kind == journalYielded {
			why = e.Err
		}
	}
	if countKind(entries, journalYielded) != 1 || !containsAll(why, "recv", name+"/main.ch0") {
		t.Errorf("journal: %d yields, last saying %q; want one, waiting on recv from the run's channel",
			countKind(entries, journalYielded), why)
	}
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}

var joining struct {
	attempts atomic.Int32
	children atomic.Int32
}

// slowChild takes a while, and counts its runs.
var slowChild = flow.Define("test.slowChild", func(ctx flow.Context, d time.Duration) (string, error) {
	joining.children.Add(1)
	select {
	case <-time.After(d):
		return "finished", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
})

// awaitsASlowChild forks a thread that takes a while and waits for it.
var awaitsASlowChild = flow.Define("test.awaitsASlowChild", func(ctx flow.Context, in int) (string, error) {
	joining.attempts.Add(1)
	return ctx.Go(slowChild, 700*time.Millisecond).Await(ctx)
})

// THE POINT: a parent unloaded while it waits for a thread it forked is
// woken by that thread finishing, and its replay finds the result kept for
// it rather than asking again.
func TestAParentUnloadedOnAJoinIsWokenByItsChild(t *testing.T) {
	oldUnload, oldReport := unloadAfter, parkReport
	unloadAfter, parkReport = 150*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { unloadAfter, parkReport = oldUnload, oldReport })
	joining.attempts.Store(0)
	joining.children.Store(0)

	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 1})
	name := "test-join-" + strconv.FormatUint(runSeq.Add(1), 36)

	var got string
	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		var err error
		got, err = ctx.Go(awaitsASlowChild, 0).Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != "finished" {
		t.Fatalf("got %q, want the child's result", got)
	}
	if n := joining.attempts.Load(); n != 2 {
		t.Fatalf("the parent ran %d attempts, want 2: one unloaded on the join, one woken", n)
	}
	if n := joining.children.Load(); n != 1 {
		t.Fatalf("the child ran %d times, want 1: the woken parent is answered from the kept result", n)
	}
}

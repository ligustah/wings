package wings

import (
	"context"
	"testing"
	"time"
)

// TestWorkSurvivesAWorkerBeingKilled is the at-least-once promise, exercised.
//
// THE POINT: a worker owns the queue of work assigned to it — that is the cost
// of the broker-per-worker topology — so when one dies, whatever it had not yet
// answered for is gone unless the coordinator notices and re-sends. Nothing
// else in the suite covers that path, and it is the one that matters when a
// Spot VM is reclaimed mid-run.
//
// The kill is a hard Kill, not a shutdown: a preempted machine does not get to
// close its connections politely.
func TestWorkSurvivesAWorkerBeingKilled(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	c := start(t, Config{Target: LocalProcess(), Workers: 3, Concurrency: 2})

	// Long enough that jobs are still in flight when the worker dies, short
	// enough that the test is not slow.
	const jobs = 24
	in := make([]time.Duration, jobs)
	for i := range in {
		in[i] = 300 * time.Millisecond
	}

	type outcome struct {
		got []string
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		got, err := c.Map(t.Context(), slow, in)
		done <- outcome{got, err}
	}()

	// Kill one worker while it is busy.
	time.Sleep(250 * time.Millisecond)
	victim := c.workers[0]
	if victim.proc == nil {
		t.Fatal("expected a local worker with a process to kill")
	}
	t.Logf("killing worker %s (pid %d)", victim.id, victim.proc.Pid)
	if err := victim.proc.Kill(); err != nil {
		t.Fatalf("kill worker: %v", err)
	}

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("Map failed after a worker was killed: %v", res.err)
		}
		if len(res.got) != jobs {
			t.Fatalf("got %d results, want %d", len(res.got), jobs)
		}
		for i, g := range res.got {
			if g != "finished" {
				t.Fatalf("result %d is %q; a job was lost with the worker", i, g)
			}
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Map never returned after a worker was killed; the jobs it held were not redispatched")
	}

	// The killed worker must not still be handed work.
	if !victim.dead.Load() {
		t.Error("the killed worker was never marked dead")
	}
}

// A cluster with no live workers must refuse rather than block forever.
func TestCallWithNoLiveWorkersFails(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 1})

	w := c.workers[0]
	if err := w.proc.Kill(); err != nil {
		t.Fatalf("kill worker: %v", err)
	}
	// Let the tail notice.
	deadline := time.Now().Add(30 * time.Second)
	for !w.dead.Load() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !w.dead.Load() {
		t.Fatal("the tail never noticed the worker had died")
	}

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	if _, err := c.Call(ctx, double, 1); err == nil {
		t.Fatal("want an error when no worker is live, got nil")
	}
}

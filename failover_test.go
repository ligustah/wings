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
		got, err := Map(c.Bind(t.Context()), slow, in)
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

	if _, err := double(c.Bind(ctx), 1); err == nil {
		t.Fatal("want an error when no worker is live, got nil")
	}
}

// THE POINT: a worker that is merely unreachable must not be thrown away. The
// coordinator used to treat any read error as death, so a network blip cost a
// healthy machine that was still holding its queue — and on a cloud target,
// reaping it destroyed the VM.
//
// A closed tunnel is the honest local stand-in for that: the worker process is
// alive and working, only the path to it is gone.
func TestATransientOutageDoesNotKillAWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	c := start(t, Config{
		Target:           LocalProcess(),
		Workers:          1,
		Concurrency:      1,
		ReconnectTimeout: 30 * time.Second,
	})

	// Real work first, so the connection is established and the tail is actively
	// reading through it. Breaking a link that was never used tests something
	// else.
	if _, err := double(c.Bind(t.Context()), 1); err != nil {
		t.Fatalf("warm-up call: %v", err)
	}

	w := c.workers[0]

	// Break the connection without touching the process: closing the client
	// fails the read the way a dropped link would.
	if err := w.client.Close(); err != nil {
		t.Logf("closing the worker client: %v", err)
	}

	// It must NOT be declared dead. Give the tail plenty of chances to.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if w.dead.Load() {
			t.Fatal("a worker whose connection dropped was declared dead; " +
				"its queue was intact and it should have been retried")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if w.hasExited() {
		t.Fatal("the worker process exited; this test meant to break only the connection")
	}
}

// And the window has to end: a worker that never comes back must eventually be
// given up on, or its jobs are outstanding forever.
func TestAnOutageThatNeverEndsGivesUp(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	c := start(t, Config{
		Target:           LocalProcess(),
		Workers:          2,
		Concurrency:      1,
		ReconnectTimeout: 500 * time.Millisecond,
	})

	if _, err := double(c.Bind(t.Context()), 1); err != nil {
		t.Fatalf("warm-up call: %v", err)
	}

	w := c.workers[0]
	if err := w.client.Close(); err != nil {
		t.Logf("closing the worker client: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for !w.dead.Load() && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if !w.dead.Load() {
		t.Fatal("a worker that never came back was never given up on")
	}
}

// Results the coordinator has seen must be on its own durable streams, not only
// in a map that dies with the process. That is what keeps the coordinator from
// being the half that must not crash.
func TestResultsAreMirroredToTheCoordinator(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})

	in := []int{1, 2, 3, 4, 5}
	if _, err := Map(c.Bind(t.Context()), double, in); err != nil {
		t.Fatalf("Map: %v", err)
	}

	w := c.workers[0]
	stream, err := c.shared.OpenStream[mirroredResult](mirrorStreamFor(w.id))
	if err != nil {
		t.Fatalf("open mirror: %v", err)
	}

	var got []mirroredResult
	for from := int64(0); ; {
		recs, err := stream.Read(t.Context(), from, 256)
		if err != nil {
			t.Fatalf("read mirror: %v", err)
		}
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			got = append(got, r.Record)
			from = r.Offset + 1
		}
	}

	if len(got) != len(in) {
		t.Fatalf("the mirror holds %d results, want %d", len(got), len(in))
	}

	// Source offsets must be strictly increasing, but NOT dense: a worker writes
	// its results transactionally, so its stream carries commit markers between
	// them and the offsets have gaps. That is exactly why the source offset is
	// recorded rather than inferred from the mirror's own length.
	prev := int64(-1)
	for i, m := range got {
		if m.Source <= prev {
			t.Errorf("mirrored result %d records source offset %d, not above the previous %d",
				i, m.Source, prev)
		}
		prev = m.Source

		if m.Worker != w.id {
			t.Errorf("mirrored result %d names worker %q, want %q", i, m.Worker, w.id)
		}
		if m.Result.Error != "" {
			t.Errorf("mirrored result %d carries an error: %s", i, m.Result.Error)
		}
	}
}

// A mirror that already holds results must not have them read again — that is
// the offset doing its job, and it is what makes a reattach cheap.
func TestTheMirrorRemembersWhereItGotTo(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})

	if _, err := Map(c.Bind(t.Context()), double, []int{1, 2, 3}); err != nil {
		t.Fatalf("Map: %v", err)
	}

	w := c.workers[0]
	live := w.mirror.next
	if live == 0 {
		t.Fatal("the mirror still resumes from 0 after three results")
	}

	// A fresh mirror over the same stream must agree — that is what a reattach
	// after a restart does, and the position has to come off the stream rather
	// than out of a map that died with the process.
	again, err := c.openMirror(t.Context(), w.id)
	if err != nil {
		t.Fatalf("reopen mirror: %v", err)
	}
	if again.next != live {
		t.Fatalf("a reopened mirror resumes from %d, the live one from %d; "+
			"the position must be recoverable from the stream alone", again.next, live)
	}

	// And it must not re-deliver: more work on the same worker keeps moving
	// forward from there.
	if _, err := Map(c.Bind(t.Context()), double, []int{7, 8}); err != nil {
		t.Fatalf("Map: %v", err)
	}
	if w.mirror.next <= live {
		t.Fatalf("after two more results the mirror resumes from %d, not past %d",
			w.mirror.next, live)
	}
}

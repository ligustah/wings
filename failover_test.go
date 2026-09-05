package wings

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
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
		got, err := mapOn(t.Context(), c, slow, in)
		done <- outcome{got, err}
	}()

	// Kill one worker while it is busy.
	time.Sleep(250 * time.Millisecond)
	victim := firstWorker(t, c)
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

// THE POINT: a fixed worker count is a size to keep. A cluster whose only
// worker died used to refuse every call from then on; now the worker is
// replaced, and a call that lands in the gap waits for the replacement rather
// than failing. The caller's context is the bound on that wait.
func TestACallWithNoLiveWorkerWaitsForTheReplacement(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 1,
		Scaling: Scaling{Min: 1, Max: 1, Interval: 100 * time.Millisecond}})

	w := firstWorker(t, c)
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

	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()
	got, err := double(c.Bind(ctx), 21)
	if err != nil {
		t.Fatalf("a call made while the only worker was being replaced: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	if c.Workers() != 1 {
		t.Fatalf("the fleet is %d workers after the replacement, want 1", c.Workers())
	}
}

// THE POINT: a worker with nothing to do is not a worker that has gone quiet.
// The coordinator's poll of a worker's results expires when the worker produced
// nothing in the interval, and over gRPC that expiry arrives as a status rather
// than the context's own error; it was being read as lost contact, and a worker
// that then sat idle through the reconnect window — the replacement machine on
// a fleet whose survivor had taken every job — was declared dead and its
// machine deleted, twice over on the run that found this.
func TestAnIdleWorkerIsNotALostOne(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	// Polls short enough that the worker sits through many of them, and a
	// reconnect window they would exhaust several times over if any one of
	// them were taken for trouble.
	old := pollInterval
	pollInterval = 200 * time.Millisecond
	t.Cleanup(func() { pollInterval = old })

	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 1,
		ReconnectTimeout: time.Second})
	w := firstWorker(t, c)

	time.Sleep(3 * time.Second)

	if w.dead.Load() {
		t.Fatal("an idle worker was declared dead")
	}
	if n := c.Workers(); n != 1 {
		t.Fatalf("%d workers after sitting idle, want the 1 that was there", n)
	}
	entries := awaitJournal(t, c, func([]journalEntry) bool { return true })
	if n := countKind(entries, journalWorkerGone); n != 0 {
		t.Fatalf("%d worker-gone entries for a worker that only sat idle: %+v", n, entries)
	}
	got, err := double(c.Bind(t.Context()), 21)
	if err != nil {
		t.Fatalf("a call after the worker sat idle: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
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

	w := firstWorker(t, c)

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

	w := firstWorker(t, c)
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
	if _, err := mapOn(t.Context(), c, double, in); err != nil {
		t.Fatalf("Map: %v", err)
	}

	w := firstWorker(t, c)
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

	if _, err := mapOn(t.Context(), c, double, []int{1, 2, 3}); err != nil {
		t.Fatalf("Map: %v", err)
	}

	w := firstWorker(t, c)
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
	if _, err := mapOn(t.Context(), c, double, []int{7, 8}); err != nil {
		t.Fatalf("Map: %v", err)
	}
	if w.mirror.next <= live {
		t.Fatalf("after two more results the mirror resumes from %d, not past %d",
			w.mirror.next, live)
	}
}

// A dead worker is reaped only once nothing is outstanding on it — and a worker
// that dies mid-job always has something outstanding. Moving its jobs away
// without crediting them back left it permanently in flight, so it was never
// reaped and, on a cloud target, its machine billed on until the cluster
// stopped. Which is precisely the case reaping exists for.
// THE POINT: a cloud that takes a machine back usually says so a little
// ahead. A worker that hears it tells the coordinator at once, so what it
// owed is moved in the time the cloud gave rather than when the connection is
// found dead, and it stops taking new work. Here the cloud is a fake metadata
// server, and the notice is given to one worker of two.
func TestAWorkerToldItIsBeingTakenBackHandsItsWorkOver(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	var (
		mu     sync.Mutex
		victim string // the worker being taken back, once chosen
	)
	metadata := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing Metadata-Flavor header", http.StatusForbidden)
			return
		}
		if victim != "" && r.Header.Get("X-Wings-Worker") == victim {
			io.WriteString(w, "TRUE")
			return
		}
		io.WriteString(w, "FALSE")
	}))
	defer metadata.Close()
	// Inherited by the child processes this test's workers are.
	t.Setenv(PreemptionURLEnv, metadata.URL)

	c := start(t, Config{Target: LocalProcess(), Workers: 2, Concurrency: 1,
		ReconnectTimeout: 2 * time.Minute})

	done := make(chan error, 1)
	go func() {
		_, err := slow(c.Bind(t.Context()), 4*time.Second)
		done <- err
	}()
	var on *workerConn
	waitFor(t, "the job to start on a worker", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, p := range c.pending {
			if !p.started.IsZero() {
				on = p.worker
			}
		}
		return on != nil
	})

	// The cloud decides.
	mu.Lock()
	victim = on.id
	mu.Unlock()

	// The job is moved and finishes elsewhere, without the reconnect window
	// having been waited out.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("the job on the worker being taken back: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the job never finished; the notice was not acted on")
	}
	// And the worker leaves service, as a dead one would. Its replacement
	// arrives right behind it, so the count is no measure; that this one is
	// gone is.
	waitFor(t, "the worker to be released", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return !slices.Contains(c.workers, on)
	})
	// And the record says why. The release that follows writes its own line
	// too, as it does for any dead worker; the notice is the one that matters.
	awaitJournal(t, c, func(es []journalEntry) bool {
		for _, e := range es {
			if e.Kind == journalWorkerGone && e.Worker == on.id && e.Err == "preempted" {
				return true
			}
		}
		return false
	})
	for _, e := range readJournal(t, c) {
		if e.Kind == journalWorkerGone && e.Worker == on.id && e.Err == "preempted" {
			return
		}
	}
	t.Fatal("the record never says the worker was preempted")
}

func TestAWorkerThatDiesMidJobIsReaped(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	// No Scaling: reaping a dead worker is not a scaling decision, and a
	// cluster with a fixed worker count used to have nothing that did it.
	c := start(t, Config{Target: LocalProcess(), Workers: 3, Concurrency: 2})

	in := make([]time.Duration, 12)
	for i := range in {
		in[i] = 300 * time.Millisecond
	}
	done := make(chan error, 1)
	go func() {
		_, err := mapOn(t.Context(), c, slow, in)
		done <- err
	}()

	time.Sleep(250 * time.Millisecond)
	victim := firstWorker(t, c)
	if victim.proc == nil {
		t.Fatal("expected a local worker with a process to kill")
	}
	if err := victim.proc.Kill(); err != nil {
		t.Fatalf("kill worker: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Map: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Map never returned")
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		inflight := victim.inflight
		var listed bool
		for _, w := range c.workers {
			if w == victim {
				listed = true
			}
		}
		c.mu.Unlock()
		if inflight == 0 && !listed {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	c.mu.Lock()
	inflight := victim.inflight
	c.mu.Unlock()
	t.Fatalf("the killed worker still holds %d jobs and was never reaped; its machine would bill on until Stop", inflight)
}

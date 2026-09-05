package wings

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// A work function is run code: it may fork, use a channel, call other
// functions, read the clock and sleep, on whichever worker it lands on.
var activity = flow.Define("test.activity", func(ctx flow.Context, in int) (string, error) {
	ch := ctx.NewBufferedChannel[int](2)
	producer := ctx.Spawn(func(ctx flow.Context) (int, error) {
		for i := range 2 {
			if err := ch.Send(ctx, in+i); err != nil {
				return 0, err
			}
		}
		return 0, ch.Close(ctx)
	})
	sum := 0
	for {
		v, ok, err := ch.Recv(ctx)
		if err != nil {
			return "", err
		}
		if !ok {
			break
		}
		sum += v
	}
	if _, err := producer.Await(ctx); err != nil {
		return "", err
	}
	doubled, err := ctx.Map(double, []int{sum})
	if err != nil {
		return "", err
	}
	now, err := ctx.Now()
	if err != nil {
		return "", err
	}
	if err := ctx.Sleep(5 * time.Millisecond); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d/%d/%t", sum, doubled[0], !now.IsZero()), nil
})

// THE POINT: a function on a worker is not a lesser kind of code than the
// workflow that called it. It runs as a run of its own, so everything a run's
// body may do it may do — and on every target the same way.
func TestAWorkFunctionIsRunCode(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target Target
	}{
		{"inprocess", InProcess()},
		{"local", LocalProcess()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "local" && testing.Short() {
				t.Skip("spawns child processes")
			}
			c := start(t, Config{Target: tc.target, Workers: 1, Concurrency: 2})
			got, err := activity(c.Bind(t.Context()), 10)
			if err != nil {
				t.Fatalf("activity: %v", err)
			}
			if want := "21/42/true"; got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
}

var replayed struct {
	nested   atomic.Int32
	attempts atomic.Int32
	release  chan struct{}
	once     sync.Once
}

// nestedCounted leaves a trace of every execution where the test can find it
// from outside the process: a recording, named for the attempt that ran it.
var nestedCounted = flow.Define("test.nestedCounted", func(ctx flow.Context, in int) (int, error) {
	replayed.nested.Add(1)
	rec, err := Record[int](ctx, "nested")
	if err != nil {
		return 0, err
	}
	if err := rec.Record(in); err != nil {
		return 0, err
	}
	if err := rec.Close(); err != nil {
		return 0, err
	}
	return in * 3, nil
})

type stalled struct {
	V   int    `json:"v"`
	Job string `json:"job"`
}

// forksThenStalls makes a nested call, reports progress — which commits its
// history — and then, on its first attempt, goes quiet until moved.
var forksThenStalls = flow.Define("test.forksThenStalls", func(ctx flow.Context, in int) (stalled, error) {
	replayed.attempts.Add(1)
	v, err := ctx.Go(nestedCounted, in).Await(ctx)
	if err != nil {
		return stalled{}, err
	}
	if err := ctx.Heartbeat(v); err != nil {
		return stalled{}, err
	}
	if ctx.Attempt() == 0 {
		select {
		case <-replayed.release:
		case <-ctx.Done():
		}
		return stalled{}, errors.New("test.forksThenStalls: the first attempt was abandoned")
	}
	return stalled{V: v, Job: jobFrom(ctx).id}, nil
}, flow.WithHeartbeatTimeout(300*time.Millisecond))

// THE POINT: a moved function replays what it already did rather than doing
// it again. The history of its first attempt reached the coordinator at the
// heartbeat that committed it, was put on the next worker under the retry's
// name, and the retry's nested call is answered from it.
//
// In process the counters say so directly. Across processes they cannot, so
// the nested call leaves a recording under the job it ran as, and one
// recording in the whole cluster is the proof.
func TestAMovedWorkFunctionReplaysItsHistory(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target Target
	}{
		{"inprocess", InProcess()},
		{"local", LocalProcess()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "local" && testing.Short() {
				t.Skip("spawns child processes")
			}
			replayed.release = make(chan struct{})
			replayed.nested.Store(0)
			replayed.attempts.Store(0)
			t.Cleanup(func() { replayed.once = sync.Once{} })
			t.Cleanup(func() { replayed.once.Do(func() { close(replayed.release) }) })

			c := start(t, Config{Target: tc.target, Workers: 2, Concurrency: 2})

			got, err := forksThenStalls(c.Bind(t.Context()), 7)
			if err != nil {
				t.Fatalf("forksThenStalls: %v", err)
			}
			if got.V != 21 {
				t.Fatalf("got %d, want 21", got.V)
			}
			if tc.name == "inprocess" {
				if n := replayed.attempts.Load(); n != 2 {
					t.Fatalf("the function ran %d times, want 2: once stalled, once moved", n)
				}
				if n := replayed.nested.Load(); n != 1 {
					t.Fatalf("the nested call ran %d times across the move, want 1: the retry must replay it from the history", n)
				}
			}
			// The nested call is a job of its own, and every execution of it
			// leaves a recording under its job. One recording means it ran once:
			// the retry was answered from the history rather than calling again.
			names, err := c.shared.ListStreams(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			var recordings []string
			for _, name := range names {
				if o, ok := parseOutput(name); ok && o.Prefix == recordingPrefix && o.Name == "nested" {
					recordings = append(recordings, name)
				}
			}
			if len(recordings) != 1 {
				t.Fatalf("the nested call left %d recordings, want 1: %v", len(recordings), recordings)
			}
		})
	}
}

var visible struct {
	job     chan string
	proceed chan struct{}
}

// recordsInTwoHalves records, waits to be looked at, reports progress, waits
// again, records more and returns.
var recordsInTwoHalves = flow.Define("test.recordsInTwoHalves", func(ctx flow.Context, _ int) (Recording, error) {
	rec, err := Record[int](ctx, "log")
	if err != nil {
		return Recording{}, err
	}
	for i := range 3 {
		if err := rec.Record(i); err != nil {
			return Recording{}, err
		}
	}
	if err := rec.Flush(); err != nil {
		return Recording{}, err
	}
	visible.job <- jobFrom(ctx).id
	<-visible.proceed
	if err := ctx.Heartbeat(3); err != nil {
		return Recording{}, err
	}
	visible.job <- "committed"
	<-visible.proceed
	for i := 3; i < 5; i++ {
		if err := rec.Record(i); err != nil {
			return Recording{}, err
		}
	}
	if err := rec.Close(); err != nil {
		return Recording{}, err
	}
	return rec.Recording(), nil
})

// THE POINT: what a job writes becomes visible at its commit points and not
// before. Three events sent and not committed are three events nobody can
// read; the heartbeat commits them; the return commits the rest.
func TestWhatAJobWritesIsVisibleAtItsCommitPoints(t *testing.T) {
	visible.job = make(chan string, 2)
	visible.proceed = make(chan struct{})

	c := start(t, Config{Target: InProcess(), Workers: 1})

	type result struct {
		rec Recording
		err error
	}
	done := make(chan result, 1)
	go func() {
		rec, err := recordsInTwoHalves(c.Bind(t.Context()), 0)
		done <- result{rec, err}
	}()

	jobID := <-visible.job
	name := outputName{Prefix: recordingPrefix, Job: jobID, Attempt: 0, Name: "log"}.String()
	count := func() int {
		t.Helper()
		st, err := eventStream[int](c.shared, name)
		if err != nil {
			t.Fatalf("open %s: %v", name, err)
		}
		recs, err := st.Read(t.Context(), 0, 100)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		return len(recs)
	}

	if n := count(); n != 0 {
		t.Fatalf("%d events are readable before any commit point; want 0", n)
	}
	visible.proceed <- struct{}{}
	if got := <-visible.job; got != "committed" {
		t.Fatalf("got %q", got)
	}
	if n := count(); n != 3 {
		t.Fatalf("%d events are readable after the heartbeat; want the 3 it committed", n)
	}
	visible.proceed <- struct{}{}

	r := <-done
	if r.err != nil {
		t.Fatalf("recordsInTwoHalves: %v", r.err)
	}
	if n := count(); n != 5 {
		t.Fatalf("%d events are readable after the return; want 5", n)
	}
	var events []int
	for ev, err := range Replay[int](c.Bind(t.Context()), r.rec) {
		if err != nil {
			t.Fatalf("Replay: %v", err)
		}
		events = append(events, ev)
	}
	if len(events) != 5 || !r.rec.Complete || r.rec.Events != 5 {
		t.Fatalf("replayed %v, handle %+v; want 5 events and a complete log", events, r.rec)
	}
}

type placement struct {
	Parent string `json:"parent"`
	Child  string `json:"child"`
}

// whereAmI answers with the worker it ran on.
var whereAmI = flow.Define("test.whereAmI", func(ctx flow.Context, _ int) (string, error) {
	return jobFrom(ctx).node.id, nil
})

// callsWhere makes one call and reports where both ran.
var callsWhere = flow.Define("test.callsWhere", func(ctx flow.Context, _ int) (placement, error) {
	child, err := ctx.Go(whereAmI, 0).Await(ctx)
	if err != nil {
		return placement{}, err
	}
	return placement{Parent: jobFrom(ctx).node.id, Child: child}, nil
})

// THE POINT: a call a work function makes is the cluster's to place. With a
// second worker idle it goes there; with one worker whose only slot the caller
// holds, it still runs — a call is not queued behind the job waiting for it.
func TestACallFromAJobIsPlacedByTheCluster(t *testing.T) {
	for _, tc := range []struct {
		name    string
		target  Target
		workers int
	}{
		{"inprocess/two", InProcess(), 2},
		{"inprocess/one", InProcess(), 1},
		{"local/two", LocalProcess(), 2},
		{"local/one", LocalProcess(), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.workers == 0 || (strings.HasPrefix(tc.name, "local") && testing.Short()) {
				t.Skip("spawns child processes")
			}
			c := start(t, Config{Target: tc.target, Workers: tc.workers, Concurrency: 1})
			got, err := callsWhere(c.Bind(t.Context()), 0)
			if err != nil {
				t.Fatalf("callsWhere: %v", err)
			}
			if got.Parent == "" || got.Child == "" {
				t.Fatalf("got %+v", got)
			}
			if tc.workers == 2 && got.Child == got.Parent {
				t.Fatalf("the call ran on %s, the same worker as its caller, while another was idle", got.Child)
			}
			if tc.workers == 1 && got.Child != got.Parent {
				t.Fatalf("the call ran on %s; there is only %s", got.Child, got.Parent)
			}
		})
	}
}

var patient struct{ attempts atomic.Int32 }

// waitsOnASlowCall must heartbeat every 200ms, and instead waits 700ms on a
// call it made.
var waitsOnASlowCall = flow.Define("test.waitsOnASlowCall", func(ctx flow.Context, _ int) (string, error) {
	patient.attempts.Add(1)
	return ctx.Go(slow, 700*time.Millisecond).Await(ctx)
}, flow.WithHeartbeatTimeout(200*time.Millisecond))

// THE POINT: a job waiting on a call it made is quiet for as long as the call
// takes, and the coordinator — which is running the call — does not mistake
// that for a stuck job and move it.
func TestAJobWaitingOnItsCallIsNotMovedForSilence(t *testing.T) {
	patient.attempts.Store(0)
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 1})
	got, err := waitsOnASlowCall(c.Bind(t.Context()), 0)
	if err != nil {
		t.Fatalf("waitsOnASlowCall: %v", err)
	}
	if got != "finished" {
		t.Fatalf("got %q", got)
	}
	if n := patient.attempts.Load(); n != 1 {
		t.Fatalf("the job ran %d times; waiting on its own call must not count as silence", n)
	}
}

// callsDirectly makes the same call as callsWhere, but directly rather than
// with Go.
var callsDirectly = flow.Define("test.callsDirectly", func(ctx flow.Context, _ int) (placement, error) {
	child, err := whereAmI(ctx, 0)
	if err != nil {
		return placement{}, err
	}
	return placement{Parent: jobFrom(ctx).node.id, Child: child}, nil
})

// THE POINT: a direct call runs where it is made. It blocks its caller either
// way, so another worker — even an idle one — would gain nothing but a round
// trip. Go and Map are the fan-out, and those the cluster places.
func TestADirectCallRunsWhereItIsMade(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 1})
	got, err := callsDirectly(c.Bind(t.Context()), 0)
	if err != nil {
		t.Fatalf("callsDirectly: %v", err)
	}
	if got.Child != got.Parent {
		t.Fatalf("a direct call ran on %s, away from its caller on %s", got.Child, got.Parent)
	}
}

// whoRunsMe reports which process is running it: a worker by its id, or the
// coordinator, which has no job around the call.
var whoRunsMe = flow.Define("test.whoRunsMe", func(ctx flow.Context, _ int) (string, error) {
	if j := jobFrom(ctx); j != nil {
		return j.node.id, nil
	}
	return "coordinator", nil
})

// THE POINT: the same rule on the coordinator. A workflow body that calls a
// function directly runs it where the body is; the threads it forks are what
// go to workers — and the history says which was which, a call and its
// return for the one, a fork and its join for the others.
func TestAWorkflowsDirectCallRunsOnTheCoordinator(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 1})
	name := "test-direct-" + strconv.FormatUint(runSeq.Add(1), 36)

	var direct string
	var forked []string
	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		var err error
		if direct, err = whoRunsMe(ctx, 0); err != nil {
			return err
		}
		forked, err = ctx.Map(whoRunsMe, []int{0, 0})
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if direct != "coordinator" {
		t.Fatalf("the direct call ran on %q, want the coordinator", direct)
	}
	for i, w := range forked {
		if w == "coordinator" || w == "" {
			t.Fatalf("forked thread %d ran on %q, want a worker", i, w)
		}
	}

	client, err := c.sharedClient()
	if err != nil {
		t.Fatal(err)
	}
	events, err := flow.NewStore(client).Events(t.Context(), name, "main")
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, ev := range events {
		counts[protos.EventType(ev)]++
	}
	if counts["CallEvent"] != 1 || counts["ReturnEvent"] != 1 {
		t.Errorf("main's history holds %d calls and %d returns, want 1 of each for the direct call: %v",
			counts["CallEvent"], counts["ReturnEvent"], counts)
	}
	if counts["ForkEvent"] != 2 || counts["JoinEvent"] != 2 {
		t.Errorf("main's history holds %d forks and %d joins, want 2 of each for the fan-out: %v",
			counts["ForkEvent"], counts["JoinEvent"], counts)
	}
}

// THE POINT: a job is a thread of the run that forked it, wherever the fork
// was made. A thread forked by a workflow is named under the workflow's
// main; a thread forked by that thread, on a worker, is named under it —
// and the record says so in those terms, not in terms of jobs.
func TestAJobIsAThreadOfItsRun(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 1})
	name := "test-lineage-" + strconv.FormatUint(runSeq.Add(1), 36)

	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		_, err := ctx.Go(callsWhere, 0).Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	threads := map[string]string{} // thread → function
	awaitJournal(t, c, func(es []journalEntry) bool {
		for _, e := range es {
			if e.Kind == journalSubmitted && e.Run == name {
				threads[e.Thread] = e.Func
			}
		}
		return len(threads) >= 2
	})
	if threads["main.0"] != "test.callsWhere" {
		t.Errorf("the workflow's fork was submitted as thread %q running %q, want main.0 running test.callsWhere",
			"main.0", threads["main.0"])
	}
	if threads["main.0.0"] != "test.whereAmI" {
		t.Errorf("the worker's fork was submitted as %v, want main.0.0 running test.whereAmI", threads)
	}
	for th, fn := range threads {
		if th != "main.0" && th != "main.0.0" {
			t.Errorf("unexpected thread %q running %q in run %s", th, fn, name)
		}
	}
}

// THE POINT: a thread that waits is not running, and the slot it held is
// free for the thread it is waiting on. One worker with one slot: a function
// blocked on a channel the workflow will only feed once ANOTHER function has
// returned. If the waiting function held the slot this would deadlock — the
// other could never run — and it used to.
func TestAWaitingThreadGivesUpItsSlot(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 1})
	name := "test-parked-" + strconv.FormatUint(runSeq.Add(1), 36)

	var total int
	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		receiver := ctx.Go(sums, feed{Values: ch}) // takes the slot, then waits
		v, err := ctx.Go(double, 21).Await(ctx)    // needs the slot
		if err != nil {
			return err
		}
		if err := ch.Send(ctx, v); err != nil {
			return err
		}
		if err := ch.Close(ctx); err != nil {
			return err
		}
		total, err = receiver.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if total != 42 {
		t.Fatalf("got %d, want 42", total)
	}
}

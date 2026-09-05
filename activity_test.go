package wings

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
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
// the nested call leaves a recording named for the attempt that ran it, and
// the retry's absence of one is the proof.
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
			// The retry (attempt 1) must not have run the nested call, so it
			// left no recording of its own; the abandoned attempt's is gone
			// with the job's settling, so neither exists.
			retry := outputName{Prefix: recordingPrefix, Job: got.Job, Attempt: 1, Name: "nested"}.String()
			if ok, err := c.shared.StreamExists(t.Context(), retry); err != nil {
				t.Fatal(err)
			} else if ok {
				t.Fatalf("the retry ran the nested call again: %s exists", retry)
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

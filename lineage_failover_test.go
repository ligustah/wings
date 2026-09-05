package wings

import (
	"context"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// sighting is where a thread of run code found itself running, and a value
// it recorded there.
type sighting struct {
	Worker string `json:"worker"`
	Value  int    `json:"value"`
}

var sightings = make(chan sighting, 4)

// movesItsSpawn forks a closure that records an effect, commits it, reports
// where it is, and then — on its first attempt — waits to be killed with its
// worker. Its retry, wherever it lands, must replay the effect rather than
// draw it again.
var movesItsSpawn = flow.DefineWorkflow("test.movesItsSpawn", func(ctx flow.Context, _ int) error {
	seen := ctx.NewBufferedChannel[sighting](2)
	fut := ctx.Spawn(func(ctx flow.Context) (sighting, error) {
		r, err := ctx.Effect(func() (int, error) { return rand.IntN(1<<30) + 1, nil })
		if err != nil {
			return sighting{}, err
		}
		// A commit point: the effect is in the history the coordinator
		// holds before the worker goes.
		if err := ctx.Heartbeat(1); err != nil {
			return sighting{}, err
		}
		if err := seen.Send(ctx, sighting{Worker: where(ctx), Value: r}); err != nil {
			return sighting{}, err
		}
		if ctx.Attempt() == 0 {
			<-ctx.Done()
			return sighting{}, ctx.Err()
		}
		return sighting{Worker: where(ctx), Value: r}, nil
	})
	first, _, err := seen.Recv(ctx)
	if err != nil {
		return err
	}
	sightings <- first
	again, err := fut.Await(ctx)
	if err != nil {
		return err
	}
	sightings <- again
	return nil
})

// THE POINT: a thread of run code whose worker dies is moved like any job,
// and the worker it lands on is given its history — the ancestors it is
// reached through and what the thread itself recorded — so the retry
// replays what its predecessor did rather than doing it again.
func TestAThreadOfRunCodeMovedToAnotherWorkerReplaysItsHistory(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := start(t, Config{Target: LocalProcess(), Workers: 2, Concurrency: 1, ReconnectTimeout: time.Second})
	for len(sightings) > 0 {
		<-sightings
	}

	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.RunWorkflow(ctx, movesItsSpawn, 0) }()

	var first sighting
	select {
	case first = <-sightings:
	case err := <-done:
		t.Fatalf("the workflow ended before its thread reported: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the thread never reported where it was running")
	}
	if first.Worker == "" || first.Value == 0 {
		t.Fatalf("the thread reported %+v", first)
	}

	// The machine under it goes.
	var killed bool
	for _, w := range c.fleet() {
		if w.id == first.Worker && w.proc != nil {
			if err := w.proc.Kill(); err != nil {
				t.Fatalf("kill worker %s: %v", w.id, err)
			}
			killed = true
		}
	}
	if !killed {
		t.Fatalf("no local worker process is called %s", first.Worker)
	}

	var again sighting
	select {
	case again = <-sightings:
	case err := <-done:
		t.Fatalf("the workflow ended without its thread's result: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("the thread was not run again after its worker died")
	}
	if err := <-done; err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	if again.Worker == first.Worker {
		t.Fatalf("the thread ran again on %s, the worker that was killed", again.Worker)
	}
	if again.Value != first.Value {
		t.Fatalf("the retry recorded %d where its predecessor recorded %d: it did not replay its history",
			again.Value, first.Value)
	}
}

var spawnsASlowChild = flow.DefineWorkflow("test.spawnsASlowChild", func(ctx flow.Context, takes time.Duration) error {
	var err error
	rejoined, err = ctx.Spawn(func(ctx flow.Context) (string, error) { return slowChild(ctx, takes) }).Await(ctx)
	return err
})

var rejoined string

// THE POINT: a coordinator that dies while a thread of run code is on a
// worker, and starts again over the same Dir, rejoins that thread — still
// running, or finished meanwhile — when its replay forks it again, rather
// than sending it a second time. The recovered job has no lineage until the
// fork brings it.
func TestARestartedCoordinatorRejoinsAThreadOfRunCode(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	for _, tc := range []struct {
		name  string
		takes time.Duration
		gap   time.Duration
	}{
		{"still running", 4 * time.Second, 0},
		{"finished meanwhile", 300 * time.Millisecond, 1500 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cloud := newFakeCloud(t)
			dir := t.TempDir()
			rejoined = ""

			first := startRemote(t, dir, cloud, 1)
			firstCtx, stopFirst := context.WithCancel(t.Context())
			firstDone := make(chan error, 1)
			go func() { firstDone <- first.RunWorkflow(firstCtx, spawnsASlowChild, tc.takes) }()
			awaitJournal(t, first, func(es []journalEntry) bool {
				return countKind(es, journalSubmitted) >= 1
			})
			abandon(t, first)
			stopFirst()
			if err := <-firstDone; err == nil || !errors.Is(err, context.Canceled) {
				t.Logf("the first coordinator's run ended with %v", err)
			}
			time.Sleep(tc.gap)

			second := startRemote(t, dir, cloud, 1)
			defer func() {
				if err := second.Stop(context.Background()); err != nil {
					t.Errorf("Stop: %v", err)
				}
			}()
			if err := second.RunWorkflow(t.Context(), spawnsASlowChild, tc.takes); err != nil {
				t.Fatalf("RunWorkflow after the restart: %v", err)
			}
			if rejoined != "finished" {
				t.Fatalf("got %q, want the thread's result", rejoined)
			}
			entries := awaitJournal(t, second, func(es []journalEntry) bool {
				return countKind(es, journalCompleted) >= 1 && countKind(es, journalAttached) >= 1
			})
			var submitted, recovered, attached int
			var seen []string
			for _, e := range entries {
				if e.Run != spawnsASlowChild.Name() {
					continue
				}
				seen = append(seen, e.Kind+":"+e.Job+"@"+e.Worker)
				switch e.Kind {
				case journalSubmitted:
					submitted++
				case journalRecovered:
					recovered++
				case journalAttached:
					attached++
				}
			}
			if submitted != 1 || recovered != 1 || attached != 1 {
				t.Fatalf("journal: %d submitted, %d recovered, %d attached; want 1 of each — "+
					"the thread was forked once, taken back, and rejoined: %v", submitted, recovered, attached, seen)
			}
		})
	}
}

var spawnsSeveralSlowChildren = flow.DefineWorkflow("test.spawnsSeveralSlowChildren", func(ctx flow.Context, takes time.Duration) error {
	var futs []*flow.Future[string]
	for i := range 3 {
		d := takes + time.Duration(i)*100*time.Millisecond
		futs = append(futs, ctx.Spawn(func(ctx flow.Context) (string, error) { return slowChild(ctx, d) }))
	}
	several = nil
	for _, f := range futs {
		v, err := f.Await(ctx)
		if err != nil {
			return err
		}
		several = append(several, v)
	}
	return nil
})

var several []string

// THE POINT: a restart with several threads of run code outstanding takes
// every one of them back, and the replay rejoins each in turn.
func TestARestartedCoordinatorRejoinsSeveralThreadsOfRunCode(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	cloud := newFakeCloud(t)
	dir := t.TempDir()

	first := startRemote(t, dir, cloud, 1)
	firstCtx, stopFirst := context.WithCancel(t.Context())
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.RunWorkflow(firstCtx, spawnsSeveralSlowChildren, 4*time.Second) }()
	awaitJournal(t, first, func(es []journalEntry) bool { return countKind(es, journalSubmitted) >= 3 })
	abandon(t, first)
	stopFirst()
	<-firstDone

	second := startRemote(t, dir, cloud, 1)
	defer func() {
		if err := second.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()
	if err := second.RunWorkflow(t.Context(), spawnsSeveralSlowChildren, 4*time.Second); err != nil {
		t.Fatalf("RunWorkflow after the restart: %v", err)
	}
	if len(several) != 3 || several[0] != "finished" || several[1] != "finished" || several[2] != "finished" {
		t.Fatalf("got %v, want three results", several)
	}
	entries := awaitJournal(t, second, func(es []journalEntry) bool { return countKind(es, journalCompleted) >= 3 })
	var submitted, recovered, attached int
	for _, e := range entries {
		if e.Run != spawnsSeveralSlowChildren.Name() {
			continue
		}
		switch e.Kind {
		case journalSubmitted:
			submitted++
		case journalRecovered:
			recovered++
		case journalAttached:
			attached++
		}
	}
	if submitted != 3 || recovered != 3 || attached != 3 {
		t.Fatalf("journal: %d submitted, %d recovered, %d attached; want 3 of each", submitted, recovered, attached)
	}
}

type report struct {
	Seen *flow.Channel[sighting] `json:"seen"`
}

// parentOfSpawn is a work function that forks a thread of run code and waits
// for it: the parent's worker is killed while the child runs.
var parentOfSpawn = flow.Define("test.parentOfSpawn", func(ctx flow.Context, in report) (int, error) {
	if err := in.Seen.Send(ctx, sighting{Worker: where(ctx)}); err != nil {
		return 0, err
	}
	child := ctx.Spawn(func(ctx flow.Context) (int, error) {
		r, err := ctx.Effect(func() (int, error) { return rand.IntN(1<<30) + 1, nil })
		if err != nil {
			return 0, err
		}
		if err := ctx.Heartbeat(1); err != nil {
			return 0, err
		}
		if err := in.Seen.Send(ctx, sighting{Worker: where(ctx), Value: r}); err != nil {
			return 0, err
		}
		time.Sleep(3 * time.Second)
		return r, nil
	})
	return child.Await(ctx)
})

var parentMoves = flow.DefineWorkflow("test.parentMoves", func(ctx flow.Context, _ int) error {
	seen := ctx.NewBufferedChannel[sighting](4)
	fut := ctx.Go(parentOfSpawn, report{Seen: seen})
	for range 2 {
		s, _, err := seen.Recv(ctx)
		if err != nil {
			return err
		}
		sightings <- s
	}
	v, err := fut.Await(ctx)
	if err != nil {
		return err
	}
	sightings <- sighting{Worker: "result", Value: v}
	return nil
})

// THE POINT: a work function that forked a thread of run code and is waiting
// on it is moved when its worker dies, and its retry — replaying the fork —
// rejoins the thread, which was dispatched once and, if it was on the same
// worker, moved once.
func TestAJobMovedWhileItsSpawnRunsRejoinsIt(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := start(t, Config{Target: LocalProcess(), Workers: 2, Concurrency: 2, ReconnectTimeout: time.Second})
	for len(sightings) > 0 {
		<-sightings
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.RunWorkflow(ctx, parentMoves, 0) }()

	next := func(what string) sighting {
		select {
		case s := <-sightings:
			return s
		case err := <-done:
			t.Fatalf("the workflow ended before %s: %v", what, err)
		case <-time.After(40 * time.Second):
			t.Fatalf("no %s", what)
		}
		return sighting{}
	}
	parent := next("the parent's report")
	child := next("the child's report")
	if parent.Worker == "" || child.Worker == "" || child.Value == 0 {
		t.Fatalf("parent %+v, child %+v", parent, child)
	}
	killed := false
	for _, w := range c.fleet() {
		if w.id == parent.Worker && w.proc != nil {
			if err := w.proc.Kill(); err != nil {
				t.Fatalf("kill: %v", err)
			}
			killed = true
		}
	}
	if !killed {
		t.Fatalf("no local worker process is called %s", parent.Worker)
	}
	result := next("the parent's result")
	if err := <-done; err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	if result.Value != child.Value {
		t.Fatalf("the parent's retry got %d from its thread, which reported %d", result.Value, child.Value)
	}
	entries := awaitJournal(t, c, func(es []journalEntry) bool { return countKind(es, journalCompleted) >= 2 })
	childSubmits := 0
	for _, e := range entries {
		if e.Run == parentMoves.Name() && e.Thread == "main.0.0" && e.Kind == journalSubmitted {
			childSubmits++
		}
	}
	if childSubmits != 1 {
		t.Fatalf("the thread was submitted %d times, want once: the parent's retry rejoins it", childSubmits)
	}
}

package wings

import (
	"strings"
	"testing"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// TestForkReaches covers the predicate hydrateLineage uses to decide whether an
// ancestor's history — as read from the coordinator's engine, which can come
// back short under kill churn — records the fork of the next thread on a
// lineage, and so is complete enough to ship for a replay to reach it.
func TestForkReaches(t *testing.T) {
	fork := func(child string) *protos.Event {
		return &protos.Event{Payload: &protos.Event_Fork{Fork: &protos.ForkEvent{ThreadId: child}}}
	}
	history := []*protos.Event{{ThreadId: "main"}, fork("main.0"), fork("main.1")}

	if !forkReaches(history, "main.1") {
		t.Error("a history that records main.1's fork should reach it")
	}
	if forkReaches(history, "main.2") {
		t.Error("a history missing main.2's fork should not reach it (a short read)")
	}
	if forkReaches(nil, "main.0") {
		t.Error("an empty history reaches no fork")
	}
}

// TestThreadHistoryFindsAForgottenAncestorViaRanAs covers the lookup a
// descendant's placement depends on: a thread of run code dispatched after its
// ancestor's job is already gone must still find that ancestor's history to
// replay through. The ancestor→job mapping lives in byOrigin only while the job
// is outstanding; once forgotten it is kept in ranAs, and the durable history
// (named by job id) outlives the job. Without that fallback the lookup reads the
// coordinator's own store — where a thread that ran as a JOB never wrote — and
// finds nothing, which is the "thread ... has no history to replay" hang.
func TestThreadHistoryFindsAForgottenAncestorViaRanAs(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 1})
	ctx := t.Context()
	client, err := c.sharedClient()
	if err != nil {
		t.Fatalf("sharedClient: %v", err)
	}

	const run, thread, jobID, child = "deep", "main.0", "jobabc", "main.0.0"

	// A durable history for the job that ran main.0, with the fork of its child.
	name := historyName(jobID, 0)
	if err := ensureStream(ctx, client, name); err != nil {
		t.Fatalf("ensureStream: %v", err)
	}
	st, err := eventStream[*protos.Event](client, name)
	if err != nil {
		t.Fatalf("eventStream: %v", err)
	}
	if _, err := st.Append(ctx, []*protos.Event{
		{ThreadId: thread, Payload: &protos.Event_Fork{Fork: &protos.ForkEvent{ThreadId: child}}},
	}); err != nil {
		t.Fatalf("append history: %v", err)
	}

	// The ancestor's job is in neither pending nor byOrigin. Before ranAs
	// remembers it, the lookup falls to the coordinator's own store, where a
	// thread that ran as a job never wrote, and finds nothing.
	if evs, err := c.threadHistory(ctx, run, thread); err != nil {
		t.Fatalf("threadHistory (no ranAs): %v", err)
	} else if len(evs) != 0 {
		t.Fatalf("without ranAs a thread that ran as a job has no coordinator history; got %d events", len(evs))
	}

	// Once forget has remembered which job last ran the call, the same lookup
	// finds the durable history and reaches the child's fork.
	key := flow.Origin{Run: run, Thread: thread}.Key()
	c.mu.Lock()
	c.ranAs[key] = jobID
	c.mu.Unlock()
	evs, err := c.threadHistory(ctx, run, thread)
	if err != nil {
		t.Fatalf("threadHistory (ranAs): %v", err)
	}
	if !forkReaches(evs, child) {
		t.Fatalf("with ranAs remembering job %s, threadHistory should return a history reaching %s; got %d events", jobID, child, len(evs))
	}
}

// where is the worker a thread is running on, or "" on the coordinator.
func where(ctx flow.Context) string {
	if j := jobFrom(ctx); j != nil {
		return j.node.id
	}
	return ""
}

type spawnReport struct {
	Total int
	Ran   string
}

// producesOnAWorker forks a thread of run code that feeds a channel the
// workflow drains: the thread should run on a worker, the workflow stays on
// the coordinator, and the channel joins them.
var producesOnAWorker = flow.DefineWorkflow("test.producesOnAWorker", func(ctx flow.Context, base int) error {
	ch := ctx.NewChannel[int]()
	producer := ctx.Spawn(func(ctx flow.Context) (string, error) {
		for i := 1; i <= 3; i++ {
			if err := ch.Send(ctx, base+i); err != nil {
				return "", err
			}
		}
		return where(ctx), ch.Close(ctx)
	})
	total := 0
	for {
		v, ok, err := ch.Recv(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		total += v
	}
	ran, err := producer.Await(ctx)
	if err != nil {
		return err
	}
	produced = spawnReport{Total: total, Ran: ran}
	return nil
})

var produced spawnReport

// THE POINT: a thread of run code forked by the workflow is placed on a
// worker like a function would be. The worker is sent the thread's lineage
// and the workflow's history, replays the workflow to the fork, and runs the
// closure — which uses a channel made before it left.
func TestAThreadOfRunCodeIsPlacedOnAWorker(t *testing.T) {
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
			c := start(t, Config{Target: tc.target, Workers: 1, Concurrency: 1})
			produced = spawnReport{}
			if err := c.RunWorkflow(t.Context(), producesOnAWorker, 10); err != nil {
				t.Fatalf("RunWorkflow: %v", err)
			}
			if produced.Total != 36 {
				t.Fatalf("the workflow received a total of %d, want 36", produced.Total)
			}
			if produced.Ran == "" {
				t.Fatalf("the thread ran on the coordinator, not on a worker")
			}
			// The worker's copy of the job's history holds the workflow's
			// too, put there for the replay, with the fork of this very
			// thread in it. That fork is the workflow's, already dispatched
			// — not one the job made — and must not be dispatched again
			// from the job's history, which would attach the job to itself.
			entries := awaitJournal(t, c, func(es []journalEntry) bool { return countKind(es, journalCompleted) >= 1 })
			if n := countKind(entries, journalAttached); n != 0 {
				t.Fatalf("the journal shows %d forks attached to jobs in flight, want none: every fork was made once", n)
			}
		})
	}
}

type nestedSpawnReport struct {
	Outer, Inner string
	Value        int
}

// spawnsInAJob is a work function — itself a thread on a worker — that
// forks a thread of run code. That thread's lineage is three long: the
// workflow, this function, the closure.
var spawnsInAJob = flow.Define("test.spawnsInAJob", func(ctx flow.Context, base int) (nestedSpawnReport, error) {
	results := ctx.NewBufferedChannel[int](1)
	inner := ctx.Spawn(func(ctx flow.Context) (string, error) {
		return where(ctx), results.Send(ctx, base*2)
	})
	ran, err := inner.Await(ctx)
	if err != nil {
		return nestedSpawnReport{}, err
	}
	v, _, err := results.Recv(ctx)
	if err != nil {
		return nestedSpawnReport{}, err
	}
	return nestedSpawnReport{Outer: where(ctx), Inner: ran, Value: v}, nil
})

var forksAJobThatSpawns = flow.DefineWorkflow("test.forksAJobThatSpawns", func(ctx flow.Context, base int) error {
	var err error
	nested, err = ctx.Go(spawnsInAJob, base).Await(ctx)
	return err
})

var nested nestedSpawnReport

// THE POINT: lineages nest across machines. A thread of run code forked by a
// work function is dispatched by the coordinator from the function's
// history, with the function's own lineage one thread longer, and lands on
// another worker — the idle one, when there is one.
func TestAThreadOfRunCodeForkedByAJobIsPlacedByTheCluster(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 1})
	nested = nestedSpawnReport{}
	if err := c.RunWorkflow(t.Context(), forksAJobThatSpawns, 7); err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	if nested.Value != 14 {
		t.Fatalf("the function received %d from its thread, want 14", nested.Value)
	}
	if nested.Outer == "" || nested.Inner == "" {
		t.Fatalf("got %+v; both should have run on workers", nested)
	}
	if nested.Outer == nested.Inner {
		t.Fatalf("the thread ran on %s, the same worker as the function that forked it, while another was idle", nested.Inner)
	}
}

// THE POINT: a run with a bare body has no root a worker could start from,
// and its threads of run code run on the coordinator, as before.
func TestAThreadOfABareRunRunsOnTheCoordinator(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 1})
	var ran string
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		var err error
		ran, err = ctx.Spawn(func(ctx flow.Context) (string, error) { return "coordinator:" + where(ctx), nil }).Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !strings.HasSuffix(ran, ":") {
		t.Fatalf("the thread ran on %q, want the coordinator", ran)
	}
}

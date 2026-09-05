package wings

import (
	"strings"
	"testing"

	"github.com/ligustah/wings/flow"
)

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

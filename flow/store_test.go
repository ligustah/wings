package flow_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/durable_streams/broker/embed"
	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings"
	"github.com/ligustah/wings/flow"
)

// streams opens a broker-less embedded instance in dir, the way a coordinator
// keeps its own records, and returns it with the means to close it.
//
// Closing is the caller's business rather than t.Cleanup's because a log
// directory admits exactly one instance at a time — the durable-streams engine
// takes an OS lock on it — so a test that reopens a directory must close the
// first one first. That constraint is real and worth meeting head-on: it is the
// same reason the coordinator keeps ONE engine rather than one per worker.
func streams(t *testing.T, dir string) (*dsclient.Client, func()) {
	t.Helper()

	b, err := embed.StartInProcess(embed.InProcessConfig{Dir: dir})
	if err != nil {
		t.Fatalf("start embedded streams: %v", err)
	}
	var once sync.Once
	closeFn := func() {
		once.Do(func() {
			if err := b.Close(); err != nil {
				t.Errorf("close embedded streams: %v", err)
			}
		})
	}
	t.Cleanup(closeFn)
	return dsclient.Wrap(b.Client()), closeFn
}

// THE POINT: history on a durable stream, not in a map. A workflow's replay
// must survive the store being closed and reopened, because surviving a process
// is the only reason to write it down at all.
func TestHistorySurvivesReopeningTheStore(t *testing.T) {
	ctx := cluster(t)
	dir := filepath.Join(t.TempDir(), "engine")
	instance := flow.NewInstance()

	before := calls.double.Load()

	// First store: run until it fails, leaving a completed call behind. Closed
	// before the second one opens, so nothing is carried over in memory.
	func() {
		client, closeStreams := streams(t, dir)
		defer closeStreams()
		store := flow.NewStore(client)

		wf := flow.Define("t.durable", func(ctx context.Context, in int) (int, error) {
			if _, err := double(ctx, in); err != nil {
				return 0, err
			}
			return 0, errors.New("stop after the call")
		}, flow.MaxAttempts(1), flow.Backoff(time.Millisecond, time.Millisecond))

		if _, err := flow.Run(ctx, wf, instance, 21, flow.WithStore(store)); err == nil {
			t.Fatal("want the first run to fail")
		}
	}()

	if n := calls.double.Load() - before; n != 1 {
		t.Fatalf("the work function ran %d times in the first run, want 1", n)
	}

	// A SECOND instance of the store over the same directory — the engine was
	// closed and reopened in between, so anything remembered in memory is gone.
	client, _ := streams(t, dir)
	store := flow.NewStore(client)

	wf := flow.Define("t.durable", func(ctx context.Context, in int) (int, error) {
		return double(ctx, in)
	})

	got, err := flow.Run(ctx, wf, instance, 21, flow.WithStore(store))
	if err != nil {
		t.Fatalf("resumed Run: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	// Still one: the second run replayed the call the first one recorded.
	if n := calls.double.Load() - before; n != 1 {
		t.Fatalf("the work function ran %d times in total; the reopened store should have replayed it", n)
	}
}

func TestStoredHistoryIsReadableAsEvents(t *testing.T) {
	ctx := cluster(t)
	client, _ := streams(t, filepath.Join(t.TempDir(), "engine"))
	store := flow.NewStore(client)
	instance := flow.NewInstance()

	wf := flow.Define("t.readable", func(ctx context.Context, in int) (int, error) {
		return double(ctx, in)
	})
	if _, err := flow.Run(ctx, wf, instance, 2, flow.WithStore(store)); err != nil {
		t.Fatalf("Run: %v", err)
	}

	events, err := store.Events(context.Background(), "t.readable", instance)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no events were recorded")
	}

	// The account has to be legible on its own: an attempt that opened, a call,
	// its result, and an attempt that closed.
	var start, call, ret, end int
	for _, ev := range events {
		switch {
		case ev.GetRunStart() != nil:
			start++
		case ev.GetCall() != nil:
			call++
		case ev.GetReturn() != nil:
			ret++
		case ev.GetRunEnd() != nil:
			end++
		}
	}
	if start != 1 || call != 1 || ret != 1 || end != 1 {
		t.Errorf("recorded start=%d call=%d return=%d end=%d, want one of each", start, call, ret, end)
	}
}

// Two instances of the same workflow must not read each other's history, or a
// replay would resume somebody else's run.
func TestInstancesAreIsolated(t *testing.T) {
	ctx := cluster(t)
	client, _ := streams(t, filepath.Join(t.TempDir(), "engine"))
	store := flow.NewStore(client)

	var ran atomic.Int64
	wf := flow.Define("t.isolated", func(ctx context.Context, in int) (int, error) {
		ran.Add(1)
		return double(ctx, in)
	})

	a, err := flow.Run(ctx, wf, "instance-a", 1, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Run a: %v", err)
	}
	b, err := flow.Run(ctx, wf, "instance-b", 10, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Run b: %v", err)
	}

	if a != 2 || b != 20 {
		t.Fatalf("got %d and %d, want 2 and 20", a, b)
	}
	if n := ran.Load(); n != 2 {
		t.Fatalf("the workflow body ran %d times; each instance should have run once", n)
	}
}

// The coordinator's own instance is the intended home for this, so it has to
// work: a cluster's embedded engine, reached the way flow expects.
func TestWorkflowOnTheClusterOwnStreams(t *testing.T) {
	dir := t.TempDir()

	c, err := wings.Start(t.Context(), wings.Config{Target: wings.InProcess(), Dir: dir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() {
		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	ctx := c.Bind(t.Context())

	// The cluster's OWN instance, not a second one. A log directory admits one
	// engine at a time, so this is the difference between a workflow history
	// that sits beside the coordinator's other records and one that needs a
	// directory of its own.
	store, err := flow.ClusterStore(ctx)
	if err != nil {
		t.Fatalf("ClusterStore: %v", err)
	}

	wf := flow.Define("t.cluster", func(ctx context.Context, in []int) ([]int, error) {
		return wings.Map(ctx, double, in)
	})

	got, err := flow.Run(ctx, wf, flow.NewInstance(), []int{1, 2, 3}, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(got) != 3 || got[0] != 2 || got[1] != 4 || got[2] != 6 {
		t.Fatalf("got %v, want [2 4 6]", got)
	}
}

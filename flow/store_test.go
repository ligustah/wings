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

	"github.com/ligustah/wings/flow"
)

// streams opens a broker-less embedded instance in dir, the way a coordinator
// keeps its own records, and returns it with the means to close it.
//
// Closing is the caller's business rather than t.Cleanup's because a log
// directory admits exactly one instance at a time — the durable-streams engine
// takes an OS lock on it — so a test that reopens a directory must close the
// first one first.
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

// THE POINT: history on a durable stream, not in a map. A run's replay must
// survive the store being closed and reopened, because surviving a process is
// the only reason to write it down at all.
func TestHistorySurvivesReopeningTheStore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "engine")
	name := flow.NewName()

	before := calls.double.Load()

	// First store: run until it fails, leaving a completed call behind. Closed
	// before the second one opens, so nothing is carried over in memory.
	func() {
		client, closeStreams := streams(t, dir)
		defer closeStreams()

		err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
			if _, err := double(ctx, 21); err != nil {
				return err
			}
			return errors.New("stop after the call")
		}, flow.WithStore(flow.NewStore(client)), flow.MaxAttempts(1), quick)
		if err == nil {
			t.Fatal("want the first run to fail")
		}
	}()

	if n := calls.double.Load() - before; n != 1 {
		t.Fatalf("the function ran %d times in the first run, want 1", n)
	}

	// A SECOND instance of the store over the same directory — the engine was
	// closed and reopened in between, so anything remembered in memory is gone.
	client, _ := streams(t, dir)

	var got int
	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		var err error
		got, err = double(ctx, 21)
		return err
	}, flow.WithStore(flow.NewStore(client)))
	if err != nil {
		t.Fatalf("resumed Run: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	// Still one: the second run replayed the call the first one recorded.
	if n := calls.double.Load() - before; n != 1 {
		t.Fatalf("the function ran %d times in total; the reopened store should have replayed it", n)
	}
}

func TestStoredHistoryIsReadableAsEvents(t *testing.T) {
	client, _ := streams(t, filepath.Join(t.TempDir(), "engine"))
	store := flow.NewStore(client)
	name := flow.NewName()

	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		_, err := double(ctx, 2)
		return err
	}, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	events, err := store.Events(context.Background(), name, "main")
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

// Two runs must not read each other's history, or a replay would resume
// somebody else's run.
func TestRunsAreIsolated(t *testing.T) {
	client, _ := streams(t, filepath.Join(t.TempDir(), "engine"))
	store := flow.NewStore(client)

	var ran atomic.Int64
	run := func(name string, in int) (int, error) {
		var out int
		err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
			ran.Add(1)
			var err error
			out, err = double(ctx, in)
			return err
		}, flow.WithStore(store))
		return out, err
	}

	a, err := run("run-a", 1)
	if err != nil {
		t.Fatalf("Run a: %v", err)
	}
	b, err := run("run-b", 10)
	if err != nil {
		t.Fatalf("Run b: %v", err)
	}

	if a != 2 || b != 20 {
		t.Fatalf("got %d and %d, want 2 and 20", a, b)
	}
	if n := ran.Load(); n != 2 {
		t.Fatalf("the body ran %d times; each run should have run once", n)
	}
}

// A run that was left mid-flight and is started again later picks up where it
// stopped, however long it was gone.
func TestARunLeftMidFlightResumesLater(t *testing.T) {
	client, _ := streams(t, filepath.Join(t.TempDir(), "engine"))
	store := flow.NewStore(client)
	name := flow.NewName()

	var attempts atomic.Int64
	body := func(ctx flow.Context) error {
		if _, err := double(ctx, 1); err != nil {
			return err
		}
		if attempts.Add(1) == 1 {
			return errors.New("gone")
		}
		_, err := double(ctx, 2)
		return err
	}
	if err := flow.Run(t.Context(), name, body, flow.WithStore(store), flow.MaxAttempts(1)); err == nil {
		t.Fatal("want the first run to fail")
	}
	time.Sleep(10 * time.Millisecond)
	before := calls.double.Load()
	if err := flow.Run(t.Context(), name, body, flow.WithStore(store)); err != nil {
		t.Fatalf("resumed Run: %v", err)
	}
	if n := calls.double.Load() - before; n != 1 {
		t.Fatalf("the resumed run made %d calls, want only the one the first run had not reached", n)
	}
}

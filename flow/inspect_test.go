package flow

import (
	"context"
	"slices"
	"testing"
)

func TestParseThreadStream(t *testing.T) {
	cases := []struct {
		name, run, thread string
		ok                bool
	}{
		{"flow.thread.order-42.main", "order-42", "main", true},
		{"flow.thread.order-42.main.0", "order-42", "main.0", true},
		{"flow.thread.order-42.main.0.3", "order-42", "main.0.3", true},
		{"flow.thread.a.b.c.main", "a.b.c", "main", true},
		// A run whose own name contains ".main" splits at the real thread suffix.
		{"flow.thread.run.main.x.main.1", "run.main.x", "main.1", true},
		{"other.stream", "", "", false},
		{"flow.thread.no-thread-here", "", "", false},
	}
	for _, tc := range cases {
		run, thread, ok := ParseThreadStream(tc.name)
		if ok != tc.ok || run != tc.run || thread != tc.thread {
			t.Errorf("ParseThreadStream(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tc.name, run, thread, ok, tc.run, tc.thread, tc.ok)
		}
	}
	// ThreadStream and ParseThreadStream are inverses.
	for _, in := range []struct{ run, thread string }{{"r", "main"}, {"r.x", "main.2.1"}} {
		run, thread, ok := ParseThreadStream(ThreadStream(in.run, in.thread))
		if !ok || run != in.run || thread != in.thread {
			t.Errorf("round-trip %v: got (%q, %q, %v)", in, run, thread, ok)
		}
	}
}

var inspectChild = Define(func(c Context, in int) (int, error) {
	if err := c.Sleep(0); err != nil {
		return 0, err
	}
	return in + 1, nil
}, WithName("inspect.child"))

// THE POINT: a completed run's recorded history decodes into a thread tree with
// decoded events, discoverable end to end from the store alone.
func TestInspectDecodesRunHistory(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()

	err := Run(ctx, "fib", func(c Context) error {
		child := c.Go(inspectChild, 6)
		got, err := child.Await(c)
		if err != nil {
			return err
		}
		if got != 7 {
			t.Errorf("child returned %d, want 7", got)
		}
		return nil
	}, WithStore(store))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	runs, err := ListRuns(ctx, store)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	if !slices.Contains(runs, "fib") {
		t.Fatalf("runs %v missing fib", runs)
	}

	snap, err := InspectRun(ctx, store, "fib")
	if err != nil {
		t.Fatalf("inspect: %v", err)
	}
	if snap.Run != "fib" {
		t.Fatalf("snapshot run %q", snap.Run)
	}
	main := findThread(snap, "main")
	if main == nil {
		t.Fatalf("no main thread in %+v", snap)
	}
	if main.Status != "completed" {
		t.Fatalf("main status %q, want completed", main.Status)
	}
	if !hasEvent(main, "fork") {
		t.Fatalf("main missing a fork event: %+v", main.Events)
	}
}

func findThread(s Snapshot, id string) *ThreadView {
	for i := range s.Threads {
		if s.Threads[i].ID == id {
			return &s.Threads[i]
		}
	}
	return nil
}

func hasEvent(t *ThreadView, kind string) bool {
	for _, e := range t.Events {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

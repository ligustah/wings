package flow

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/ligustah/wings/flow/protos"
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

// THE POINT: Status is the last RunEnd's status, or "running" when a thread has
// not ended (or was never recorded).
func TestStatusIsTheLastRunEnd(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()
	if err := Run(ctx, "done", func(c Context) error { return nil }, WithStore(store)); err != nil {
		t.Fatalf("run: %v", err)
	}
	if s, err := Status(ctx, store, "done", "main"); err != nil {
		t.Fatalf("status: %v", err)
	} else if s != "completed" {
		t.Fatalf("status = %q, want completed", s)
	}
	if s, err := Status(ctx, store, "never", "main"); err != nil {
		t.Fatalf("status: %v", err)
	} else if s != "running" {
		t.Fatalf("status of an unrecorded run = %q, want running", s)
	}
}

// countingStore counts how many events a read hands back, split by path, so a
// test can prove Status reads the tail and not the whole history.
type countingStore struct {
	Store
	whole int
	tail  int
}

func (c *countingStore) Events(ctx context.Context, run, thread string) ([]*protos.Event, error) {
	evs, err := c.Store.Events(ctx, run, thread)
	c.whole += len(evs)
	return evs, err
}

func (c *countingStore) Tail(ctx context.Context, run, thread string, n int) ([]EventAt, error) {
	evs, err := c.Store.(Tailer).Tail(ctx, run, thread, n)
	c.tail += len(evs)
	return evs, err
}

// THE POINT: a run list reads each run's status from the tail, not by decoding
// its whole main thread — the inspector's -inspect run list otherwise held a
// wave's decisions in RAM per run just to print its status (the write-up's #7).
func TestStatusDoesNotReadTheWholeHistory(t *testing.T) {
	mem := NewMemStore()
	ctx := context.Background()
	const n = 500
	err := Run(ctx, "big", func(c Context) error {
		for i := 0; i < n; i++ {
			if _, err := c.Effect(func() ([]byte, error) { return []byte{byte(i)}, nil }); err != nil {
				return err
			}
		}
		return nil
	}, WithStore(mem))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	cs := &countingStore{Store: mem}
	if s, err := Status(ctx, cs, "big", "main"); err != nil {
		t.Fatalf("status: %v", err)
	} else if s != "completed" {
		t.Fatalf("status = %q, want completed", s)
	}
	if cs.whole != 0 {
		t.Fatalf("Status decoded %d events via the whole-history path; it should read the tail", cs.whole)
	}
	if cs.tail > statusWindow {
		t.Fatalf("Status read %d events from the tail, want <= %d", cs.tail, statusWindow)
	}
}

// THE POINT: a thread's events are read a page at a time from an offset cursor,
// and its header (status, attempts) comes from the ends of its history — never
// the whole of it — so the run view need not hold a whole thread in RAM.
func TestReadEventsPagesAThread(t *testing.T) {
	store := NewMemStore()
	ctx := context.Background()
	const n = 250
	err := Run(ctx, "paged", func(c Context) error {
		for i := 0; i < n; i++ {
			if _, err := c.Effect(func() ([]byte, error) { return []byte{byte(i)}, nil }); err != nil {
				return err
			}
		}
		return nil
	}, WithStore(store))
	if err != nil {
		t.Fatalf("run: %v", err)
	}

	cs := &countingStore{Store: store}
	info, err := InspectThread(ctx, cs, "paged", "main")
	if err != nil {
		t.Fatalf("InspectThread: %v", err)
	}
	if info.Status != "completed" {
		t.Fatalf("status %q, want completed", info.Status)
	}
	if cs.whole != 0 {
		t.Fatalf("InspectThread decoded %d events via the whole-history path; it should read the ends", cs.whole)
	}

	var got, pages int
	for from := int64(0); ; {
		page, err := ReadEvents(ctx, store, "paged", "main", from, 100)
		if err != nil {
			t.Fatalf("ReadEvents: %v", err)
		}
		pages++
		if len(page.Events) > 100 {
			t.Fatalf("page of %d exceeds the limit", len(page.Events))
		}
		for _, ev := range page.Events {
			if ev.Kind == "effect" {
				got++
			}
		}
		if page.Done {
			break
		}
		if page.Next <= from {
			t.Fatalf("cursor did not advance past %d", from)
		}
		from = page.Next
	}
	if got != n {
		t.Fatalf("paged %d effect events, want %d", got, n)
	}
	if pages < 2 {
		t.Fatalf("expected more than one page, got %d", pages)
	}
}

func TestFormatValue(t *testing.T) {
	big := strings.Repeat("x", valueCap+500)
	bin := make([]byte, 40)
	for i := range bin {
		bin[i] = byte(i)
	}
	bigBin := make([]byte, valueCap)
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{"empty", nil, ""},
		{"text", []byte(`{"ok":true}`), `{"ok":true}`},
		{"tabs and newlines", []byte("a\tb\nc"), "a\tb\nc"},
		{"long text truncated", []byte(big), string(big[:valueCap]) + fmt.Sprintf("… (+%d bytes)", 500)},
		{"binary as hex", bin, hex.EncodeToString(bin)},
		{"long binary truncated", bigBin, hex.EncodeToString(bigBin[:valueCap/2]) + fmt.Sprintf("… (+%d bytes)", valueCap-valueCap/2)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatValue(tc.in); got != tc.want {
				t.Fatalf("formatValue = %q, want %q", got, tc.want)
			}
		})
	}

	if !printableUTF8([]byte("hello")) {
		t.Fatal("printableUTF8 rejected plain text")
	}
	if printableUTF8([]byte{0x00, 0x01}) {
		t.Fatal("printableUTF8 accepted control bytes")
	}
	if printableUTF8([]byte{0xff, 0xfe}) {
		t.Fatal("printableUTF8 accepted invalid UTF-8")
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

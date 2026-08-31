package wings

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
)

// Tick is a made-up simulation event, standing in for whatever a long job
// actually produces.
type Tick struct {
	At   int    `json:"at"`
	What string `json:"what"`
}

// simulate produces a long event log and returns only a handle to it. The
// point of the whole feature: the log never travels as a value.
var simulate = Define("test.simulate", func(ctx context.Context, steps int) (Artifact, error) {
	rec, err := Record[Tick](ctx, "replay")
	if err != nil {
		return Artifact{}, err
	}
	for i := range steps {
		if err := rec.Record(Tick{At: i, What: fmt.Sprintf("step %d", i)}); err != nil {
			return Artifact{}, err
		}
	}
	if err := rec.Close(); err != nil {
		return Artifact{}, err
	}
	return rec.Artifact(), nil
})

// render writes bytes rather than events, which is the other half of the same
// storage.
var render = Define("test.render", func(ctx context.Context, size int) (Artifact, error) {
	out, err := Create(ctx, "render")
	if err != nil {
		return Artifact{}, err
	}
	// Written in small pieces, as a real producer would, so the chunking is
	// exercised rather than a single big Write.
	line := bytes.Repeat([]byte("x"), 997)
	for written := 0; written < size; written += len(line) {
		if _, err := out.Write(line); err != nil {
			return Artifact{}, err
		}
	}
	if err := out.Close(); err != nil {
		return Artifact{}, err
	}
	return out.Artifact(), nil
})

// THE POINT: a job whose output is measured in megabytes cannot return it as a
// value — one record on one stream, held whole in memory at both ends, and past
// a few megabytes not carried by the transport at all. It streams out in pieces
// as it is produced, lands on the coordinator, and comes back as events.
func TestALongJobsEventsComeHomeWithoutTravellingAsAValue(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	// A process boundary on purpose: in process the two halves share an engine,
	// and it is the crossing that this is about.
	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 1})
	ctx := c.Bind(t.Context())

	const steps = 50_000
	art, err := simulate(ctx, steps)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if art.Zero() {
		t.Fatal("no artifact came back")
	}
	if art.Count != steps {
		t.Fatalf("the handle says %d events, want %d", art.Count, steps)
	}
	// The handle is small however big the log is. That is the whole trick.
	if art.Size < 100_000 {
		t.Fatalf("the log is only %d bytes; this test meant to exceed one chunk", art.Size)
	}
	if art.Chunks < 2 {
		t.Fatalf("the log arrived in %d chunks; it was meant to be streamed", art.Chunks)
	}

	seen := 0
	for ev, err := range Replay[Tick](ctx, art) {
		if err != nil {
			t.Fatalf("replay at event %d: %v", seen, err)
		}
		if ev.At != seen {
			t.Fatalf("event %d says it is %d; the order must be the order they were recorded", seen, ev.At)
		}
		if ev.What != fmt.Sprintf("step %d", seen) {
			t.Fatalf("event %d is %q", seen, ev.What)
		}
		seen++
	}
	if seen != steps {
		t.Fatalf("replayed %d events, want %d", seen, steps)
	}
}

// The same storage, written as bytes. A worker that produced this much as a
// return value would kill its own connection.
func TestBulkBytesCrossAProcessBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 1})
	ctx := c.Bind(t.Context())

	const size = 6 << 20
	art, err := render(ctx, size)
	if err != nil {
		t.Fatalf("render: %v", err)
	}

	r, err := Open(ctx, art)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	n, err := io.Copy(io.Discard, r)
	if err != nil {
		t.Fatalf("reading it back: %v", err)
	}
	if n != art.Size {
		t.Fatalf("read %d bytes, want the %d the handle claims", n, art.Size)
	}
	if n < size {
		t.Fatalf("read %d bytes, want at least %d", n, size)
	}
}

// Reading a log of events as a file, or the other way round, is a mistake worth
// naming rather than a stream of nonsense.
func TestReadingAnArtifactTheWrongWayIsAnError(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	art, err := simulate(ctx, 4)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if _, err := Open(ctx, art); err == nil {
		t.Fatal("want an error from opening an event log as bytes")
	} else if !strings.Contains(err.Error(), "holds events") {
		t.Fatalf("got %v", err)
	}
}

// Bulk output lives on the coordinator's disk until somebody says it can go,
// and a run that never says so fills one.
func TestDiscardRemovesAnArtifact(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	art, err := simulate(ctx, 16)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	for _, err := range Replay[Tick](ctx, art) {
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
	}

	if err := Discard(ctx, art); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	if ok, err := c.shared.StreamExists(t.Context(), artifactCopyOf(art.ID)); err != nil {
		t.Fatalf("StreamExists: %v", err)
	} else if ok {
		t.Fatal("the artifact's storage is still there after it was discarded")
	}

	// Discarding twice is not an error: a caller that cleans up in a defer and
	// again on the happy path should not have to care which ran.
	if err := Discard(ctx, art); err != nil {
		t.Fatalf("second Discard: %v", err)
	}
}

// Outside a work function there is no job for an artifact to belong to.
func TestCreatingAnArtifactOutsideAJobIsAnError(t *testing.T) {
	if _, err := Create(context.Background(), "x"); err == nil {
		t.Fatal("want an error from Create with no job")
	}
	if _, err := Record[Tick](context.Background(), "x"); err == nil {
		t.Fatal("want an error from Record with no job")
	}
}

// Replay hands events out one at a time, so a reader that has seen enough stops
// reading. A resume that wants the first ten minutes of a two-hour log should
// not pay for the other hundred and ten.
func TestReplayStopsWhenTheReaderDoes(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	art, err := simulate(ctx, 20_000)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if art.Chunks < 3 {
		t.Fatalf("the log is %d chunks; this test needs several", art.Chunks)
	}

	seen := 0
	for ev, err := range Replay[Tick](ctx, art) {
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		if ev.At != seen {
			t.Fatalf("event %d says it is %d", seen, ev.At)
		}
		seen++
		if seen == 5 {
			break
		}
	}
	if seen != 5 {
		t.Fatalf("read %d events after breaking at 5", seen)
	}

	// And the log is still whole afterwards: stopping is not consuming.
	again := 0
	for range Replay[Tick](ctx, art) {
		again++
	}
	if again != 20_000 {
		t.Fatalf("a second pass read %d events, want 20000", again)
	}
}

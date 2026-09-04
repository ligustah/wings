package wings

import (
	"bytes"
	"context"
	"io"
	"testing"
)

// render writes bytes rather than events, which is the other, entirely separate
// half of what a job can leave behind.
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

// A worker that produced this much as a return value would kill its own
// connection.
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
	if art.Chunks < 2 {
		t.Fatalf("the file arrived in %d chunks; it was meant to be streamed", art.Chunks)
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

// A file lives on the coordinator's disk until somebody says it can go, and a
// run that never says so fills one.
func TestDiscardRemovesAnArtifact(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	art, err := render(ctx, 4096)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	r, err := Open(ctx, art)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatalf("reading it back: %v", err)
	}
	r.Close()

	if err := art.Discard(ctx); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	if ok, err := c.shared.StreamExists(t.Context(), art.ID); err != nil {
		t.Fatalf("StreamExists: %v", err)
	} else if ok {
		t.Fatal("the artifact's storage is still there after it was discarded")
	}

	// Discarding twice is not an error: a caller that cleans up in a defer and
	// again on the happy path should not have to care which ran.
	if err := art.Discard(ctx); err != nil {
		t.Fatalf("second Discard: %v", err)
	}
}

// Outside a work function there is no job for an artifact to belong to.
func TestCreatingAnArtifactOutsideAJobIsAnError(t *testing.T) {
	if _, err := Create(context.Background(), "x"); err == nil {
		t.Fatal("want an error from Create with no job")
	}
}

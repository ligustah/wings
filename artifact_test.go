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

// THE POINT: a handle promises the whole file, and the file is still being
// copied off the worker when the handle arrives. A Stop that cancelled the
// copy first, then destroyed the worker, left a persistent Dir holding the
// front of a file — and a caller who reopened it later read to the end of what
// had arrived and then waited for the rest of a machine that was gone.
func TestStopKeepsWhatWasStillBeingCopied(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	dir := t.TempDir()
	c, err := Start(t.Context(), Config{Target: LocalProcess(), Workers: 1, Concurrency: 1, Dir: dir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Large, so the copy is still behind the result when Stop is called.
	art, err := render(c.Bind(t.Context()), 48<<20)
	if err != nil {
		t.Fatalf("render: %v", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// The worker is gone. Whatever the coordinator kept is all there is.
	again, err := Start(t.Context(), Config{Target: InProcess(), Dir: dir})
	if err != nil {
		t.Fatalf("Start again: %v", err)
	}
	defer again.Stop(context.Background())

	r, err := Open(again.Bind(t.Context()), art)
	if err != nil {
		t.Fatalf("Open after Stop: %v", err)
	}
	defer r.Close()
	n, err := io.Copy(io.Discard, r)
	if err != nil {
		t.Fatalf("reading it back after Stop: %v", err)
	}
	if n != art.Size {
		t.Fatalf("read %d bytes of the %d the handle promised; Stop let the worker go before its output was copied", n, art.Size)
	}
}

// renderOnce hands the whole file to Write in a single call, which an encoder
// that builds its output in memory and writes it at the end does.
var renderOnce = Define("test.render-once", func(ctx context.Context, size int) (Artifact, error) {
	out, err := Create(ctx, "render")
	if err != nil {
		return Artifact{}, err
	}
	if _, err := out.Write(pattern(size)); err != nil {
		return Artifact{}, err
	}
	if err := out.Close(); err != nil {
		return Artifact{}, err
	}
	return out.Artifact(), nil
})

// pattern is size bytes that a wrong chunk boundary would visibly scramble.
func pattern(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i*7 + i/1000)
	}
	return b
}

// THE POINT: a single Write larger than a chunk used to be copied whole into
// the buffer before being cut up, so "one chunk in memory" was true only of
// callers who wrote in small pieces. And a reader taking small reads used to
// shift the rest of a chunk on every call. Neither shows in a result; what
// shows is whether the bytes come back in order across every boundary.
func TestOneWriteLargerThanAChunkArrivesWholeAndInOrder(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	// Several chunks and a partial one, in one call.
	const size = 3*artifactChunk + 4097
	art, err := renderOnce(ctx, size)
	if err != nil {
		t.Fatalf("renderOnce: %v", err)
	}
	if art.Size != size || art.Chunks != 4 {
		t.Fatalf("handle says %d bytes in %d chunks; want %d in 4", art.Size, art.Chunks, size)
	}

	r, err := Open(ctx, art)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	// Read back in pieces that never line up with a chunk.
	var got bytes.Buffer
	if _, err := io.CopyBuffer(&got, struct{ io.Reader }{r}, make([]byte, 1000)); err != nil {
		t.Fatalf("reading it back: %v", err)
	}
	if !bytes.Equal(got.Bytes(), pattern(size)) {
		t.Fatalf("the bytes came back different from the ones written (%d of %d)", got.Len(), size)
	}
}

// Outside a work function there is no job for an artifact to belong to.
func TestCreatingAnArtifactOutsideAJobIsAnError(t *testing.T) {
	if _, err := Create(context.Background(), "x"); err == nil {
		t.Fatal("want an error from Create with no job")
	}
}

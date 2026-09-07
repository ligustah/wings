package flow_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/ligustah/wings/flow"
)

// pattern is size bytes that a wrong chunk boundary would visibly scramble.
func pattern(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i*7 + i/1000)
	}
	return b
}

// A byte stream is a channel underneath, so it has to carry bytes from one
// thread to another before anything else about it matters. This is what a job
// that once produced a file does now: it streams the bytes over a channel and
// the reader drains them.
func TestAByteStreamCarriesBytesBetweenThreads(t *testing.T) {
	const size = 3*flow.ByteChunk + 4097
	var got bytes.Buffer

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewBufferedChannel[flow.Bytes](4)

		producer := ctx.Spawn(func(ctx flow.Context) (flow.None, error) {
			w := flow.NewByteWriter(ctx, ch)
			if _, err := w.Write(pattern(size)); err != nil {
				return flow.None{}, err
			}
			return flow.None{}, w.Close()
		})

		// Read back in pieces that never line up with a chunk boundary.
		r := flow.NewByteReader(ctx, ch)
		if _, err := io.CopyBuffer(&got, struct{ io.Reader }{r}, make([]byte, 1000)); err != nil {
			return err
		}
		_, err := producer.Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !bytes.Equal(got.Bytes(), pattern(size)) {
		t.Fatalf("the bytes came back different from the ones written (%d of %d)", got.Len(), size)
	}
}

// THE POINT: a large write is split into ByteChunk values rather than sent as
// one enormous record, so neither the history nor a mirror ever has to carry the
// whole thing at once. What shows is the number of values on the channel.
func TestALargeWriteIsSplitIntoChunks(t *testing.T) {
	const size = 3*flow.ByteChunk + 1 // three full chunks and a byte
	var sends int

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewBufferedChannel[flow.Bytes](8)

		producer := ctx.Spawn(func(ctx flow.Context) (flow.None, error) {
			w := flow.NewByteWriter(ctx, ch)
			// Hand it the whole thing in one Write, as an encoder that builds its
			// output in memory would.
			if _, err := w.Write(pattern(size)); err != nil {
				return flow.None{}, err
			}
			return flow.None{}, w.Close()
		})

		for {
			_, ok, err := ch.Recv(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			sends++
		}
		_, err := producer.Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if sends != 4 {
		t.Fatalf("the stream arrived in %d values; want 4 (three full chunks and a partial)", sends)
	}
}

// An empty stream — a writer closed without a write — reads back as no bytes and
// a clean EOF, not a hang waiting for a chunk that never comes.
func TestAnEmptyByteStreamReadsCleanly(t *testing.T) {
	var got bytes.Buffer

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewBufferedChannel[flow.Bytes](1)

		producer := ctx.Spawn(func(ctx flow.Context) (flow.None, error) {
			w := flow.NewByteWriter(ctx, ch)
			return flow.None{}, w.Close()
		})

		r := flow.NewByteReader(ctx, ch)
		if _, err := io.Copy(&got, r); err != nil {
			return err
		}
		_, err := producer.Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got.Len() != 0 {
		t.Fatalf("read %d bytes from an empty stream, want 0", got.Len())
	}
}

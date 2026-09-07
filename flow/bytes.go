package flow

import (
	"errors"
	"io"
)

// Bytes is a chunk of raw bytes carried on a channel.
//
// A Channel[Bytes] is how a run streams opaque output — a render, an archive, a
// core dump — from one thread to another: its values are recorded and relayed
// like any channel's, so a moved thread replays the same bytes in the same
// order, and everything the sending attempt writes is committed with it. A
// Bytes rides the wire as its own bytes rather than the base64 a []byte would
// cost in JSON — the codec waterfall takes a BinaryMarshaler ahead of JSON — so
// a stream of them is the size of the data and not a third more.
//
// Pair it with a [ByteWriter] and a [ByteReader] to treat the channel as an
// ordinary io.Writer and io.Reader. This is what a job that once returned a
// file does now: it streams the bytes over a channel the workflow handed it,
// and the workflow reads them wherever it runs.
type Bytes []byte

// MarshalBinary puts a Bytes on the wire verbatim.
func (b Bytes) MarshalBinary() ([]byte, error) { return b, nil }

// UnmarshalBinary reads one back, copying so the value does not alias the
// decoder's buffer.
func (b *Bytes) UnmarshalBinary(p []byte) error {
	*b = append((*b)[:0], p...)
	return nil
}

// ByteChunk is the most a [ByteWriter] puts in one value, and so the largest
// record a byte stream makes.
//
// A stream of bytes has no natural unit the way a log of events does, so one is
// picked: large enough that a hundred megabytes is a few hundred values, small
// enough that neither end holds much at a time.
const ByteChunk = 256 << 10

// ByteWriter streams bytes onto a channel. It is an [io.WriteCloser].
//
// Each ByteChunk that fills becomes one value sent on the channel — recorded in
// the run's history and committed with the sending attempt, like any send — and
// [ByteWriter.Close] sends what is left and closes the channel, which is what
// tells a [ByteReader] the stream has ended. Writes are buffered and sent in
// chunks, so a job that produces a hundred megabytes never holds a hundred
// megabytes: what is in memory at any moment is one chunk, however large the
// slice handed to Write.
//
// Give it a buffered channel ([Context.NewBufferedChannel]): on an unbuffered
// one every chunk waits for the reader to take it, a round trip apiece.
type ByteWriter struct {
	ctx Context
	ch  *Channel[Bytes]

	buf    []byte
	closed bool
	err    error
}

// NewByteWriter returns a ByteWriter that sends onto ch.
func NewByteWriter(ctx Context, ch *Channel[Bytes]) *ByteWriter {
	return &ByteWriter{ctx: ctx, ch: ch}
}

// Write buffers p, sending whole chunks as they fill.
//
// A slice at least a chunk long is sent straight from the caller's memory rather
// than copied into the buffer first: [Channel.Send] encodes the value before it
// returns and the channel keeps its own copy, so the caller's slice is free to
// reuse the moment Write does.
func (w *ByteWriter) Write(p []byte) (int, error) {
	if w.closed {
		return 0, errors.New("flow: write to a closed byte stream")
	}
	if w.err != nil {
		return 0, w.err
	}
	n := 0
	for len(p) > 0 {
		if len(w.buf) == 0 && len(p) >= ByteChunk {
			if err := w.send(p[:ByteChunk]); err != nil {
				return n, err
			}
			p, n = p[ByteChunk:], n+ByteChunk
			continue
		}
		take := min(ByteChunk-len(w.buf), len(p))
		w.buf = append(w.buf, p[:take]...)
		p, n = p[take:], n+take
		if len(w.buf) == ByteChunk {
			if err := w.send(w.buf); err != nil {
				return n, err
			}
			w.buf = w.buf[:0]
		}
	}
	return n, nil
}

func (w *ByteWriter) send(b []byte) error {
	if err := w.ch.Send(w.ctx, Bytes(b)); err != nil {
		w.err = err
		return err
	}
	return nil
}

// Close sends what is buffered and closes the channel.
//
// Safe to call twice: the second call returns what the first did, so a deferred
// Close beside an explicit one does not close the channel again.
func (w *ByteWriter) Close() error {
	if w.closed {
		return w.err
	}
	w.closed = true
	if w.err != nil {
		return w.err
	}
	if len(w.buf) > 0 {
		if err := w.send(w.buf); err != nil {
			return err
		}
		w.buf = nil
	}
	if err := w.ch.Close(w.ctx); err != nil {
		w.err = err
		return err
	}
	return nil
}

// ByteReader streams bytes off a channel. It is an [io.Reader], and a no-op
// Closer so it drops in wherever an io.ReadCloser is wanted.
//
// Read yields the bytes the channel's values carry, in order, and returns
// [io.EOF] once the channel is closed and every value has been drained — exactly
// when the [ByteWriter] on the other end has closed and its last chunk has
// arrived.
type ByteReader struct {
	ctx  Context
	ch   *Channel[Bytes]
	buf  Bytes
	pos  int
	done bool
}

// NewByteReader returns a ByteReader that receives from ch.
func NewByteReader(ctx Context, ch *Channel[Bytes]) *ByteReader {
	return &ByteReader{ctx: ctx, ch: ch}
}

// Read fills p from the current chunk, taking the next value off the channel
// when it runs out.
func (r *ByteReader) Read(p []byte) (int, error) {
	for r.pos >= len(r.buf) {
		if r.done {
			return 0, io.EOF
		}
		v, ok, err := r.ch.Recv(r.ctx)
		if err != nil {
			return 0, err
		}
		if !ok {
			r.done = true
			return 0, io.EOF
		}
		r.buf, r.pos = v, 0
	}
	n := copy(p, r.buf[r.pos:])
	r.pos += n
	return n, nil
}

// Close does nothing; a ByteReader owns no resource of its own. It is here so a
// ByteReader satisfies io.ReadCloser.
func (r *ByteReader) Close() error { return nil }

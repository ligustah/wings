package flow

import (
	"errors"
	"io"
)

// Bytes is a chunk of raw bytes carried on a channel. It marshals as its own
// bytes rather than the base64 a []byte costs in JSON.
type Bytes []byte

func (b Bytes) MarshalBinary() ([]byte, error) { return b, nil }

func (b *Bytes) UnmarshalBinary(p []byte) error {
	*b = append((*b)[:0], p...)
	return nil
}

// ByteChunk is the most a [ByteWriter] puts in one value.
const ByteChunk = 256 << 10

// ByteWriter streams bytes onto a channel as an [io.WriteCloser], one [ByteChunk]
// value per full chunk. Close sends the remainder and closes the channel. Use a
// buffered channel.
type ByteWriter struct {
	ctx Context
	ch  *Channel[Bytes]

	buf    []byte
	closed bool
	err    error
}

func NewByteWriter(ctx Context, ch *Channel[Bytes]) *ByteWriter {
	return &ByteWriter{ctx: ctx, ch: ch}
}

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

// Close sends what is buffered and closes the channel. Safe to call twice.
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

// ByteReader streams bytes off a channel as an [io.Reader], returning [io.EOF]
// once the channel is closed and drained. Its Close is a no-op.
type ByteReader struct {
	ctx  Context
	ch   *Channel[Bytes]
	buf  Bytes
	pos  int
	done bool
}

func NewByteReader(ctx Context, ch *Channel[Bytes]) *ByteReader {
	return &ByteReader{ctx: ctx, ch: ch}
}

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

func (r *ByteReader) Close() error { return nil }

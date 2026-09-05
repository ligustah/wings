package wings

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/ligustah/durable_streams/dsclient"
)

// artifactChunk is how much of a file rides in one record.
//
// A file has no natural unit the way a log of events does, so one is picked:
// large enough that a hundred megabytes is a few hundred records, small enough
// that neither end holds much at a time. It is the biggest record wings makes,
// and artifactBatch is what keeps a batch of them a sane message.
const artifactChunk = 256 << 10

// artifactBatch is how many chunks are asked for at once.
const artifactBatch = 8

// fileChunk is one slice of a file.
//
// It marshals to exactly its bytes — the encoding waterfall takes a
// BinaryMarshaler ahead of JSON — so a megabyte of video costs a megabyte on the
// stream rather than a base64 third more.
type fileChunk []byte

func (c fileChunk) MarshalBinary() ([]byte, error) { return c, nil }

func (c *fileChunk) UnmarshalBinary(b []byte) error {
	*c = append((*c)[:0], b...)
	return nil
}

// Artifact names a file a job produced. It is a handle, not the bytes: return it
// from a work function and open it on the coordinator.
//
// Small on purpose. It travels inside an ordinary result, which is one record on
// one stream and is held whole in memory at both ends, so anything that could be
// megabytes must not be in here.
type Artifact struct {
	// Name is what the work function called it.
	Name string `json:"name"`
	// ID identifies the file, on the worker that wrote it and on the
	// coordinator that kept it alike.
	ID string `json:"id"`
	// Attempt is which dispatch of the job wrote it, counting from zero.
	Attempt int `json:"attempt,omitempty"`
	// Size is how many bytes were written and Chunks how many records they
	// arrived in, which is how a reader knows it has all of them.
	Size   int64 `json:"size"`
	Chunks int   `json:"chunks"`
}

// Zero reports whether a names nothing, which is what a job that produced no
// file returns.
func (a Artifact) Zero() bool { return a.ID == "" }

// Create opens a file for this job to write.
//
// A result is one record on one stream, held whole in memory at both ends. That
// is the wrong shape for output measured in megabytes — a render, an archive, a
// core dump — and past a few of them the transport will not carry it at all.
// Write it here instead: it leaves the worker as it is produced, lands on the
// coordinator's own storage, and outlives the machine that made it.
//
//	out, err := wings.Create(ctx, "render")
//	if err != nil {
//	    return Result{}, err
//	}
//	if err := encode(ctx, out); err != nil {     // an ordinary io.Writer
//	    return Result{}, err
//	}
//	if err := out.Close(); err != nil {
//	    return Result{}, err
//	}
//	return Result{Video: out.Artifact()}, nil    // a handle, not the bytes
//
// What the job writes is opaque: bytes, in whatever format the job and its
// caller agree on. wings stores them and gives them back, and never reads them.
//
// For output that is a sequence of typed events rather than a file — and that a
// retry should be able to pick up from — use [Record]. That is a separate
// feature with its own storage and its own lifetime, and the two have nothing to
// do with each other.
func Create(ctx context.Context, name string) (*Output, error) {
	st := beatFrom(ctx)
	if st == nil {
		return nil, errors.New("wings: Create was called outside a work function; " +
			"an artifact belongs to a job, and there is no job here")
	}
	client, id, err := jobOutput(ctx, artifactPrefix, name)
	if err != nil {
		return nil, err
	}
	stream, err := client.OpenStream[fileChunk](id)
	if err != nil {
		return nil, fmt.Errorf("wings: open %s: %w", id, err)
	}
	return &Output{stream: stream, name: name, id: id, attempt: st.attempt}, nil
}

// Output is where a job writes a file. It is an [io.WriteCloser].
//
// Writes are buffered and sent in chunks, so a job that produces a hundred
// megabytes never holds a hundred megabytes: what is in memory at any moment is
// one chunk, however large the slice handed to Write.
type Output struct {
	stream  *dsclient.Stream[fileChunk]
	name    string
	id      string
	attempt int

	buf    []byte
	chunks int
	size   int64
	closed bool
	err    error
}

// Write buffers p, sending whole chunks as they fill.
//
// A slice at least a chunk long is sent straight from the caller's memory
// rather than copied into the buffer first, so a single large Write costs one
// chunk of buffer and not a second copy of the whole thing.
func (o *Output) Write(p []byte) (int, error) {
	if o.closed {
		return 0, fmt.Errorf("wings: artifact %q is closed", o.name)
	}
	if o.err != nil {
		return 0, o.err
	}
	n := 0
	for len(p) > 0 {
		if len(o.buf) == 0 && len(p) >= artifactChunk {
			if err := o.send(p[:artifactChunk]); err != nil {
				return n, err
			}
			p, n = p[artifactChunk:], n+artifactChunk
			o.size += artifactChunk
			continue
		}
		take := min(artifactChunk-len(o.buf), len(p))
		o.buf = append(o.buf, p[:take]...)
		p, n = p[take:], n+take
		o.size += int64(take)
		if len(o.buf) == artifactChunk {
			if err := o.send(o.buf); err != nil {
				return n, err
			}
			o.buf = o.buf[:0]
		}
	}
	return n, nil
}

// Close sends what is left.
//
// Safe to call twice: the second call returns what the first did, so a deferred
// Close beside an explicit one is not a second file.
func (o *Output) Close() error {
	if o.closed {
		return o.err
	}
	o.closed = true
	if o.err != nil {
		return o.err
	}
	if len(o.buf) > 0 {
		if err := o.send(o.buf); err != nil {
			return err
		}
	}
	o.buf = nil
	return nil
}

func (o *Output) send(data []byte) error {
	// The stream stores what it is given verbatim, so the slice must not be the
	// buffer about to be reused, nor the caller's, which Write's contract lets
	// them overwrite the moment it returns. One chunk's worth, and the only copy.
	chunk := append(fileChunk(nil), data...)
	if _, err := o.stream.Append(context.Background(), []fileChunk{chunk}); err != nil {
		o.err = fmt.Errorf("wings: write artifact %q: %w", o.name, err)
		return o.err
	}
	o.chunks++
	return nil
}

// Artifact is the handle to what was written. Valid once Close has returned.
func (o *Output) Artifact() Artifact {
	return Artifact{Name: o.name, ID: o.id, Attempt: o.attempt, Size: o.size, Chunks: o.chunks}
}

// Open reads an artifact back.
//
// ctx must be bound to the cluster that produced it. The bytes are the
// coordinator's own copy, so this works after the worker that wrote them has
// been destroyed — which for a cloud target it usually has been.
func Open(ctx context.Context, a Artifact) (io.ReadCloser, error) {
	if a.Zero() {
		return nil, fmt.Errorf("wings: artifact %q is empty", a.Name)
	}
	client, err := streamsFor(ctx)
	if err != nil {
		return nil, err
	}
	if err := awaitStream(ctx, client, a.ID, a.Chunks > 0); err != nil {
		return nil, err
	}
	stream, err := client.OpenStream[fileChunk](a.ID)
	if err != nil {
		return nil, fmt.Errorf("wings: open %s: %w", a.ID, err)
	}
	return &artifactReader{ctx: ctx, stream: stream, want: a.Chunks, id: a.ID}, nil
}

// Discard deletes an artifact.
//
// A file lives on the coordinator's own storage until somebody says it can go,
// because only the caller knows when it has been read. A run that produces one
// per job and never discards them fills a disk.
//
// Discarding something already gone is not an error.
func (a Artifact) Discard(ctx context.Context) error {
	if a.Zero() {
		return nil
	}
	client, err := streamsFor(ctx)
	if err != nil {
		return err
	}
	return dropStream(ctx, client, a.ID)
}

// artifactReader turns the records back into a byte stream, waiting for the
// chunks the handle says are coming.
type artifactReader struct {
	ctx    context.Context
	stream *dsclient.Stream[fileChunk]
	id     string
	want   int

	from int64
	read int
	// queue is what has been read and not yet handed out, and pos is how far
	// into the first of them the reader has got. A position rather than a
	// buffer that is shifted on every Read: a caller reading a quarter-megabyte
	// chunk a kilobyte at a time would otherwise move the rest of it two
	// hundred and fifty times.
	queue []fileChunk
	pos   int
}

func (r *artifactReader) Read(p []byte) (int, error) {
	for len(r.queue) == 0 {
		if r.read >= r.want {
			return 0, io.EOF
		}
		// Blocking, because the last chunks of a large file may still be behind
		// the result that named it: they travel separately, so the handle can
		// arrive first. A reader that stopped anyway would return a short file
		// and no error, which is the worst way to lose data.
		deadline, cancel := context.WithTimeout(r.ctx, outputWait)
		recs, err := r.stream.ReadBlocking(deadline, r.from, artifactBatch)
		cancel()
		if err != nil {
			if r.ctx.Err() == nil {
				return 0, fmt.Errorf("wings: artifact %s has %d of its %d chunks: %w",
					r.id, r.read, r.want, err)
			}
			return 0, fmt.Errorf("wings: read %s: %w", r.id, err)
		}
		if len(recs) == 0 {
			return 0, io.ErrUnexpectedEOF
		}
		for _, rec := range recs {
			r.from = rec.Offset + 1
			r.read++
			if len(rec.Record) > 0 {
				r.queue = append(r.queue, rec.Record)
			}
		}
	}
	n := copy(p, r.queue[0][r.pos:])
	r.pos += n
	if r.pos == len(r.queue[0]) {
		r.queue[0] = nil
		r.queue = r.queue[1:]
		r.pos = 0
	}
	return n, nil
}

func (r *artifactReader) Close() error { return nil }

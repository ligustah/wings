package wings

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/internal/invoke"
)

// artifactStreamPrefix is where a worker puts the bulk output of its jobs, one
// stream per worker; artifactPrefix is where the coordinator keeps its own copy,
// one stream per artifact.
const (
	artifactStreamPrefix = "wings.artifacts."
	artifactPrefix       = "wings.artifact."

	// chunkTarget is how much payload one record carries.
	//
	// Small enough that a chunk is never the thing that breaks a transport,
	// large enough that a hundred megabytes is a few hundred records rather
	// than a hundred thousand — and, for events, that a stream of millions of
	// them is not a stream of millions of records.
	chunkTarget = 256 << 10

	// chunkEvents caps how many events ride in one record regardless of size,
	// so a stream of tiny events still reaches the coordinator promptly.
	chunkEvents = 4096

	// artifactWait bounds how long a reader waits for the last chunks to
	// arrive. Long enough for a slow link to finish what it started, short
	// enough to be an error rather than a hang.
	artifactWait = 2 * time.Minute
)

// Artifact kinds.
const (
	artifactBytes  = "bytes"
	artifactEvents = "events"
)

func artifactStreamFor(workerID string) string { return artifactStreamPrefix + workerID }
func artifactCopyOf(id string) string          { return artifactPrefix + id }

// Artifact names bulk output a job produced. It is a handle, not the bytes:
// return it from a work function and read it on the coordinator.
//
// Small on purpose. It travels inside an ordinary result, which is one record on
// one stream and is held whole in memory at both ends, so anything that could be
// megabytes must not be in here.
type Artifact struct {
	// Name is what the work function called it.
	Name string `json:"name"`
	// ID is the artifact's own identity, unique to the attempt that wrote it.
	ID string `json:"id"`
	// Kind says how it was written, so reading it the other way is an error
	// rather than nonsense.
	Kind string `json:"kind"`
	// Size is how many bytes of payload were written and Chunks how many
	// records they arrived in — which is how a reader knows it has all of it.
	Size   int64 `json:"size"`
	Chunks int   `json:"chunks"`
	// Count is how many events it holds, for one written with [Record].
	Count int64 `json:"count,omitempty"`
}

// Zero reports whether a names nothing, which is what a job that produced no
// output returns.
func (a Artifact) Zero() bool { return a.ID == "" }

// artifactChunk is one record of an artifact.
//
// The same record carries both kinds. For bytes it is a slice of the stream; for
// events it is several encoded events, each behind a four-byte length. Packing
// events rather than giving each its own record is what keeps a log of millions
// of them from being a stream of millions of records.
type artifactChunk struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Job   string `json:"job"`
	Seq   int    `json:"seq"`
	Data  []byte `json:"data,omitempty"`
	Count int    `json:"count,omitempty"`
	// Final marks the last chunk, so a reader knows it has the end even before
	// the result carrying the handle arrives.
	Final bool `json:"final,omitempty"`
}

// Create opens somewhere for this job to put bulk output.
//
// This is the way out of durable execution for a job's own internal state. A
// render that produces a video, a scan that produces a log, anything whose
// output is measured in megabytes — none of it belongs in a result value. Write
// it here instead: it leaves the worker in pieces as it is produced, lands on
// the coordinator's own durable streams, and outlives the machine that made it.
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
// What the job writes is opaque to wings: bytes, in whatever format the job and
// its caller agree on. wings does not read it, record it, replay it or have an
// opinion about it — it stores it and gives it back. For a job whose output is a
// sequence of typed events rather than a byte stream, [Record] is the same
// mechanism with the framing done for you.
//
// One name per job. Nothing is resumed from: a job moved to another worker
// starts a fresh artifact, and only the attempt that finished has a handle
// anybody holds.
func Create(ctx context.Context, name string) (*Output, error) {
	sink, id, err := openArtifact(ctx, name)
	if err != nil {
		return nil, err
	}
	return &Output{w: writer{id: id, name: name, job: sink.job, sink: sink.sink, kind: artifactBytes}}, nil
}

// Output is where a job writes bulk output. It is an [io.WriteCloser].
//
// Writes are buffered and sent in chunks, so a job that produces a hundred
// megabytes never holds a hundred megabytes: what is in memory at any moment is
// one chunk.
type Output struct{ w writer }

// Write buffers p, sending whole chunks as they fill.
func (o *Output) Write(p []byte) (int, error) { return o.w.write(p) }

// Close sends what is left.
//
// Safe to call twice: the second call returns what the first did, so a deferred
// Close beside an explicit one is not a second artifact.
func (o *Output) Close() error { return o.w.close() }

// Artifact is the handle to what was written. Valid once Close has returned.
func (o *Output) Artifact() Artifact { return o.w.artifact() }

// Record opens a typed event log for this job.
//
// The same storage as [Create] with the framing and the codec done for you, and
// the shape most long jobs actually want: a simulation, a solver, a crawl —
// anything that produces a sequence of typed things as it runs rather than one
// answer at the end.
//
//	rec, err := wings.Record[Event](ctx, "replay")
//	if err != nil {
//	    return Result{}, err
//	}
//	for step := range simulation(ctx) {
//	    if err := rec.Record(step.Event()); err != nil {
//	        return Result{}, err
//	    }
//	}
//	if err := rec.Close(); err != nil {
//	    return Result{}, err
//	}
//	return Result{Replay: rec.Artifact()}, nil
//
// Read it back with [Replay], which hands the events out one at a time in the
// order they were recorded. Neither end holds the whole log: events are packed
// into chunks on the way out and unpacked on the way in.
//
// Events are not part of the job's durable execution and are never replayed into
// it. This is the job's own record of what it did, kept for whoever asked for
// the job — which is exactly why it is separate from [Step] and [Heartbeat],
// whose business is resuming the job rather than describing it.
func Record[E any](ctx context.Context, name string) (*Recorder[E], error) {
	sink, id, err := openArtifact(ctx, name)
	if err != nil {
		return nil, err
	}
	return &Recorder[E]{
		w:     writer{id: id, name: name, job: sink.job, sink: sink.sink, kind: artifactEvents},
		codec: dswire.ReflectCodec[E]{New: allocator[E]()},
	}, nil
}

// Recorder is a job's typed event log. Safe for one goroutine at a time.
type Recorder[E any] struct {
	w     writer
	codec dswire.Codec[E]
}

// Record appends one event.
//
// Buffered: events accumulate until there are enough to be worth a message, so
// this is cheap enough to call in the inner loop of whatever is producing them.
func (r *Recorder[E]) Record(e E) error {
	b, err := dswire.EncodeRecord(r.codec, e)
	if err != nil {
		return fmt.Errorf("wings: encode event for %q: %w", r.w.name, err)
	}
	var head [4]byte
	binary.BigEndian.PutUint32(head[:], uint32(len(b)))
	return r.w.event(head[:], b)
}

// Close sends what is left.
func (r *Recorder[E]) Close() error { return r.w.close() }

// Artifact is the handle to the log. Valid once Close has returned.
func (r *Recorder[E]) Artifact() Artifact { return r.w.artifact() }

// openArtifact resolves the job and its sink, and mints an identity.
func openArtifact(ctx context.Context, name string) (struct {
	job  string
	sink artifactSink
}, string, error) {
	var out struct {
		job  string
		sink artifactSink
	}
	if name == "" {
		return out, "", errors.New("wings: an artifact needs a name")
	}
	st := beatFrom(ctx)
	if st == nil {
		return out, "", errors.New("wings: this was called outside a work function; " +
			"an artifact belongs to a job, and there is no job here")
	}
	sink, ok := st.sink.(artifactSink)
	if !ok || sink == nil {
		return out, "", errors.New("wings: this worker cannot store artifacts")
	}
	id, err := sink.openArtifact(st.job, name)
	if err != nil {
		return out, "", err
	}
	out.job, out.sink = st.job, sink
	return out, id, nil
}

// writer is the chunking shared by both kinds.
type writer struct {
	id   string
	name string
	job  string
	kind string
	sink artifactSink

	mu     sync.Mutex
	buf    []byte
	held   int // events buffered but not yet sent
	seq    int
	size   int64
	count  int64
	closed bool
	err    error
}

func (w *writer) write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.usable(); err != nil {
		return 0, err
	}
	w.buf = append(w.buf, p...)
	w.size += int64(len(p))
	for len(w.buf) >= chunkTarget {
		if err := w.flush(chunkTarget, 0, false); err != nil {
			w.err = err
			return 0, err
		}
	}
	return len(p), nil
}

func (w *writer) event(head, body []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.usable(); err != nil {
		return err
	}
	w.buf = append(append(w.buf, head...), body...)
	w.size += int64(len(head) + len(body))
	w.held++
	w.count++
	if len(w.buf) >= chunkTarget || w.held >= chunkEvents {
		if err := w.flush(len(w.buf), w.held, false); err != nil {
			w.err = err
			return err
		}
	}
	return nil
}

func (w *writer) usable() error {
	if w.closed {
		return fmt.Errorf("wings: artifact %q is closed", w.name)
	}
	return w.err
}

func (w *writer) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return w.err
	}
	w.closed = true
	if w.err != nil {
		return w.err
	}
	w.err = w.flush(len(w.buf), w.held, true)
	return w.err
}

// flush sends n bytes from the front of the buffer. Call with mu held.
func (w *writer) flush(n, events int, final bool) error {
	if err := w.sink.sendChunks([]artifactChunk{{
		ID: w.id, Name: w.name, Job: w.job, Seq: w.seq,
		Data: w.buf[:n], Count: events, Final: final,
	}}); err != nil {
		return err
	}
	w.seq++
	w.held -= events
	w.buf = w.buf[:copy(w.buf, w.buf[n:])]
	return nil
}

func (w *writer) artifact() Artifact {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Artifact{
		Name: w.name, ID: w.id, Kind: w.kind,
		Size: w.size, Chunks: w.seq, Count: w.count,
	}
}

// artifactSink is a worker's side of artifact storage.
type artifactSink interface {
	openArtifact(job, name string) (string, error)
	sendChunks(chunks []artifactChunk) error
}

// Open reads a byte artifact back.
//
// ctx must be bound to the cluster that produced it. The bytes are the
// coordinator's own copy, so this works after the worker that wrote them has
// been destroyed — which for a cloud target it usually has been.
func Open(ctx context.Context, a Artifact) (io.ReadCloser, error) {
	stream, err := artifactStream(ctx, a, artifactBytes)
	if err != nil {
		return nil, err
	}
	return &artifactReader{ctx: ctx, stream: stream, want: a.Chunks}, nil
}

// Replay hands back the events of a recorded log, in the order they were
// recorded.
//
//	for ev, err := range wings.Replay[Event](ctx, played.Replay) {
//	    if err != nil {
//	        return err
//	    }
//	    ...
//	}
//
// The sequence stops at the first error, which is reported with the zero event
// — so a loop that checks err on every step cannot mistake a truncated log for
// a short one. Nothing is held whole: chunks are read as the range advances.
//
// E must be what [Record] was called with. A mismatch is a decode error rather
// than a wrong answer.
func Replay[E any](ctx context.Context, a Artifact) iter.Seq2[E, error] {
	return func(yield func(E, error) bool) {
		var zero E

		stream, err := artifactStream(ctx, a, artifactEvents)
		if err != nil {
			yield(zero, err)
			return
		}
		codec := dswire.ReflectCodec[E]{New: allocator[E]()}

		for from := int64(0); int(from) < a.Chunks; {
			recs, err := stream.Read(ctx, from, 8)
			if err != nil {
				yield(zero, fmt.Errorf("wings: read %s: %w", stream.Name(), err))
				return
			}
			if len(recs) == 0 {
				yield(zero, fmt.Errorf("wings: artifact %s ends after %d of its %d chunks",
					a.ID, from, a.Chunks))
				return
			}
			for _, rec := range recs {
				from = rec.Offset + 1
				data := rec.Record.Data
				for len(data) > 0 {
					if len(data) < 4 {
						yield(zero, fmt.Errorf("wings: artifact %s: a chunk ends mid-event", a.ID))
						return
					}
					n := int(binary.BigEndian.Uint32(data[:4]))
					if len(data) < 4+n {
						yield(zero, fmt.Errorf("wings: artifact %s: an event runs past its chunk", a.ID))
						return
					}
					e, err := dswire.DecodeRecord(codec, data[4:4+n])
					if err != nil {
						yield(zero, fmt.Errorf("wings: decode event from %s: %w", a.ID, err))
						return
					}
					if !yield(e, nil) {
						return
					}
					data = data[4+n:]
				}
			}
		}
	}
}

// Discard deletes an artifact.
//
// Bulk output lives on the coordinator's own storage until somebody says it can
// go, because only the caller knows when it has been read. Call this once the
// events have been replayed or the bytes copied somewhere they belong; a run
// that produces one of these per job and never discards them fills a disk.
//
// Artifacts from attempts that were abandoned — a job moved to another worker,
// a job the coordinator gave up on — are removed without being asked, since
// nobody holds a handle to them.
//
// Discarding something already gone is not an error.
func Discard(ctx context.Context, a Artifact) error {
	if a.Zero() {
		return nil
	}
	client, err := streamsFor(ctx)
	if err != nil {
		return err
	}
	return dropArtifact(ctx, client, a.ID)
}

func dropArtifact(ctx context.Context, client *dsclient.Client, id string) error {
	name := artifactCopyOf(id)
	ok, err := client.StreamExists(ctx, name)
	if err != nil {
		return fmt.Errorf("wings: check %s: %w", name, err)
	}
	if !ok {
		return nil
	}
	if err := client.DeleteStream(ctx, name); err != nil {
		return fmt.Errorf("wings: discard artifact %s: %w", id, err)
	}
	return nil
}

// artifactStream opens the coordinator's copy and waits for it to be complete.
//
// Waiting matters. The handle arrives with the result, and the last chunks of a
// large artifact may still be in flight behind it; a reader that started anyway
// would return a short file and no error, which is the worst way to lose data.
func artifactStream(ctx context.Context, a Artifact, kind string) (*dsclient.Stream[artifactChunk], error) {
	if a.Zero() {
		return nil, errors.New("wings: this artifact is empty")
	}
	if a.Kind != kind {
		return nil, fmt.Errorf("wings: artifact %q holds %s and is being read as %s", a.Name, a.Kind, kind)
	}
	client, err := streamsFor(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := client.OpenStream[artifactChunk](artifactCopyOf(a.ID))
	if err != nil {
		return nil, fmt.Errorf("wings: open artifact %s: %w", a.ID, err)
	}

	deadline, cancel := context.WithTimeout(ctx, artifactWait)
	defer cancel()
	for {
		info, err := stream.Info(deadline)
		if err != nil {
			return nil, fmt.Errorf("wings: artifact %s: %w", a.ID, err)
		}
		if info.Newest+1 >= int64(a.Chunks) {
			return stream, nil
		}
		select {
		case <-deadline.Done():
			return nil, fmt.Errorf("wings: artifact %s has %d of its %d chunks after %s",
				a.ID, info.Newest+1, a.Chunks, artifactWait)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// streamsFor is the cluster's own durable streams, from a bound context.
func streamsFor(ctx context.Context) (*dsclient.Client, error) {
	h := invoke.From(ctx)
	if h == nil {
		return nil, errors.New("wings: this context is not bound to a cluster; " +
			"use the context Coordinate was given, or Cluster.Bind")
	}
	client := h.Streams()
	if client == nil {
		return nil, errors.New("wings: this cluster has no durable streams")
	}
	return client, nil
}

// artifactReader turns a stream of chunks back into a byte stream.
type artifactReader struct {
	ctx    context.Context
	stream *dsclient.Stream[artifactChunk]
	want   int

	from int64
	rest []byte
	done bool
}

func (r *artifactReader) Read(p []byte) (int, error) {
	for len(r.rest) == 0 {
		if r.done || int(r.from) >= r.want {
			return 0, io.EOF
		}
		recs, err := r.stream.Read(r.ctx, r.from, 8)
		if err != nil {
			return 0, fmt.Errorf("wings: read %s: %w", r.stream.Name(), err)
		}
		if len(recs) == 0 {
			return 0, io.ErrUnexpectedEOF
		}
		for _, rec := range recs {
			r.rest = append(r.rest, rec.Record.Data...)
			r.from = rec.Offset + 1
			if rec.Record.Final {
				r.done = true
			}
		}
	}
	n := copy(p, r.rest)
	r.rest = r.rest[:copy(r.rest, r.rest[n:])]
	return n, nil
}

func (r *artifactReader) Close() error { return nil }

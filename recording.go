package wings

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"sync"

	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow"
)

// recordBatch is how many events go in one append; each is still its own record.
const recordBatch = 512

// Recording is a handle to a log of typed events a job wrote as it ran. Return
// it from a work function and replay it elsewhere with [Replay].
type Recording struct {
	// Name is what the work function called it.
	Name string `json:"name"`
	// ID identifies the log, the same on the worker that wrote it and the
	// coordinator that kept it.
	ID string `json:"id"`
	// Attempt is which dispatch of the job wrote it, from zero.
	Attempt int `json:"attempt,omitempty"`
	// Events is how many were recorded; zero means unknown (a log from an
	// attempt that died), which [Replay] reads to whatever end it has.
	Events int64 `json:"events,omitempty"`
	// Complete says the job closed the log rather than stopping mid-run; false
	// on one recovered through [Priors].
	Complete bool `json:"complete,omitempty"`
}

// Zero reports whether r names nothing.
func (r Recording) Zero() bool { return r.ID == "" }

// Record opens a log for this job to stream typed events into as it runs — for a
// simulation, solver, or crawl whose progress is a sequence of events. It is
// durability for the job's own state, separate from wings' durable execution. A
// moved job resumes from what earlier attempts wrote, via [Priors]. E must be
// the same type passed to [Replay]. Errors outside a work function.
func Record[E any](ctx context.Context, name string) (*Recorder[E], error) {
	j := jobFrom(ctx)
	if j == nil {
		return nil, errors.New("wings: Record was called outside a work function; " +
			"a recording belongs to a job, and there is no job here")
	}
	client, id, err := jobOutput(ctx, recordingPrefix, name)
	if err != nil {
		return nil, err
	}
	stream, err := eventStream[E](client, id)
	if err != nil {
		return nil, err
	}
	// The recording belongs to the thread that makes it: its writes go in that
	// thread's transaction, so a flush never tears a sibling thread's staged send,
	// and the history event that names the recording commits with it.
	_, thread, _ := flow.Self(ctx)
	if thread == "" {
		thread = flow.MainThread
	}
	outputs := j.txns.For(thread)
	r := &Recorder[E]{
		ctx:     ctx,
		stream:  stream,
		outputs: outputs,
		name:    name,
		id:      id,
		attempt: j.attempt,
	}
	r.unregister = outputs.register(r.Flush)
	return r, nil
}

// eventStream opens a recording's storage with a codec that can allocate a
// pointer event.
func eventStream[E any](client *dsclient.Client, name string) (*dsclient.Stream[E], error) {
	s, err := client.OpenStream[E](name,
		dsclient.WithCodec[E](dswire.ReflectCodec[E]{New: allocator[E]()}))
	if err != nil {
		return nil, fmt.Errorf("wings: open %s: %w", name, err)
	}
	return s, nil
}

// allocator returns a factory for E when E is a pointer type, and nil otherwise;
// dswire.ReflectCodec needs one to decode into a pointer.
func allocator[E any]() func() E {
	var zero E
	rt := reflect.TypeOf(&zero).Elem()
	if rt.Kind() != reflect.Pointer {
		return nil
	}
	return func() E {
		return reflect.New(rt.Elem()).Interface().(E)
	}
}

// Recorder is a job's event log. Safe for concurrent use, though the event order
// is then the callers'.
type Recorder[E any] struct {
	ctx     context.Context
	stream  *dsclient.Stream[E]
	outputs *attemptOutputs
	name    string
	id      string
	attempt int

	unregister func()

	mu     sync.Mutex
	batch  []E
	count  int64
	closed bool
	err    error
}

// Record appends one event. Cheap enough for an inner loop: events batch into
// one append, each still its own record.
func (r *Recorder[E]) Record(e E) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("wings: recording %q is closed", r.name)
	}
	if r.err != nil {
		return r.err
	}
	r.batch = append(r.batch, e)
	if len(r.batch) < recordBatch {
		return nil
	}
	return r.flush()
}

// Flush sends what has been recorded but not yet appended. What a job writes
// becomes readable at the attempt's next commit point regardless; this is for a
// job that wants it on its way now.
func (r *Recorder[E]) Flush() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.flush()
}

func (r *Recorder[E]) flush() error {
	if len(r.batch) == 0 {
		return r.err
	}
	if err := r.ctx.Err(); err != nil {
		r.err = fmt.Errorf("wings: record %d events to %s: %w", len(r.batch), r.name, err)
		return r.err
	}
	ctx, cancel := context.WithTimeout(r.ctx, outputAppend)
	defer cancel()
	if err := r.outputs.append(ctx, r.stream, r.batch); err != nil {
		r.err = fmt.Errorf("wings: record %d events to %s: %w", len(r.batch), r.name, err)
		return r.err
	}
	r.count += int64(len(r.batch))
	r.batch = r.batch[:0]
	return nil
}

// Close sends what is left and marks the log finished. Safe to call twice.
func (r *Recorder[E]) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.err
	}
	if err := r.flush(); err != nil {
		return err
	}
	r.closed = true
	r.unregister()
	return nil
}

// Recording returns the handle. Valid once [Recorder.Close] returns, and
// meaningful before it: it names everything flushed so far.
func (r *Recorder[E]) Recording() Recording {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Recording{
		Name:     r.name,
		ID:       r.id,
		Attempt:  r.attempt,
		Events:   r.count,
		Complete: r.closed,
	}
}

// Priors returns the logs earlier attempts of this job left behind, oldest
// first. Each is a prefix of the same work, not a continuation, so replay the
// one that got furthest (usually the last), not all of them. Empty on a first
// attempt and outside a work function.
func Priors(ctx context.Context) []Recording {
	j := jobFrom(ctx)
	if j == nil {
		return nil
	}
	return j.priors
}

// Replay yields the events of a log in recorded order, stopping at the first
// error (reported with the zero event), so a truncated log is not mistaken for a
// short one. Breaking out stops reading. Works on the coordinator and inside a
// work function. E must be what [Record] used.
func Replay[E any](ctx context.Context, rec Recording) iter.Seq2[E, error] {
	return func(yield func(E, error) bool) {
		var zero E

		if rec.Zero() {
			yield(zero, errors.New("wings: this recording names nothing"))
			return
		}
		client, err := streamsFor(ctx)
		if err != nil {
			yield(zero, err)
			return
		}
		if err := awaitStream(ctx, client, rec.ID, rec.Complete || rec.Events > 0); err != nil {
			yield(zero, err)
			return
		}
		stream, err := eventStream[E](client, rec.ID)
		if err != nil {
			yield(zero, err)
			return
		}

		// A known length means the writer finished and the last events may still
		// be behind the result that named them; wait for those. No length is a
		// dead attempt's log: what is there is all there is.
		deadline := ctx
		if rec.Events > 0 {
			var cancel context.CancelFunc
			deadline, cancel = context.WithTimeout(ctx, outputWait)
			defer cancel()
		}

		var seen int64
		for from := int64(0); ; {
			var recs []dsclient.OffsetRecord[E]
			if seen < rec.Events {
				recs, err = stream.ReadBlocking(deadline, from, recordBatch)
			} else {
				recs, err = stream.Read(ctx, from, recordBatch)
			}
			if err != nil {
				if seen < rec.Events && deadline.Err() != nil && ctx.Err() == nil {
					yield(zero, fmt.Errorf("wings: recording %s has %d of its %d events after %s",
						rec.ID, seen, rec.Events, outputWait))
					return
				}
				yield(zero, fmt.Errorf("wings: read %s: %w", rec.ID, err))
				return
			}
			if len(recs) == 0 {
				if seen < rec.Events {
					yield(zero, fmt.Errorf("wings: recording %s ends after %d of its %d events",
						rec.ID, seen, rec.Events))
				}
				return
			}
			for _, r := range recs {
				from = r.Offset + 1
				seen++
				if !yield(r.Record, nil) {
					return
				}
			}
		}
	}
}

// Discard deletes a recording; earlier attempts' logs are dropped automatically
// once the job finishes. Discarding something already gone is not an error.
func (r Recording) Discard(ctx context.Context) error {
	if r.Zero() {
		return nil
	}
	client, err := streamsFor(ctx)
	if err != nil {
		return err
	}
	return dropStream(ctx, client, r.ID)
}

package wings

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"
)

// recordBatch is how many events go in one append.
//
// Every event is still its own record — this is one round trip carrying many,
// not many events packed into one record. It exists because a simulation
// produces events far faster than a network answers, and nothing else.
const recordBatch = 512

// Recording names a log of typed events a job wrote as it ran. It is a handle,
// not the events: return it from a work function and replay it elsewhere.
//
// Small on purpose. It travels inside an ordinary result, which is one record on
// one stream, so what goes in here must stay a name and a count.
type Recording struct {
	// Name is what the work function called it.
	Name string `json:"name"`
	// ID identifies the log. It is the same on the worker that wrote it and on
	// the coordinator that kept it, so a handle means one thing everywhere.
	ID string `json:"id"`
	// Attempt is which dispatch of the job wrote it, counting from zero.
	Attempt int `json:"attempt,omitempty"`
	// Events is how many were recorded.
	//
	// Zero means unknown rather than empty — which is what a log recovered from
	// an attempt that died says, since nobody was left to count. [Replay] reads
	// such a log to whatever end it has.
	Events int64 `json:"events,omitempty"`
	// Complete says the job closed the log rather than stopping mid-run. False
	// on one recovered from an attempt that died, which is the case [Priors]
	// exists for: what it holds is a prefix of what the job was doing, and a
	// true prefix is exactly what a resume wants.
	Complete bool `json:"complete,omitempty"`
}

// Zero reports whether r names nothing, which is what a job that recorded
// nothing returns.
func (r Recording) Zero() bool { return r.ID == "" }

// Record opens a log for this job to stream typed events into as it runs.
//
// This is durability for a job's OWN state, alongside the durable execution
// wings does for the job itself and deliberately not part of it. A long
// simulation, a solver, a crawl — anything whose progress is a sequence of
// events — can write that sequence here instead of being cut into a hundred
// tiny work functions to keep any of it.
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
//	return Result{Replay: rec.Recording()}, nil
//
// One event is one record. That is the whole design: events leave the worker
// while the job is still running, land on the coordinator's own storage as they
// arrive, and come back out one at a time in the order they went in — so a
// reader takes as many as it wants and pays for no more.
//
// A job that is moved to another worker is handed what its previous attempts
// wrote, through [Priors], and can play those events back into itself to reach
// the point the last one stopped at rather than starting from nothing. That is
// why this is a stream of events and not a file: a file is opaque, and a log is
// a position.
//
// Events are encoded the way everything else in wings is — protobuf for a
// proto.Message, then binary or text marshalling, then JSON — and E must be the
// same type on both sides.
func Record[E any](ctx context.Context, name string) (*Recorder[E], error) {
	st := beatFrom(ctx)
	if st == nil {
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
	return &Recorder[E]{
		stream:  stream,
		name:    name,
		id:      id,
		attempt: st.attempt,
	}, nil
}

// eventStream opens a recording's storage with a codec that can allocate.
//
// The allocator is what makes a pointer event work, and the events worth
// recording are usually pointers — a generated protobuf type is one, and so is
// anything with an UnmarshalBinary of its own. Without it a codec has a nil
// pointer and nothing to point it at: protobuf says so and refuses, and a custom
// unmarshaller is called on a nil receiver. Every other typed boundary in wings
// passes one; this is the same one.
func eventStream[E any](client *dsclient.Client, name string) (*dsclient.Stream[E], error) {
	s, err := client.OpenStream[E](name,
		dsclient.WithCodec[E](dswire.ReflectCodec[E]{New: allocator[E]()}))
	if err != nil {
		return nil, fmt.Errorf("wings: open %s: %w", name, err)
	}
	return s, nil
}

// Recorder is a job's event log. Safe for one goroutine at a time.
type Recorder[E any] struct {
	stream  *dsclient.Stream[E]
	name    string
	id      string
	attempt int

	batch  []E
	count  int64
	closed bool
	err    error
}

// Record appends one event.
//
// Events accumulate briefly and go out in one append, so this is cheap enough
// to call in the inner loop of whatever produces them. Each one is still its own
// record; the batching is a round trip, not a container.
func (r *Recorder[E]) Record(e E) error {
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
	return r.Flush()
}

// Flush sends what has been recorded but not yet gone out.
//
// Rarely needed: [Recorder.Close] flushes, and so does a full batch. It is here
// for a job that wants what it has recorded to be readable NOW — a slow producer
// whose progress somebody is watching.
func (r *Recorder[E]) Flush() error {
	if len(r.batch) == 0 {
		return r.err
	}
	if _, err := r.stream.Append(context.Background(), r.batch); err != nil {
		r.err = fmt.Errorf("wings: record %d events to %s: %w", len(r.batch), r.name, err)
		return r.err
	}
	r.count += int64(len(r.batch))
	r.batch = r.batch[:0]
	return nil
}

// Close sends what is left and marks the log finished.
//
// Safe to call twice: the second call returns what the first did, so a deferred
// Close beside an explicit one is not a second log.
func (r *Recorder[E]) Close() error {
	if r.closed {
		return r.err
	}
	if err := r.Flush(); err != nil {
		return err
	}
	r.closed = true
	return nil
}

// Recording is the handle to the log.
//
// Valid once [Recorder.Close] has returned, and meaningful before it: a job that
// wants to say how far it has got can hand one out mid-run, and what it names is
// everything flushed so far.
func (r *Recorder[E]) Recording() Recording {
	return Recording{
		Name:     r.name,
		ID:       r.id,
		Attempt:  r.attempt,
		Events:   r.count,
		Complete: r.closed,
	}
}

// Priors returns the logs earlier attempts of this job left behind, oldest
// attempt first.
//
// This is how a moved job picks up where it stopped. Each one is a PREFIX of the
// same work, not a continuation of the one before it: attempt 0 and attempt 1
// both start from the beginning, so replaying all of them into a simulation
// would play the opening twice. Take the one that got furthest — usually the
// last — and play that.
//
//	if priors := wings.Priors(ctx); len(priors) > 0 {
//	    for ev, err := range wings.Replay[Event](ctx, priors[len(priors)-1]) {
//	        if err != nil {
//	            break // a log that stops early is expected; take what arrived
//	        }
//	        sim.Apply(ev)
//	    }
//	}
//
// None of them is [Recording.Complete] and none reports its [Recording.Events],
// because the attempt that would have said so did not survive to say it. That is
// not a fault: a truncated log is precisely a record of how far the work got, and
// [Replay] reads one to whatever end it has.
//
// Empty on a first attempt, and outside a work function.
func Priors(ctx context.Context) []Recording {
	st := beatFrom(ctx)
	if st == nil {
		return nil
	}
	return st.priors
}

// Replay hands back the events of a log, in the order they were recorded.
//
//	for ev, err := range wings.Replay[Event](ctx, played.Replay) {
//	    if err != nil {
//	        return err
//	    }
//	    sim.Apply(ev)
//	}
//
// The sequence stops at the first error, reported with the zero event — so a
// loop that checks err on every step cannot mistake a truncated log for a short
// one. Breaking out stops reading: a resume that wants the first ten minutes of
// a two-hour log does not pay for the other hundred and ten.
//
// Works on the coordinator and inside a work function alike. On a worker it
// reads through that worker's own storage, which is how a retry replays what
// [Priors] handed it.
//
// E must be what [Record] was called with. A mismatch is a decode error rather
// than a wrong answer.
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
		// Still coming when the writer finished: what it recorded is on its
		// way behind the handle that named it. A log nobody closed is all
		// there will ever be of it.
		if err := awaitStream(ctx, client, rec.ID, rec.Complete || rec.Events > 0); err != nil {
			yield(zero, err)
			return
		}
		stream, err := eventStream[E](client, rec.ID)
		if err != nil {
			yield(zero, err)
			return
		}

		// A handle that knows its length is one whose writer finished, and the
		// last of its events may still be behind the result that named it —
		// results and events travel separately, so the handle can arrive first.
		// Wait for those. A handle with no length is a dead attempt's, and
		// whatever is there is all there will ever be.
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

// Discard deletes a recording.
//
// A log lives on the coordinator's own storage until somebody says it can go,
// because only the caller knows when it has been read. The logs of a job's
// earlier attempts are dropped without being asked once the job finishes, since
// nobody holds a handle to those.
//
// Discarding something already gone is not an error.
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

// recordWait bounds how long a reader waits for events still in flight behind
// the result that named them.
const recordWait = 2 * time.Minute

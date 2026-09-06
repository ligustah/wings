package flow

import (
	"context"
	"errors"
	"fmt"

	"github.com/ligustah/durable_streams/dswire"
)

// Heartbeat reports that a call is still making progress, and records where it
// has got to.
//
// Two jobs in one call, and both matter. It is the liveness signal that
// [WithHeartbeatTimeout] measures — a call that stops beating is presumed stuck
// on its machine rather than slow, and an executor that can do so moves it to
// another. And progress is the checkpoint that makes moving it cheap: whatever
// was last passed here is handed to the next attempt, which reads it with
// [Context.Checkpoint] and carries on from there instead of starting over.
//
// Only the LATEST value survives. This is a position, not a log: the point is
// for a retry to know where to resume, and every earlier answer to that
// question is wrong.
//
// It is best-effort about delivery and deliberately so. A beat that does not
// reach the executor costs a retry that redoes a little work; a beat that
// blocked the function to guarantee delivery would cost the work itself.
//
// Cheap to call, but not free — each one is a report — so call it per unit of
// real progress rather than per loop iteration. Outside a running call it is
// an error: there is no attempt to report on.
func (c Context) Heartbeat[T any](progress T) error {
	ctx := c
	if t := threadFrom(ctx); t != nil && t.readonly {
		return nil // a replay reports nothing
	}
	st := progressFrom(ctx)
	if st == nil {
		return errors.New("flow: Heartbeat was called outside a running function; " +
			"it reports the progress of a call, and there is no call here")
	}
	b, err := dswire.EncodeRecord(dswire.ReflectCodec[T]{New: allocator[T]()}, progress)
	if err != nil {
		return fmt.Errorf("flow: encode heartbeat progress: %w", err)
	}
	if st.sink == nil {
		return nil
	}
	return st.sink.Heartbeat(ctx, b)
}

// Checkpoint returns the progress a previous attempt of this call reported,
// and whether there was one.
//
// False on the first attempt, and on any attempt whose predecessor never
// heartbeated — so the zero value must be a sensible place to start. That is
// the whole contract: a function that reads its checkpoint and resumes from it
// is idempotent in the only sense that matters here, since an executor that
// moves calls delivers at least once and a retry is always possible.
//
// T must be what [Context.Heartbeat] was called with. A mismatch is reported as a
// decode error rather than a wrong answer.
func (c Context) Checkpoint[T any]() (T, bool, error) {
	var zero T

	st := progressFrom(c)
	if st == nil || len(st.resume.Checkpoint) == 0 {
		return zero, false, nil
	}
	if t := threadFrom(c); t != nil && t.readonly {
		return zero, false, nil // the checkpoint is the live thread's, not a replay's
	}
	v, err := dswire.DecodeRecord(dswire.ReflectCodec[T]{New: allocator[T]()}, st.resume.Checkpoint)
	if err != nil {
		return zero, false, fmt.Errorf("flow: decode checkpoint: %w", err)
	}
	return v, true, nil
}

// Attempt reports how many times this call has been started before, counting
// from zero.
//
// A function that resumes rather than restarting wants to know; without this
// a retry looks exactly like a first run. Zero outside a running call.
func (c Context) Attempt() int {
	st := progressFrom(c)
	if st == nil {
		return 0
	}
	return st.resume.Attempt
}

// Progress is where a running function's reports go. An executor that runs
// functions somewhere they can be lost implements one and installs it with
// [WithProgress] around each call it starts; [Context.Heartbeat] finds it on
// the context.
type Progress interface {
	// Heartbeat delivers one checkpoint: the latest position, encoded.
	Heartbeat(ctx context.Context, checkpoint []byte) error
}

// Resume is what an executor hands a retry: which attempt this is, and what
// the attempts before it reported.
type Resume struct {
	// Attempt counts prior starts of this call, from zero.
	Attempt int
	// Checkpoint is the last progress a previous attempt reported through
	// [Context.Heartbeat]. Empty on a first attempt, and on a retry of something that
	// never heartbeated.
	Checkpoint []byte
}

// WithProgress returns a context on which [Context.Heartbeat],
// [Context.Checkpoint] and [Context.Attempt] work: reports go to p, and r is
// what a previous attempt left.
//
// For executors. Install it for every call rather than only for functions
// that declare a heartbeat bound: calling Heartbeat is always allowed, and it
// is the checkpoint that makes a retry cheap whether or not anything is
// watching the clock.
func WithProgress(ctx context.Context, p Progress, r Resume) context.Context {
	return context.WithValue(ctx, progressKey{}, &progressState{sink: p, resume: r})
}

// progressState is what a running call needs to report progress: somewhere to
// send, and whatever the last attempt left behind.
type progressState struct {
	sink   Progress
	resume Resume
}

type progressKey struct{}

func progressFrom(ctx context.Context) *progressState {
	st, _ := ctx.Value(progressKey{}).(*progressState)
	return st
}

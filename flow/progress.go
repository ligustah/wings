package flow

import (
	"context"
	"errors"
	"fmt"

	"github.com/ligustah/durable_streams/dswire"
)

// Heartbeat reports that a call is still progressing and records where it has
// reached. It is the liveness signal [WithHeartbeatTimeout] measures, and the
// checkpoint the next attempt reads with [Context.Checkpoint] to resume rather
// than restart. Only the latest value survives. Best-effort; call it per unit of
// real progress, not per loop iteration.
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

// Checkpoint returns the progress a previous attempt reported, and whether there
// was one. False on the first attempt and when the predecessor never
// heartbeated, so the zero value must be a usable starting point. T must match
// what [Context.Heartbeat] was called with.
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

// Attempt reports how many times this call has been started before, from zero.
// Zero outside a running call.
func (c Context) Attempt() int {
	st := progressFrom(c)
	if st == nil {
		return 0
	}
	return st.resume.Attempt
}

// Progress is where a running function's heartbeats go. An executor implements
// one and installs it with [WithProgress] around each call it starts.
type Progress interface {
	// Heartbeat delivers one checkpoint: the latest position, encoded.
	Heartbeat(ctx context.Context, checkpoint []byte) error
}

// Resume is what an executor hands a retry: which attempt this is, and what the
// attempts before it reported.
type Resume struct {
	// Attempt counts prior starts of this call, from zero.
	Attempt int
	// Checkpoint is the last progress a previous attempt reported. Empty on a
	// first attempt, and on a retry of something that never heartbeated.
	Checkpoint []byte
}

// WithProgress returns a context on which [Context.Heartbeat],
// [Context.Checkpoint] and [Context.Attempt] work: heartbeats go to p, and r is
// what a previous attempt left. For executors; install it for every call.
func WithProgress(ctx context.Context, p Progress, r Resume) context.Context {
	return context.WithValue(ctx, progressKey{}, &progressState{sink: p, resume: r})
}

type progressState struct {
	sink   Progress
	resume Resume
}

type progressKey struct{}

func progressFrom(ctx context.Context) *progressState {
	st, _ := ctx.Value(progressKey{}).(*progressState)
	return st
}

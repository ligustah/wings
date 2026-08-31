package wings

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ligustah/durable_streams/dswire"
)

// Heartbeat reports that a job is still making progress, and records where it
// has got to.
//
// Two jobs in one call, and both matter. It is the liveness signal that
// [WithHeartbeatTimeout] measures — a job that stops beating is presumed stuck
// on its machine rather than slow, and is moved to another. And progress is the
// checkpoint that makes moving it cheap: whatever was last passed here is handed
// to the next attempt, which reads it with [Checkpoint] and carries on from
// there instead of starting over.
//
// Only the LATEST value survives. This is a position, not a log: the point is
// for a retry to know where to resume, and every earlier answer to that question
// is wrong.
//
// It is best-effort about delivery and deliberately so. A beat that does not
// reach the coordinator costs a retry that redoes a little work; a beat that
// blocked the work function to guarantee delivery would cost the work itself.
//
// Cheap to call, but not free — each one is an append — so call it per unit of
// real progress rather than per loop iteration.
func Heartbeat[T any](ctx context.Context, progress T) error {
	st := beatFrom(ctx)
	if st == nil {
		return errors.New("wings: Heartbeat was called outside a work function; " +
			"it reports the progress of a job, and there is no job here")
	}
	b, err := dswire.EncodeRecord(dswire.ReflectCodec[T]{New: allocator[T]()}, progress)
	if err != nil {
		return fmt.Errorf("wings: encode heartbeat progress: %w", err)
	}
	return st.send(ctx, b)
}

// Checkpoint returns the progress a previous attempt of this job reported, and
// whether there was one.
//
// False on the first attempt, and on any attempt whose predecessor never
// heartbeated — so the zero value must be a sensible place to start. That is
// the whole contract: a work function that reads its checkpoint and resumes
// from it is idempotent in the only sense that matters here, since wings
// guarantees at-least-once delivery and a retry is always possible.
//
// T must be what [Heartbeat] was called with. A mismatch is reported as a
// decode error rather than a wrong answer.
func Checkpoint[T any](ctx context.Context) (T, bool, error) {
	var zero T

	st := beatFrom(ctx)
	if st == nil || len(st.in) == 0 {
		return zero, false, nil
	}
	v, err := dswire.DecodeRecord(dswire.ReflectCodec[T]{New: allocator[T]()}, st.in)
	if err != nil {
		return zero, false, fmt.Errorf("wings: decode checkpoint: %w", err)
	}
	return v, true, nil
}

// beatSink is where a worker's heartbeats go.
type beatSink interface {
	sendBeat(ctx context.Context, b beatEnvelope) error
}

// beatState is what a running job needs to report progress: somewhere to send,
// and whatever the last attempt left behind.
type beatState struct {
	job  string
	sink beatSink
	in   []byte

	// steps are what a previous attempt completed, and next is how far this one
	// has replayed through them. Guarded because Step may be called from a work
	// function that does several things at once — though it must not be, and
	// says so when it is.
	mu    sync.Mutex
	steps []stepRecord
	next  int
}

func (b *beatState) send(ctx context.Context, checkpoint []byte) error {
	if b.sink == nil {
		return nil
	}
	return b.sink.sendBeat(ctx, beatEnvelope{Job: b.job, Checkpoint: checkpoint})
}

type beatKey struct{}

func withBeat(ctx context.Context, b *beatState) context.Context {
	return context.WithValue(ctx, beatKey{}, b)
}

func beatFrom(ctx context.Context) *beatState {
	b, _ := ctx.Value(beatKey{}).(*beatState)
	return b
}

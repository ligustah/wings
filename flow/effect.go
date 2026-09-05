package flow

import (
	"errors"
	"fmt"

	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow/protos"
)

// Effect calls f once and records what it returned, so a replay hands back the
// same outcome without calling f again.
//
// It is the wrapper for the things a run body needs from the outside world
// that would answer differently each time: the host's name, a random number, a
// fresh identifier, a read of a file that may since have changed. Written bare,
// such a call makes the run's history contradict its next attempt; written as
// an effect, the answer is part of the history.
//
//	host, err := ctx.Effect(os.Hostname)
//
// An error from f is recorded too, and replayed: the effect failed, and that is
// what happened. Retry inside f if a transient failure should not settle the
// matter. f itself must not use the run's context — it is not a thread of the
// run, and nothing it does is recorded but its result.
//
// T is encoded the way a function's result is, so it must be a value the
// codec can round-trip: a plain value, a struct with exported fields, or a
// pointer to one.
func (c Context) Effect[T any](f func() (T, error)) (T, error) {
	var zero T
	t := threadFrom(c)
	if t == nil {
		return zero, errors.New("flow: Effect called outside a Run")
	}
	if f == nil {
		return zero, errors.New("flow: Effect requires a non-nil function")
	}
	codec := dswire.ReflectCodec[T]{New: allocator[T]()}

	ev, err := t.expect[*protos.EffectEvent]()
	if err != nil {
		return zero, err
	}
	if ev != nil {
		payload, err := unpackResult(ev.GetResult())
		if err != nil {
			return zero, err
		}
		out, err := dswire.DecodeRecord(codec, payload)
		if err != nil {
			return zero, fmt.Errorf("flow: decode recorded effect: %w", err)
		}
		return out, nil
	}

	out, ferr := f()
	var payload []byte
	if ferr == nil {
		payload, err = dswire.EncodeRecord(codec, out)
		if err != nil {
			return zero, fmt.Errorf("flow: encode effect result: %w", err)
		}
	}
	t.record(&protos.EffectEvent{Result: packResult(payload, ferr)})
	if err := t.err(); err != nil {
		return zero, err
	}
	return out, ferr
}

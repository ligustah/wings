package flow

import (
	"errors"
	"fmt"

	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow/protos"
)

// Effect calls f once and records its result (error included), so a replay
// returns the same outcome without calling f again. Use it for the
// nondeterministic reads a run body needs — the clock, a random number, a
// hostname, a file that may change. f must not use the run's context, and T
// must be round-trippable by the codec.
//
//	host, err := ctx.Effect(os.Hostname)
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

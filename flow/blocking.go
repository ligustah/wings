package flow

import (
	"errors"
	"fmt"
	"time"

	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow/protos"
)

// Blocking calls f on a background goroutine and records its result like
// [Context.Effect], while the calling thread stays here committing its transaction
// and reporting liveness on a timer. So f may run longer than the backend's
// transaction timeout or a heartbeat bound without the transaction being reaped or
// the thread judged stalled — use it for CPU-bound or otherwise blocking f. Like
// Effect, f must not use the run's context (it runs off-thread and owns no producer,
// which is what keeps the calling thread the sole writer), and a replay returns the
// recorded result without calling f again.
//
//	sum, err := ctx.Blocking(func() (int, error) { return crunch(data), nil })
func (c Context) Blocking[T any](f func() (T, error)) (T, error) {
	var zero T
	t := threadFrom(c)
	if t == nil {
		return zero, errors.New("flow: Blocking called outside a Run")
	}
	if f == nil {
		return zero, errors.New("flow: Blocking requires a non-nil function")
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
			return zero, fmt.Errorf("flow: decode recorded blocking result: %w", err)
		}
		return out, nil
	}

	out, ferr := runOffThread(c, t, f)
	var payload []byte
	if ferr == nil {
		payload, err = dswire.EncodeRecord(codec, out)
		if err != nil {
			return zero, fmt.Errorf("flow: encode blocking result: %w", err)
		}
	}
	t.record(&protos.EffectEvent{Result: packResult(payload, ferr)})
	if err := t.err(); err != nil {
		return zero, err
	}
	return out, ferr
}

// runOffThread runs f on its own goroutine and keeps t's transaction alive until f
// returns. f uses no run context, so the calling thread stays the sole writer to
// the producer. The done channel is buffered so the goroutine never leaks even if f
// outlives interest in its result.
func runOffThread[T any](c Context, t *threadState, f func() (T, error)) (T, error) {
	type result struct {
		out T
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := f()
		done <- result{out, err}
	}()

	beat := t.run.opts.blockingBeat
	if beat <= 0 {
		beat = defaultBlockingBeat
	}
	tk := time.NewTicker(beat)
	defer tk.Stop()
	for {
		select {
		case r := <-done:
			return r.out, r.err
		case <-tk.C:
			_ = t.keepAlive(c)
		}
	}
}

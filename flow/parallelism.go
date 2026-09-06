package flow

import (
	"context"
	"errors"
	"runtime"
)

// MaxParallelism reports how many calls can run at once where this run's work
// goes: for a cluster, the largest the worker fleet may grow to times how many
// each worker runs; for an in-process run, the process's own parallelism. Use
// it to size a fan-out — how many inputs to have in flight before waiting on
// one — without wiring the executor's shape into the work.
//
//	n, err := ctx.MaxParallelism()
//	if err != nil {
//		return err
//	}
//	for chunk := range chunks(work, n) {
//		results, err := ctx.Map(Digest, chunk)
//		...
//	}
//
// The number is read once and recorded, so every attempt of the thread sees
// the same value even if the fleet has since grown or shrunk — a replay that
// decided differently would contradict its own history. It is a capacity, not
// a limit the run must respect: more work than this may be forked, and the
// surplus queues until a slot frees.
func (c Context) MaxParallelism() (int, error) {
	if threadFrom(c) == nil {
		return 0, errors.New("flow: MaxParallelism called outside a Run")
	}
	// Recorded through Effect, because the fleet's capacity is exactly the kind
	// of outside answer that changes between attempts and must be pinned to the
	// history's first read.
	n := maxParallelismFrom(c)
	return c.Effect(func() (int, error) { return n, nil })
}

type maxParallelismKey struct{}

// WithMaxParallelism tells the run reachable through ctx how many calls it can
// run at once, which [Context.MaxParallelism] reports. An executor sets it to
// its own capacity — wings uses the fleet's — before it runs a thread; a run
// given no value falls back to the process's own GOMAXPROCS.
//
// This is the injection seam for an executor, not part of a run body's
// vocabulary: a body reads the value with [Context.MaxParallelism] and does not
// set it.
func WithMaxParallelism(ctx context.Context, n int) context.Context {
	return context.WithValue(ctx, maxParallelismKey{}, n)
}

// maxParallelismFrom is the capacity an executor injected, or the process's own
// parallelism when none was, never below one.
func maxParallelismFrom(ctx context.Context) int {
	if n, ok := ctx.Value(maxParallelismKey{}).(int); ok && n > 0 {
		return n
	}
	if n := runtime.GOMAXPROCS(0); n > 0 {
		return n
	}
	return 1
}

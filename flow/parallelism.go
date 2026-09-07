package flow

import (
	"context"
	"errors"
	"runtime"
)

// MaxParallelism reports how many calls can run at once where this run's work
// goes — the fleet's capacity for a cluster, GOMAXPROCS in process — to size a
// fan-out without wiring the executor's shape into the work. It is a capacity,
// not a limit: more may be forked, and the surplus queues. Read once and
// recorded, so every attempt sees the same value.
//
//	n, err := ctx.MaxParallelism()
//	for chunk := range chunks(work, n) {
//		results, err := ctx.Map(Digest, chunk)
//		...
//	}
func (c Context) MaxParallelism() (int, error) {
	if threadFrom(c) == nil {
		return 0, errors.New("flow: MaxParallelism called outside a Run")
	}
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

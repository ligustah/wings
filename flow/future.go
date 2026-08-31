package flow

import (
	"context"
	"errors"
	"fmt"

	"github.com/ligustah/wings"
	"github.com/ligustah/wings/internal/invoke"
)

// Future is a call that is running while the workflow does something else.
//
// Get one from [Go]. It is not a promise you can pass anywhere: it belongs to
// the thread that created it and must be awaited by that thread, because the
// order of the forks and joins is part of what makes the run replayable.
type Future[Out any] struct {
	out  Out
	err  error
	done chan struct{}

	awaited bool
	child   *threadState
	parent  *threadState
}

// Go starts f on its own workflow thread and returns immediately.
//
//	a := flow.Go(ctx, Digest, left)
//	b := flow.Go(ctx, Digest, right)
//	x, err := a.Await(ctx)
//	y, err := b.Await(ctx)
//
// Use this rather than a bare goroutine. The thread it forks has a name derived
// from the parent and the number of forks the parent has already made, so the
// same code produces the same names in the same order on every attempt — which
// a goroutine cannot promise, and which replay cannot do without.
//
// For a fan-out over a slice, prefer [wings.Map]: it forks the same way and
// keeps the results in order.
func Go[In, Out any](ctx context.Context, f wings.Func[In, Out], in In) *Future[Out] {
	return spawn(ctx, "Go", func(ctx context.Context) (Out, error) {
		return f(ctx, in)
	})
}

// Spawn runs body on a workflow thread of its own and returns immediately.
//
// This is [Go] for a piece of workflow code rather than a single work function:
// body may call work functions, sleep, use a [Channel], fork further threads —
// anything the workflow function itself may do. It is what makes channels worth
// having, since a channel between threads needs threads that do more than one
// thing.
//
//	ch := flow.NewChannel[int](ctx)
//	producer := flow.Spawn(ctx, func(ctx context.Context) (int, error) {
//	    for _, item := range work {
//	        v, err := Digest(ctx, item)
//	        if err != nil {
//	            return 0, err
//	        }
//	        if err := ch.Send(ctx, v); err != nil {
//	            return 0, err
//	        }
//	    }
//	    return 0, ch.Close(ctx)
//	})
//
// Use this rather than a bare goroutine, for the reason [Go] gives: the thread
// it forks is named deterministically, and a goroutine's is not.
//
// body must be given the context IT receives, not the one Spawn was called
// with. The child thread is bound to it, and code that uses the outer context
// records on the parent thread instead — which replay will then find in the
// wrong order.
func Spawn[Out any](ctx context.Context, body func(ctx context.Context) (Out, error)) *Future[Out] {
	return spawn(ctx, "Spawn", body)
}

func spawn[Out any](ctx context.Context, who string, body func(ctx context.Context) (Out, error)) *Future[Out] {
	fut := &Future[Out]{done: make(chan struct{})}

	parent := threadFrom(ctx)
	if parent == nil {
		fut.err = fmt.Errorf("flow: %s called outside a workflow", who)
		close(fut.done)
		return fut
	}

	child := parent.fork()
	fut.parent, fut.child = parent, child
	if err := recordFork(parent, child); err != nil {
		fut.err = err
		close(fut.done)
		return fut
	}

	go func() {
		defer close(fut.done)
		defer func() {
			if r := recover(); r != nil {
				fut.err = fmt.Errorf("flow: panic in forked work: %v", r)
			}
		}()
		fut.out, fut.err = body(withThread(ctx, child))
	}()

	return fut
}

// Await blocks until the call finishes and returns its result.
//
// Awaiting twice is a programming error rather than a second wait: the join is
// recorded, and recording it twice would put an event in the history that the
// next attempt does not produce.
func (f *Future[Out]) Await(ctx context.Context) (Out, error) {
	var zero Out

	if f.parent == nil {
		return zero, f.err
	}
	if f.awaited {
		return zero, errors.New("flow: this Future has already been awaited")
	}
	f.awaited = true

	select {
	case <-f.done:
	case <-ctx.Done():
		return zero, ctx.Err()
	}

	if err := recordJoin(f.parent, f.child); err != nil {
		return zero, err
	}
	if f.err != nil {
		return zero, f.err
	}
	return f.out, f.parent.run.err()
}

// Assert at compile time that a cluster host and the recording host agree on
// the seam, since Go and Map both depend on them being interchangeable.
var _ invoke.Host = host{}

package flow

import (
	"context"
	"errors"
	"fmt"
)

// Future is a call that is running while the run does something else.
//
// Get one from [Go] or [Spawn]. It is not a promise you can pass anywhere: it
// belongs to the thread that created it and must be awaited by that thread,
// because the order of the forks and joins is part of what makes the run
// replayable.
type Future[Out any] struct {
	out  Out
	err  error
	done chan struct{}

	awaited bool
	child   *threadState
	parent  *threadState
}

// Go starts f on its own thread and returns immediately.
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
// For a fan-out over a slice, prefer [Map]: it is this, once per input, with
// the results kept in order.
func Go[In, Out any](ctx context.Context, f Func[In, Out], in In) *Future[Out] {
	return spawn(ctx, "Go", func(ctx context.Context) (Out, error) {
		return f(ctx, in)
	})
}

// Spawn runs body on a thread of its own and returns immediately.
//
// This is [Go] for a piece of run code rather than a single function: body may
// call functions, sleep, use a [Channel], fork further threads — anything the
// run's own body may do. It is what makes channels worth having, since a
// channel between threads needs threads that do more than one thing.
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
		fut.err = fmt.Errorf("flow: %s called outside a Run; a thread can only be forked from a run's body, "+
			"with the context it was given", who)
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

// Map runs f on every input and returns the results in the order the inputs
// were given.
//
// It is [Go] once per input and then [Future.Await] on each in order, and
// nothing more: every input is dispatched before any result is waited on, so
// the work is in flight in parallel rather than merely submitted in a loop,
// and the forks and joins land in the history in the same order every attempt.
//
// If some inputs fail, the successful outputs are still returned in their
// places and the error joins every failure, each naming its index. Check the
// error before trusting a position you did not verify.
func Map[In, Out any](ctx context.Context, f Func[In, Out], ins []In) ([]Out, error) {
	outs := make([]Out, len(ins))
	if len(ins) == 0 {
		return outs, nil
	}

	futures := make([]*Future[Out], len(ins))
	for i, in := range ins {
		futures[i] = Go(ctx, f, in)
	}

	var joined []error
	for i, fut := range futures {
		out, err := fut.Await(ctx)
		if err != nil {
			joined = append(joined, fmt.Errorf("flow: input %d: %w", i, err))
			continue
		}
		outs[i] = out
	}
	return outs, errors.Join(joined...)
}

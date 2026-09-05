package flow

import (
	"errors"
	"fmt"
)

// Future is a call that is running while the run does something else.
//
// Get one from [Context.Go] or [Context.Spawn]. It is not a promise you can
// pass anywhere: it belongs to the thread that created it and must be awaited
// by that thread, because the order of the forks and joins is part of what
// makes the run replayable.
type Future[Out any] struct {
	out  Out
	err  error
	done chan struct{}

	awaited bool
	child   *threadState
	parent  *threadState
}

// spawn forks a thread for body: the work behind [Context.Go] and
// [Context.Spawn].
func spawn[Out any](ctx Context, who string, body func(ctx Context) (Out, error)) *Future[Out] {
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
		fut.out, fut.err = body(Context{withThread(ctx, child)})
	}()

	return fut
}

// Await blocks until the call finishes and returns its result.
//
// Awaiting twice is a programming error rather than a second wait: the join is
// recorded, and recording it twice would put an event in the history that the
// next attempt does not produce.
func (f *Future[Out]) Await(ctx Context) (Out, error) {
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

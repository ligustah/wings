package flow

import (
	"context"
	"errors"
	"fmt"

	"github.com/ligustah/durable_streams/dswire"
)

// Future is a thread that is running while the run does something else.
//
// Get one from [Context.Go] or [Context.Spawn]. It is not a promise you can
// pass anywhere: it belongs to the thread that created it and must be awaited
// by that thread, because the order of the forks and joins is part of what
// makes the run replayable.
type Future[Out any] struct {
	out  []byte
	err  error
	done chan struct{}

	awaited bool
	thread  Thread
	parent  *threadState
	codec   dswire.Codec[Out]
}

// spawn forks a thread: the work behind [Context.Go] and [Context.Spawn].
//
// fn and input say what the thread runs when it runs a defined function,
// and body is that function bound to that input — or, for a thread of run
// code, the code. The fork is recorded on the parent with fn and input, so a
// reader of the parent's history alone can start the thread; then the
// thread goes to the placer, unless the parent's history already holds its
// join, in which case it is over and its result is waiting there.
func spawn[Out any](ctx Context, who, fn string, input []byte, codec dswire.Codec[Out], body func(ctx Context) ([]byte, error)) *Future[Out] {
	fut := &Future[Out]{done: make(chan struct{}), codec: codec}

	parent := threadFrom(ctx)
	if parent == nil {
		fut.err = fmt.Errorf("flow: %s called outside a Run; a thread can only be forked from a run's body, "+
			"with the context it was given", who)
		close(fut.done)
		return fut
	}

	th := Thread{Run: parent.run.name, ID: parent.nextChild(), Parent: parent.id, Fn: fn, Input: input}
	fut.parent, fut.thread = parent, th
	if err := parent.recordFork(th); err != nil {
		fut.err = err
		close(fut.done)
		return fut
	}

	// Joined on a previous attempt: the thread is over and its result is in
	// the parent's history, where Await will find it. Nothing to run.
	if parent.joined(th.ID) {
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
		fut.out, fut.err = parent.run.placer.Place(ctx, th, body)
	}()

	return fut
}

// Await blocks until the thread finishes and returns its result.
//
// Awaiting twice is a programming error rather than a second wait: the join is
// recorded, and recording it twice would put an event in the history that the
// next attempt does not produce.
//
// The error a thread ends with is on record, like a call's: a run that
// returns it is not retried, since the thread was retried already and a
// replay would find the same answer. Handle it in the body if the run can go
// on without that thread.
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
	default:
		// Not over yet: the thread waits, and is not running while it does.
		resume := f.parent.park(ctx, WaitJoin)
		select {
		case <-f.done:
		case <-ctx.Done():
			return zero, ctx.Err()
		}
		if err := resume(ctx); err != nil {
			return zero, err
		}
	}

	// Interrupted rather than finished: the thread was cut short by the
	// caller's own context, and recording that as its result would have the
	// next attempt replay a failure that never happened. The fork stays
	// without a join, which is what makes that attempt run the thread again.
	if f.err != nil && ctx.Err() != nil {
		return zero, f.err
	}
	replaying := f.parent.peek() != nil

	out, callErr, err := f.parent.recordJoin(f.thread.ID, f.out, f.err)
	if err != nil {
		return zero, err
	}
	if !replaying {
		// Over, and its result is the parent's now. The thread's own history
		// has nothing left to say; a drop that fails leaves it behind, which
		// costs storage and nothing else.
		_ = f.parent.run.store.Drop(context.WithoutCancel(ctx), f.thread.Run, f.thread.ID)
	}
	if callErr != nil {
		return zero, &callError{name: f.thread.describe(), err: callErr}
	}
	v, err := dswire.DecodeRecord(f.codec, out)
	if err != nil {
		return zero, fmt.Errorf("flow: decode the result of %s: %w", f.thread.describe(), err)
	}
	return v, nil
}

// describe names the thread in an error: by its function when it runs one,
// which is what the reader will recognise.
func (th Thread) describe() string {
	if th.Fn != "" {
		return th.Fn
	}
	return "thread " + th.ID
}

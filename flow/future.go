package flow

import (
	"context"
	"errors"
	"fmt"

	"github.com/ligustah/durable_streams/dswire"
)

// Future is a running thread, from [Context.Go] or [Context.Spawn]. It must be
// awaited by the thread that created it: the order of forks and joins is part of
// the run's replay.
type Future[Out any] struct {
	out  []byte
	err  error
	done chan struct{}

	awaited bool
	thread  Thread
	parent  *threadState
	codec   dswire.Codec[Out]
}

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
	th.Root, th.Lineage = parent.run.opts.root, lineageOf(parent.run.opts.rootID, th.ID)
	fut.parent, fut.thread = parent, th
	if err := parent.recordFork(th); err != nil {
		fut.err = err
		close(fut.done)
		return fut
	}

	// Joined on a previous attempt: over, result already in the parent's
	// history. Nothing to run.
	joined := parent.joined(th.ID)
	if parent.run.forked != nil {
		parent.run.forked(th, joined)
	}
	if joined {
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

// Await blocks until the thread finishes and returns its result. Await it once.
// The thread's error is on record like a call's: returning it does not retry the
// run, so handle it in the body if the run can continue without the thread.
func (f *Future[Out]) Await(ctx Context) (Out, error) {
	var zero Out

	if f.parent == nil {
		return zero, f.err
	}
	if f.awaited {
		return zero, errors.New("flow: this Future has already been awaited")
	}
	f.awaited = true

	// Given up on last time by the caller's timeout or cancel: give up again at
	// once. The thread runs on regardless.
	if err, ok := f.parent.interrupted("join"); ok {
		return zero, err
	}

	select {
	case <-f.done:
	default:
		resume := f.parent.park(ctx, WaitJoin)
		select {
		case <-f.done:
		case <-ctx.Done():
			// Reclaim the slot given up to wait before continuing.
			_ = resume(f.parent.base())
			return zero, f.parent.interrupt("join", ctx.Err())
		}
		if err := resume(ctx); err != nil {
			return zero, err
		}
	}

	// A context-cancelled thread keeps its fork unjoined, so the next attempt
	// reruns it rather than replaying a failure that never happened.
	if f.err != nil && (ctx.Err() != nil || errors.Is(f.err, context.Canceled) || errors.Is(f.err, context.DeadlineExceeded)) {
		return zero, f.err
	}
	replaying := f.parent.peek() != nil

	out, callErr, err := f.parent.recordJoin(f.thread.ID, f.out, f.err)
	if err != nil {
		return zero, err
	}
	if !replaying {
		// The result is the parent's now; drop the thread's own history.
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

func (th Thread) describe() string {
	if th.Fn != "" {
		return th.Fn
	}
	return "thread " + th.ID
}

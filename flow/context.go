package flow

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Context is the context a run's body and a running function are given.
//
// It is a [context.Context] — pass it to anything that wants one — with
// everything this package does for that code as methods on it: fork with
// [Context.Go], [Context.Spawn] and [Context.Map]; read the clock with
// [Context.Now] and wait with [Context.Sleep]; make a [Channel]; report a
// running call's progress with [Context.Heartbeat] and [Context.Step]. There
// are no package functions that take a context first: the context IS the
// handle, and what it can do is what it says.
//
// It is a value, cheap to copy, and everything it knows lives in the
// context.Context it wraps, so a context derived from one with the standard
// library — context.WithTimeout, say — loses nothing; [From] wraps it back.
type Context struct {
	context.Context
}

// From gives a Context over any context.
//
// A run's body and a running function are handed one already; From is for
// code that derived a plain context from it and wants the methods back, and
// for a caller that bound an executor with [Bind] some time ago and has only
// the context.Context to show for it. A context that carries nothing of this
// package's still gets a Context, whose methods say so when used.
func From(ctx context.Context) Context {
	if c, ok := ctx.(Context); ok {
		return c
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return Context{ctx}
}

// WithCancel is [context.WithCancel], keeping the type.
func (c Context) WithCancel() (Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(c.Context)
	return Context{ctx}, cancel
}

// WithTimeout is [context.WithTimeout], keeping the type.
func (c Context) WithTimeout(d time.Duration) (Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.Context, d)
	return Context{ctx}, cancel
}

// WithValue is [context.WithValue], keeping the type.
func (c Context) WithValue(key, val any) Context {
	return Context{context.WithValue(c.Context, key, val)}
}

// Go starts f on its own thread and returns immediately.
//
//	a := ctx.Go(Digest, left)
//	b := ctx.Go(Digest, right)
//	x, err := a.Await(ctx)
//	y, err := b.Await(ctx)
//
// Use this rather than a bare goroutine. The thread it forks has a name derived
// from the parent and the number of forks the parent has already made, so the
// same code produces the same names in the same order on every attempt — which
// a goroutine cannot promise, and which replay cannot do without.
//
// For a fan-out over a slice, prefer [Context.Map]: it is this, once per
// input, with the results kept in order.
func (c Context) Go[In, Out any](f Func[In, Out], in In) *Future[Out] {
	return spawn(c, "Go", func(ctx Context) (Out, error) {
		return f(ctx, in)
	})
}

// Spawn runs body on a thread of its own and returns immediately.
//
// This is [Context.Go] for a piece of run code rather than a single function:
// body may call functions, sleep, use a [Channel], fork further threads —
// anything the run's own body may do. It is what makes channels worth having,
// since a channel between threads needs threads that do more than one thing.
//
//	ch := ctx.NewChannel[int]()
//	producer := ctx.Spawn(func(ctx flow.Context) (int, error) {
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
// Use this rather than a bare goroutine, for the reason [Context.Go] gives:
// the thread it forks is named deterministically, and a goroutine's is not.
//
// body must use the context IT receives, not the one Spawn was called on. The
// child thread is bound to it, and code that uses the outer context records on
// the parent thread instead — which replay will then find in the wrong order.
func (c Context) Spawn[Out any](body func(ctx Context) (Out, error)) *Future[Out] {
	return spawn(c, "Spawn", body)
}

// Map runs f on every input and returns the results in the order the inputs
// were given.
//
// It is [Context.Go] once per input and then [Future.Await] on each in order,
// and nothing more: every input is dispatched before any result is waited on,
// so the work is in flight in parallel rather than merely submitted in a loop,
// and the forks and joins land in the history in the same order every attempt.
//
// If some inputs fail, the successful outputs are still returned in their
// places and the error joins every failure, each naming its index. Check the
// error before trusting a position you did not verify.
func (c Context) Map[In, Out any](f Func[In, Out], ins []In) ([]Out, error) {
	outs := make([]Out, len(ins))
	if len(ins) == 0 {
		return outs, nil
	}

	futures := make([]*Future[Out], len(ins))
	for i, in := range ins {
		futures[i] = c.Go(f, in)
	}

	var joined []error
	for i, fut := range futures {
		out, err := fut.Await(c)
		if err != nil {
			joined = append(joined, fmt.Errorf("flow: input %d: %w", i, err))
			continue
		}
		outs[i] = out
	}
	return outs, errors.Join(joined...)
}

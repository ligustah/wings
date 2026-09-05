package flow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ligustah/durable_streams/dswire"
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
//
// The zero Context is usable and carries nothing: it behaves as
// [context.Background] and every method of this package on it says, in its
// error, that there is no run, call or executor here. It never panics.
type Context struct {
	context.Context
}

// base is the wrapped context, or Background for the zero Context, so that a
// Context that was never given anything reports an error instead of a nil
// dereference.
func (c Context) base() context.Context {
	if c.Context == nil {
		return context.Background()
	}
	return c.Context
}

// Deadline is [context.Context.Deadline].
func (c Context) Deadline() (time.Time, bool) { return c.base().Deadline() }

// Done is [context.Context.Done].
func (c Context) Done() <-chan struct{} { return c.base().Done() }

// Err is [context.Context.Err].
func (c Context) Err() error { return c.base().Err() }

// Value is [context.Context.Value].
func (c Context) Value(key any) any { return c.base().Value(key) }

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
//
// A wait cut short by a context the body derives — a join, a receive, a
// send, a sleep — is on record as the body's giving up, and the next
// attempt gives up at the same point with the same error, at once. What
// the run's own context cuts short is not: that attempt is over, and the
// next one waits again.
func (c Context) WithCancel() (Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(c.base())
	return Context{ctx}, cancel
}

// WithTimeout is [context.WithTimeout], keeping the type. See [Context.WithCancel]
// for what a wait it cuts short leaves on record.
func (c Context) WithTimeout(d time.Duration) (Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(c.base(), d)
	return Context{ctx}, cancel
}

// WithValue is [context.WithValue], keeping the type.
func (c Context) WithValue(key, val any) Context {
	return Context{context.WithValue(c.base(), key, val)}
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
// The thread runs f on in and nothing else, and that is what the fork
// records, so the run's [Placer] can run it anywhere f is defined: this is
// how work leaves the process. A direct call of f, by contrast, runs on the
// calling thread, where it is made.
//
// For a fan-out over a slice, prefer [Context.Map]: it is this, once per
// input, with the results kept in order.
func (c Context) Go[In, Out any](f Func[In, Out], in In) *Future[Out] {
	call, err := describe(c, f, in)
	if err != nil {
		return failedFuture[Out](err)
	}
	name, input := call.name, call.payload
	return spawn(c, "Go", name, input, call.codec.(dswire.Codec[Out]), functionBody(name, input))
}

// Spawn runs body on a thread of its own and returns immediately.
//
// This is [Context.Go] for a piece of run code rather than a single function:
// body may call functions, sleep, use a [Channel], fork further threads —
// anything the run's own body may do. It is what makes channels worth having,
// since a channel between threads needs threads that do more than one thing.
// Its result must be encodable, as a function's output must, since the join
// records it.
//
// The fork records no function a placer could run elsewhere, only that a
// thread of run code was started; what a placer can send instead is the way
// to the code — the thread's lineage, see [Thread] and [RunLineage] — for a
// process holding the same code to replay its way to the closure. That
// process computes again whatever the parent computed between the events of
// its history up to the fork, so keep that the replayable kind, and know
// that a Spawn after a [Context.Step] cannot leave: steps are kept with the
// call, not in the history. A thread of a run started under [Run] with a
// bare body has no lineage another process could start, and runs where its
// parent is.
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
	codec := dswire.ReflectCodec[Out]{New: allocator[Out]()}
	return spawn(c, "Spawn", "", nil, codec, func(ctx Context) ([]byte, error) {
		out, err := body(ctx)
		if err != nil {
			return nil, err
		}
		b, err := threadFrom(ctx).encode(func() ([]byte, error) { return dswire.EncodeRecord(codec, out) })
		if err != nil {
			return nil, fmt.Errorf("flow: encode the result of a spawned thread: %w", err)
		}
		return b, nil
	})
}

// failedFuture is a Future that failed before its thread was forked.
func failedFuture[Out any](err error) *Future[Out] {
	fut := &Future[Out]{err: err, done: make(chan struct{})}
	close(fut.done)
	return fut
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

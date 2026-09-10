package flow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ligustah/durable_streams/dswire"
)

// Context is the run context handed to a run's body and to a running function.
// It is a [context.Context] with this package's operations as methods: fork with
// [Context.Go], [Context.Spawn] and [Context.Map]; read the clock with
// [Context.Now]; wait with [Context.Sleep]; make a [Channel]; report progress
// with [Context.Heartbeat]. It is a cheap value; the zero Context behaves as
// [context.Background] and its methods return an error saying there is no run.
type Context struct {
	context.Context
}

// base is the wrapped context, or Background for the zero Context.
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

// From returns a Context over ctx, giving this package's methods back to code
// that holds only a plain context derived from a run's. A context carrying
// nothing of a run still works; its methods then report there is no run.
func From(ctx context.Context) Context {
	if c, ok := ctx.(Context); ok {
		return c
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return Context{ctx}
}

// WithCancel is [context.WithCancel], keeping the type. A wait a body-derived
// cancel cuts short is recorded, and the next attempt gives up at the same point
// at once; a wait the run's own cancellation cuts short is not, and is retried.
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

// Go starts f on its own thread and returns a [Future] for its result. Use it
// rather than a goroutine: the thread is named deterministically, which replay
// needs. The fork records f and its input, so the run's [Placer] can run it
// wherever f is defined — this is how work leaves the process. A direct call of
// f runs in place instead. For a fan-out over a slice, use [Context.Map].
//
//	a := ctx.Go(Digest, left)
//	b := ctx.Go(Digest, right)
//	x, err := a.Await(ctx)
//	y, err := b.Await(ctx)
func (c Context) Go[In, Out any](f Func[In, Out], in In) *Future[Out] {
	call, err := describe(c, f, in)
	if err != nil {
		return failedFuture[Out](err)
	}
	name, input := call.name, call.payload
	return spawn(c, "Go", name, input, call.codec.(dswire.Codec[Out]), functionBody(name, input))
}

// Spawn runs body on its own thread and returns a [Future]. Unlike [Context.Go]
// it takes run code, not a single function: body may call functions, sleep, use
// a [Channel], and fork. Its result must be encodable. body must use the context
// it is given, not the one Spawn was called on. A thread of run code carries its
// lineage so another process can replay its way to the closure; see [Thread] and
// [RunLineage].
//
//	r, w := ctx.NewChannel[int]()
//	producer := ctx.Spawn(func(ctx flow.Context) (int, error) {
//	    for _, item := range work {
//	        v, err := Digest(ctx, item)
//	        if err != nil {
//	            return 0, err
//	        }
//	        if err := w.Send(ctx, v); err != nil {
//	            return 0, err
//	        }
//	    }
//	    return 0, w.Close(ctx)
//	})
//	// ... the spawning thread receives on r.
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

func failedFuture[Out any](err error) *Future[Out] {
	fut := &Future[Out]{err: err, done: make(chan struct{})}
	close(fut.done)
	return fut
}

// Map runs f on every input in parallel and returns the results in input order.
// On failure the successful outputs keep their places and the error joins every
// failure, each naming its index.
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

package wings

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/dswire"
	"github.com/ligustah/wings/internal/invoke"
)

// Func is a callable handle to a named work function.
//
// It is a function type, so you call it: `out, err := Digest(ctx, in)`. Where
// that runs is decided by the context, not by the call — a worker in a cluster,
// or a worker plus an event in a workflow's log if the context is a workflow's.
// The definition and the call site are identical in both, which is the point:
// wrapping the same function a second time to make it usable from a workflow
// would mean two registrations, two names and two ways to get them out of step.
//
// Create one with [Define] at package scope.
type Func[In, Out any] func(ctx context.Context, in In) (Out, error)

// def is everything a Func needs that a function value cannot carry: its name
// and its codecs.
//
// The Func closes over one of these and the registry holds the same pointer, so
// nothing ever has to recover metadata FROM a function value. (The project this
// design comes from did have to, and did it by calling the function with a
// sentinel context and reading the answer out of a deliberately-thrown error,
// which its own comment calls a hack. Closing over the definition costs
// nothing and removes the need.)
type def[In, Out any] struct {
	name string
	fn   func(context.Context, In) (Out, error)
	opts defOptions

	inCodec  dswire.Codec[In]
	outCodec dswire.Codec[Out]
}

// defOptions are the properties of a work function that are not its code.
//
// Per function rather than per cluster because a bound on how long something
// may take is a fact about the work, not about the machines running it: one
// function is a millisecond of arithmetic and another is an hour of transcoding,
// and a single cluster-wide number is either useless to one or fatal to the
// other. [Config.JobTimeout] remains as the default for functions that say
// nothing.
type defOptions struct {
	timeout time.Duration
	beat    time.Duration
	start   time.Duration
}

// Option configures a work function at [Define] time.
type Option func(*defOptions)

// WithTimeout bounds one call of this function, from the moment a worker starts
// it to the moment it returns.
//
// A call that exceeds it FAILS rather than being retried. Exceeding a bound on
// total duration is a statement about the work — it is too slow, or it is stuck
// on something no other machine would be luckier with — and retrying it would
// spend the same time again to reach the same answer. Use [WithHeartbeatTimeout]
// for the case where the machine is the suspect.
//
// Overrides [Config.JobTimeout] for this function. Zero means no bound.
func WithTimeout(d time.Duration) Option {
	return func(o *defOptions) { o.timeout = d }
}

// WithHeartbeatTimeout requires this function to report progress at least this
// often, using [Heartbeat].
//
// A job that goes quiet for longer is presumed stuck rather than slow, and is
// redispatched to another worker — carrying the last checkpoint it reported, so
// the retry resumes rather than starting over. That is the difference from
// [WithTimeout]: here the suspicion falls on the machine, and moving is the
// remedy.
//
// The clock starts when a worker starts the job — not when it was sent, since a
// job can wait behind others on a busy worker — so a function that declares
// this must heartbeat; one that never calls [Heartbeat] will be moved on every
// worker in turn. Zero means no such bound, and no obligation.
func WithHeartbeatTimeout(d time.Duration) Option {
	return func(o *defOptions) { o.beat = d }
}

// WithStartTimeout bounds how long a job may wait on a worker's queue before
// the worker begins it.
//
// The other two bounds are about the work and run only once it has started;
// this one is about the wait in front of it. A job that has not been started
// within d is moved to another worker, on the suspicion that the one holding
// it is not draining its queue. Nothing is lost by moving a job that has not
// begun. Zero, the default, means a job waits as long as it must — the right
// answer for a saturated cluster, where every worker's queue is long and moving
// a job only puts it at the back of another.
func WithStartTimeout(d time.Duration) Option {
	return func(o *defOptions) { o.start = d }
}

// Define registers a work function under name and returns a callable handle.
//
// Call it in a package-scope var. Both the coordinator and the worker are the
// same binary, and the worker resolves incoming jobs through the registry that
// Define populates — so a function defined inside main's body exists only in
// the process that ran main, which is never the worker.
//
// The name is what travels on the wire, so it — not the Go symbol — is what a
// worker resolves a job against; renaming the variable is free, renaming the
// string is a protocol change.
//
// Panics if name is empty or already defined. Both are programming errors, and
// at package-init time a panic is the report that cannot be ignored.
func Define[In, Out any](name string, fn func(context.Context, In) (Out, error), opts ...Option) Func[In, Out] {
	if name == "" {
		panic("wings: Define requires a non-empty name")
	}
	if fn == nil {
		panic("wings: Define requires a non-nil function")
	}
	d := &def[In, Out]{
		name:     name,
		fn:       fn,
		inCodec:  dswire.ReflectCodec[In]{New: allocator[In]()},
		outCodec: dswire.ReflectCodec[Out]{New: allocator[Out]()},
	}
	for _, opt := range opts {
		opt(&d.opts)
	}
	register(d)

	return func(ctx context.Context, in In) (Out, error) {
		return d.dispatch(ctx, in)
	}
}

// dispatch sends one call wherever the context says work goes.
func (d *def[In, Out]) dispatch(ctx context.Context, in In) (Out, error) {
	var zero Out

	host := invoke.From(ctx)
	if host == nil {
		// Deliberately an error rather than a quiet local call. Running the work
		// here would be the wrong kind of convenience: a whole program's work
		// would silently execute on the coordinator, at the speed of one
		// machine, and nothing would look broken.
		return zero, fmt.Errorf("wings: %s was called on a context that is not bound to a cluster "+
			"or a workflow; use the context Coordinate was given, or Cluster.Bind", d.name)
	}

	payload, err := dswire.EncodeRecord(d.inCodec, in)
	if err != nil {
		return zero, fmt.Errorf("wings: encode input for %q: %w", d.name, err)
	}
	out, err := host.Invoke(ctx, d.name, payload)
	if err != nil {
		return zero, err
	}
	return d.decode(out)
}

func (d *def[In, Out]) decode(payload []byte) (Out, error) {
	out, err := dswire.DecodeRecord(d.outCodec, payload)
	if err != nil {
		var zero Out
		return zero, fmt.Errorf("wings: decode output for %q: %w", d.name, err)
	}
	return out, nil
}

// Name returns the wire name this function is dispatched under.
func (d *def[In, Out]) Name() string { return d.name }

// options returns this function's bounds, through the erased handler interface.
func (d *def[In, Out]) options() defOptions { return d.opts }

// invoke decodes a job payload, runs the function, and encodes its result.
//
// This is the type-erased entry point the worker uses. Everything above it
// deals in []byte and a name; In and Out stop here.
func (d *def[In, Out]) invoke(ctx context.Context, payload []byte) ([]byte, error) {
	in, err := dswire.DecodeRecord(d.inCodec, payload)
	if err != nil {
		return nil, fmt.Errorf("decode input for %q: %w", d.name, err)
	}
	out, err := d.fn(ctx, in)
	if err != nil {
		return nil, err
	}
	b, err := dswire.EncodeRecord(d.outCodec, out)
	if err != nil {
		return nil, fmt.Errorf("encode output for %q: %w", d.name, err)
	}
	return b, nil
}

// handler is the non-generic boundary that lets differently-typed definitions
// live in one registry. invoke is unexported, so nothing outside this package
// can satisfy it — the set of handlers is exactly the set of defined functions.
type handler interface {
	Name() string
	options() defOptions
	invoke(ctx context.Context, payload []byte) ([]byte, error)
}

var (
	registryMu sync.RWMutex
	registry   = map[string]handler{}
)

func register(h handler) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[h.Name()]; dup {
		panic(fmt.Sprintf("wings: work function %q is already defined", h.Name()))
	}
	registry[h.Name()] = h
}

func lookup(name string) (handler, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	h, ok := registry[name]
	return h, ok
}

// optionsFor returns the bounds declared for a function, or none if it is not
// defined in this binary. A coordinator dispatching to a worker built from the
// same source sees the same answer the worker will.
func optionsFor(name string) defOptions {
	h, ok := lookup(name)
	if !ok {
		return defOptions{}
	}
	return h.options()
}

// definedNames reports every registered name, for diagnostics on a failed
// lookup. A worker that cannot resolve a job is nearly always a worker built
// from different source than the coordinator, and naming what it DOES have is
// what makes that visible.
func definedNames() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	return names
}

// allocator returns a factory for T when T is a pointer type, and nil
// otherwise.
//
// dswire.ReflectCodec needs one to decode into a pointer — it has no way to
// allocate the pointed-to value itself — and needs nothing for a value type.
// Users never see this; it is the price of accepting an arbitrary T.
func allocator[T any]() func() T {
	var zero T
	rt := reflect.TypeOf(&zero).Elem()
	if rt.Kind() != reflect.Pointer {
		return nil
	}
	return func() T {
		return reflect.New(rt.Elem()).Interface().(T)
	}
}

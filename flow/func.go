package flow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/dswire"
)

// Func is a callable handle to a named function.
//
// It is a function type, so you call it: `out, err := Digest(ctx, in)`. Where
// that runs is decided by the context, not by the call. Inside a [Run] the call
// is recorded in the run's history and handed to the run's [Executor]; a replay
// of that run returns the recorded answer without running anything. On a
// context bound to an executor with [Bind] and no run around it, the call is
// handed straight to the executor. The definition and the call site are the
// same in both, which is the point.
//
// Create one with [Define] at package scope.
type Func[In, Out any] func(ctx Context, in In) (Out, error)

// def is everything a Func needs that a function value cannot carry: its name,
// its bounds and its codecs.
//
// The Func closes over one of these and the registry holds the same pointer,
// so nothing ever has to recover metadata FROM a function value.
type def[In, Out any] struct {
	name   string
	fn     func(Context, In) (Out, error)
	bounds Bounds

	inCodec  dswire.Codec[In]
	outCodec dswire.Codec[Out]
}

// Bounds are the properties of a function that are not its code: how long one
// call may take, how often it must report progress, how long it may wait to
// be started.
//
// Declared per function rather than per executor because a bound on how long
// something may take is a fact about the work, not about the machines running
// it: one function is a millisecond of arithmetic and another an hour of
// transcoding, and a single global number is either useless to one or fatal to
// the other. An executor reads them with [BoundsOf]; what it does about them is
// its business. Zero means no bound.
type Bounds struct {
	// Timeout bounds one call, from the moment it starts to the moment it
	// returns. See [WithTimeout].
	Timeout time.Duration
	// Heartbeat is how often a running call must report progress. See
	// [WithHeartbeatTimeout].
	Heartbeat time.Duration
	// Start bounds how long a call may wait before it begins. See
	// [WithStartTimeout].
	Start time.Duration
}

// definition is what an [Option] configures at [Define] time: the name a
// function is registered under and its bounds.
type definition struct {
	name   string
	bounds Bounds
}

// Option configures a function at [Define] time.
type Option func(*definition)

// WithName sets the name a function is registered and recorded under.
//
// The name identifies the function everywhere but the source — in a run's
// history, on the wire to an executor — so renaming the variable is free and
// changing this string is a change to every history that mentions it. The
// `wings` build step infers the name from the variable a definition is assigned
// to, so most code needs this only to override that, or when the name cannot be
// inferred.
func WithName(name string) Option {
	return func(d *definition) { d.name = name }
}

// WithTimeout bounds one call of this function, from the moment an executor
// starts it to the moment it returns.
//
// A call that exceeds it FAILS rather than being retried. Exceeding a bound on
// total duration is a statement about the work — it is too slow, or it is stuck
// on something no other machine would be luckier with — and retrying it would
// spend the same time again to reach the same answer. Use [WithHeartbeatTimeout]
// for the case where the machine is the suspect.
func WithTimeout(d time.Duration) Option {
	return func(def *definition) { def.bounds.Timeout = d }
}

// WithHeartbeatTimeout requires this function to report progress at least this
// often, using [Context.Heartbeat].
//
// A call that goes quiet for longer is presumed stuck rather than slow, and an
// executor that can do so runs it again elsewhere — carrying the last
// checkpoint it reported, so the retry resumes rather than starting over. That
// is the difference from [WithTimeout]: here the suspicion falls on the
// machine, and moving is the remedy.
//
// The clock starts when the call starts, so a function that declares this must
// heartbeat; one that never does will be moved on every machine in turn.
func WithHeartbeatTimeout(d time.Duration) Option {
	return func(def *definition) { def.bounds.Heartbeat = d }
}

// WithStartTimeout bounds how long a call may wait in an executor's queue
// before it begins.
//
// The other two bounds are about the work and run only once it has started;
// this one is about the wait in front of it. Nothing is lost by moving a call
// that has not begun. Zero, the default, means a call waits as long as it
// must — the right answer for a saturated cluster, where every queue is long
// and moving a call only puts it at the back of another.
func WithStartTimeout(d time.Duration) Option {
	return func(def *definition) { def.bounds.Start = d }
}

// Define registers a function and returns a callable handle. The closure is
// the only fixed argument; everything else, the name included, is an [Option].
//
// Call it in a package-scope var. An executor that runs functions in another
// process resolves calls through the registry Define populates, and a function
// defined inside main's body exists only in the process that ran main.
//
// The name comes from [WithName], or from the `wings` build step, which infers
// it from the variable this is assigned to. It is what identifies the function
// everywhere but the source — in a run's history, on the wire to an executor —
// so renaming the variable is free and changing the name is a change to every
// history that mentions it.
//
// Panics if the function is nil, if no name was given, or if the name is
// already defined. All are programming errors, and at package-init time a panic
// is the report that cannot be ignored.
func Define[In, Out any](fn func(Context, In) (Out, error), opts ...Option) Func[In, Out] {
	if fn == nil {
		panic("flow: Define requires a non-nil function")
	}
	var cfg definition
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.name == "" {
		panic("flow: Define requires a name; pass flow.WithName, or let the wings build step infer it")
	}
	d := &def[In, Out]{
		name:     cfg.name,
		fn:       fn,
		bounds:   cfg.bounds,
		inCodec:  dswire.ReflectCodec[In]{New: allocator[In]()},
		outCodec: dswire.ReflectCodec[Out]{New: allocator[Out]()},
	}
	register(d)

	return func(ctx Context, in In) (Out, error) {
		return d.dispatch(ctx, in)
	}
}

// capture is how [Context.Go] learns which function a Func is without
// running it: it calls the Func on a context carrying one of these, and
// dispatch, finding it, writes down the name and the encoded input and
// returns without doing anything.
//
// The alternative — recovering the definition from the function value — has
// nothing to hold on to: a Func is a closure, and Go gives closures no
// identity worth comparing.
type capture struct {
	taken   bool
	name    string
	payload []byte
	codec   any // the function's output codec, a dswire.Codec[Out]
	err     error
}

type captureKey struct{}

// describe calls f on a context that captures the call rather than making
// it, and reports what f would have dispatched.
func describe[In, Out any](ctx Context, f Func[In, Out], in In) (*capture, error) {
	if f == nil {
		return nil, errors.New("flow: Go requires a function made by Define, and was given nil")
	}
	cap := &capture{}
	_, _ = f(Context{context.WithValue(ctx.base(), captureKey{}, cap)}, in)
	if !cap.taken {
		return nil, errors.New("flow: Go requires a function made by Define; " +
			"a closure of your own has no name a thread could be placed under")
	}
	return cap, cap.err
}

// encodeInput encodes a call's input as the calling thread's, when there is
// one: a channel in the input is shared on that thread's behalf.
func (d *def[In, Out]) encodeInput(ctx context.Context, in In) ([]byte, error) {
	encode := func() ([]byte, error) { return dswire.EncodeRecord(d.inCodec, in) }
	var payload []byte
	var err error
	if t := threadFrom(ctx); t != nil {
		payload, err = t.encode(encode)
	} else {
		payload, err = encode()
	}
	if err != nil {
		return nil, fmt.Errorf("flow: encode input for %q: %w", d.name, err)
	}
	return payload, nil
}

// dispatch sends one call wherever the context says calls go.
func (d *def[In, Out]) dispatch(ctx Context, in In) (Out, error) {
	var zero Out

	if cap, ok := ctx.Value(captureKey{}).(*capture); ok && !cap.taken {
		// Asked what this call would be, not to make it. See capture.
		cap.taken, cap.name, cap.codec = true, d.name, d.outCodec
		cap.payload, cap.err = d.encodeInput(ctx, in)
		return zero, nil
	}

	payload, err := d.encodeInput(ctx, in)
	if err != nil {
		return zero, err
	}

	var out []byte
	if t := threadFrom(ctx); t != nil {
		out, err = t.call(ctx, d.name, payload)
	} else if e := executorFrom(ctx); e != nil {
		out, err = e.Invoke(ctx, d.name, payload)
	} else {
		// Deliberately an error rather than a quiet local call. Running the
		// work here would be the wrong kind of convenience: a whole program's
		// work would silently execute in one process, at the speed of one
		// machine, and nothing would look broken.
		return zero, fmt.Errorf("flow: %s was called on a context that is not inside a Run "+
			"and not bound to an executor; use the Context the run's body was given, or Bind", d.name)
	}
	if err != nil {
		return zero, err
	}
	return d.decode(out)
}

func (d *def[In, Out]) decode(payload []byte) (Out, error) {
	out, err := dswire.DecodeRecord(d.outCodec, payload)
	if err != nil {
		var zero Out
		return zero, fmt.Errorf("flow: decode output for %q: %w", d.name, err)
	}
	return out, nil
}

// Name returns the name this function is defined under.
func (d *def[In, Out]) Name() string { return d.name }

func (d *def[In, Out]) limits() Bounds { return d.bounds }

// invoke decodes a payload, runs the function, and encodes its result.
//
// This is the type-erased entry point an executor uses. Everything above it
// deals in []byte and a name; In and Out stop here.
func (d *def[In, Out]) invoke(ctx context.Context, payload []byte) ([]byte, error) {
	in, err := dswire.DecodeRecord(d.inCodec, payload)
	if err != nil {
		return nil, fmt.Errorf("decode input for %q: %w", d.name, err)
	}
	out, err := d.fn(From(ctx), in)
	if err != nil {
		return nil, err
	}
	encode := func() ([]byte, error) { return dswire.EncodeRecord(d.outCodec, out) }
	var b []byte
	if t := threadFrom(ctx); t != nil {
		b, err = t.encode(encode)
	} else {
		b, err = encode()
	}
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
	limits() Bounds
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
		panic(fmt.Sprintf("flow: function %q is already defined", h.Name()))
	}
	registry[h.Name()] = h
}

func lookup(name string) (handler, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	h, ok := registry[name]
	return h, ok
}

// BoundsOf returns the bounds declared for a function, and whether the
// function is defined in this process at all.
//
// For executors: a cluster dispatching to a worker built from the same source
// sees the same answer the worker will.
func BoundsOf(name string) (Bounds, bool) {
	h, ok := lookup(name)
	if !ok {
		return Bounds{}, false
	}
	return h.limits(), true
}

// Execute runs the function called name, here, on an encoded input, and
// returns its encoded output.
//
// This is what an executor calls once a call has reached the process that is
// to run it: the other end of [Executor.Invoke]. A function that is not
// defined in this process is reported with the names that are — nearly always
// the process was built from different source than the one that made the call,
// and naming what it DOES have is what makes that visible.
func Execute(ctx context.Context, name string, payload []byte) ([]byte, error) {
	h, ok := lookup(name)
	if !ok {
		return nil, fmt.Errorf("flow: no function %q is defined in this process; it defines: %s",
			name, strings.Join(defined(), ", "))
	}
	return h.invoke(ctx, payload)
}

// defined reports every registered name, sorted, for diagnostics.
func defined() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// Executor is somewhere calls go.
//
// It deals in a name and encoded bytes, deliberately: this is the erasure
// boundary. A Func knows In and Out and does the encoding; everything past this
// interface is a name and a payload, which is what lets one executor carry
// calls to a dozen differently-typed functions, and what lets it be a process
// on another machine.
//
// The one in this package, [Local], runs the function in the calling process.
// A cluster is another: its Invoke sends the call to a worker and waits for
// what comes back. A function that failed is reported as an error here, not as
// an empty result.
type Executor interface {
	Invoke(ctx context.Context, name string, payload []byte) ([]byte, error)
}

// Local runs every call in the calling process, in the calling goroutine. It
// is the executor a [Run] uses when given no other, and the one an executor
// that has carried a call to another process installs there, so that calls
// made from inside the running function run where it runs.
func Local() Executor { return local{} }

type local struct{}

func (local) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	return Execute(ctx, name, payload)
}

type executorKey struct{}

// Bind returns a context on which calls go to e.
//
// Inside a [Run] the run's executor is used and this is not needed. Bind is
// for a call made outside any run — a script, a test, a function running on a
// worker that calls another — which is dispatched and not recorded: nothing
// replays it, because there is no history for it to be in.
func Bind(ctx context.Context, e Executor) Context {
	return Context{context.WithValue(ctx, executorKey{}, e)}
}

func executorFrom(ctx context.Context) Executor {
	e, _ := ctx.Value(executorKey{}).(Executor)
	return e
}

// Origin says which run a call belongs to, and where in it.
//
// A call made outside a run has no origin and needs none. The same call inside
// a run does: an executor's record of what it ran where is far more useful if
// it can say WHICH RUN each call was a step of, and the run is the only thing
// that knows. So the run stamps it on the context before handing the call to
// its executor, and the executor reads it back with [OriginFrom] when it
// writes the call down.
//
// Purely for the executor's own record. A function's behaviour must not depend
// on who called it, or the same input stops meaning the same thing.
type Origin struct {
	// Run is the name of the run the call belongs to. Empty for a call made
	// outside any run.
	Run string
	// Thread and Step locate the call within that run, which is what makes an
	// executor's record line up with a position in the run's history.
	Thread  string
	Step    uint64
	Attempt uint64
}

// Zero reports whether o names nothing.
func (o Origin) Zero() bool { return o.Run == "" }

// Key identifies one call of one run, stably across attempts of that run.
//
// Attempt is deliberately not part of it. The whole use of this key is to
// recognise, on a later attempt, the call the previous attempt was making — and
// a key that changed with the attempt could never do that. Everything else in
// it is deterministic: replay puts the same call at the same position of the
// same thread every time.
func (o Origin) Key() string {
	if o.Zero() {
		return ""
	}
	return fmt.Sprintf("%s/%s#%d", o.Run, o.Thread, o.Step)
}

type originKey struct{}

// WithOrigin returns a context whose calls are recorded as part of o.
func WithOrigin(ctx context.Context, o Origin) context.Context {
	return context.WithValue(ctx, originKey{}, o)
}

// OriginFrom returns the origin bound to ctx, zero when there is none.
func OriginFrom(ctx context.Context) Origin {
	o, _ := ctx.Value(originKey{}).(Origin)
	return o
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

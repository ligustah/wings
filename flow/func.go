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

// Func is a callable handle to a named function: out, err := Digest(ctx, in).
// Where the call runs is decided by the context. Inside a [Run] it is recorded
// and handed to the run's [Executor], and a replay returns the recorded answer;
// on a context bound with [Bind], it goes straight to the executor. Create one
// with [Define] at package scope.
type Func[In, Out any] func(ctx Context, in In) (Out, error)

// def is everything a Func needs that a function value cannot carry. The Func
// closes over one and the registry holds the same pointer.
type def[In, Out any] struct {
	name   string
	fn     func(Context, In) (Out, error)
	bounds Bounds

	// callSite is the "<file>:<line>" a nameless def was defined on, for the
	// build step's table to name later. See names.go.
	callSite string

	inCodec  dswire.Codec[In]
	outCodec dswire.Codec[Out]
}

// Bounds are the timing properties of a function, declared per function because
// they are facts about the work, not the machines. An executor reads them with
// [BoundsOf]. Zero means no bound.
type Bounds struct {
	// Timeout bounds one call, start to return. See [WithTimeout].
	Timeout time.Duration
	// Heartbeat is how often a running call must report progress. See
	// [WithHeartbeatTimeout].
	Heartbeat time.Duration
	// Start bounds how long a call may wait before it begins. See
	// [WithStartTimeout].
	Start time.Duration
}

// definition is what an [Option] configures at [Define] time.
type definition struct {
	name   string
	bounds Bounds
}

// Option configures a function at [Define] time.
type Option func(*definition)

// WithName sets the name a function is registered and recorded under, overriding
// the name the wings build step infers from the variable. Changing it changes
// every history that mentions the function.
func WithName(name string) Option {
	return func(d *definition) { d.name = name }
}

// WithTimeout bounds one call, start to return. A call that exceeds it fails
// rather than retrying: the work is too slow or stuck, and another attempt would
// spend the same time. Use [WithHeartbeatTimeout] when the machine is the suspect.
func WithTimeout(d time.Duration) Option {
	return func(def *definition) { def.bounds.Timeout = d }
}

// WithHeartbeatTimeout requires this function to report progress at least this
// often with [Context.Heartbeat]. A call that goes quiet longer is moved to
// another worker, carrying its last checkpoint. A function that declares this
// must heartbeat, or it is moved on every machine in turn.
func WithHeartbeatTimeout(d time.Duration) Option {
	return func(def *definition) { def.bounds.Heartbeat = d }
}

// WithStartTimeout bounds how long a call may wait in a queue before it begins;
// past it the call is moved. Zero, the default, waits as long as it must.
func WithStartTimeout(d time.Duration) Option {
	return func(def *definition) { def.bounds.Start = d }
}

// Define registers a function and returns a callable handle. Call it in a
// package-scope var, so an executor in another process can resolve the call.
//
// The name comes from [WithName], or from the wings build step, which infers it
// from the variable this is assigned to; failing both it falls back to the call
// site "<file>:<line>".
//
// Panics if fn is nil, or if the name — once known — is already defined. A
// missing name is not a panic: the build step's table may fill it in after all
// definitions run (see names.go), and only a call to a function that never got
// one fails.
func Define[In, Out any](fn func(Context, In) (Out, error), opts ...Option) Func[In, Out] {
	if fn == nil {
		panic("flow: Define requires a non-nil function")
	}
	var cfg definition
	for _, opt := range opts {
		opt(&cfg)
	}
	d := &def[In, Out]{
		name:     cfg.name,
		fn:       fn,
		bounds:   cfg.bounds,
		inCodec:  dswire.ReflectCodec[In]{New: allocator[In]()},
		outCodec: dswire.ReflectCodec[Out]{New: allocator[Out]()},
	}
	if cfg.name != "" {
		register(d)
	} else {
		// runtime.Caller(1) is the flow.Define call itself.
		d.callSite = callSite(1)
		deferName(d.callSite, func(name string) {
			d.name = name
			register(d)
		})
	}

	return func(ctx Context, in In) (Out, error) {
		return d.dispatch(ctx, in)
	}
}

// capture lets [Context.Go] learn a Func's name without running it: it calls the
// Func on a context carrying one of these, and dispatch fills it in and returns.
// A Func is a closure, with no identity to recover a definition from.
type capture struct {
	taken   bool
	name    string
	payload []byte
	codec   any     // the function's output codec, a dswire.Codec[Out]
	handler handler // the def itself, so a caller can read its name once resolved
	err     error
}

type captureKey struct{}

// describe calls f on a capturing context and reports what f would have dispatched.
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

// encodeInput encodes a call's input on the calling thread when there is one, so
// a channel in the input is shared on that thread's behalf.
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
		cap.taken, cap.name, cap.codec, cap.handler = true, d.name, d.outCodec, d
		cap.payload, cap.err = d.encodeInput(ctx, in)
		return zero, nil
	}

	if d.name == "" {
		// Deferred and unresolved: last moment to settle names, defaulting this
		// one to its call site.
		ensureNamesResolved()
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

// invoke is the type-erased entry point an executor uses: decode, run, encode.
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
// share one registry. invoke is unexported, so the set of handlers is exactly
// the set of defined functions.
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

// deferredDef is a definition waiting for the build step's table to name it.
type deferredDef struct {
	site  string
	apply func(name string)
}

var (
	pendingMu   sync.Mutex
	pendingDefs []deferredDef
)

// deferName records a nameless definition and tries to resolve it now, in case
// [RegisterCallSiteNames] has already run. It does not default here.
func deferName(site string, apply func(name string)) {
	pendingMu.Lock()
	pendingDefs = append(pendingDefs, deferredDef{site: site, apply: apply})
	pendingMu.Unlock()
	resolvePendingDefs(false)
}

// resolvePendingDefs names and registers deferred definitions. With defaulting
// off it takes only those the table covers; with it on, at first use, every
// remaining definition takes its call site as its name.
func resolvePendingDefs(defaulting bool) {
	pendingMu.Lock()
	defer pendingMu.Unlock()
	kept := pendingDefs[:0]
	for _, d := range pendingDefs {
		name := nameForSite(d.site)
		if name == "" {
			if !defaulting {
				kept = append(kept, d)
				continue
			}
			name = d.site
		}
		d.apply(name)
	}
	pendingDefs = kept
}

func lookup(name string) (handler, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	h, ok := registry[name]
	return h, ok
}

// BoundsOf returns the bounds declared for a function, and whether it is defined
// in this process. For executors.
func BoundsOf(name string) (Bounds, bool) {
	h, ok := lookup(name)
	if !ok {
		return Bounds{}, false
	}
	return h.limits(), true
}

// Execute runs the function called name here on an encoded input and returns its
// encoded output — the other end of [Executor.Invoke]. A function not defined in
// this process is reported with the names that are.
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

// Executor is somewhere calls go. It deals in a name and encoded bytes — the
// erasure boundary that lets one executor carry calls to differently-typed
// functions, on another machine. [Local] runs them in the calling process; a
// cluster's Invoke sends them to a worker. A failed call is an error here.
type Executor interface {
	Invoke(ctx context.Context, name string, payload []byte) ([]byte, error)
}

// Local runs every call in the calling process and goroutine. It is the default
// executor, and the one carried to another process so calls made inside a
// running function run where it runs.
func Local() Executor { return local{} }

type local struct{}

func (local) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	return Execute(ctx, name, payload)
}

type executorKey struct{}

// Bind returns a context on which calls go to e. For calls outside any run,
// which are dispatched and not recorded; inside a [Run] the run's executor is
// used instead.
func Bind(ctx context.Context, e Executor) Context {
	return Context{context.WithValue(ctx, executorKey{}, e)}
}

func executorFrom(ctx context.Context) Executor {
	e, _ := ctx.Value(executorKey{}).(Executor)
	return e
}

// Origin says which run a call belongs to, and where in it, for an executor's
// own record. A run stamps it on the context before handing a call to its
// executor, which reads it back with [OriginFrom]. A function's behaviour must
// not depend on it.
type Origin struct {
	// Run is the name of the run the call belongs to. Empty for a call made
	// outside any run.
	Run string
	// Thread and Step locate the call within that run.
	Thread  string
	Step    uint64
	Attempt uint64
}

// Zero reports whether o names nothing.
func (o Origin) Zero() bool { return o.Run == "" }

// Key identifies one call of one run, stably across attempts — so a later
// attempt recognises the call the previous one was making. Attempt is
// deliberately not part of it.
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
// otherwise; dswire.ReflectCodec needs one to decode into a pointer.
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

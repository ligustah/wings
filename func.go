package wings

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/ligustah/durable_streams/dswire"
)

// Func is a named unit of work with a typed input and output.
//
// Create one with [Define] at package scope. The name is what travels on the
// wire, so it — not the Go symbol — is what a worker resolves a job against;
// renaming the variable is free, renaming the string is a protocol change.
type Func[In, Out any] struct {
	name string
	fn   func(context.Context, In) (Out, error)

	inCodec  dswire.Codec[In]
	outCodec dswire.Codec[Out]
}

// Define registers a work function under name and returns a handle to it.
//
// Call it in a package-scope var. Both the coordinator and the worker are the
// same binary, and the worker resolves incoming jobs through the registry that
// Define populates — so a function defined inside main's body exists only in
// the process that ran main, which is never the worker.
//
// Panics if name is empty or already defined. Both are programming errors, and
// at package-init time a panic is the report that cannot be ignored.
func Define[In, Out any](name string, fn func(context.Context, In) (Out, error)) *Func[In, Out] {
	if name == "" {
		panic("wings: Define requires a non-empty name")
	}
	if fn == nil {
		panic("wings: Define requires a non-nil function")
	}
	f := &Func[In, Out]{
		name:     name,
		fn:       fn,
		inCodec:  dswire.ReflectCodec[In]{New: allocator[In]()},
		outCodec: dswire.ReflectCodec[Out]{New: allocator[Out]()},
	}
	register(f)
	return f
}

// Name returns the wire name this function is dispatched under.
func (f *Func[In, Out]) Name() string { return f.name }

// Call runs the function in THIS process, distributing nothing.
//
// It exists for tests and for the degenerate case where a caller has a handle
// and just wants the work done here; [Cluster.Call] is the distributed one.
func (f *Func[In, Out]) Call(ctx context.Context, in In) (Out, error) { return f.fn(ctx, in) }

// invoke decodes a job payload, runs the function, and encodes its result.
//
// This is the type-erased entry point the worker uses. Everything above it
// deals in []byte and a name; In and Out stop here.
func (f *Func[In, Out]) invoke(ctx context.Context, payload []byte) ([]byte, error) {
	in, err := dswire.DecodeRecord(f.inCodec, payload)
	if err != nil {
		return nil, fmt.Errorf("decode input for %q: %w", f.name, err)
	}
	out, err := f.fn(ctx, in)
	if err != nil {
		return nil, err
	}
	b, err := dswire.EncodeRecord(f.outCodec, out)
	if err != nil {
		return nil, fmt.Errorf("encode output for %q: %w", f.name, err)
	}
	return b, nil
}

// handler is the non-generic boundary that lets differently-typed Funcs live in
// one registry. invoke is unexported, so nothing outside this package can
// satisfy it — the set of handlers is exactly the set of Funcs.
type handler interface {
	Name() string
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

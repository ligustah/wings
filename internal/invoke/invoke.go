// Package invoke is the seam between a work function and whatever is going to
// execute it.
//
// It exists so that `Digest(ctx, in)` means the same thing everywhere it is
// written: dispatch this named work to wherever this context says work goes. On
// a coordinator that is a worker; inside a workflow it is a worker plus an entry
// in the workflow's event log, so a re-run replays the answer instead of paying
// for it twice. The call site does not change, and neither does the definition.
//
// It is internal on purpose. The host it describes deals in encoded payloads,
// and that does not belong in wings' public API — the same reason no stream,
// offset or broker appears there. Packages inside this module wire themselves
// together through here; nobody outside can.
package invoke

import (
	"context"

	"github.com/ligustah/durable_streams/dsclient"
)

// Host executes named work on behalf of a caller that holds only a name and
// bytes.
//
// Deliberately non-generic: it is the erasure boundary. A wings.Func knows In
// and Out and does the encoding; everything past this interface deals in a name
// and a payload, which is what lets one workflow hold calls to a dozen
// differently-typed functions.
type Host interface {
	// Invoke runs the work function called name on the encoded input and
	// returns its encoded output. A work function that failed is reported as an
	// error here, not as an empty result.
	Invoke(ctx context.Context, name string, payload []byte) ([]byte, error)

	// Parallel runs body for each of n indices and returns once all have
	// finished, giving each its own context.
	//
	// It is a host primitive rather than a loop over goroutines because the two
	// hosts disagree about what parallelism IS. A cluster can simply start n
	// goroutines. A workflow cannot: its record of what happened is a sequence
	// per thread, and n goroutines appending to one thread would record a
	// different order every run, which is precisely what replay cannot survive.
	// So the workflow host forks n threads with deterministic names instead,
	// and every fan-out in the library — Map, futures — is built on this rather
	// than on `go`.
	//
	// body's error for index i is returned at errs[i]; a nil entry means that
	// index succeeded.
	Parallel(ctx context.Context, n int, body func(ctx context.Context, i int) error) (errs []error)

	// Streams is the host's own embedded durable-streams instance: the
	// broker-less one, with no listener and no port, that the coordinator keeps
	// its records on.
	//
	// Exposed here so a workflow engine can put its event log on the instance
	// that already exists rather than opening a second one — a log directory
	// admits exactly one engine at a time, so "open your own" is not a free
	// choice, it is a second directory and a second lock.
	Streams() *dsclient.Client
}

type ctxKey struct{}

// With returns a context in which work dispatches to h.
func With(ctx context.Context, h Host) context.Context {
	return context.WithValue(ctx, ctxKey{}, h)
}

// From returns the host bound to ctx, or nil.
func From(ctx context.Context) Host {
	h, _ := ctx.Value(ctxKey{}).(Host)
	return h
}

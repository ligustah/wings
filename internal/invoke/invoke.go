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
	"fmt"

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

// Origin says which larger piece of work a call belongs to.
//
// A bare Digest(ctx, w) has no origin and needs none. The same line inside a
// workflow does: the coordinator's record of what it sent where is far more
// useful if it can say WHICH RUN each job was a step of, and a workflow is the
// only thing that knows that. So the workflow host stamps it on the context and
// the cluster reads it back out when it writes the job down.
//
// Purely for the coordinator's own record. It is not put on the wire and no
// worker ever sees it — a work function's behaviour must not depend on who
// called it, or the same input stops meaning the same thing.
type Origin struct {
	// Flow is the workflow definition's name; Run is the instance it is a step
	// of. Both empty for a call made outside a workflow.
	Flow string
	Run  string
	// Thread and Step locate the call within that run, which is what makes a
	// journal entry line up with a position in the workflow's history.
	Thread  string
	Step    uint64
	Attempt uint64
}

// Zero reports whether o names nothing.
func (o Origin) Zero() bool { return o.Flow == "" && o.Run == "" }

// Key identifies one call of one workflow, stably across attempts of that
// workflow.
//
// Attempt is deliberately not part of it. The whole use of this key is to
// recognise, on a later attempt, the call the previous attempt was making —
// and a key that changed with the attempt could never do that. Everything else
// in it is deterministic: replay puts the same call at the same position of the
// same thread every time.
func (o Origin) Key() string {
	if o.Zero() {
		return ""
	}
	return fmt.Sprintf("%s/%s/%s#%d", o.Flow, o.Run, o.Thread, o.Step)
}

type originKey struct{}

// WithOrigin returns a context whose dispatched work is recorded as part of o.
func WithOrigin(ctx context.Context, o Origin) context.Context {
	return context.WithValue(ctx, originKey{}, o)
}

// OriginFrom returns the origin bound to ctx, zero when there is none.
func OriginFrom(ctx context.Context) Origin {
	o, _ := ctx.Value(originKey{}).(Origin)
	return o
}

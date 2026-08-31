package flow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow/protos"
	"github.com/ligustah/wings/internal/invoke"
)

// host makes a work function call into an entry in the workflow's history.
//
// It wraps the host underneath — the cluster — and adds exactly one thing:
// before dispatching, ask the log whether this call already happened. That is
// the entire difference between `Digest(ctx, w)` in a coordinator and the same
// line inside a workflow, and it is why the line does not have to change.
type host struct {
	inner invoke.Host
}

// Invoke records the call, replays its result if the log already has one, and
// otherwise dispatches it and records what came back.
//
// The pair of events matters. A call event alone says "this was attempted"; the
// return event says "and this is what it produced". A run that died between them
// replays the call event, finds no return, and does the work again — which is
// the right answer, because nobody can say whether it finished.
func (h host) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	t := threadFrom(ctx)
	if t == nil {
		return nil, errors.New("flow: work called outside a workflow thread")
	}

	call, err := expect[*protos.CallEvent](t)
	if err != nil {
		return nil, err
	}

	if call != nil {
		// Replaying. The call must be the same one, or the code has changed
		// under a live run and everything after this point is guesswork.
		if call.GetName() != name {
			return nil, continuityf("thread %q previously called %q at this point, but is now calling %q",
				t.id, call.GetName(), name)
		}
		// Compared as ENCODED bytes rather than as Go values. The engine this
		// comes from used a deep value diff, which panics on unexported fields
		// unless it is handed options for every such type; the payload is
		// already encoded here, and two calls are the same call exactly when
		// they put the same bytes on the wire.
		if stored := call.GetParams().GetSerialized(); !bytes.Equal(stored, payload) {
			return nil, continuityf("thread %q previously called %q with different arguments "+
				"(%d bytes recorded, %d bytes now)", t.id, name, len(stored), len(payload))
		}

		ret, err := expect[*protos.ReturnEvent](t)
		if err != nil {
			return nil, err
		}
		if ret != nil {
			// Already done, and this is what it produced. No worker is touched.
			return unpackResult(ret.GetResult())
		}
		// Attempted but never finished: fall through and do it again.
	} else {
		record(t, &protos.CallEvent{
			Name:   name,
			Params: &protos.Data{Serialized: payload},
		})
	}

	if err := t.run.err(); err != nil {
		return nil, err
	}

	// Tell whatever runs this which step of which run it is. The cursor now
	// sits just past the call event, so the call's own position is one back —
	// and that is the number a reader of the journal can find in the history.
	out, callErr := h.inner.Invoke(invoke.WithOrigin(ctx, invoke.Origin{
		Flow:    t.run.name,
		Run:     t.run.instance,
		Thread:  t.id,
		Step:    t.at() - 1,
		Attempt: t.run.attempt,
	}), name, payload)

	// Recorded either way. A failed activity is a fact about the run, and one
	// that a retry must not repeat blindly — the error is what the next attempt
	// replays if this call is not meant to be retried.
	record(t, &protos.ReturnEvent{Result: packResult(out, callErr)})
	if callErr != nil {
		return nil, callErr
	}
	return out, t.run.err()
}

// Streams passes through: the workflow's history and the coordinator's other
// records belong on the same instance.
func (h host) Streams() *dsclient.Client { return h.inner.Streams() }

// Parallel forks a thread per index, so a fan-out is replayable.
//
// The names come from the parent's fork counter, in order, before any goroutine
// starts — which is what makes the assignment deterministic. Scheduling then
// decides only WHEN each child's events are written, never which thread they
// belong to or what order they appear in within it.
func (h host) Parallel(ctx context.Context, n int, body func(context.Context, int) error) []error {
	errs := make([]error, n)

	parent := threadFrom(ctx)
	if parent == nil {
		errs[0] = errors.New("flow: parallel work outside a workflow thread")
		return errs
	}

	// Forked up front, in index order, so the nth child is always "<parent>.<n>"
	// regardless of how the goroutines below interleave.
	children := make([]*threadState, n)
	for i := range n {
		children[i] = parent.fork()
		if err := recordFork(parent, children[i]); err != nil {
			// A mismatch here is fatal for the whole fan-out: the children are
			// named in order, so the rest of them are wrong too.
			for j := range n {
				errs[j] = err
			}
			return errs
		}
	}

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs[i] = fmt.Errorf("flow: panic in parallel body %d: %v", i, r)
				}
			}()
			errs[i] = body(withThread(ctx, children[i]), i)
		}()
	}
	wg.Wait()

	// Joins are recorded after the fact, in index order rather than completion
	// order — for the same reason the forks were.
	for i := range n {
		if err := recordJoin(parent, children[i]); err != nil && errs[i] == nil {
			errs[i] = err
		}
	}
	return errs
}

func packResult(out []byte, err error) *protos.Result {
	if err != nil {
		return &protos.Result{Payload: &protos.Result_Error{
			Error: &protos.Error{Message: err.Error()},
		}}
	}
	return &protos.Result{Payload: &protos.Result_Data{
		Data: &protos.Data{Serialized: out},
	}}
}

func unpackResult(r *protos.Result) ([]byte, error) {
	if e := r.GetError(); e != nil {
		// Only the message survives, here as everywhere across a process
		// boundary in wings. Match on content, not identity.
		return nil, errors.New(e.GetMessage())
	}
	return r.GetData().GetSerialized(), nil
}

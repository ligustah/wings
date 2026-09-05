package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ligustah/durable_streams/dswire"
)

// Heartbeat reports that a call is still making progress, and records where it
// has got to.
//
// Two jobs in one call, and both matter. It is the liveness signal that
// [WithHeartbeatTimeout] measures — a call that stops beating is presumed stuck
// on its machine rather than slow, and an executor that can do so moves it to
// another. And progress is the checkpoint that makes moving it cheap: whatever
// was last passed here is handed to the next attempt, which reads it with
// [Context.Checkpoint] and carries on from there instead of starting over.
//
// Only the LATEST value survives. This is a position, not a log: the point is
// for a retry to know where to resume, and every earlier answer to that
// question is wrong.
//
// It is best-effort about delivery and deliberately so. A beat that does not
// reach the executor costs a retry that redoes a little work; a beat that
// blocked the function to guarantee delivery would cost the work itself.
//
// Cheap to call, but not free — each one is a report — so call it per unit of
// real progress rather than per loop iteration. Outside a running call it is
// an error: there is no attempt to report on.
func (c Context) Heartbeat[T any](progress T) error {
	ctx := c
	if t := threadFrom(ctx); t != nil && t.readonly {
		return nil // a replay reports nothing
	}
	st := progressFrom(ctx)
	if st == nil {
		return errors.New("flow: Heartbeat was called outside a running function; " +
			"it reports the progress of a call, and there is no call here")
	}
	b, err := dswire.EncodeRecord(dswire.ReflectCodec[T]{New: allocator[T]()}, progress)
	if err != nil {
		return fmt.Errorf("flow: encode heartbeat progress: %w", err)
	}
	if st.sink == nil {
		return nil
	}
	return st.sink.Heartbeat(ctx, b)
}

// Checkpoint returns the progress a previous attempt of this call reported,
// and whether there was one.
//
// False on the first attempt, and on any attempt whose predecessor never
// heartbeated — so the zero value must be a sensible place to start. That is
// the whole contract: a function that reads its checkpoint and resumes from it
// is idempotent in the only sense that matters here, since an executor that
// moves calls delivers at least once and a retry is always possible.
//
// T must be what [Context.Heartbeat] was called with. A mismatch is reported as a
// decode error rather than a wrong answer.
func (c Context) Checkpoint[T any]() (T, bool, error) {
	var zero T

	st := progressFrom(c)
	if st == nil || len(st.resume.Checkpoint) == 0 {
		return zero, false, nil
	}
	if t := threadFrom(c); t != nil && t.readonly {
		return zero, false, nil // the checkpoint is the live thread's, not a replay's
	}
	v, err := dswire.DecodeRecord(dswire.ReflectCodec[T]{New: allocator[T]()}, st.resume.Checkpoint)
	if err != nil {
		return zero, false, fmt.Errorf("flow: decode checkpoint: %w", err)
	}
	return v, true, nil
}

// Step runs a phase of a long call once, and remembers what it produced.
//
// This is [Context.Heartbeat] and [Context.Checkpoint] with the bookkeeping taken away. A call
// that is moved to another machine — because its own went quiet, or died —
// replays the steps that already finished, which costs a decode each, and
// carries on from the first one that did not. The function reads as though
// none of that were happening:
//
//	func restore(ctx flow.Context, in Backup) (Report, error) {
//	    snap, err := ctx.Step("snapshot", func(ctx flow.Context) (Snapshot, error) {
//	        return takeSnapshot(ctx, in.Source)      // twenty minutes
//	    })
//	    if err != nil {
//	        return Report{}, err
//	    }
//	    return ctx.Step("restore", func(ctx flow.Context) (Report, error) {
//	        return restoreInto(ctx, snap, in.Target) // another forty
//	    })
//	}
//
// Steps must be called in the same order every time, from one goroutine: their
// position is their identity, and the name is checked against it. A step whose
// name does not match the one recorded at that position is reported as an error
// rather than silently returning somebody else's value.
//
// It is for coarse phases, not for loops. Each completed step is one report to
// the executor, so a few dozen is nothing and a few hundred thousand is a
// different program — report a position with [Context.Heartbeat] for the inside of a
// loop, and use steps for the phases the loop sits between.
//
// A step that fails is not recorded. The call fails, and the next attempt runs
// that step again from the last one that succeeded.
//
// Delivery of the record is best-effort, like every heartbeat: a report that
// does not reach the executor before the machine dies means that step runs
// again. Steps are therefore at-least-once, exactly as calls are.
func (c Context) Step[T any](name string, body func(ctx Context) (T, error)) (T, error) {
	var zero T
	ctx := c

	if name == "" {
		return zero, errors.New("flow: Step requires a name")
	}
	if t := threadFrom(ctx); t != nil && t.readonly {
		// A step is kept with the call, not in the thread's history, so a
		// replay has nothing to replay it from and must not run it.
		return zero, fmt.Errorf("flow: step %q cannot be replayed: a thread of run code forked after a "+
			"Step cannot be run elsewhere", name)
	}
	st := progressFrom(ctx)
	if st == nil {
		// Outside a call there is nothing to resume and nowhere to report, so
		// running the body would be a silent lie about what this does.
		return zero, errors.New("flow: Step was called outside a running function; " +
			"it records a phase of a call, and there is no call here")
	}

	codec := dswire.ReflectCodec[T]{New: allocator[T]()}

	if rec, ok, err := st.replay(name); err != nil {
		return zero, err
	} else if ok {
		v, err := dswire.DecodeRecord(codec, rec.Value)
		if err != nil {
			return zero, fmt.Errorf("flow: decode recorded step %q: %w", name, err)
		}
		return v, nil
	}

	index := st.claim()
	out, err := body(ctx)
	if err != nil {
		return zero, err
	}

	value, err := dswire.EncodeRecord(codec, out)
	if err != nil {
		return zero, fmt.Errorf("flow: encode result of step %q: %w", name, err)
	}
	// Reported but not waited on. A step is finished whether or not the news
	// travels, and blocking the work to guarantee the news would make the
	// bookkeeping cost more than the thing it is saving.
	if st.sink != nil {
		if err := st.sink.Step(ctx, StepRecord{Index: index, Name: name, Value: value}); err != nil {
			return out, err
		}
	}
	return out, nil
}

// Attempt reports how many times this call has been started before, counting
// from zero.
//
// A function that resumes rather than restarting wants to know; without this
// a retry looks exactly like a first run. Zero outside a running call.
func (c Context) Attempt() int {
	st := progressFrom(c)
	if st == nil {
		return 0
	}
	return st.resume.Attempt
}

// Progress is where a running function's reports go. An executor that runs
// functions somewhere they can be lost implements one and installs it with
// [WithProgress] around each call it starts; [Context.Heartbeat] and [Context.Step] find it on
// the context.
type Progress interface {
	// Heartbeat delivers one checkpoint: the latest position, encoded.
	Heartbeat(ctx context.Context, checkpoint []byte) error
	// Step delivers one newly completed [Context.Step].
	Step(ctx context.Context, step StepRecord) error
}

// StepRecord is one completed [Context.Step]: where it sat in the call, what it was
// called, and what it produced.
//
// The index is carried rather than implied by position because these travel
// one at a time over a best-effort channel, and one that goes missing must
// leave a detectable hole rather than a silently shifted list — a step log off
// by one is a retry that skips work it never did.
type StepRecord struct {
	Index int    `json:"i"`
	Name  string `json:"name"`
	Value []byte `json:"value,omitempty"`
}

// Resume is what an executor hands a retry: which attempt this is, and what
// the attempts before it reported.
type Resume struct {
	// Attempt counts prior starts of this call, from zero.
	Attempt int
	// Checkpoint is the last progress a previous attempt reported through
	// [Context.Heartbeat]. Empty on a first attempt, and on a retry of something that
	// never heartbeated.
	Checkpoint []byte
	// Steps are the [Context.Step] calls a previous attempt completed, in order. A
	// retry replays them instead of running them again.
	Steps []StepRecord
}

// WithProgress returns a context on which [Context.Heartbeat],
// [Context.Step], [Context.Checkpoint] and [Context.Attempt] work: reports go
// to p, and r is what a previous attempt left.
//
// For executors. Install it for every call rather than only for functions
// that declare a heartbeat bound: calling Heartbeat is always allowed, and it
// is the checkpoint that makes a retry cheap whether or not anything is
// watching the clock.
func WithProgress(ctx context.Context, p Progress, r Resume) context.Context {
	return context.WithValue(ctx, progressKey{}, &progressState{sink: p, resume: r})
}

// progressState is what a running call needs to report progress: somewhere to
// send, and whatever the last attempt left behind.
type progressState struct {
	sink   Progress
	resume Resume

	// next is how far this attempt has replayed through the resumed steps.
	// Guarded because Step may be called from a function that does several
	// things at once — though it must not be, and says so when it is.
	mu   sync.Mutex
	next int
}

type progressKey struct{}

func progressFrom(ctx context.Context) *progressState {
	st, _ := ctx.Value(progressKey{}).(*progressState)
	return st
}

// replay returns the record at this call's next step position, if a previous
// attempt got that far.
func (p *progressState) replay(name string) (StepRecord, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.next >= len(p.resume.Steps) {
		return StepRecord{}, false, nil
	}
	rec := p.resume.Steps[p.next]
	if rec.Name != name {
		return StepRecord{}, false, fmt.Errorf(
			"flow: step %d of this call was %q last time and is %q now; "+
				"steps are identified by their position, so they must be called in the same order every attempt",
			p.next, rec.Name, name)
	}
	p.next++
	return rec, true, nil
}

// claim takes the next step position for a step about to run.
func (p *progressState) claim() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	i := p.next
	p.next++
	return i
}

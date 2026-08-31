package wings

import (
	"context"
	"errors"
	"fmt"

	"github.com/ligustah/durable_streams/dswire"
)

// Step runs a phase of a long job once, and remembers what it produced.
//
// This is [Heartbeat] and [Checkpoint] with the bookkeeping taken away. A job
// that is moved to another worker — because its own went quiet, or died —
// replays the steps that already finished, which costs a decode each, and
// carries on from the first one that did not. The work function reads as though
// none of that were happening:
//
//	func restore(ctx context.Context, in Backup) (Report, error) {
//	    snap, err := wings.Step(ctx, "snapshot", func(ctx context.Context) (Snapshot, error) {
//	        return takeSnapshot(ctx, in.Source)      // twenty minutes
//	    })
//	    if err != nil {
//	        return Report{}, err
//	    }
//	    return wings.Step(ctx, "restore", func(ctx context.Context) (Report, error) {
//	        return restoreInto(ctx, snap, in.Target) // another forty
//	    })
//	}
//
// Steps must be called in the same order every time, from one goroutine: their
// position is their identity, and the name is checked against it. A step whose
// name does not match the one recorded at that position is reported as an error
// rather than silently returning somebody else's value.
//
// It is for coarse phases, not for loops. Each completed step is one message to
// the coordinator, so a few dozen is nothing and a few hundred thousand is a
// different program — report a position with [Heartbeat] for the inside of a
// loop, and use steps for the phases the loop sits between.
//
// A step that fails is not recorded. The job fails, and the next attempt runs
// that step again from the last one that succeeded.
//
// Delivery of the record is best-effort, like every heartbeat: a report that
// does not reach the coordinator before its worker dies means that step runs
// again. Steps are therefore at-least-once, exactly as jobs are.
func Step[T any](ctx context.Context, name string, body func(ctx context.Context) (T, error)) (T, error) {
	var zero T

	if name == "" {
		return zero, errors.New("wings: Step requires a name")
	}
	st := beatFrom(ctx)
	if st == nil {
		// Outside a job there is nothing to resume and nowhere to report, so
		// running the body would be a silent lie about what this does.
		return zero, errors.New("wings: Step was called outside a work function; " +
			"it records a phase of a job, and there is no job here")
	}

	codec := dswire.ReflectCodec[T]{New: allocator[T]()}

	if rec, ok, err := st.replay(name); err != nil {
		return zero, err
	} else if ok {
		v, err := dswire.DecodeRecord(codec, rec.Value)
		if err != nil {
			return zero, fmt.Errorf("wings: decode recorded step %q: %w", name, err)
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
		return zero, fmt.Errorf("wings: encode result of step %q: %w", name, err)
	}
	// Reported but not waited on. A step is finished whether or not the news
	// travels, and blocking the work to guarantee the news would make the
	// bookkeeping cost more than the thing it is saving.
	if err := st.report(ctx, stepRecord{Index: index, Name: name, Value: value}); err != nil {
		return out, err
	}
	return out, nil
}

// replay returns the record at this job's next step position, if a previous
// attempt got that far.
func (b *beatState) replay(name string) (stepRecord, bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.next >= len(b.steps) {
		return stepRecord{}, false, nil
	}
	rec := b.steps[b.next]
	if rec.Name != name {
		return stepRecord{}, false, fmt.Errorf(
			"wings: step %d of this job was %q last time and is %q now; "+
				"steps are identified by their position, so they must be called in the same order every attempt",
			b.next, rec.Name, name)
	}
	b.next++
	return rec, true, nil
}

// claim takes the next step position for a step about to run.
func (b *beatState) claim() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := b.next
	b.next++
	return i
}

// report tells the coordinator a step finished.
func (b *beatState) report(ctx context.Context, rec stepRecord) error {
	if b.sink == nil {
		return nil
	}
	return b.sink.sendBeat(ctx, beatEnvelope{Job: b.job, Step: &rec})
}

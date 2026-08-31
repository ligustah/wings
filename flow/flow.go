// Package flow runs durable workflows on a wings cluster.
//
// A workflow is an ordinary Go function that calls work functions. What makes it
// durable is that every call it makes is written down before it happens and its
// answer is written down after, so a workflow that fails part-way can be run
// again and will not repeat the work it already paid for — it replays the
// history it has and carries on from the end of it.
//
//	var Digest = wings.Define("digest", func(ctx context.Context, w Work) (Result, error) { ... })
//
//	var Pipeline = flow.Define("pipeline", func(ctx context.Context, in Input) (Output, error) {
//		first, err := Digest(ctx, in.Head)      // dispatched, recorded, replayed on a retry
//		if err != nil {
//			return Output{}, err
//		}
//		rest, err := wings.Map(ctx, Digest, in.Tail)
//		...
//	})
//
// The work function is defined once and called the same way whether or not a
// workflow is involved. Nothing in the body above names this package.
//
// # What you give up
//
// Replay means the workflow function is re-executed from the top on every
// attempt, so IT MUST BE DETERMINISTIC. Everything it decides must come from its
// input or from something the log recorded:
//
//   - use [Now], not time.Now
//   - use [Sleep], not time.Sleep
//   - do not read a random number, an environment variable, or a clock
//   - do not let map iteration order change what it does
//   - use [wings.Map], [Go] or [Spawn] to do things at once, never a bare
//     goroutine
//
// A workflow that breaks these does not fail loudly on the first run — it fails
// on the retry, as a continuity error, which is why [IsContinuity] names the
// cause plainly. Work functions themselves are under no such constraint: they
// run once, on a worker, and may do anything.
//
// # Threads and channels
//
// [Spawn] runs a piece of workflow code on a thread of its own, and [Channel]
// passes typed values between threads. Both are the replayable versions of
// things Go already has, and a channel is where the difference shows: a Go
// receive takes whichever value happens to arrive first, and nothing about the
// workflow decides which that is. So a receive is RECORDED — which thread's
// which send it took — and a replay waits for exactly that item instead. A
// send needs no such treatment, since a thread's nth send always carries the
// same value; what a send records is when it COMPLETED, which on an unbuffered
// channel is somebody else's decision.
//
//	ch := flow.NewChannel[Result](ctx)
//	producer := flow.Spawn(ctx, func(ctx context.Context) (int, error) {
//		for _, item := range work {
//			r, err := Digest(ctx, item)
//			if err != nil {
//				return 0, err
//			}
//			if err := ch.Send(ctx, r); err != nil {
//				return 0, err
//			}
//		}
//		return 0, ch.Close(ctx)
//	})
//
// A workflow that deadlocks on a channel deadlocks the same way a Go program
// would; there is no scheduler here doing anything clever about it.
//
// # Long activities
//
// A work function called from a workflow checkpoints its own progress the same
// way it would anywhere else: [github.com/ligustah/wings.Step] for coarse
// phases, [github.com/ligustah/wings.Heartbeat] for a position inside one.
// Nothing about being in a workflow changes that, and nothing in the workflow
// function has to know about it.
//
// What a workflow adds is a second way for a long call to be interrupted: the
// workflow itself can fail and be retried while the call is still running. When
// the retry reaches that call again it REJOINS the one already in flight rather
// than dispatching a second copy — two copies of an hour of work would be waste
// on their own, and the copy would start from nothing while the original is
// most of the way through, holding the very progress that makes it cheap to
// move.
//
// # What it is not
//
// The workflow function runs on the coordinator, and only the calls it makes are
// distributed. That is the right trade for fanning work out of one program, and
// the wrong one for orchestration that must outlive any single machine.
package flow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/ligustah/durable_streams/dswire"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ligustah/wings/flow/protos"
	"github.com/ligustah/wings/internal/invoke"
)

// Definition is a named workflow. Create one with [Define].
type Definition[In, Out any] struct {
	name string
	fn   func(context.Context, In) (Out, error)
	opts options

	inCodec  dswire.Codec[In]
	outCodec dswire.Codec[Out]
}

// Define declares a workflow.
//
// Unlike [wings.Define], this does not return something callable: a workflow is
// started, not called, because starting one means naming the instance it belongs
// to and deciding what to do about a previous attempt. Use [Run].
func Define[In, Out any](name string, fn func(ctx context.Context, in In) (Out, error), opts ...Option) *Definition[In, Out] {
	if name == "" {
		panic("flow: Define requires a non-empty name")
	}
	if fn == nil {
		panic("flow: Define requires a non-nil function")
	}
	return &Definition[In, Out]{
		name:     name,
		fn:       fn,
		opts:     newOptions(opts),
		inCodec:  dswire.ReflectCodec[In]{New: allocator[In]()},
		outCodec: dswire.ReflectCodec[Out]{New: allocator[Out]()},
	}
}

// Name returns the workflow's name, which is what its history is filed under.
func (d *Definition[In, Out]) Name() string { return d.name }

// NewInstance mints an instance id. Use it when a workflow has no natural
// identity of its own; prefer an id derived from what the work is about, since
// that is what makes a resumed run find its history.
func NewInstance() string { return uuid.NewString() }

// Run executes a workflow to completion and returns its result.
//
// ctx must be bound to a cluster — the context [wings.CoordinatorMain] hands to
// Coordinate already is. instance names this run's history: run the same
// workflow with the same instance again and it resumes rather than restarts, and
// a run that already completed returns its stored result without executing
// anything.
//
// A workflow that fails is retried with backoff until it succeeds, returns a
// [Permanent] error, or runs out of attempts. A workflow that suspends is waited
// for. Both of those are why this can take a long time and why ctx matters.
func Run[In, Out any](ctx context.Context, d *Definition[In, Out], instance string, in In, opts ...RunOption) (Out, error) {
	var zero Out

	ro := newRunOptions(opts)

	cluster := invoke.From(ctx)
	if cluster == nil {
		return zero, errors.New("flow: Run was called on a context that is not bound to a cluster; " +
			"use the context Coordinate was given, or Cluster.Bind")
	}
	if instance == "" {
		return zero, errors.New("flow: Run requires an instance id; use flow.NewInstance for an arbitrary one")
	}

	store := ro.store
	if store == nil {
		return zero, errors.New("flow: Run requires a Store; pass flow.WithStore(flow.NewStore(...)) " +
			"or flow.WithStore(flow.NewMemStore())")
	}

	history, err := store.Events(ctx, d.name, instance)
	if err != nil {
		return zero, err
	}

	// A finished instance is finished. Re-running it would repeat every effect
	// its work functions had, which is the opposite of what a durable workflow
	// is for.
	if done, result, ok := finished(history); ok {
		if done == protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
			return d.decode(result)
		}
		_, err := unpackResult(result)
		return zero, fmt.Errorf("flow: workflow %s/%s already failed permanently: %w", d.name, instance, err)
	}

	sink, err := store.Sink(ctx, d.name, instance)
	if err != nil {
		return zero, err
	}

	payload, err := dswire.EncodeRecord(d.inCodec, in)
	if err != nil {
		return zero, fmt.Errorf("flow: encode input for %q: %w", d.name, err)
	}

	attempt := lastAttempt(history)
	for {
		attempt++

		out, status, runErr := d.attempt(ctx, cluster, sink, instance, history, attempt, payload, in)

		switch status {
		case protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:
			return out, nil

		case protos.WorkflowStatus_WORKFLOW_STATUS_FAILED:
			return zero, runErr

		case protos.WorkflowStatus_WORKFLOW_STATUS_SUSPENDED:
			_, until := IsSuspended(runErr)
			if err := wait(ctx, time.Until(until)); err != nil {
				return zero, err
			}

		default: // backoff
			if attempt >= uint64(d.opts.maxAttempts) {
				return zero, fmt.Errorf("flow: workflow %s/%s failed after %d attempts: %w",
					d.name, instance, attempt, runErr)
			}
			if err := wait(ctx, d.backoff(attempt)); err != nil {
				return zero, err
			}
		}

		// The next attempt replays everything recorded so far, including what
		// this one managed to do before it stopped.
		if history, err = store.Events(ctx, d.name, instance); err != nil {
			return zero, err
		}
	}
}

// attempt runs the workflow function once over the history it is given.
func (d *Definition[In, Out]) attempt(
	ctx context.Context,
	cluster invoke.Host,
	sink Sink,
	instance string,
	history []*protos.Event,
	attempt uint64,
	payload []byte,
	in In,
) (Out, protos.WorkflowStatus, error) {
	var zero Out

	run := &runState{
		name:     d.name,
		instance: instance,
		attempt:  attempt,
		threads:  threadsOf(history),
		sink:     sink,
	}
	main := &threadState{id: mainThread, run: run}
	// Whatever a thread of this attempt does after it returns is this attempt's
	// business and not the record's.
	defer run.finish()

	reason := protos.StartReason_START_REASON_INIT
	if len(history) > 0 {
		reason = protos.StartReason_START_REASON_RETRY
	}
	// Recorded on the run's own thread bookkeeping rather than main's cursor:
	// a start marker is about the attempt, not about what the function did, and
	// putting it in main's sequence would shift every replay position by one.
	appendMarker(run, &protos.RunStartEvent{
		Attempt:      attempt,
		Reason:       reason,
		WorkflowName: d.name,
		InstanceId:   instance,
		Version:      uint64(d.opts.version),
		Input:        &protos.Data{Serialized: payload},
	})

	// The workflow's body sees a context bound to the recording host, so a work
	// function called inside it is journalled instead of merely dispatched.
	wfCtx := withThread(invoke.With(ctx, host{inner: cluster}), main)

	out, err := d.fn(wfCtx, in)

	if perr := run.err(); perr != nil {
		// Persistence failed somewhere in there. Not retryable in any useful
		// sense: the next attempt would replay an incomplete history.
		appendMarker(run, &protos.RunEndEvent{
			Status: protos.WorkflowStatus_WORKFLOW_STATUS_FAILED,
			Result: packResult(nil, perr),
		})
		return zero, protos.WorkflowStatus_WORKFLOW_STATUS_FAILED, perr
	}

	status := d.classify(err)
	end := &protos.RunEndEvent{Status: status}

	if status == protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		encoded, encErr := dswire.EncodeRecord(d.outCodec, out)
		if encErr != nil {
			err = fmt.Errorf("flow: encode result of %q: %w", d.name, encErr)
			status = protos.WorkflowStatus_WORKFLOW_STATUS_FAILED
			end.Status, end.Result = status, packResult(nil, err)
		} else {
			end.Result = packResult(encoded, nil)
		}
	} else {
		end.Result = packResult(nil, err)
		if status == protos.WorkflowStatus_WORKFLOW_STATUS_SUSPENDED {
			if _, until := IsSuspended(err); !until.IsZero() {
				end.ScheduledFor = timestamppb.New(until)
			}
		}
	}

	appendMarker(run, end)
	if status == protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		return out, status, nil
	}
	return zero, status, err
}

// classify decides what an error the workflow returned means for the next
// attempt.
//
// Retry is the default and the other answers are the exceptions, because the
// errors that dominate distributed work are transient. A continuity error is
// never retried: a retry replays the same history against the same code and
// reaches the same contradiction.
func (d *Definition[In, Out]) classify(err error) protos.WorkflowStatus {
	switch {
	case err == nil:
		return protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED
	case IsContinuity(err):
		return protos.WorkflowStatus_WORKFLOW_STATUS_FAILED
	case IsPermanent(err):
		return protos.WorkflowStatus_WORKFLOW_STATUS_FAILED
	}
	if ok, _ := IsSuspended(err); ok {
		return protos.WorkflowStatus_WORKFLOW_STATUS_SUSPENDED
	}
	for _, target := range d.opts.permanent {
		if errors.Is(err, target) {
			return protos.WorkflowStatus_WORKFLOW_STATUS_FAILED
		}
	}
	return protos.WorkflowStatus_WORKFLOW_STATUS_BACKOFF
}

func (d *Definition[In, Out]) backoff(attempt uint64) time.Duration {
	delay := time.Duration(float64(d.opts.initialDelay) * math.Pow(2, float64(attempt-1)))
	if delay > d.opts.maxDelay || delay <= 0 {
		return d.opts.maxDelay
	}
	return delay
}

func (d *Definition[In, Out]) decode(r *protos.Result) (Out, error) {
	var zero Out
	payload, err := unpackResult(r)
	if err != nil {
		return zero, err
	}
	out, err := dswire.DecodeRecord(d.outCodec, payload)
	if err != nil {
		return zero, fmt.Errorf("flow: decode result of %q: %w", d.name, err)
	}
	return out, nil
}

// appendMarker records a run-level event, which belongs to no thread.
//
// Kept out of every thread's sequence deliberately: replay walks a thread's
// events in order and compares each to what the function is doing, and a start
// marker is not something the function did.
func appendMarker[E protos.Events](run *runState, payload E) {
	run.mu.Lock()
	defer run.mu.Unlock()

	ev := &protos.Event{
		Timestamp: timestamppb.New(time.Now().Truncate(time.Microsecond)),
		Attempt:   run.attempt,
		Payload:   protos.PackEventPayload(payload),
	}
	if run.sink != nil && run.sinkErr == nil {
		if err := run.sink.Append(context.Background(), ev); err != nil {
			run.sinkErr = fmt.Errorf("flow: persist event: %w", err)
		}
	}
}

// threadsOf rebuilds each thread's sequence from a flat history.
//
// Run-level markers have no thread and are skipped, which is what keeps them out
// of the positions replay compares against.
func threadsOf(history []*protos.Event) map[string][]*protos.Event {
	threads := map[string][]*protos.Event{mainThread: nil}
	for _, ev := range history {
		if ev.GetThreadId() == "" {
			continue
		}
		threads[ev.GetThreadId()] = append(threads[ev.GetThreadId()], ev)
	}
	return threads
}

// finished reports the terminal outcome of an instance, if it reached one.
func finished(history []*protos.Event) (protos.WorkflowStatus, *protos.Result, bool) {
	for i := len(history) - 1; i >= 0; i-- {
		end := history[i].GetRunEnd()
		if end == nil {
			continue
		}
		switch end.GetStatus() {
		case protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
			protos.WorkflowStatus_WORKFLOW_STATUS_FAILED:
			return end.GetStatus(), end.GetResult(), true
		}
		return end.GetStatus(), end.GetResult(), false
	}
	return protos.WorkflowStatus_WORKFLOW_STATUS_UNKNOWN, nil, false
}

func lastAttempt(history []*protos.Event) uint64 {
	var n uint64
	for _, ev := range history {
		if ev.GetAttempt() > n {
			n = ev.GetAttempt()
		}
	}
	return n
}

func wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

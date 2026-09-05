// Package flow is a framework for durable execution: functions that run
// somewhere, and runs that call them and can be replayed.
//
// A function is declared once with [Define] and called like any other:
//
//	var Digest = flow.Define("digest", func(ctx flow.Context, w Work) (Result, error) { ... })
//
// Where a call runs is decided by the context, not by the call. Inside a
// [Run] every call is written down before it happens and its answer is written
// down after, so a run that fails part-way can be run again and will not
// repeat the work it already paid for — it replays the history it has and
// carries on from the end of it. The calls themselves go to the run's
// [Executor]: [Local] runs them in this process, and a cluster runs them on
// its workers. Nothing in the body below names either:
//
//	err := flow.Run(ctx, "pipeline", func(ctx flow.Context) error {
//		first, err := Digest(ctx, head)      // dispatched, recorded, replayed on a retry
//		if err != nil {
//			return err
//		}
//		rest, err := ctx.Map(Digest, tail)   // the same, once per input, in parallel
//		...
//	}, flow.WithStore(store))
//
// The context a body is given is a [Context]: a context.Context with this
// package's operations as methods on it, so what a run can do is what its
// context can do. A running function is given one too.
//
// # What you give up
//
// Replay means the run's body is re-executed from the top on every attempt,
// so IT MUST BE DETERMINISTIC. Everything it decides must come from its input
// or from something the history recorded:
//
//   - use [Context.Now], not time.Now
//   - use [Context.Sleep], not time.Sleep
//   - do not read a random number, an environment variable, or a clock
//   - do not let map iteration order change what it does
//   - use [Context.Map], [Context.Go] or [Context.Spawn] to do things at
//     once, never a bare goroutine
//
// A run that breaks these does not fail loudly on the first attempt — it fails
// on the retry, as a continuity error, which is why [IsContinuity] names the
// cause plainly. The functions a run calls are under no such constraint: each
// runs once, wherever the executor puts it, and may do anything.
//
// # Threads and channels
//
// [Context.Spawn] runs a piece of run code on a thread of its own, and
// [Channel] passes typed values between threads. Both are the replayable versions of
// things Go already has, and a channel is where the difference shows: a Go
// receive takes whichever value happens to arrive first, and nothing about the
// run decides which that is. So a receive is RECORDED — which thread's which
// send it took — and a replay waits for exactly that item instead. A send
// needs no such treatment, since a thread's nth send always carries the same
// value; what a send records is when it COMPLETED, which on an unbuffered
// channel is somebody else's decision.
//
// A run that deadlocks on a channel deadlocks the same way a Go program would;
// there is no scheduler here doing anything clever about it.
//
// # Long calls
//
// A function that takes a long time reports where it has got to:
// [Context.Step] for coarse phases, [Context.Heartbeat] for a position inside
// one. An executor that can
// lose a machine uses those reports to run the call again elsewhere from
// where it was, rather than from nothing; an executor that cannot ignores
// them. The function is written the same way either way.
//
// A run adds a second way for a long call to be interrupted: the run itself
// can fail and be retried while the call is still in flight. When the retry
// reaches that call again it hands the executor the same origin, and an
// executor that recognises one REJOINS the call already running rather than
// starting a second copy.
//
// # What it is not
//
// The run's body executes in the process that called Run, and only the calls
// it makes go to the executor. That is the right trade for fanning work out of
// one program, and the wrong one for orchestration that must outlive any
// single machine.
package flow

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ligustah/wings/flow/protos"
)

// Run executes body as a durable run named name, to completion.
//
// name is the run's identity in the store: run the same name again and it
// resumes rather than restarts, and a run that already completed returns at
// once without executing anything. Choose a name that says what the run is
// about, since that is what makes a resumed run find its history; [NewName]
// mints an arbitrary one.
//
// A run that fails is retried with backoff until it succeeds, returns a
// [Permanent] error, lets a call's failure through (see [IsCallFailure]), or
// runs out of attempts. A run that suspends is waited for. Both of those are
// why this can take a long time and why ctx matters.
func Run(ctx context.Context, name string, body func(ctx Context) error, opts ...RunOption) error {
	ro := newRunOptions(opts)

	if name == "" {
		return errors.New("flow: Run requires a name; use flow.NewName for an arbitrary one")
	}
	if body == nil {
		return errors.New("flow: Run requires a body")
	}
	if ro.store == nil {
		return errors.New("flow: Run requires a Store; pass flow.WithStore(flow.NewStore(...)) " +
			"or flow.WithStore(flow.NewMemStore())")
	}

	history, err := ro.store.Events(ctx, name)
	if err != nil {
		return err
	}

	// A finished run is finished. Re-running it would repeat every effect its
	// calls had, which is the opposite of what a durable run is for.
	if done, result, ok := finished(history); ok {
		if done == protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
			return nil
		}
		_, err := unpackResult(result)
		return fmt.Errorf("flow: run %s already failed permanently: %w", name, err)
	}

	sink, err := ro.store.Sink(ctx, name)
	if err != nil {
		return err
	}

	r := &runner{name: name, body: body, opts: ro, sink: sink}

	attempt := lastAttempt(history)
	for {
		attempt++

		status, runErr := r.attempt(ctx, history, attempt)

		switch status {
		case protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:
			return nil

		case protos.WorkflowStatus_WORKFLOW_STATUS_FAILED:
			return runErr

		case protos.WorkflowStatus_WORKFLOW_STATUS_SUSPENDED:
			_, until := IsSuspended(runErr)
			if err := wait(ctx, time.Until(until)); err != nil {
				return err
			}

		default: // backoff
			if attempt >= uint64(ro.maxAttempts) {
				return fmt.Errorf("flow: run %s failed after %d attempts: %w", name, attempt, runErr)
			}
			if err := wait(ctx, r.backoff(attempt)); err != nil {
				return err
			}
		}

		// The next attempt replays everything recorded so far, including what
		// this one managed to do before it stopped.
		if history, err = ro.store.Events(ctx, name); err != nil {
			return err
		}
	}
}

// runner is one Run in progress: its body, its options and its sink.
type runner struct {
	name string
	body func(ctx Context) error
	opts runOptions
	sink Sink
}

// attempt runs the body once over the history it is given.
func (r *runner) attempt(ctx context.Context, history []*protos.Event, attempt uint64) (protos.WorkflowStatus, error) {
	run := &runState{
		name:    r.name,
		attempt: attempt,
		threads: threadsOf(history),
		sink:    r.sink,
		exec:    r.opts.executor,
	}
	main := &threadState{id: mainThread, run: run}
	// Whatever a thread of this attempt does after it returns is this attempt's
	// business and not the record's.
	defer run.finish()

	reason := protos.StartReason_START_REASON_INIT
	if len(history) > 0 {
		reason = protos.StartReason_START_REASON_RETRY
	}
	// Recorded on the run's own bookkeeping rather than main's cursor: a start
	// marker is about the attempt, not about what the body did, and putting it
	// in main's sequence would shift every replay position by one.
	appendMarker(run, &protos.RunStartEvent{
		Attempt:      attempt,
		Reason:       reason,
		WorkflowName: r.name,
		InstanceId:   r.name,
		Version:      uint64(r.opts.version),
	})

	err := r.body(Context{withThread(ctx, main)})

	if perr := run.err(); perr != nil {
		// Persistence failed somewhere in there. Not retryable in any useful
		// sense: the next attempt would replay an incomplete history.
		appendMarker(run, &protos.RunEndEvent{
			Status: protos.WorkflowStatus_WORKFLOW_STATUS_FAILED,
			Result: packResult(nil, perr),
		})
		return protos.WorkflowStatus_WORKFLOW_STATUS_FAILED, perr
	}

	status := r.classify(err)

	end := &protos.RunEndEvent{Status: status, Result: packResult(nil, err)}
	if status == protos.WorkflowStatus_WORKFLOW_STATUS_SUSPENDED {
		if _, until := IsSuspended(err); !until.IsZero() {
			end.ScheduledFor = timestamppb.New(until)
		}
	}
	appendMarker(run, end)
	return status, err
}

// classify decides what an error the body returned means for the next
// attempt.
//
// Retry is the default and the other answers are the exceptions, because the
// errors that dominate distributed work are transient. A continuity error is
// never retried: a retry replays the same history against the same code and
// reaches the same contradiction. Nor is the failure of a call the body did
// not handle: the call's failure is recorded, a retry replays it, and the body
// — deterministic — fails the same way again, which used to be ten attempts
// and four minutes of backoff to reach the answer the first one had.
func (r *runner) classify(err error) protos.WorkflowStatus {
	switch {
	case err == nil:
		return protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED
	case IsContinuity(err), IsPermanent(err), IsCallFailure(err):
		return protos.WorkflowStatus_WORKFLOW_STATUS_FAILED
	}
	if ok, _ := IsSuspended(err); ok {
		return protos.WorkflowStatus_WORKFLOW_STATUS_SUSPENDED
	}
	for _, target := range r.opts.permanent {
		if errors.Is(err, target) {
			return protos.WorkflowStatus_WORKFLOW_STATUS_FAILED
		}
	}
	return protos.WorkflowStatus_WORKFLOW_STATUS_BACKOFF
}

func (r *runner) backoff(attempt uint64) time.Duration {
	delay := time.Duration(float64(r.opts.initialDelay) * math.Pow(2, float64(attempt-1)))
	if delay > r.opts.maxDelay || delay <= 0 {
		return r.opts.maxDelay
	}
	return delay
}

// call records one function call on this thread, replays its result if the
// history already has one, and otherwise hands it to the executor and records
// what came back.
//
// The pair of events matters. A call event alone says "this was attempted";
// the return event says "and this is what it produced". A run that died
// between them replays the call event, finds no return, and does the work
// again — which is the right answer, because nobody can say whether it
// finished — carrying the same origin, so an executor that can recognise the
// call still in flight rejoins it.
func (t *threadState) call(ctx context.Context, name string, payload []byte) ([]byte, error) {
	call, err := t.expect[*protos.CallEvent]()
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
		// Compared as ENCODED bytes rather than as Go values: the payload is
		// already encoded here, and two calls are the same call exactly when
		// they put the same bytes on the wire.
		if stored := call.GetParams().GetSerialized(); !bytesEqual(stored, payload) {
			return nil, continuityf("thread %q previously called %q with different arguments "+
				"(%d bytes recorded, %d bytes now)", t.id, name, len(stored), len(payload))
		}

		ret, err := t.expect[*protos.ReturnEvent]()
		if err != nil {
			return nil, err
		}
		if ret != nil {
			// Already done, and this is what it produced. Nothing runs.
			out, err := unpackResult(ret.GetResult())
			if err != nil {
				return nil, &callError{name: name, err: err}
			}
			return out, nil
		}
		// Attempted but never finished: fall through and do it again.
	} else {
		t.record(&protos.CallEvent{
			Name:   name,
			Params: &protos.Data{Serialized: payload},
		})
	}

	if err := t.run.err(); err != nil {
		return nil, err
	}

	// Tell whatever runs this which step of which run it is. The cursor now
	// sits just past the call event, so the call's own position is one back —
	// and that is the number a reader of an executor's record can find in the
	// history.
	out, callErr := t.run.exec.Invoke(WithOrigin(ctx, Origin{
		Run:     t.run.name,
		Thread:  t.id,
		Step:    t.at() - 1,
		Attempt: t.run.attempt,
	}), name, payload)

	// A call cut short by the caller's own context did not fail; it was
	// interrupted, and recording an interruption as the call's answer would
	// have the next attempt — the restart the interruption is usually followed
	// by — replay a failure that never happened. The call event stays without
	// a return, which is what makes that attempt do the call again.
	if callErr != nil && ctx.Err() != nil {
		return nil, callErr
	}

	// Recorded either way. A failed call is a fact about the run, and one that
	// a retry must not repeat blindly — the error is what the next attempt
	// replays.
	t.record(&protos.ReturnEvent{Result: packResult(out, callErr)})
	if callErr != nil {
		return nil, &callError{name: name, err: callErr}
	}
	return out, t.run.err()
}

// callError is the failure of a call, as the run's body sees it.
//
// Its own type so the run can tell a failure that is recorded — and would be
// replayed — from one in the body's own logic that a retry might not repeat.
// The message is the call's own, unadorned, so a body matching on it sees
// what the function said.
type callError struct {
	name string
	err  error
}

func (e *callError) Error() string { return e.err.Error() }
func (e *callError) Unwrap() error { return e.err }

// IsCallFailure reports whether err is, or wraps, the failure of a call the
// run made. A run that returns one is not retried, since the failure is on
// record and a retry would replay it; handle the error in the body instead
// if the run can go on without that call.
func IsCallFailure(err error) bool {
	_, ok := errors.AsType[*callError](err)
	return ok
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
		// boundary. Match on content, not identity.
		return nil, errors.New(e.GetMessage())
	}
	return r.GetData().GetSerialized(), nil
}

// appendMarker records a run-level event, which belongs to no thread.
//
// Kept out of every thread's sequence deliberately: replay walks a thread's
// events in order and compares each to what the body is doing, and a start
// marker is not something the body did.
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
// Run-level markers have no thread and are skipped, which is what keeps them
// out of the positions replay compares against.
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

// finished reports the terminal outcome of a run, if it reached one.
func finished(history []*protos.Event) (protos.WorkflowStatus, *protos.Result, bool) {
	for _, h := range slices.Backward(history) {
		end := h.GetRunEnd()
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

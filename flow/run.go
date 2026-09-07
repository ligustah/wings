// Package flow is a framework for durable execution: functions that run
// somewhere, and runs that call them and can be replayed.
//
// A function is declared once with [Define] and called like any other; where it
// runs is decided by the context. Inside a [Run] every call is recorded before
// it happens and its result after, so a run that fails part-way replays its
// history and carries on rather than repeating work. A direct call runs in
// place; one forked with [Context.Go] or [Context.Map] is a thread, placed by
// the run's [Placer].
//
//	var Digest = flow.Define(func(ctx flow.Context, w Work) (Result, error) { ... })
//
//	err := flow.Run(ctx, "pipeline", func(ctx flow.Context) error {
//		first, err := Digest(ctx, head)      // here, recorded, replayed on a retry
//		if err != nil {
//			return err
//		}
//		rest, err := ctx.Map(Digest, tail)   // one thread per input, in parallel
//		...
//	}, flow.WithStore(store))
//
// # Threads
//
// A thread is the unit of work. The body is the main thread; every fork makes
// another, each with its own history on its own stream, which is what lets a
// thread run on another machine. The parent's history holds the fork and later
// the join; a replay that finds the join never reruns the thread.
//
// # Determinism
//
// A thread's body is re-executed on every attempt, so it must be deterministic —
// everything it decides must come from its input or its recorded history:
//
//   - use [Context.Now], not time.Now
//   - use [Context.Sleep], not time.Sleep
//   - wrap other nondeterministic reads in [Context.Effect]
//   - fork with [Context.Map], [Context.Go] or [Context.Spawn], never a goroutine
//
// A run that breaks these fails on the retry as a continuity error; see
// [IsContinuity].
//
// # Channels
//
// [Context.Spawn] runs run code on its own thread, and [Channel] passes typed
// values between threads. A receive is recorded — which send it took — so a
// replay takes the same value; a send records only when it completed.
//
// # Long calls
//
// A long function reports progress with [Context.Heartbeat] and reads it back
// with [Context.Checkpoint], so an executor that loses a machine resumes the call
// elsewhere from where it was. A parent retried while such a thread runs hands
// the placer the same [Thread], which a placer can rejoin rather than restart.
//
// # What it is not
//
// The run body executes in the process that called [Run]; only forked threads go
// to the placer. Good for fanning work out of one program, wrong for
// orchestration that must outlive any single machine.
package flow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ligustah/wings/flow/protos"
)

// Run executes body as a durable run named name, to completion. name is the
// run's identity in the store: run the same name again and it resumes rather
// than restarts, and a completed run returns at once. [NewName] mints an
// arbitrary name. A failed run is retried with backoff until it succeeds,
// returns a [Permanent] error, lets a call failure through (see [IsCallFailure]),
// or runs out of attempts; a suspended run is waited for.
func Run(ctx context.Context, name string, body func(ctx Context) error, opts ...RunOption) error {
	if body == nil {
		return errors.New("flow: Run requires a body")
	}
	_, err := execute(ctx, name, mainThread, "", nil,
		func(ctx Context) ([]byte, error) { return nil, body(ctx) }, opts)
	return err
}

// Replay re-runs body against the recorded history of run name, doing no new
// work and writing nothing. It returns a continuity error if the body diverges
// from what was recorded, so a completed run that replays clean is proof the
// body is deterministic. For tests; see package
// [github.com/ligustah/wings/flow/flowtest].
func Replay(ctx context.Context, name string, body func(ctx Context) error, opts ...RunOption) error {
	if body == nil {
		return errors.New("flow: Replay requires a body")
	}
	ensureNamesResolved()
	ro := newRunOptions(opts)
	if name == "" {
		return errors.New("flow: Replay requires a run name")
	}
	if ro.store == nil {
		return errors.New("flow: Replay requires a Store")
	}
	ro.rootID = mainThread
	r := &threadRunner{
		name: name, id: mainThread, opts: ro, top: true, readonly: true, replay: true,
		body: func(ctx Context) ([]byte, error) { return nil, body(ctx) },
	}
	_, err := r.execute(ctx)
	return err
}

// RunCall executes the function defined under fn, on payload, as a durable run
// named name, and returns its encoded output — [Execute] made durable, replayed
// rather than repeated on a retry. For executors that have storage of their own.
func RunCall(ctx context.Context, name, fn string, payload []byte, opts ...RunOption) ([]byte, error) {
	return RunThread(ctx, name, mainThread, fn, payload, opts...)
}

// RunThread executes thread thread of run run: the function defined under fn, on
// payload, returning its encoded output. For placers handed a forked thread that
// have its history in their store; the thread resumes from where it stopped, and
// the threads it forks go to this run's placer.
func RunThread(ctx context.Context, run, thread, fn string, payload []byte, opts ...RunOption) ([]byte, error) {
	if fn == "" {
		return nil, errors.New("flow: RunThread requires a function name")
	}
	if thread == "" {
		return nil, errors.New("flow: RunThread requires a thread name")
	}
	return execute(ctx, run, thread, fn, payload, functionBody(fn, payload), opts)
}

// functionBody is the body of a thread that runs one function, with its error
// marked as the function's own answer (a call failure, recorded and replayed)
// rather than a transient the thread retries.
func functionBody(fn string, input []byte) func(ctx Context) ([]byte, error) {
	return func(ctx Context) ([]byte, error) {
		out, err := Execute(ctx, fn, input)
		if err != nil {
			// A suspension is not the function's answer; it says when to ask again.
			if ok, _ := IsSuspended(err); ok {
				return nil, err
			}
			return nil, &callError{name: fn, err: err}
		}
		return out, nil
	}
}

// execute runs one thread of a run in a process where it has no parent.
func execute(ctx context.Context, run, thread, fn string, input []byte, body func(ctx Context) ([]byte, error), opts []RunOption) ([]byte, error) {
	// Settle any names the build step did not supply before the body runs.
	ensureNamesResolved()
	ro := newRunOptions(opts)

	if run == "" {
		return nil, errors.New("flow: Run requires a name; use flow.NewName for an arbitrary one")
	}
	if ro.store == nil {
		return nil, errors.New("flow: Run requires a Store; pass flow.WithStore(flow.NewStore(...)) " +
			"or flow.WithStore(flow.NewMemStore())")
	}
	ro.rootID = thread
	if !ro.root.known() && fn != "" {
		ro.root = Root{Function: fn, Input: input}
	}
	r := &threadRunner{name: run, id: thread, fn: fn, input: input, body: body, opts: ro, top: true}
	if thread == mainThread {
		r.input = ro.input
		r.inputType = ro.inputType
	}
	return r.execute(ctx)
}

// threadRunner is one thread being run to completion.
type threadRunner struct {
	// run is the state shared with the parent when it is in this process; nil for
	// a thread with no parent here, which makes its own per attempt.
	run *runState

	name string // the run
	id   string // the thread
	fn   string // the function the thread runs, "" for a body of run code
	body func(ctx Context) ([]byte, error)
	opts runOptions

	// input is the body's input, recorded; inputType what it expects, when known.
	input     []byte
	inputType reflect.Type

	// top says this is the thread the process was asked to run, the one [Once] is
	// about. readonly says the thread is replayed only; see lineage.go. replay is
	// a readonly pass over a finished thread's whole history, to check it; see
	// [Replay].
	top      bool
	readonly bool
	replay   bool
}

// execute runs the thread to completion, attempt after attempt over its history.
func (r *threadRunner) execute(ctx context.Context) ([]byte, error) {
	store := r.opts.store

	history, err := store.Events(ctx, r.name, r.id)
	if err != nil {
		return nil, err
	}

	// A thread's input is fixed by its first attempt; later attempts must get the
	// same. Checked before the finished short-cut, so a finished run handed
	// different input is not answered with silence.
	if recorded, ok := recordedInput(history); ok {
		if r.input != nil && !bytes.Equal(r.input, recorded) {
			return nil, fmt.Errorf("flow: run %s was started with different input; "+
				"a run's input is fixed by its first attempt, so leave it off to resume or use a new name", r.name)
		}
		r.input = recorded
	} else if r.input == nil && r.inputType != nil {
		return nil, fmt.Errorf("flow: run %s takes a %s as input and none was given; it looks like %s",
			r.name, r.inputType, exampleInput(r.inputType))
	}

	// A finished thread is finished; what it produced is on record and is the
	// answer.
	if done, result, ok := finished(history); ok {
		switch {
		case r.replay:
			// Re-run read-only over the whole history to check it replays; fall
			// through to the attempt loop.
		case r.readonly:
			return nil, fmt.Errorf("flow: %s has already finished; there is nothing left to fork", r.describe())
		default:
			out, err := unpackResult(result)
			if done == protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
				return out, nil
			}
			return nil, fmt.Errorf("flow: %s already failed permanently: %w", r.describe(), err)
		}
	}

	var sink Sink
	if !r.readonly {
		if sink, err = store.Sink(ctx, r.name, r.id); err != nil {
			return nil, err
		}
	}

	attempt := lastAttempt(history)
	for {
		attempt++

		out, status, runErr := r.attempt(ctx, history, attempt, sink)

		if r.readonly {
			// One pass over the history is all a replay is.
			if status == protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
				return out, nil
			}
			return nil, runErr
		}

		switch status {
		case protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:
			return out, nil

		case protos.WorkflowStatus_WORKFLOW_STATUS_FAILED:
			return nil, runErr

		case protos.WorkflowStatus_WORKFLOW_STATUS_SUSPENDED:
			if r.opts.once && r.top {
				return nil, runErr // somebody else decides when to resume; see IsSuspended
			}
			_, until := IsSuspended(runErr)
			if err := wait(ctx, r.opts.clock, until.Sub(r.opts.clock.Now())); err != nil {
				return nil, err
			}

		default: // backoff
			if r.opts.once && r.top {
				return nil, runErr // somebody else decides about retries
			}
			if attempt >= uint64(r.opts.maxAttempts) {
				return nil, fmt.Errorf("flow: %s failed after %d attempts: %w", r.describe(), attempt, runErr)
			}
			if err := wait(ctx, r.opts.clock, r.backoff(attempt)); err != nil {
				return nil, err
			}
		}

		// The next attempt replays everything recorded so far.
		if history, err = store.Events(ctx, r.name, r.id); err != nil {
			return nil, err
		}
	}
}

// describe names the thread in an error: the run alone for main.
func (r *threadRunner) describe() string {
	if r.id == mainThread {
		return "run " + r.name
	}
	return "thread " + r.id + " of run " + r.name
}

// attempt runs the body once over the given history and returns what it produced
// and what became of it.
func (r *threadRunner) attempt(ctx context.Context, history []*protos.Event, attempt uint64, sink Sink) ([]byte, protos.WorkflowStatus, error) {
	run := r.run
	if run == nil {
		run = newRunState(r.name, r.opts)
		// Cancel forked threads when this attempt ends, so the next attempt does
		// not race them.
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		defer run.finish()
		defer cancel()
	}
	t := &threadState{
		id:       r.id,
		attempt:  attempt,
		run:      run,
		events:   replayable(history),
		sink:     sink,
		readonly: r.readonly,
		ctx:      ctx,
	}

	reason := protos.StartReason_START_REASON_INIT
	if len(history) > 0 {
		reason = protos.StartReason_START_REASON_RETRY
	}
	start := &protos.RunStartEvent{
		Attempt:      attempt,
		Reason:       reason,
		WorkflowName: r.name,
		InstanceId:   r.id,
		Version:      uint64(r.opts.version),
		Function:     r.fn,
	}
	if r.input != nil {
		start.Input = &protos.Data{Serialized: r.input}
	}
	t.marker(start)

	out, err := r.body(Context{withThread(withInput(ctx, r.input), t)})

	if perr := t.err(); perr != nil {
		// Persistence failed: the next attempt would replay an incomplete history.
		t.marker(&protos.RunEndEvent{
			Status: protos.WorkflowStatus_WORKFLOW_STATUS_FAILED,
			Result: packResult(nil, perr),
		})
		return nil, protos.WorkflowStatus_WORKFLOW_STATUS_FAILED, perr
	}

	status := r.classify(ctx, err)

	end := &protos.RunEndEvent{Status: status, Result: packResult(out, err)}
	if status == protos.WorkflowStatus_WORKFLOW_STATUS_SUSPENDED {
		if _, until := IsSuspended(err); !until.IsZero() {
			end.ScheduledFor = timestamppb.New(until)
		}
	}
	t.marker(end)
	return out, status, err
}

// classify decides what an error the body returned means for the next attempt.
// Retry is the default; continuity, permanent, and unhandled call failures are
// fatal; a suspension suspends. An attempt whose own context ended was
// interrupted, not failed, and backs off to be resumed.
func (r *threadRunner) classify(ctx context.Context, err error) protos.WorkflowStatus {
	switch {
	case err == nil:
		return protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED
	case ctx.Err() != nil:
		return protos.WorkflowStatus_WORKFLOW_STATUS_BACKOFF
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

func (r *threadRunner) backoff(attempt uint64) time.Duration {
	delay := time.Duration(float64(r.opts.initialDelay) * math.Pow(2, float64(attempt-1)))
	if delay > r.opts.maxDelay || delay <= 0 {
		return r.opts.maxDelay
	}
	return delay
}

// call records a function call on this thread, replays its result if the history
// has one, or hands it to the executor and records what came back. A run that
// died between the call event and the return replays the call, finds no return,
// and does the work again under the same origin, so an executor can rejoin a
// call still in flight.
func (t *threadState) call(ctx context.Context, name string, payload []byte) ([]byte, error) {
	call, err := t.expect[*protos.CallEvent]()
	if err != nil {
		return nil, err
	}

	if call != nil {
		// Replaying: the call must be the same one, or the code changed under a
		// live run.
		if call.GetName() != name {
			return nil, continuityf("thread %q previously called %q at this point, but is now calling %q",
				t.id, call.GetName(), name)
		}
		// Compared as encoded bytes: two calls are the same when they put the same
		// bytes on the wire.
		if stored := call.GetParams().GetSerialized(); !bytesEqual(stored, payload) {
			return nil, continuityf("thread %q previously called %q with different arguments "+
				"(%d bytes recorded, %d bytes now)", t.id, name, len(stored), len(payload))
		}

		ret, err := t.expect[*protos.ReturnEvent]()
		if err != nil {
			return nil, err
		}
		if ret != nil {
			// Already done; nothing runs.
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

	if err := t.err(); err != nil {
		return nil, err
	}

	// The call.s position is one back from the cursor; an executor records it.
	step := t.at() - 1
	out, callErr := t.run.exec.Invoke(WithOrigin(ctx, Origin{
		Run:     t.run.name,
		Thread:  t.id,
		Step:    step,
		Attempt: t.attempt,
	}), name, payload)

	// A call cut short by the caller.s context is not its answer; the call event
	// stays without a return so the next attempt does it again.
	if callErr != nil && ctx.Err() != nil {
		return nil, callErr
	}

	// Recorded either way, named for its call: a failed call is a fact the next
	// attempt replays.
	t.record(&protos.ReturnEvent{Result: packResult(out, callErr), CallSerial: step})
	if callErr != nil {
		return nil, &callError{name: name, err: callErr}
	}
	return out, t.err()
}

// callError is the failure of a call the run made or a thread it forked — its
// own type so the run can tell a recorded, replayed failure from one in the
// body's own logic. Its message is the call's own.
type callError struct {
	name string
	err  error
}

func (e *callError) Error() string { return e.err.Error() }
func (e *callError) Unwrap() error { return e.err }

// IsCallFailure reports whether err is, or wraps, the failure of a call the run
// made or a thread it forked. A run that returns one is not retried; handle it in
// the body if the run can go on without that call.
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
		// Only the message survives a process boundary; match on content.
		return nil, errors.New(e.GetMessage())
	}
	return r.GetData().GetSerialized(), nil
}

// isMarker reports whether an event is about a thread's attempt rather than what
// its body did — the events replay does not compare against.
func isMarker(ev *protos.Event) bool {
	return ev.GetRunStart() != nil || ev.GetRunEnd() != nil
}

// replayable is a thread's history without its attempt markers.
func replayable(history []*protos.Event) []*protos.Event {
	var out []*protos.Event
	for _, ev := range history {
		if !isMarker(ev) {
			out = append(out, ev)
		}
	}
	return out
}

// finished reports the terminal outcome of a thread, if it reached one.
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

// recordedInput is the input the thread's first attempt was given, if any.
func recordedInput(history []*protos.Event) ([]byte, bool) {
	for _, ev := range history {
		if start := ev.GetRunStart(); start != nil {
			return start.GetInput().GetSerialized(), start.GetInput() != nil
		}
	}
	return nil, false
}

type inputKey struct{}

// withInput puts the run's input where the body can find it.
func withInput(ctx context.Context, input []byte) context.Context {
	return context.WithValue(ctx, inputKey{}, input)
}

func inputFrom(ctx context.Context) []byte {
	b, _ := ctx.Value(inputKey{}).([]byte)
	return b
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

func wait(ctx context.Context, clk Clock, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	select {
	case <-clk.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

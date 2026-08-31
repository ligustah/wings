package flow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ligustah/wings/flow/protos"
)

// mainThread is the thread the workflow function itself runs on. Forked threads
// are named "<parent>.<n>", so a thread's name is also its lineage.
const mainThread = "main"

// runState is one attempt of one workflow, shared by every thread in it.
//
// The mutex is not decoration: forked threads run concurrently and all of them
// append to this run. What it protects is the log, not the ORDER of the log —
// each thread has its own sequence, and threads never write to each other's.
// That separation is the whole reason concurrency and replay can coexist here.
type runState struct {
	name     string
	instance string
	attempt  uint64

	mu      sync.Mutex
	threads map[string][]*protos.Event
	sink    Sink
	sinkErr error // the first persistence failure, if any
}

// threadState is one thread's cursor through the log.
//
// serial is the position replay has reached. Everything before it has been
// matched against history; at or after it is either still to be matched or has
// not happened yet.
type threadState struct {
	id     string
	serial uint64

	// counter names the next child thread. Deterministic by construction: the
	// nth fork a thread performs is always "<id>.<n>", whatever order the
	// children are scheduled in.
	counter uint64

	run *runState
}

type ctxKey struct{}

func withThread(ctx context.Context, t *threadState) context.Context {
	return context.WithValue(ctx, ctxKey{}, t)
}

// threadFrom returns the thread bound to ctx, or nil when ctx is not inside a
// workflow.
func threadFrom(ctx context.Context) *threadState {
	t, _ := ctx.Value(ctxKey{}).(*threadState)
	return t
}

// peek returns the event at the cursor without consuming it, or nil once replay
// has caught up with history and the workflow is in new territory.
func (t *threadState) peek() *protos.Event {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	evs := t.run.threads[t.id]
	if t.serial < uint64(len(evs)) {
		return evs[t.serial]
	}
	return nil
}

// expect consumes the event at the cursor and asserts its payload type.
//
// A nil result means nothing is recorded there yet, which is the signal to do
// the thing for real. A recorded event of the wrong type is a continuity error,
// and this is where nearly all of them are caught: the workflow has reached a
// point where last time it made a call and this time it wants to sleep, which
// means the code changed underneath a live run.
func expect[E protos.Events](t *threadState) (E, error) {
	var zero E

	ev := t.peek()
	if ev == nil {
		return zero, nil
	}

	payload, ok := protos.UnpackEventPayload(ev).(E)
	if !ok {
		return zero, continuityf("at position %d of thread %q the history has a %s, but the workflow is now doing a %s",
			t.serial, t.id, protos.EventType(ev), payloadName[E]())
	}

	t.run.mu.Lock()
	t.serial++
	t.run.mu.Unlock()
	return payload, nil
}

// record appends an event to this thread and hands it to the sink.
//
// The append is what makes the workflow durable, so a sink failure is kept and
// fails the run: a run whose history was not written down cannot be replayed,
// and carrying on as though it could is the one outcome worse than stopping.
//
// A free function rather than a method because it is generic in the payload,
// and because every caller already has the thread in hand.
func record[E protos.Events](t *threadState, payload E) *protos.Event {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()

	ev := &protos.Event{
		Timestamp: timestamppb.New(time.Now().Truncate(time.Microsecond)),
		Serial:    t.serial,
		Attempt:   t.run.attempt,
		ThreadId:  t.id,
		Payload:   protos.PackEventPayload(payload),
	}
	t.run.threads[t.id] = append(t.run.threads[t.id], ev)
	t.serial++

	if t.run.sink != nil && t.run.sinkErr == nil {
		// Background, not the workflow's context: an event describing what has
		// already happened must be written even while the run is being torn
		// down, or the history stops exactly where it is most interesting.
		if err := t.run.sink.Append(context.Background(), ev); err != nil {
			t.run.sinkErr = fmt.Errorf("flow: persist event: %w", err)
		}
	}
	return ev
}

// fork creates a child thread, or re-adopts the one a previous attempt created.
//
// Re-adoption is what makes a fork replayable: the child's name comes from the
// parent's fork counter, not from when it started, so the same code produces the
// same names in the same order however the goroutines happened to be scheduled.
func (t *threadState) fork() *threadState {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()

	id := fmt.Sprintf("%s.%d", t.id, t.counter)
	t.counter++
	if _, ok := t.run.threads[id]; !ok {
		t.run.threads[id] = nil
	}
	return &threadState{id: id, run: t.run}
}

// recordFork consumes the fork already in history, or records a new one.
//
// Consuming matters. Forks and joins are events like any other, and a replay
// that appends them again grows the history by a fork and a join per parallel
// call per attempt — and then the log claims the workflow forked more threads
// than it did, which is a lie told to whoever reads it after a failure.
func recordFork(parent, child *threadState) error {
	ev, err := expect[*protos.ForkEvent](parent)
	if err != nil {
		return err
	}
	if ev != nil {
		if ev.GetThreadId() != child.id {
			return continuityf("thread %q previously forked %q at this point, but is now forking %q",
				parent.id, ev.GetThreadId(), child.id)
		}
		return parent.run.err()
	}
	record(parent, &protos.ForkEvent{ParentThreadId: parent.id, ThreadId: child.id})
	return parent.run.err()
}

// recordJoin is recordFork for the other end.
func recordJoin(parent, child *threadState) error {
	ev, err := expect[*protos.JoinEvent](parent)
	if err != nil {
		return err
	}
	if ev != nil {
		if ev.GetThreadId() != child.id {
			return continuityf("thread %q previously joined %q at this point, but is now joining %q",
				parent.id, ev.GetThreadId(), child.id)
		}
		return parent.run.err()
	}
	record(parent, &protos.JoinEvent{ThreadId: child.id})
	return parent.run.err()
}

// err reports the first persistence failure of the run, if any.
func (r *runState) err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sinkErr
}

// payloadName is the message name of an event payload type, for error messages.
func payloadName[E protos.Events]() string {
	var e E
	return protos.EventType(&protos.Event{Payload: protos.PackEventPayload(e)})
}

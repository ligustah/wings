package flow

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ligustah/wings/flow/protos"
)

// mainThread is the thread the run's body itself runs on. Forked threads are
// named "<parent>.<n>", so a thread's name is also its lineage.
const mainThread = "main"

// runState is what the threads of one run share while they run in one
// process: the run's name, where its work goes, and the channels between
// them.
//
// Made once per attempt of the main thread. The threads main forks belong to
// that attempt — they are cancelled when it ends, and the next attempt forks
// them again from its history — so the state they share ends with it too.
// What does NOT live here is any thread's history: each thread has its own,
// on its own stream, and threads never write to each other's. That
// separation is the whole reason a thread can run on another machine.
type runState struct {
	name   string
	store  Store
	exec   Executor
	placer Placer
	parker Parker
	opts   runOptions
	// host carries channels to and from other runs; nil for a run that
	// shares none. linkCtx bounds the links, and ends with the attempt.
	host     ChannelHost
	linkCtx  context.Context
	linkStop context.CancelFunc

	mu       sync.Mutex
	channels map[string]*chanState
	over     bool // this attempt has returned

	// encMu serialises the run's encodes, and encoder is the thread whose
	// encode is under way. See encoding.
	encMu   sync.Mutex
	encoder *threadState
}

func newRunState(name string, opts runOptions) *runState {
	return &runState{
		name:   name,
		store:  opts.store,
		exec:   opts.executor,
		placer: opts.placer,
		parker: opts.parker,
		opts:   opts,
		host:   opts.host,
	}
}

// finish marks an attempt over, after which nothing more of it is written down.
//
// A forked thread can outlive the attempt that made it by a moment — the
// attempt's context is cancelled, but the thread's goroutine has to notice —
// and when it finally returns it records what it found. Writing that into a
// history whose NEXT attempt is already under way is at best noise and at
// worst a stale answer landing among fresh ones. The thread's own bookkeeping
// is left alone; only the durable record is closed.
func (r *runState) finish() {
	r.mu.Lock()
	r.over = true
	r.mu.Unlock()
	r.closeLinks()
}

// declareChannel registers a channel's runtime the first time it is created.
//
// Idempotent, because a replay creates the same channels again and finding the
// existing one is the whole point: values a re-run sender puts on it have to
// reach a re-run receiver through the same queue.
func (r *runState) declareChannel(name string, capacity int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.channels == nil {
		r.channels = map[string]*chanState{}
	}
	if _, ok := r.channels[name]; !ok {
		r.channels[name] = newChanState(capacity)
	}
}

// channel returns a declared channel's runtime, or nil.
func (r *runState) channel(name string) *chanState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channels[name]
}

// threadState is one attempt of one thread: its history, and a cursor
// through it.
//
// serial is the position replay has reached. Everything before it has been
// matched against history; at or after it is either still to be matched or has
// not happened yet.
type threadState struct {
	id      string
	attempt uint64
	run     *runState

	// events is this thread's history, minus the attempt markers, with what
	// this attempt records appended as it goes. Guarded by run.mu, because
	// the thread's own goroutine is not the only one that reads it: a fork
	// looks ahead in the parent's history from the goroutine placing the
	// child.
	events []*protos.Event
	serial uint64

	sink    Sink
	sinkErr error // the first persistence failure, if any

	// counter names the next child thread. Deterministic by construction: the
	// nth fork a thread performs is always "<id>.<n>", whatever order the
	// children are scheduled in.
	counter uint64

	// channels names the next channel this thread creates, on the same
	// principle as counter; sends counts this thread's sends per channel, so a
	// receive on another thread can name exactly one of them, and recvs its
	// receives, so a host can name exactly one of those.
	channels uint64
	sends    map[string]uint64
	recvs    map[string]uint64
}

// newChannelName mints the next channel name for this thread.
//
// Derived from the thread rather than given by the caller, for the same reason
// thread names are: a name the user chose can be got wrong — reused, or built
// from something that varies between attempts — and this one cannot. A channel
// created at a point every attempt reaches gets the same name every time.
func (t *threadState) newChannelName() string {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	name := fmt.Sprintf("%s.ch%d", t.id, t.channels)
	t.channels++
	return name
}

// qualified is this thread's name to other runs: the run's name and its own.
// It is the sender of everything the thread puts on a channel, so a value
// from a thread of another run cannot be mistaken for one from here.
func (t *threadState) qualified() string { return t.run.name + "/" + t.id }

// nextSend returns this thread's sequence number for its next send on a
// channel.
func (t *threadState) nextSend(channel string) uint64 {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	if t.sends == nil {
		t.sends = map[string]uint64{}
	}
	seq := t.sends[channel]
	t.sends[channel] = seq + 1
	return seq
}

// nextRecv returns this thread's sequence number for its next receive on a
// channel: what a want is named by, so that a receive asked for twice — the
// attempt ended while it waited, and the replay is asking again — is one
// want, and gets one value.
func (t *threadState) nextRecv(channel string) uint64 {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	if t.recvs == nil {
		t.recvs = map[string]uint64{}
	}
	seq := t.recvs[channel]
	t.recvs[channel] = seq + 1
	return seq
}

// encode runs encode as this thread's. See runState.encoding.
func (t *threadState) encode(encode func() ([]byte, error)) ([]byte, error) {
	return t.run.encoding(t, encode)
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
// has caught up with history and the run is in new territory.
func (t *threadState) peek() *protos.Event {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	if t.serial < uint64(len(t.events)) {
		return t.events[t.serial]
	}
	return nil
}

// at returns the thread's current position.
//
// Only this thread's own goroutine ever moves the cursor, so the value is
// stable to its caller; the lock is here because other threads are appending to
// the same run concurrently.
func (t *threadState) at() uint64 {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	return t.serial
}

// expect consumes the event at the cursor and asserts its payload type.
//
// A nil result means nothing is recorded there yet, which is the signal to do
// the thing for real. A recorded event of the wrong type is a continuity error,
// and this is where nearly all of them are caught: the run has reached a
// point where last time it made a call and this time it wants to sleep, which
// means the code changed underneath a live run.
func (t *threadState) expect[E protos.Events]() (E, error) {
	var zero E

	ev := t.peek()
	if ev == nil {
		return zero, nil
	}

	payload, ok := protos.UnpackEventPayload(ev).(E)
	if !ok {
		return zero, continuityf("at position %d of thread %q the history has a %s, but the run is now doing a %s",
			t.serial, t.id, protos.EventType(ev), payloadName[E]())
	}

	t.run.mu.Lock()
	t.serial++
	t.run.mu.Unlock()
	return payload, nil
}

// record appends an event to this thread and hands it to the sink.
//
// The append is what makes the thread durable, so a sink failure is kept and
// fails the thread: a history that was not written down cannot be replayed,
// and carrying on as though it could is the one outcome worse than stopping.
func (t *threadState) record[E protos.Events](payload E) *protos.Event {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()

	ev := &protos.Event{
		Timestamp: timestamppb.New(time.Now().Truncate(time.Microsecond)),
		Serial:    t.serial,
		Attempt:   t.attempt,
		ThreadId:  t.id,
		Payload:   protos.PackEventPayload(payload),
	}
	t.events = append(t.events, ev)
	t.serial++
	t.persistLocked(ev)
	return ev
}

// marker records an attempt marker: an event about the thread's attempt
// rather than about what its body did.
//
// Kept out of the thread's sequence deliberately: replay walks a thread's
// events in order and compares each to what the body is doing, and a start
// marker is not something the body did. It carries the thread's name all the
// same, so a reader of a stream that holds several threads' events can tell
// whose attempt it opens.
func (t *threadState) marker[E protos.Events](payload E) {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	t.persistLocked(&protos.Event{
		Timestamp: timestamppb.New(time.Now().Truncate(time.Microsecond)),
		Attempt:   t.attempt,
		ThreadId:  t.id,
		Payload:   protos.PackEventPayload(payload),
	})
}

// persistLocked hands an event to the sink. Call with run.mu held.
func (t *threadState) persistLocked(ev *protos.Event) {
	if t.sink == nil || t.sinkErr != nil || t.run.over {
		return
	}
	// Background, not the run's context: an event describing what has
	// already happened must be written even while the run is being torn
	// down, or the history stops exactly where it is most interesting.
	if err := t.sink.Append(context.Background(), ev); err != nil {
		t.sinkErr = fmt.Errorf("flow: persist event: %w", err)
	}
}

// err reports the first persistence failure of this thread, if any.
func (t *threadState) err() error {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	return t.sinkErr
}

// nextChild names the next thread this one forks.
//
// The name comes from the parent's fork counter, not from when the child
// started, so the same code produces the same names in the same order however
// the goroutines happened to be scheduled — which is what lets a replay
// re-adopt the child a previous attempt created.
func (t *threadState) nextChild() string {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	id := fmt.Sprintf("%s.%d", t.id, t.counter)
	t.counter++
	return id
}

// joined reports whether this thread's history, from the cursor on, already
// holds the join of child — in which case the child is over, its result is
// here, and nothing needs to run for it.
func (t *threadState) joined(child string) bool {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	for _, ev := range t.events[min(t.serial, uint64(len(t.events))):] {
		if ev.GetJoin().GetThreadId() == child {
			return true
		}
	}
	return false
}

// recordFork consumes the fork already in history, or records a new one.
//
// Consuming matters. Forks and joins are events like any other, and a replay
// that appends them again grows the history by a fork and a join per parallel
// call per attempt — and then the log claims the run forked more threads
// than it did, which is a lie told to whoever reads it after a failure.
func (t *threadState) recordFork(th Thread) error {
	ev, err := t.expect[*protos.ForkEvent]()
	if err != nil {
		return err
	}
	if ev != nil {
		if ev.GetThreadId() != th.ID {
			return continuityf("thread %q previously forked %q at this point, but is now forking %q",
				t.id, ev.GetThreadId(), th.ID)
		}
		if ev.GetFunction() != th.Fn {
			return continuityf("thread %q previously forked %q to run %q, but now wants it to run %q",
				t.id, th.ID, ev.GetFunction(), th.Fn)
		}
		if stored := ev.GetInput().GetSerialized(); !bytesEqual(stored, th.Input) {
			return continuityf("thread %q previously forked %q with different input "+
				"(%d bytes recorded, %d bytes now)", t.id, th.ID, len(stored), len(th.Input))
		}
		return t.err()
	}
	fork := &protos.ForkEvent{ParentThreadId: t.id, ThreadId: th.ID, Function: th.Fn}
	if th.Fn != "" {
		fork.Input = &protos.Data{Serialized: th.Input}
	}
	t.record(fork)
	return t.err()
}

// recordJoin is recordFork for the other end: it consumes the join already
// in history and returns the result it holds, or records the result the
// child just produced.
func (t *threadState) recordJoin(child string, out []byte, callErr error) ([]byte, error, error) {
	ev, err := t.expect[*protos.JoinEvent]()
	if err != nil {
		return nil, nil, err
	}
	if ev != nil {
		if ev.GetThreadId() != child {
			return nil, nil, continuityf("thread %q previously joined %q at this point, but is now joining %q",
				t.id, ev.GetThreadId(), child)
		}
		out, callErr = unpackResult(ev.GetResult())
		return out, callErr, t.err()
	}
	t.record(&protos.JoinEvent{ThreadId: child, Result: packResult(out, callErr)})
	return out, callErr, t.err()
}

// payloadName is the message name of an event payload type, for error messages.
func payloadName[E protos.Events]() string {
	var e E
	return protos.EventType(&protos.Event{Payload: protos.PackEventPayload(e)})
}

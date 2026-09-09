package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ligustah/wings/flow/protos"
)

// mainThread is the run body's own thread. Forked threads are named
// "<parent>.<n>", so a thread's name is also its lineage.
const mainThread = "main"

// runState is what the threads of one run share in one process. Made once per
// attempt of the main thread and ending with it. No thread's history lives here —
// each thread has its own, on its own stream — which is what lets a thread run on
// another machine.
type runState struct {
	name   string
	store  Store
	exec   Executor
	placer Placer
	parker Parker
	clock  Clock
	opts   runOptions
	// host carries channels to and from other runs; nil when none are shared.
	// linkCtx bounds the links and ends with the attempt.
	host     ChannelHost
	linkCtx  context.Context
	linkStop context.CancelFunc

	// fragment says this process holds only some of the run's threads, so every
	// channel is shared as it is made. See lineage.go.
	fragment bool
	// forked, when set, is told of every fork before the placer is, on the
	// forking thread; joined says the fork is already over in the parent's
	// history. See lineage.go.
	forked func(th Thread, joined bool)

	mu       sync.Mutex
	channels map[string]*chanState
	over     bool // this attempt has returned

	// encMu serialises the run's encodes, and encoder is the thread whose encode
	// is under way. See encoding.
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
		clock:  opts.clock,
		opts:   opts,
		host:   opts.host,
	}
}

// finish marks an attempt over, after which nothing more of it is written down —
// a forked thread that returns late must not record into the next attempt's
// history. Only the durable record is closed; thread bookkeeping is left alone.
func (r *runState) finish() {
	r.mu.Lock()
	r.over = true
	r.mu.Unlock()
	r.closeLinks()
}

// declareChannel registers a channel's runtime the first time it is created.
// Idempotent: a replay creates the same channels again and must find the same
// queue.
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

// threadState is one attempt of one thread: its history and a cursor through it.
// serial is the position replay has reached.
type threadState struct {
	id      string
	attempt uint64
	run     *runState
	// readonly says the thread is being replayed only: it records nothing, parks
	// nowhere, and stops where its history ends. See lineage.go.
	readonly bool

	// ctx is the attempt's own context. A wait ended by a context derived from it
	// while live was cut short by the body (recorded, see interrupt); one ended
	// because it is done was interrupted with the attempt (not recorded).
	ctx context.Context

	// events is this thread's history with what this attempt records appended.
	// Guarded by run.mu: a fork reads ahead in the parent's history from the
	// goroutine placing the child. Big channel values are dropped from it at load;
	// offsets holds each event's stream offset so a dropped value can be read
	// back when replay reaches it. offsets aligns with events over the replayed
	// prefix (nothing this attempt records is added to either).
	events  []*protos.Event
	offsets []int64
	// valueIdx aligns with events: for a receive whose value lives on the side
	// stream it is that value's index there; -1 otherwise (a receive that kept
	// its value inline in the event, or a non-receive). See [replayable].
	valueIdx []int64
	// values caches received values read ahead of the cursor by offset, so a
	// resume does not reopen and reread the stream once per receive. Filled and
	// drained only by this thread's own replay goroutine, so it needs no lock.
	values map[int64][]byte
	// sideValues is the same cache for values read from the side stream, keyed by
	// their index there rather than by a stream offset.
	sideValues map[int64][]byte
	serial     uint64

	sink    Sink
	sinkErr error // the first persistence failure, if any

	// counter names the next child thread; the nth fork is always "<id>.<n>"
	// whatever order the children are scheduled in.
	counter uint64

	// channels names the next channel this thread creates; sends and recvs count
	// this thread's sends and receives per channel, so each can be named exactly.
	channels uint64
	sends    map[string]uint64
	recvs    map[string]uint64
}

// newChannelName mints the next channel name for this thread, derived from the
// thread rather than the caller so a replay gets the same name.
func (t *threadState) newChannelName() string {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	name := fmt.Sprintf("%s.ch%d", t.id, t.channels)
	t.channels++
	return name
}

// qualified is this thread's name to other runs, the sender of everything it
// puts on a channel.
func (t *threadState) qualified() string { return t.run.name + "/" + t.id }

// nextSend returns this thread's sequence number for its next send on a channel.
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

// nextRecv returns this thread's sequence number for its next receive — what a
// want is named by, so a receive asked for twice is one want and gets one value.
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

// threadFrom returns the thread bound to ctx, or nil when ctx is not inside a run.
func threadFrom(ctx context.Context) *threadState {
	t, _ := ctx.Value(ctxKey{}).(*threadState)
	return t
}

// peek returns the event at the cursor without consuming it, or nil once replay
// has caught up with history.
func (t *threadState) peek() *protos.Event {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	if t.serial < uint64(len(t.events)) {
		return t.events[t.serial]
	}
	return nil
}

// at returns the thread's current position. The lock guards against other
// threads appending to the same run concurrently.
func (t *threadState) at() uint64 {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	return t.serial
}

// expect consumes the event at the cursor and asserts its payload type. A nil
// result means nothing is recorded there yet — do the thing for real. A recorded
// event of the wrong type is a continuity error: the code changed under a live run.
func (t *threadState) expect[E protos.Events]() (E, error) {
	var zero E

	ev := t.peek()
	if ev == nil {
		if t.readonly {
			return zero, errExhausted
		}
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

// valuePrefetch is how many events a value read pulls in at once. Receives
// replay in ascending offset order, so a windowed read amortises the stream open
// and read over the receives that follow; the cache holds at most this many
// values, drained as replay reaches them.
const valuePrefetch = 64

// recordedValue reads back the channel value dropped at load from the event
// replayed at position pos, from this thread's own stream by the offset load
// kept. Called only while replaying a recorded receive, whose value the body is
// owed. The thread's own stream is not dropped while it replays, so the value is
// there to be read. Values ahead of pos are cached, so a run of receives reads
// the stream in windows rather than one open and read each.
func (t *threadState) recordedValue(ctx context.Context, pos uint64) ([]byte, error) {
	t.run.mu.Lock()
	off := t.offsets[pos]
	idx := int64(-1)
	if pos < uint64(len(t.valueIdx)) {
		idx = t.valueIdx[pos]
	}
	t.run.mu.Unlock()

	if idx >= 0 {
		return t.sideValue(ctx, idx)
	}

	if data, ok := t.values[off]; ok {
		delete(t.values, off)
		return data, nil
	}

	batch, err := t.run.store.Read(ctx, t.run.name, t.id, off, valuePrefetch)
	if err != nil {
		return nil, err
	}
	var found []byte
	got := false
	for _, ea := range batch {
		v := ea.Event.GetChannelRecv().GetValue().GetSerialized()
		if v == nil {
			continue
		}
		switch {
		case ea.Offset == off:
			found, got = v, true
		case ea.Offset > off:
			if t.values == nil {
				t.values = map[int64][]byte{}
			}
			t.values[ea.Offset] = v
		}
	}
	if !got {
		return nil, fmt.Errorf("flow: the recorded value at offset %d of thread %q is gone", off, t.id)
	}
	return found, nil
}

// sideValue reads a received value off the side stream by its index there, the
// value having been moved off the event stream at record time (see
// [ValueReader]). Values ahead of idx are cached, so a run of receives reads the
// side stream in windows rather than once per receive.
func (t *threadState) sideValue(ctx context.Context, idx int64) ([]byte, error) {
	if data, ok := t.sideValues[idx]; ok {
		delete(t.sideValues, idx)
		return data, nil
	}
	reader, ok := t.run.store.(ValueReader)
	if !ok {
		return nil, fmt.Errorf("flow: thread %q has a value on a side stream but its store cannot read one back", t.id)
	}
	values, err := reader.ReadValues(ctx, t.run.name, t.id, idx, valuePrefetch)
	if err != nil {
		return nil, err
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("flow: the recorded value at index %d of thread %q is gone", idx, t.id)
	}
	for i, v := range values[1:] {
		if t.sideValues == nil {
			t.sideValues = map[int64][]byte{}
		}
		t.sideValues[idx+1+int64(i)] = v
	}
	return values[0], nil
}

// record appends an event to this thread and hands it to the sink. A sink
// failure is kept and fails the thread: a history not written down cannot be
// replayed.
func (t *threadState) record[E protos.Events](payload E) *protos.Event {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()

	ev := &protos.Event{
		Timestamp: timestamppb.New(t.run.clock.Now().Truncate(time.Microsecond)),
		Serial:    t.serial,
		Attempt:   t.attempt,
		ThreadId:  t.id,
		Payload:   protos.PackEventPayload(payload),
	}
	// Not retained in t.events: a recorded event is persisted to the sink and
	// never read back this attempt (peek/expect/joined past the replay boundary
	// are live), and a later attempt replays it from the store. Keeping it held
	// a whole run's events — channel values included — in memory for the run's
	// life. t.events holds only what is still to be replayed. See [Store].
	t.serial++
	t.persistLocked(ev)
	return ev
}

// marker records an attempt marker — an event about the attempt, not about what
// the body did — kept out of the thread's replayed sequence but carrying the
// thread's name so a shared stream's reader can tell whose attempt it opens.
func (t *threadState) marker[E protos.Events](payload E) {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	t.persistLocked(&protos.Event{
		Timestamp: timestamppb.New(t.run.clock.Now().Truncate(time.Microsecond)),
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
	// Background, not the run's context: an event about what already happened
	// must be written even while the run is torn down.
	if err := t.sink.Append(context.Background(), ev); err != nil {
		t.sinkErr = fmt.Errorf("flow: persist event: %w", err)
	}
}

// base is the attempt's own context, for what must go on after a wait the body's
// narrower context cut short, and Background for a thread given none.
func (t *threadState) base() context.Context {
	if t.ctx != nil {
		return t.ctx
	}
	return context.Background()
}

// interrupted reports the interruption a previous attempt recorded at this point,
// if any: the named wait was cut short by the body's context, and returns the
// same error now, at once.
func (t *threadState) interrupted(wait string) (error, bool) {
	ev := t.peek()
	in := ev.GetInterrupted()
	if in == nil {
		return nil, false
	}
	if in.GetWait() != wait {
		return continuityf("at position %d of thread %q the history has an interrupted %s, but the run is now doing a %s",
			t.serial, t.id, in.GetWait(), wait), true
	}
	t.run.mu.Lock()
	t.serial++
	t.run.mu.Unlock()
	switch in.GetCause() {
	case protos.InterruptCause_INTERRUPT_CAUSE_DEADLINE_EXCEEDED:
		return context.DeadlineExceeded, true
	default:
		return context.Canceled, true
	}
}

// interrupt records err as a wait's outcome when the context that ended it was
// the body's own, so the next attempt goes on the same way. An interruption by
// the thread's own context is not recorded; the next attempt waits again.
func (t *threadState) interrupt(wait string, err error) error {
	if t.readonly || (t.ctx != nil && t.ctx.Err() != nil) {
		return err
	}
	cause := protos.InterruptCause_INTERRUPT_CAUSE_CANCELED
	if errors.Is(err, context.DeadlineExceeded) {
		cause = protos.InterruptCause_INTERRUPT_CAUSE_DEADLINE_EXCEEDED
	}
	t.record(&protos.WaitInterruptedEvent{Wait: wait, Cause: cause})
	return err
}

// err reports the first persistence failure of this thread, if any.
func (t *threadState) err() error {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	return t.sinkErr
}

// nextChild names the next thread this one forks, from the fork counter rather
// than start order, so a replay re-adopts the child a previous attempt created.
func (t *threadState) nextChild() string {
	t.run.mu.Lock()
	defer t.run.mu.Unlock()
	id := fmt.Sprintf("%s.%d", t.id, t.counter)
	t.counter++
	return id
}

// joined reports whether this thread's history, from the cursor on, already holds
// the join of child.
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
// Consuming matters: re-appending would grow the history by a fork and join per
// call per attempt and misreport how many threads the run forked.
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

// recordJoin consumes the join already in history and returns the result it
// holds, or records the result the child just produced.
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

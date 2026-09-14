package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow/protos"
)

// Channel carries typed values between the threads of one run, like a Go
// channel. A receive is recorded — which send it took — so a replay waits for
// that same value rather than whatever the scheduler offers first; a send
// records only when it completed.
//
// Create one with [Context.NewChannel] inside a Run, at a point every attempt
// reaches; it hands back a [Reader] and a [Writer] to pass to threads forked by
// [Context.Go] or [Context.Map]. Safe to use from all of them at once. Not
// usable outside a Run.
type Channel[T any] struct {
	name  string
	run   *runState
	codec dswire.Codec[T]

	// id and capacity are set on a handle that arrived from another run; mu
	// guards binding it to this run on first use. mode is how it arrived — read,
	// write, or both — so a run handed only a writer is known to never receive.
	id       string
	capacity int
	mode     handleMode
	mu       sync.Mutex
}

type handleMode string

const (
	modeBoth  handleMode = ""
	modeRead  handleMode = "r"
	modeWrite handleMode = "w"
)

// NewChannel creates a shared channel and returns its two ends: a [Reader] to
// receive and a [Writer] to send and close. Hand one end to another thread or
// run and keep the other, the way [io.Pipe] splits a pipe. Without options the
// channel is unbounded — a send always completes at once and never parks (so it
// never forces the job to unload), but the buffer can grow without limit if the
// receiver falls behind. Pass [WithCapacity] for backpressure.
func (c Context) NewChannel[T any](opts ...ChannelOption) (Reader[T], Writer[T]) {
	cfg := channelConfig{capacity: unbounded}
	for _, o := range opts {
		o(&cfg)
	}
	ch := newChannel[T](c, cfg.capacity)
	return ch.Reader(), ch.Writer()
}

// ChannelOption configures a channel at creation. See [WithCapacity].
type ChannelOption func(*channelConfig)

type channelConfig struct{ capacity int }

// WithCapacity bounds a channel so a send waits once this many values are
// buffered and unconsumed, giving the sender backpressure. The capacity is
// approximate: a sender learns what the reader has consumed only as the host
// relays it, so the buffer may briefly run over. A capacity below 1 is raised to
// 1; without this option the channel is unbounded.
func WithCapacity(capacity int) ChannelOption {
	return func(c *channelConfig) {
		if capacity < 1 {
			capacity = 1
		}
		c.capacity = capacity
	}
}

// unbounded is the capacity of a channel a send never waits on. Negative so it
// cannot be reached by a count of queued items.
const unbounded = -1

func newChannel[T any](ctx Context, capacity int) *Channel[T] {
	t := threadFrom(ctx)
	if t == nil {
		// A channel that errors on use, not a nil deref somewhere else.
		return &Channel[T]{}
	}
	name := t.newChannelName()
	t.run.declareChannel(name, capacity)
	// Every channel is stream-backed: start its pumps at creation so its values go
	// to the value stream from the first send and nothing but a bounded prefetch is
	// held in memory. The Store is where the records live — durable under a real
	// store, in-process under a MemStore — so a same-run channel routes through it
	// too, holding no traffic in memory.
	if cs := t.run.channel(name); cs != nil {
		cs.activate(t.run.linkContext(), t.run.store, t.run.channelID(name), modeBoth)
	}
	return &Channel[T]{
		name:  name,
		run:   t.run,
		codec: dswire.ReflectCodec[T]{New: allocator[T]()},
	}
}

// Name is the channel's identity in its run's history, for diagnostics.
func (c *Channel[T]) Name() string { return c.name }

// sharedID is the channel's id to other runs. Valid after bind sets c.run.
func (c *Channel[T]) sharedID() string {
	if c.id != "" {
		return c.id
	}
	return c.run.channelID(c.name)
}

// channelHandle is how a channel travels in encoded input or output; the
// capacity goes with it so a sender in another run knows the room.
type channelHandle struct {
	Channel  string     `json:"channel"`
	Capacity int        `json:"capacity,omitempty"`
	Mode     handleMode `json:"mode,omitempty"`
}

// share encodes one side of the channel so it can travel in a call's input, a
// result, or a value sent on another channel: the run's host is told and untaken
// sends go with it, so the run needs a [ChannelHost]. See [WithChannelHost],
// [Writer.MarshalJSON] and [Reader.MarshalJSON].
func (c *Channel[T]) share(mode handleMode) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, capacity := c.id, c.capacity
	if c.run != nil {
		// Every channel is stream-backed from creation, so sharing a side is just
		// naming it: its records are already on the value and consume streams, which
		// the receiving run reaches by the same id.
		id = c.run.channelID(c.name)
		if cs := c.run.channel(c.name); cs != nil {
			capacity = cs.capacity
		}
	}
	if id == "" {
		return nil, errors.New("flow: this channel was created outside a Run and cannot be shared")
	}
	return json.Marshal(channelHandle{Channel: id, Capacity: capacity, Mode: mode})
}

// UnmarshalJSON receives a channel another run shared; it binds to this run on
// first use, which also needs a [ChannelHost].
func (c *Channel[T]) UnmarshalJSON(b []byte) error {
	var h channelHandle
	if err := json.Unmarshal(b, &h); err != nil {
		return err
	}
	if h.Channel == "" {
		return errors.New("flow: a shared channel needs an id")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.id, c.run, c.name, c.capacity, c.mode = h.Channel, nil, h.Channel, h.Capacity, h.Mode
	c.codec = dswire.ReflectCodec[T]{New: allocator[T]()}
	return nil
}

// ErrChannelClosed is returned by [Channel.Send] on a channel closed before the
// send: nothing is sent, and it is returned again on replay.
var ErrChannelClosed = errors.New("flow: send on a closed channel")

// Send puts a value on the channel, blocking until it is taken or the buffer
// has room. A send that completed on a previous attempt returns as soon as the
// value is queued. On a closed channel it sends nothing and returns
// [ErrChannelClosed] rather than panicking.
func (c *Channel[T]) Send(ctx Context, v T) error {
	t, cs, err := c.bind(ctx, false)
	if err != nil {
		return err
	}

	data, err := t.encode(func() ([]byte, error) { return dswire.EncodeRecord(c.codec, v) })
	if err != nil {
		return fmt.Errorf("flow: encode value for channel %s: %w", c.name, err)
	}

	seq := t.nextSend(c.name)

	// Consume the event before waiting: a replay that waited first would wait
	// for a receive already replayed away.
	ev, err := t.expect[*protos.ChannelSendEvent]()
	if err != nil {
		return err
	}
	if ev != nil {
		if ev.GetChannel() != c.name || ev.GetSeq() != seq || ev.GetClosed() {
			return continuityf("thread %q previously sent %s#%d at this point, but is now sending %s#%d",
				t.id, ev.GetChannel(), ev.GetSeq(), c.name, seq)
		}
		if ev.GetRefused() {
			return fmt.Errorf("%w: %s", ErrChannelClosed, c.name)
		}
		// Recorded on a previous attempt: the value came home with this event in
		// one transaction (pull.go), so it is durably on the value stream — queue a
		// placeholder locally again without re-announcing. The pump reads the value
		// back off the stream and fills it here.
		item := cs.put(ctx, t.qualified(), seq, data, false)
		// The backpressure wait was cut short by the body before: same outcome now.
		if ierr, ok := t.interrupted("send"); ok {
			return ierr
		}
		// More history follows, so the send's wait already played out; do not
		// repeat it. Only when this event is the last recorded did the attempt
		// unload waiting here, and a bounded channel's backpressure must survive the
		// reload — so wait again.
		if t.peek() != nil {
			return t.err()
		}
		if err := cs.awaitTaken(ctx, t, c.sharedID(), item); err != nil {
			if ctx.Err() != nil {
				err = t.interrupt("send", err)
			}
			return err
		}
		return t.err()
	}

	if cs.isClosed() {
		t.record(&protos.ChannelSendEvent{Channel: c.name, Seq: seq, Refused: true})
		if err := t.err(); err != nil {
			return err
		}
		return fmt.Errorf("%w: %s", ErrChannelClosed, c.name)
	}

	// Announce the value on the value stream, record the send, and commit the two
	// together: under a transactional store the record and this event go home in one
	// transaction (pull.go), so a replay that finds the event knows the value is
	// durably queued and need not re-announce. Announce before recording so a
	// non-transactional store never leaves an event with nothing queued; a torn send
	// there has no event and replays afresh. The value is not recorded in history:
	// it is read back off the value stream (see Recv).
	valEv := &protos.Event{Payload: protos.PackEventPayload(&protos.ChannelItem{From: t.qualified(), Seq: seq, Data: data})}
	if err := t.tx.AppendTo(ctx, ChannelValueStream(c.sharedID()), valEv); err != nil {
		return fmt.Errorf("flow: send on channel %s: %w", c.name, err)
	}
	item := cs.put(ctx, t.qualified(), seq, data, false)
	t.record(&protos.ChannelSendEvent{Channel: c.name, Seq: seq})
	if err := t.commit(ctx); err != nil {
		return err
	}
	if err := t.err(); err != nil {
		return err
	}

	if err := cs.awaitTaken(ctx, t, c.sharedID(), item); err != nil {
		if ctx.Err() != nil {
			err = t.interrupt("send", err)
		}
		return err
	}
	return t.err()
}

// Recv takes the next value off the channel. It blocks until there is something
// to take or the channel is closed; the second result is false when the channel
// is closed and drained, as a Go receive reports it.
func (c *Channel[T]) Recv(ctx Context) (T, bool, error) {
	var zero T

	t, cs, err := c.bind(ctx, true)
	if err != nil {
		return zero, false, err
	}

	// Counted before consulting the history, like a send: the number names the
	// receive to the host and must come out the same on a replay.
	recvSeq := t.nextRecv(c.name)

	if err, ok := t.interrupted("recv"); ok {
		return zero, false, err
	}
	pos := t.at()
	ev, err := t.expect[*protos.ChannelRecvEvent]()
	if err != nil {
		return zero, false, err
	}

	if ev != nil {
		if ev.GetChannel() != c.name {
			return zero, false, continuityf("thread %q previously received from %q at this point, but is now receiving from %q",
				t.id, ev.GetChannel(), c.name)
		}
		if ev.GetClosed() {
			return zero, false, t.err()
		}
		// Replay takes the value the history names, not the first going: races
		// ordered one way last time and the run acted on that. Claimed rather
		// than waited for, since its sender may since have been joined; a copy
		// that turns up from a replaying sender is taken on arrival.
		cs.claim(ev.GetFromThreadId(), ev.GetFromSeq())
		// The value was dropped from the replay slice at load; read it back from
		// its offset now, rather than have held every received value in memory.
		data, err := t.recordedValue(ctx.base(), pos)
		if err != nil {
			return zero, false, fmt.Errorf("flow: read the recorded value from channel %s: %w", c.name, err)
		}
		v, err := dswire.DecodeRecord(c.codec, data)
		if err != nil {
			return zero, false, fmt.Errorf("flow: decode the recorded value from channel %s: %w", c.name, err)
		}
		return v, true, t.err()
	}

	// About to wait for a value: tell the engine this worker reads, so it pushes
	// the writer's values here. Only now is the read role known — an eagerly shared
	// channel activates before any thread has received.
	if err := cs.announceReader(ctx, t); err != nil {
		return zero, false, err
	}

	item, err := cs.awaitAny(ctx, t, c.sharedID(), recvSeq)
	if err != nil {
		if ctx.Err() != nil {
			err = t.interrupt("recv", err)
		}
		return zero, false, err
	}
	if item == nil {
		t.record(&protos.ChannelRecvEvent{Channel: c.name, Closed: true})
		return zero, false, t.err()
	}
	// The value's one durable copy is on the channel's value stream, read back on
	// replay by (from, seq); the receive is recorded by identity alone. See
	// [threadState.channelValue].
	rec := &protos.ChannelRecvEvent{Channel: c.name, FromThreadId: item.from, FromSeq: item.seq}
	t.record(rec)
	// Report the take and commit it with the receive, so the consume report is
	// never home without the receive that justified it (pull.go). A replay does not
	// report again — the original report came home with the receive it replays.
	if reported, err := cs.reportConsumed(ctx, t, item); err != nil {
		return zero, false, err
	} else if reported {
		if err := t.commit(ctx); err != nil {
			return zero, false, err
		}
	}
	v, err := c.decode(item)
	if err != nil {
		return zero, false, err
	}
	// The value has been decoded for the caller and recorded to history; the
	// buffered item is never read again, so drop its bytes rather than keep the
	// channel's whole traffic in memory. The recorded event holds its own copy.
	cs.consume(item)
	return v, true, t.err()
}

func (c *Channel[T]) decode(item *chanItem) (T, error) {
	v, err := dswire.DecodeRecord(c.codec, item.data)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("flow: decode value from channel %s: %w", c.name, err)
	}
	return v, nil
}

// Close says nothing more will be sent. Receives drain what is queued and then
// report the channel closed. Recorded and ordered like a send.
func (c *Channel[T]) Close(ctx Context) error {
	t, cs, err := c.bind(ctx, false)
	if err != nil {
		return err
	}

	seq := t.nextSend(c.name)
	if err, ok := t.interrupted("close"); ok {
		return err
	}
	ev, err := t.expect[*protos.ChannelSendEvent]()
	if err != nil {
		return err
	}
	if ev != nil {
		if !ev.GetClosed() || ev.GetChannel() != c.name {
			return continuityf("thread %q previously sent %s#%d at this point, but is now closing %s",
				t.id, ev.GetChannel(), ev.GetSeq(), c.name)
		}
		// The close came home with this event (pull.go), so it is durable; shut the
		// local view without announcing it again.
		cs.shut()
		return t.err()
	}
	if err := cs.announceClose(ctx, t); err != nil {
		if ctx.Err() != nil {
			err = t.interrupt("close", err)
		}
		return err
	}
	t.record(&protos.ChannelSendEvent{Channel: c.name, Seq: seq, Closed: true})
	if err := t.commit(ctx); err != nil {
		return err
	}
	cs.shut()
	return t.err()
}

// Writer is the send side of a channel: the capability to [Writer.Send] and
// [Writer.Close], with no way to receive. [Context.NewChannel] returns one; pass
// it to a thread that should only produce.
type Writer[T any] struct{ ch *Channel[T] }

// Reader is the receive side of a channel: the capability to [Reader.Recv], with
// no way to send or close. [Context.NewChannel] returns one; pass it to a thread
// that should only consume.
type Reader[T any] struct{ ch *Channel[T] }

// Writer returns the channel's send side.
func (c *Channel[T]) Writer() Writer[T] { return Writer[T]{ch: c} }

// Reader returns the channel's receive side.
func (c *Channel[T]) Reader() Reader[T] { return Reader[T]{ch: c} }

// Send puts a value on the channel. See [Channel.Send].
func (w Writer[T]) Send(ctx Context, v T) error { return w.ch.Send(ctx, v) }

// Close says nothing more will be sent. See [Channel.Close].
func (w Writer[T]) Close(ctx Context) error { return w.ch.Close(ctx) }

// Name is the channel's identity in its run's history. See [Channel.Name].
func (w Writer[T]) Name() string { return w.ch.Name() }

// MarshalJSON shares the send side, so the run that receives it can send but not
// receive. See [Channel.MarshalJSON].
func (w Writer[T]) MarshalJSON() ([]byte, error) { return w.ch.share(modeWrite) }

// UnmarshalJSON receives a send side another run shared.
func (w *Writer[T]) UnmarshalJSON(b []byte) error { return unmarshalSide(&w.ch, b) }

// Recv takes the next value off the channel. See [Channel.Recv].
func (r Reader[T]) Recv(ctx Context) (T, bool, error) { return r.ch.Recv(ctx) }

// Name is the channel's identity in its run's history. See [Channel.Name].
func (r Reader[T]) Name() string { return r.ch.Name() }

// MarshalJSON shares the receive side. See [Channel.MarshalJSON].
func (r Reader[T]) MarshalJSON() ([]byte, error) { return r.ch.share(modeRead) }

// UnmarshalJSON receives a receive side another run shared.
func (r *Reader[T]) UnmarshalJSON(b []byte) error { return unmarshalSide(&r.ch, b) }

func unmarshalSide[T any](ch **Channel[T], b []byte) error {
	c := &Channel[T]{}
	if err := c.UnmarshalJSON(b); err != nil {
		return err
	}
	*ch = c
	return nil
}

// bind resolves the calling thread and this channel's shared state, pinning the
// used side to that thread: read is true for a receive, false for a send or close,
// so a channel keeps a single reader and a single writer (see [chanState.claimSide]).
func (c *Channel[T]) bind(ctx Context, read bool) (*threadState, *chanState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.run == nil && c.id == "" {
		return nil, nil, errors.New("flow: this channel was created outside a Run; " +
			"create it inside the run's body, with the context it was given")
	}
	t := threadFrom(ctx)
	if t == nil {
		return nil, nil, fmt.Errorf("flow: channel %s was used outside a run's thread", c.name)
	}
	if c.run == nil {
		// A handle from another run, first used here: attach through the host
		// and keep it under its id.
		cs, err := t.run.attach(ctx, c.id, c.capacity, c.mode)
		if err != nil {
			return nil, nil, err
		}
		c.run, c.name = t.run, c.id
		if c.codec == nil {
			c.codec = dswire.ReflectCodec[T]{New: allocator[T]()}
		}
		if err := cs.claimSide(c.name, t.qualified(), read); err != nil {
			return nil, nil, err
		}
		return t, cs, nil
	}
	if t.run != c.run {
		return nil, nil, fmt.Errorf("flow: channel %s belongs to another run", c.name)
	}
	cs := c.run.channel(c.name)
	if cs == nil {
		return nil, nil, fmt.Errorf("flow: channel %s is not part of this run", c.name)
	}
	if err := cs.claimSide(c.name, t.qualified(), read); err != nil {
		return nil, nil, err
	}
	return t, cs, nil
}

// claimSide pins one side of the channel to the calling thread on first use: the
// first thread to receive owns receiving, the first to send or close owns sending.
// A later thread on the same side is an error — a channel has a single reader and
// a single writer. name is the channel's name for the message; qualified is the
// caller's "<run>/<thread>". read selects the receive side.
func (cs *chanState) claimSide(name, qualified string, read bool) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	owner, side := &cs.writeOwner, "sent on"
	if read {
		owner, side = &cs.readOwner, "received from"
	}
	if *owner != "" && *owner != qualified {
		return fmt.Errorf("flow: channel %s is already %s by thread %q and cannot be used from thread %q; "+
			"a channel has a single reader and a single writer, so fan in by selecting over several channels into one", name, side, *owner, qualified)
	}
	*owner = qualified
	return nil
}

// chanItem is one value in flight, tagged with where it came from so a receive
// can name it.
type chanItem struct {
	from string
	seq  uint64
	data []byte

	taken bool
	// freed means the single reader reported consuming this value (see
	// ChannelItem.Consumed), so a sender counting room by its own mirror may free
	// the place. Distinct from taken: a reader sends the report before its receive
	// is durably recorded, so a moved receiver that replays the report must still
	// be free to take the value itself if no recorded receive accounts for it.
	freed bool
	// buffered means the send completed on arrival, because the channel had
	// room for it.
	buffered bool
	// consumed means a receive has decoded and recorded this item; nothing reads
	// it again, so it may be pruned from the queue. See [chanState.prune].
	consumed bool
}

// chanState is one channel's runtime, shared by every thread using it. Its own
// lock, not the run's: a thread blocked on a receive must not hold the lock that
// guards the event log.
type chanState struct {
	capacity int

	mu      sync.Mutex
	items   []*chanItem
	closed  bool
	changed chan struct{}
	// byKey indexes items by from#seq so find is O(1): a send dedupes against its
	// own echo and replays on every put, and a channel a worker only sends on keeps
	// every send until it is retired, so a scan per send is quadratic over a wave.
	byKey map[string]*chanItem
	// readOwner and writeOwner are the qualified ids of the one thread that receives
	// on and the one that sends on (or closes) this channel: a channel has a single
	// reader and a single writer, so a second thread on either side is rejected
	// (see [chanState.claimSide]). The two may differ — a producer thread and a
	// consumer thread — but fan-in is not a channel with many writers; it is a
	// thread that selects over several channels into one. In-memory and per attempt.
	readOwner  string
	writeOwner string
	// floor is len(items) after the last prune; the queue is compacted once it has
	// grown enough past it that pruning stays amortised. See [chanState.prune].
	floor int
	// claimed names items a replayed receive took before they were queued, so
	// they are taken on arrival.
	claimed map[string]bool
	// started is set once the pumps that follow this channel's streams are running;
	// store and id are how they and the channel's writes reach the streams. Every
	// channel is stream-backed: the single reader drains the value stream the way it
	// drains a local channel — the pump mirrors it into items — so no per-receive
	// state is kept here. stop ends this channel's pumps on retire, ahead of the
	// run-wide teardown, so a run creating channels in a loop does not hold a pump
	// per channel for its whole life (see [chanState.retire]).
	started bool
	store   Store
	id      string
	stop    context.CancelFunc
	// announced is set once the reader has written its consume stream's link marker,
	// so the announce is made once (see [chanState.announceReader]).
	announced bool
}

func newChanState(capacity int) *chanState {
	return &chanState{capacity: capacity, changed: make(chan struct{})}
}

// broadcast wakes everyone waiting. Call with mu held.
func (cs *chanState) broadcast() {
	close(cs.changed)
	cs.changed = make(chan struct{})
}

// announceReader tells the host, once, that this worker reads the channel, so a
// host that pushes values to a reader's worker knows to start. Called as the
// reader first waits for a value; the role is unknown when a run eagerly shares
// its channels for a spawned thread, so it cannot be settled at link time. A
// purely local channel (no link) and a host that needs no push do nothing.
func (cs *chanState) announceReader(ctx context.Context, t *threadState) error {
	cs.mu.Lock()
	if cs.announced || cs.id == "" {
		cs.mu.Unlock()
		return nil
	}
	cs.announced = true
	id := cs.id
	cs.mu.Unlock()
	// Write the consume stream's link marker and commit it, so it comes home and the
	// engine starts pushing the channel's values to this worker. Committed at once,
	// off any event the blocked reader has yet to record.
	ev := &protos.Event{Payload: protos.PackEventPayload(&protos.ChannelItem{Link: true})}
	if err := t.tx.AppendTo(context.WithoutCancel(ctx), ChannelConsumeStream(id), ev); err != nil {
		return err
	}
	return t.tx.Flush(context.WithoutCancel(ctx))
}

// put queues an item and reports it. viaPump marks the call as the pump
// delivering a value read back off the stream, as against the writer queuing its
// own send: the writer keeps only the identity — its bytes are on the value
// stream — and the pump supplies the bytes the reader takes, filling the writer's
// placeholder or queuing a fresh item for a reader attached to another run's channel.
func (cs *chanState) put(ctx context.Context, from string, seq uint64, data []byte, viaPump bool) *chanItem {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	// Already queued under this identity by a previous attempt's send now replaying,
	// or by the writer's own send: reuse it, and let the pump fill the bytes onto the
	// placeholder the writer queued.
	if it := cs.find(from, seq); it != nil {
		cs.fillLocked(it, data, viaPump)
		return it
	}
	// Room is what this run has been told, true a moment ago: two senders on two
	// machines can each take the last place, and the channel is briefly one over.
	// Bounded and rare, and the alternative is a round trip per send.
	item := &chanItem{from: from, seq: seq, data: data, buffered: cs.roomFor(nil)}
	if !viaPump {
		// The writer's own bytes are on the value stream, so it keeps only the
		// identity — to dedupe a replayed send and hold a place for backpressure. The
		// pump fills the bytes (viaPump) when it reads them back.
		item.data = nil
	}
	if cs.claimed[itemKey(from, seq)] {
		delete(cs.claimed, itemKey(from, seq))
		item.taken = true
		item.data = nil // claimed by a replayed receive; its value comes from the offset, not here
	}
	cs.items = append(cs.items, item)
	if cs.byKey == nil {
		cs.byKey = map[string]*chanItem{}
	}
	cs.byKey[itemKey(from, seq)] = item
	cs.broadcast()
	return item
}

// fillLocked gives a placeholder its value when the pump delivers it from the
// stream: the writer queued the item by identity alone, its bytes on the stream. A
// taken item (a replayed receive claimed it, reading its value from the offset)
// keeps none. Call with mu held.
func (cs *chanState) fillLocked(it *chanItem, data []byte, viaPump bool) {
	if viaPump && !it.taken && it.data == nil && data != nil {
		it.data = data
		cs.broadcast()
	}
}

func itemKey(from string, seq uint64) string { return fmt.Sprintf("%s#%d", from, seq) }

// claim takes one named item on behalf of a replayed receive: now, if it is
// queued, and otherwise on arrival.
func (cs *chanState) claim(from string, seq uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if it := cs.find(from, seq); it != nil {
		if !it.taken {
			it.taken = true
			cs.broadcast()
		}
		// A replayed receive reads its value back from the offset it recorded, not
		// from the buffer, so the re-sent copy queued here is dead weight — drop it
		// rather than hold the run's whole traffic in memory on a resume.
		it.data = nil
		return
	}
	if cs.claimed == nil {
		cs.claimed = map[string]bool{}
	}
	cs.claimed[itemKey(from, seq)] = true
}

// consume drops a received item's value once the receiver has decoded and
// recorded it. Nothing reads a taken item's data again — find, roomFor and
// pending use only its identity and flags — and holding it kept every value ever
// received on the channel in memory for the channel's life. The recorded event
// keeps its own reference to the bytes, so this does not disturb replay.
func (cs *chanState) consume(item *chanItem) {
	cs.mu.Lock()
	item.data = nil
	item.consumed = true
	// Amortised: compact once the queue has grown well past its last floor, so a
	// long drain does not walk (find/roomFor/awaitAny/pending) an ever-growing
	// list of received items — quadratic over a wave — and does not hold their
	// structs. Doubling keeps a backlog that never drains from thrashing.
	if len(cs.items) >= 2*cs.floor+64 {
		cs.prune()
	}
	// Freeing the bytes opens a prefetch place; wake the pump so it pulls the next
	// value from the stream (see awaitPrefetchRoom).
	cs.broadcast()
	cs.mu.Unlock()
}

// prune drops consumed items from the queue. Safe because nothing reads a
// consumed item again: a receive has it, a replayed receive reads its value from
// the store, put dedupes an echo before the item is consumed, and a cross-run
// sender's own item is never consumed here so it stays to dedupe. Call with mu held.
func (cs *chanState) prune() {
	kept := cs.items[:0]
	for _, it := range cs.items {
		if it.consumed {
			delete(cs.byKey, itemKey(it.from, it.seq))
			continue
		}
		kept = append(kept, it)
	}
	for i := len(kept); i < len(cs.items); i++ {
		cs.items[i] = nil // let the pruned items be collected
	}
	cs.items = kept
	cs.floor = len(cs.items)
}

// find returns the queued item with an identity, or nil. Call with mu held.
func (cs *chanState) find(from string, seq uint64) *chanItem {
	return cs.byKey[itemKey(from, seq)]
}

// free marks a mirrored item freed when the reader reports consuming it, so a
// bounded sender counting room by its own mirror opens the place. Not taken: a
// replaying receiver may still need to take the value itself (see chanItem.freed).
func (cs *chanState) free(from string, seq uint64) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if it := cs.find(from, seq); it != nil && !it.freed {
		it.freed = true
		cs.broadcast()
	}
}

// reportConsumed tells the host a bounded shared channel's value was taken, so a
// sender counting room by its own mirror may free the place. Reports nothing on
// an unbounded channel — no sender waits on room — or a local one. Returns whether
// it reported, so the caller commits the report with the receive that justified it.
func (cs *chanState) reportConsumed(ctx context.Context, t *threadState, item *chanItem) (bool, error) {
	cs.mu.Lock()
	report := cs.capacity >= 0
	id := cs.id
	from, seq := item.from, item.seq
	cs.mu.Unlock()
	if !report {
		return false, nil
	}
	ev := &protos.Event{Payload: protos.PackEventPayload(&protos.ChannelItem{Consumed: true, From: from, Seq: seq})}
	if err := t.tx.AppendTo(ctx, ChannelConsumeStream(id), ev); err != nil {
		return false, fmt.Errorf("flow: receive on a shared channel: %w", err)
	}
	return true, nil
}

// announceClose tells the host the channel is closed, if it is shared. sender is
// the closing thread, since a close carries no sender of its own.
func (cs *chanState) announceClose(ctx context.Context, t *threadState) error {
	cs.mu.Lock()
	id := cs.id
	cs.mu.Unlock()
	ev := &protos.Event{Payload: protos.PackEventPayload(&protos.ChannelItem{Closed: true})}
	if err := t.tx.AppendTo(ctx, ChannelValueStream(id), ev); err != nil {
		return fmt.Errorf("flow: close a shared channel: %w", err)
	}
	return nil
}

func (cs *chanState) isClosed() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.closed
}

func (cs *chanState) shut() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if !cs.closed {
		cs.closed = true
		cs.broadcast()
	}
}

// awaitTaken blocks until a sent item is received, returning at once if the
// channel had room for it. Room can open while it waits — a receive takes
// something ahead of it — and then the item is buffered and the send complete.
// The thread is parked while it waits.
func (cs *chanState) awaitTaken(ctx context.Context, t *threadState, id string, item *chanItem) error {
	parked := false
	resume := noResume
	for {
		cs.mu.Lock()
		if !item.taken && !item.buffered && cs.roomFor(item) {
			item.buffered = true
		}
		if item.taken || item.freed || item.buffered {
			cs.mu.Unlock()
			return resume(ctx)
		}
		wait := cs.changed
		cs.mu.Unlock()

		if !parked {
			parked, resume = true, t.parkOn(ctx, WaitSend, id, item.seq)
		}
		select {
		case <-wait:
		case <-ctx.Done():
			_ = resume(t.base())
			return ctx.Err()
		}
	}
}

// awaitAny blocks until something can be taken, and returns nil when the channel
// is closed and drained. The single reader takes the first untaken item, on its
// own channel and on a shared one alike: a shared channel's host mirrors its
// ordered record into the queue, so the reader consumes it in that order without
// announcing anything. The take is reported to the host afterwards, committed
// with the receive (see [Channel.Recv], [chanState.reportConsumed]). The thread
// is parked while it waits.
func (cs *chanState) awaitAny(ctx context.Context, t *threadState, id string, recvSeq uint64) (*chanItem, error) {
	parked := false
	resume := noResume
	for {
		cs.mu.Lock()
		for _, it := range cs.items {
			if it.taken {
				continue
			}
			if it.data == nil {
				// A placeholder the writer queued whose bytes the pump has not yet
				// delivered from the stream; wait for them rather than hand back an empty
				// value or skip ahead of it, which would break the channel's order.
				break
			}
			it.taken = true
			cs.broadcast()
			cs.mu.Unlock()
			return it, resume(ctx)
		}
		if cs.closed && !cs.pending() {
			cs.mu.Unlock()
			return nil, resume(ctx)
		}
		wait := cs.changed
		cs.mu.Unlock()

		if !parked {
			parked, resume = true, t.parkOn(ctx, WaitRecv, id, recvSeq)
		}
		select {
		case <-wait:
		case <-ctx.Done():
			_ = resume(t.base())
			return nil, ctx.Err()
		}
	}
}

// roomFor reports whether the buffer has a place for item, or for a new item
// when item is nil: untaken items plus the senders ahead of it still waiting
// come to fewer than the capacity. Senders ahead count because they are owed a
// place first. Call with mu held.
func (cs *chanState) roomFor(item *chanItem) bool {
	if cs.capacity < 0 {
		return true // unbounded: a send never waits for room
	}
	used := 0
	for _, it := range cs.items {
		if it == item {
			break
		}
		if !it.taken && !it.freed {
			used++
		}
	}
	return used < cs.capacity
}

// pending reports whether any item is still untaken. Call with mu held.
func (cs *chanState) pending() bool {
	for _, it := range cs.items {
		if !it.taken {
			return true
		}
	}
	return false
}

// readable reports whether a receive would return without blocking: an item is
// waiting, or the channel is closed. For a [Context.Select] recv case over the
// run's own channels.
func (cs *chanState) readable() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.closed || cs.pending()
}

// sendable reports whether a send would queue without blocking: the buffer has
// room (always, when unbounded), or the channel is closed and the send returns
// at once refused. For a [Context.Select] send case over the run's own channels.
func (cs *chanState) sendable() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.closed || cs.roomFor(nil)
}

// changedChan is the channel that closes on the next change, for a waiter to
// block on and re-check.
func (cs *chanState) changedChan() <-chan struct{} {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.changed
}

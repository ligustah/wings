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
	if t.run.fragment {
		// Fragment run: any thread of it may use this, so link now; a replay's
		// live run already announced what was on it.
		if _, err := t.run.export(ctx, name, true); err != nil {
			// A channel that cannot be shared is unusable here; report on first
			// use, where an error can be returned.
			t.run.mu.Lock()
			delete(t.run.channels, name)
			t.run.mu.Unlock()
		}
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
		_, replay := c.run.encodingThread()
		var err error
		if id, err = c.run.export(context.Background(), c.name, replay); err != nil {
			return nil, err
		}
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
	t, cs, err := c.bind(ctx)
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
		// one transaction (pull.go), so it is durably queued — queue it locally
		// again without re-announcing. The relay delivers the one copy; the host
		// will hand it back here through the pump.
		item, err := cs.put(ctx, t.qualified(), seq, data, false)
		if err != nil {
			return err
		}
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

	// Announce the value, record the send, and commit the two together: the outbox
	// record and this event go home in one transaction (pull.go), so a replay that
	// finds the event knows the value is durably queued and need not re-announce.
	// Announce before recording so a direct coordinator run — where the two are
	// separate durable writes, not one transaction — never leaves an event with
	// nothing queued; a torn send there has no event and replays afresh. The value
	// itself is not recorded here: only the receiver's copy is (see Recv).
	item, err := cs.put(ctx, t.qualified(), seq, data, true)
	if err != nil {
		return err
	}
	t.record(&protos.ChannelSendEvent{Channel: c.name, Seq: seq})
	if cs.hosted() {
		if err := t.commit(ctx); err != nil {
			return err
		}
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

	t, cs, err := c.bind(ctx)
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
	rec := &protos.ChannelRecvEvent{Channel: c.name, FromThreadId: item.from, FromSeq: item.seq}
	// On a shared channel the host keeps the one copy of the value, read back from
	// it on replay by (from, seq); only a purely local channel, which has no host
	// record, records the bytes here. See [threadState.recordedValue].
	if !cs.hosted() {
		rec.Value = &protos.Data{Serialized: item.data}
	}
	t.record(rec)
	// Report the take and commit it with the receive, so the consume report is
	// never home without the receive that justified it (pull.go). A replay does not
	// report again — the original report came home with the receive it replays.
	if reported, err := cs.reportConsumed(ctx, item); err != nil {
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
	t, cs, err := c.bind(ctx)
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
	if err := cs.announceClose(ctx); err != nil {
		if ctx.Err() != nil {
			err = t.interrupt("close", err)
		}
		return err
	}
	t.record(&protos.ChannelSendEvent{Channel: c.name, Seq: seq, Closed: true})
	if cs.hosted() {
		if err := t.commit(ctx); err != nil {
			return err
		}
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

// bind resolves the calling thread and this channel's shared state.
func (c *Channel[T]) bind(ctx Context) (*threadState, *chanState, error) {
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
		cs.noteRole(c.mode)
		return t, cs, nil
	}
	if t.run != c.run {
		return nil, nil, fmt.Errorf("flow: channel %s belongs to another run", c.name)
	}
	cs := c.run.channel(c.name)
	if cs == nil {
		return nil, nil, fmt.Errorf("flow: channel %s is not part of this run", c.name)
	}
	cs.noteRole(c.mode)
	return t, cs, nil
}

// noteRole records that a handle of this mode is bound here. A read-capable
// handle means this run may receive on the channel, so its own queued sends are
// kept; a run that only ever holds a writer never receives here, so [chanState.put]
// can drop each sent value once the host has it. A locally created channel keeps
// its sends regardless — it is not attached, so it is read-capable by default.
func (cs *chanState) noteRole(mode handleMode) {
	if mode == modeWrite {
		return
	}
	cs.mu.Lock()
	cs.reads = true
	cs.mu.Unlock()
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
	// attached is set when this runtime was reached from another run through the
	// host, rather than created here. reads is set when a read-capable handle binds.
	// A run that only holds a writer for an attached channel never receives on it,
	// so put drops each sent value once the host has the canonical copy. See
	// [chanState.noteRole].
	attached bool
	reads    bool
	// floor is len(items) after the last prune; the queue is compacted once it has
	// grown enough past it that pruning stays amortised. See [chanState.prune].
	floor int
	// claimed names items a replayed receive took before they were queued, so
	// they are taken on arrival.
	claimed map[string]bool
	// link is set once the channel is shared with other runs. The single reader
	// then consumes the host's ordered record the same way it drains a local
	// channel — the pump mirrors that record into items — so no per-receive state
	// is kept here.
	link ChannelLink
}

func newChanState(capacity int) *chanState {
	return &chanState{capacity: capacity, changed: make(chan struct{})}
}

// broadcast wakes everyone waiting. Call with mu held.
func (cs *chanState) broadcast() {
	close(cs.changed)
	cs.changed = make(chan struct{})
}

// put queues a value and reports the item it queued. On a shared channel a new
// item is announced to the host first, when announce says so.
func (cs *chanState) put(ctx context.Context, from string, seq uint64, data []byte, announce bool) (*chanItem, error) {
	cs.mu.Lock()
	// Already queued under this identity by a previous attempt's send now
	// replaying, or returned from the host as the copy of one sent here: reuse
	// it, or a receive naming that identity would find two.
	if it := cs.find(from, seq); it != nil {
		cs.mu.Unlock()
		return it, nil
	}
	link := cs.link
	cs.mu.Unlock()

	if link != nil && announce {
		if err := link.Send(ctx, ChannelItem{From: from, Seq: seq, Data: data}); err != nil {
			return nil, fmt.Errorf("flow: send on a shared channel: %w", err)
		}
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()
	if it := cs.find(from, seq); it != nil {
		return it, nil
	}
	// Room is what this run has been told, true a moment ago: two senders on
	// two machines can each take the last place, and the channel is briefly one
	// over. Bounded and rare, and the alternative is a round trip per send.
	item := &chanItem{from: from, seq: seq, data: data, buffered: cs.roomFor(nil)}
	if cs.attached && !cs.reads {
		// A writer-only run never receives here, so its own queued value is read by
		// no one — the host holds the canonical copy a receiver reads. Keep the
		// identity to dedupe a replayed send; drop the bytes the wave would pile up.
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
	return item, nil
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
func (cs *chanState) reportConsumed(ctx context.Context, item *chanItem) (bool, error) {
	cs.mu.Lock()
	link, report := cs.link, cs.capacity >= 0
	from, seq := item.from, item.seq
	cs.mu.Unlock()
	if link == nil || !report {
		return false, nil
	}
	if err := link.Send(ctx, ChannelItem{Consumed: true, From: from, Seq: seq}); err != nil {
		return false, fmt.Errorf("flow: receive on a shared channel: %w", err)
	}
	return true, nil
}

// announceClose tells the host the channel is closed, if it is shared.
func (cs *chanState) announceClose(ctx context.Context) error {
	cs.mu.Lock()
	link := cs.link
	cs.mu.Unlock()
	if link == nil {
		return nil
	}
	if err := link.Send(ctx, ChannelItem{Closed: true}); err != nil {
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

// hosted reports whether the channel is shared through a host, whose record of
// it holds the values a replay reads back — rather than a purely local channel,
// whose receives record their own copy.
func (cs *chanState) hosted() bool {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.link != nil
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

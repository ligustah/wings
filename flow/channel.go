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
// Create one with [Context.NewChannel] or [Context.NewBufferedChannel] inside a
// Run, at a point every attempt reaches, and pass it to threads forked by
// [Context.Go] or [Context.Map]. Safe to use from all of them at once. Not
// usable outside a Run.
type Channel[T any] struct {
	name  string
	run   *runState
	codec dswire.Codec[T]

	// id and capacity are set on a handle that arrived from another run; mu
	// guards binding it to this run on first use.
	id       string
	capacity int
	mu       sync.Mutex
}

// NewChannel returns an unbuffered channel: a send completes when a receive
// takes it.
func (c Context) NewChannel[T any]() *Channel[T] {
	return newChannel[T](c, 0)
}

// unbounded is the capacity of a channel a send never waits on. Negative so it
// cannot be reached by a count of queued items.
const unbounded = -1

// NewUnboundedChannel returns a channel a send never blocks on: it has no
// capacity limit, so Send always completes at once and no sender is ever parked
// (and so never unloaded, which would replay its whole job). Use it when the
// receiver is guaranteed to drain the channel and backpressure is unwanted. It
// can grow without limit if the receiver falls behind.
func (c Context) NewUnboundedChannel[T any]() *Channel[T] {
	return newChannel[T](c, unbounded)
}

// NewBufferedChannel returns a channel that accepts capacity values before a
// send has to wait for a receive.
func (c Context) NewBufferedChannel[T any](capacity int) *Channel[T] {
	if capacity < 0 {
		capacity = 0
	}
	return newChannel[T](c, capacity)
}

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
	Channel  string `json:"channel"`
	Capacity int    `json:"capacity,omitempty"`
}

// MarshalJSON shares the channel so it can travel in a call's input, a result,
// or a value sent on another channel: the run's host is told and untaken sends
// go with it, so the run needs a [ChannelHost]. See [WithChannelHost].
func (c *Channel[T]) MarshalJSON() ([]byte, error) {
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
	return json.Marshal(channelHandle{Channel: id, Capacity: capacity})
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
	c.id, c.run, c.name, c.capacity = h.Channel, nil, h.Channel, h.Capacity
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

	// Given up on before, with the value queued: queue it again for a receiver
	// of this attempt and give up again.
	if ierr, ok := t.interrupted("send"); ok {
		if _, err := cs.put(ctx, t.qualified(), seq, data, true); err != nil {
			return err
		}
		return ierr
	}
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
	} else if cs.isClosed() {
		t.record(&protos.ChannelSendEvent{Channel: c.name, Seq: seq, Refused: true})
		if err := t.err(); err != nil {
			return err
		}
		return fmt.Errorf("%w: %s", ErrChannelClosed, c.name)
	}
	// A replayed send announces to other runs again: the record is made where
	// the sender is, but the copy other runs see travels separately, so an
	// attempt that died between the two left a send nobody received. Only the
	// replay's announce repairs it; duplicates are dropped by identity.
	item, err := cs.put(ctx, t.qualified(), seq, data, true)
	if err != nil {
		return err
	}
	if ev != nil {
		return t.err()
	}

	if err := cs.awaitTaken(ctx, t, c.sharedID(), item); err != nil {
		if ctx.Err() != nil {
			err = t.interrupt("send", err)
		}
		return err
	}
	// The value is not recorded: a replayed send re-encodes it from the body, and
	// a shared channel delivers through the relay, so a sender's own copy is never
	// read back. Only the receiver's copy is (see Recv), so recording it here just
	// stored every value a second time.
	t.record(&protos.ChannelSendEvent{Channel: c.name, Seq: seq})
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
	t.record(&protos.ChannelRecvEvent{
		Channel:      c.name,
		FromThreadId: item.from,
		FromSeq:      item.seq,
		Value:        &protos.Data{Serialized: item.data},
	})
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
		// Announced again, for the reason a replayed send is.
		if err := cs.announceClose(ctx); err != nil {
			return err
		}
		cs.shut()
		return t.err()
	}
	if err := cs.announceClose(ctx); err != nil {
		if ctx.Err() != nil {
			err = t.interrupt("close", err)
		}
		return err
	}
	cs.shut()
	t.record(&protos.ChannelSendEvent{Channel: c.name, Seq: seq, Closed: true})
	return t.err()
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
		cs, err := t.run.attach(ctx, c.id, c.capacity)
		if err != nil {
			return nil, nil, err
		}
		c.run, c.name = t.run, c.id
		if c.codec == nil {
			c.codec = dswire.ReflectCodec[T]{New: allocator[T]()}
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
	return t, cs, nil
}

// chanItem is one value in flight, tagged with where it came from so a receive
// can name it.
type chanItem struct {
	from string
	seq  uint64
	data []byte

	taken bool
	// buffered means the send completed on arrival, because the channel had
	// room for it.
	buffered bool
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
	// claimed names items a replayed receive took before they were queued, so
	// they are taken on arrival.
	claimed map[string]bool
	// link is set once the channel is shared with other runs. From then the
	// host decides who takes what: asked is the wants this attempt has sent,
	// and grants maps a want key to the item key the host gave it.
	link   ChannelLink
	asked  map[string]bool
	grants map[string]string
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
	if cs.claimed[itemKey(from, seq)] {
		delete(cs.claimed, itemKey(from, seq))
		item.taken = true
		item.data = nil // claimed by a replayed receive; its value comes from the offset, not here
	}
	cs.items = append(cs.items, item)
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

// grant records the host giving one item to one want: the item is marked taken,
// and a receive here waiting on that want finds it. The item precedes its grant
// on the record, so it is always here to be found.
func (cs *chanState) grant(g ChannelItem) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if it := cs.find(g.From, g.Seq); it != nil {
		it.taken = true
	}
	if cs.grants == nil {
		cs.grants = map[string]string{}
	}
	cs.grants[itemKey(g.To, g.ToSeq)] = itemKey(g.From, g.Seq)
	cs.broadcast()
}

// consume drops a received item's value once the receiver has decoded and
// recorded it. Nothing reads a taken item's data again — find, roomFor and
// pending use only its identity and flags — and holding it kept every value ever
// received on the channel in memory for the channel's life. The recorded event
// keeps its own reference to the bytes, so this does not disturb replay.
func (cs *chanState) consume(item *chanItem) {
	cs.mu.Lock()
	item.data = nil
	cs.mu.Unlock()
}

// find returns the queued item with an identity, or nil. Call with mu held.
func (cs *chanState) find(from string, seq uint64) *chanItem {
	for _, it := range cs.items {
		if it.from == from && it.seq == seq {
			return it
		}
	}
	return nil
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
		if item.taken || item.buffered {
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
// is closed and drained. On the run's own channel it takes the first untaken
// item; on a shared channel the receive is sent to the host as a want and it
// takes the item the host's grant names. A channel can become shared while a
// receive waits, and the receive carries on. The thread is parked while it waits.
func (cs *chanState) awaitAny(ctx context.Context, t *threadState, id string, recvSeq uint64) (*chanItem, error) {
	parked := false
	resume := noResume
	want := itemKey(t.qualified(), recvSeq)
	for {
		cs.mu.Lock()
		link := cs.link
		if link == nil {
			for _, it := range cs.items {
				if !it.taken {
					it.taken = true
					cs.broadcast()
					cs.mu.Unlock()
					return it, resume(ctx)
				}
			}
		} else {
			if key, ok := cs.grants[want]; ok {
				for _, it := range cs.items {
					if itemKey(it.from, it.seq) == key {
						cs.mu.Unlock()
						return it, resume(ctx)
					}
				}
			}
			if !cs.asked[want] {
				if cs.asked == nil {
					cs.asked = map[string]bool{}
				}
				cs.asked[want] = true
				cs.mu.Unlock()
				if err := link.Send(ctx, ChannelItem{Want: true, From: t.qualified(), Seq: recvSeq}); err != nil {
					return nil, fmt.Errorf("flow: receive on a shared channel: %w", err)
				}
				continue
			}
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
		if !it.taken {
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

// changedChan is the channel that closes on the next change, for a waiter to
// block on and re-check.
func (cs *chanState) changedChan() <-chan struct{} {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.changed
}

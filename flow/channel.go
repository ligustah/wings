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

// Channel carries typed values between the threads of one run.
//
// It is what a Go channel is for, with one difference that matters: a run
// can be replayed, and a Go channel cannot promise that the same value arrives
// first twice. So a receive is RECORDED — which thread's which send it took —
// and on a later attempt it waits for exactly that item rather than for
// whatever the scheduler offers. A send is not recorded that way, because a
// thread's nth send on a channel always carries the same value; what is
// recorded about a send is when it COMPLETED, which on an unbuffered channel is
// somebody else's decision.
//
// Create one with [Context.NewChannel] or [Context.NewBufferedChannel], inside a Run, at a
// point every attempt reaches — the same rule as everything else in a run's
// body. Pass it to threads forked by [Context.Go] or [Context.Map]
// the way you would pass a Go channel to a goroutine; it is safe to use from all
// of them at once.
//
// Not usable outside a Run. There is no history there to record a receive
// in, and a channel whose receives are not recorded is exactly the
// non-determinism this type exists to remove.
type Channel[T any] struct {
	name  string
	run   *runState
	codec dswire.Codec[T]

	// id is set on a handle that arrived from another run, capacity is what
	// the handle said, and mu guards binding it to this run on first use.
	id       string
	capacity int
	mu       sync.Mutex
}

// NewChannel returns an unbuffered channel: a send completes when a receive
// takes it.
func (c Context) NewChannel[T any]() *Channel[T] {
	return newChannel[T](c, 0)
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
		// Deliberately a channel that fails on use rather than a nil one: the
		// mistake is worth an error at the point it is made, and returning nil
		// would turn it into a panic somewhere else.
		return &Channel[T]{}
	}
	name := t.newChannelName()
	t.run.declareChannel(name, capacity)
	if t.run.fragment {
		// The rest of the run is elsewhere, and any thread of it may use
		// this. Linked, and nothing announced: there is nothing on it yet,
		// and if this is a replay the live run announced then.
		if _, err := t.run.export(ctx, name, true); err != nil {
			// Reported on first use rather than here, where there is no
			// error to return: a channel that cannot be shared is one this
			// process cannot use.
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

// Name is the channel's identity in the history, derived from the thread that
// created it and how many it had created before. Exported for diagnostics.
func (c *Channel[T]) Name() string { return c.name }

// sharedID is the channel's name to other runs: the id it came with, for a
// handle from another run, and otherwise the run's name and its own. Call
// after bind, which is what sets c.run.
func (c *Channel[T]) sharedID() string {
	if c.id != "" {
		return c.id
	}
	return c.run.channelID(c.name)
}

// channelHandle is how a channel appears in a call's input or output. The
// capacity travels with it, since a sender in another run has to know how
// much room there is.
type channelHandle struct {
	Channel  string `json:"channel"`
	Capacity int    `json:"capacity,omitempty"`
}

// MarshalJSON is what lets a channel leave its run: in a call's input, in a
// value sent on another channel, in a result. Marshalling SHARES it — the
// run's host is told, and what was sent so far and not taken goes with it —
// which needs the run to have a [ChannelHost]. See [WithChannelHost].
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

// UnmarshalJSON receives a channel another run shared. It is bound to this
// run on first use, which needs this run to have a [ChannelHost] too.
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

// Send puts a value on the channel.
//
// It blocks until the value is taken, or until the buffer has room. A send that
// completed on a previous attempt returns as soon as the value is queued: it is
// already known to have got through, and making a replay wait again for
// something that already happened is how a resumed run deadlocks.
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

	// Consumed before waiting, not after. The event says the send completed,
	// and a replay that waited first would be waiting for a receive that has
	// already been replayed away.
	ev, err := t.expect[*protos.ChannelSendEvent]()
	if err != nil {
		return err
	}
	// A replayed send is not announced to other runs again: they have it,
	// and the identity would only be dropped as a copy.
	item, err := cs.put(ctx, t.qualified(), seq, data, ev == nil)
	if err != nil {
		return err
	}
	if ev != nil {
		if ev.GetChannel() != c.name || ev.GetSeq() != seq || ev.GetClosed() {
			return continuityf("thread %q previously sent %s#%d at this point, but is now sending %s#%d",
				t.id, ev.GetChannel(), ev.GetSeq(), c.name, seq)
		}
		return t.err()
	}

	if err := cs.awaitTaken(ctx, t, c.sharedID(), item); err != nil {
		return err
	}
	t.record(&protos.ChannelSendEvent{
		Channel: c.name,
		Seq:     seq,
		Value:   &protos.Data{Serialized: data},
	})
	return t.err()
}

// Recv takes the next value off the channel.
//
// The second result is false when the channel is closed and everything sent has
// been taken, exactly as a Go receive reports it. It blocks until there is
// something to take or the channel is closed.
func (c *Channel[T]) Recv(ctx Context) (T, bool, error) {
	var zero T

	t, cs, err := c.bind(ctx)
	if err != nil {
		return zero, false, err
	}

	// Counted before the history is consulted, like a send: the number is
	// the receive's name to the host, and has to come out the same on a
	// replay.
	recvSeq := t.nextRecv(c.name)

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
		// The one place replay differs from a live run: the value is the one
		// the history says was taken, not the first one going. Two sends
		// racing produced one order last time and would produce another
		// now, and the run already acted on the first. The item itself is
		// claimed rather than waited for — the thread that sent it may have
		// been joined since, and a joined thread does not run again — so a
		// copy of it that does turn up, from a sender replaying, is taken
		// on arrival and not offered to a later receive.
		cs.claim(ev.GetFromThreadId(), ev.GetFromSeq())
		v, err := dswire.DecodeRecord(c.codec, ev.GetValue().GetSerialized())
		if err != nil {
			return zero, false, fmt.Errorf("flow: decode the recorded value from channel %s: %w", c.name, err)
		}
		return v, true, t.err()
	}

	item, err := cs.awaitAny(ctx, t, c.sharedID(), recvSeq)
	if err != nil {
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

// Close says nothing more will be sent.
//
// Receives drain what is already there and then report the channel closed.
// Recorded like a send, and ordered against the closing thread's other sends,
// because a receiver observes it exactly as it observes them.
func (c *Channel[T]) Close(ctx Context) error {
	t, cs, err := c.bind(ctx)
	if err != nil {
		return err
	}

	seq := t.nextSend(c.name)
	ev, err := t.expect[*protos.ChannelSendEvent]()
	if err != nil {
		return err
	}
	if ev != nil {
		if !ev.GetClosed() || ev.GetChannel() != c.name {
			return continuityf("thread %q previously sent %s#%d at this point, but is now closing %s",
				t.id, ev.GetChannel(), ev.GetSeq(), c.name)
		}
		cs.shut()
		return t.err()
	}
	if err := cs.announceClose(ctx); err != nil {
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
		// A handle from another run, used here for the first time: reach
		// the channel through the host and keep it under its id.
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
	// buffered means this item completed its send on arrival, because the
	// channel had room for it.
	buffered bool
}

// chanState is one channel's runtime, shared by every thread using it.
//
// Its own lock rather than the run's: the run's lock guards the event log, and
// a thread blocked on a receive would hold it for as long as it waited, which
// would stop every other thread from recording anything.
type chanState struct {
	capacity int

	mu      sync.Mutex
	items   []*chanItem
	closed  bool
	changed chan struct{}
	// claimed names items a replayed receive has taken before they were
	// queued, so that they are taken on arrival.
	claimed map[string]bool
	// link is set once the channel is shared with other runs. From then on
	// the host says who takes what: a receive is a want sent on the link,
	// and what it takes is the item the host's grant names. asked is the
	// wants this attempt has sent, by want key, and grants what the host
	// has granted to whom — want key to item key — as it arrives on the
	// link, along with what other runs send.
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

// put queues a value and reports the item it queued. On a shared channel a
// new item is announced to the host first, when announce says so.
func (cs *chanState) put(ctx context.Context, from string, seq uint64, data []byte, announce bool) (*chanItem, error) {
	cs.mu.Lock()
	// An item already queued under this identity was sent by a previous attempt
	// of the same thread and is being sent again by the replay of it — or
	// came back from the host as the copy of one sent here. Reuse it, or a
	// receive naming that identity would find two.
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
	// Complete on arrival if there is room. On a shared channel the count is
	// what this run has been told, which is the truth a moment ago: two
	// senders on two machines can each see the last place free and both
	// take it, and the channel is briefly one over. Bounded, and rare, and
	// the alternative is a round trip per send.
	item := &chanItem{from: from, seq: seq, data: data, buffered: cs.roomFor(nil)}
	if cs.claimed[itemKey(from, seq)] {
		delete(cs.claimed, itemKey(from, seq))
		item.taken = true
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
		return
	}
	if cs.claimed == nil {
		cs.claimed = map[string]bool{}
	}
	cs.claimed[itemKey(from, seq)] = true
}

// grant records the host's grant of one item to one want: the item is taken,
// by whichever receiver the want names, and a receive here waiting on that
// want finds its item. The item precedes its grant on the channel's record,
// so it is always here to be found.
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

func (cs *chanState) shut() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if !cs.closed {
		cs.closed = true
		cs.broadcast()
	}
}

// awaitTaken blocks until a sent item has been received, or returns at once if
// the channel had room for it. The thread is parked while it waits.
//
// Room can open while it waits — a receive takes something ahead of the
// item — and then the item is in the buffer, as it would be in Go's, and the
// send is complete.
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
			return ctx.Err()
		}
	}
}

// awaitAny blocks until something can be taken, and returns nil when the
// channel is closed and drained. The thread is parked while it waits.
//
// On a channel of this run's own, the first untaken item is taken here. On
// a shared channel the host takes it on this receive's behalf: the receive
// is sent to the host as a want, named by the thread and its receive number,
// and what it gets is the item the host's grant names — or nothing, once
// the channel is closed and every item has gone to someone. A channel can
// become shared while a receive waits on it, and the receive carries on
// under the new rule.
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
			return nil, ctx.Err()
		}
	}
}

// roomFor reports whether the buffer has a place for item, or for a new item
// when item is nil: what is in the buffer untaken, plus the senders ahead of
// it still waiting for a place, come to fewer than the capacity. Senders
// ahead count because they are owed a place first. Call with mu held.
func (cs *chanState) roomFor(item *chanItem) bool {
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

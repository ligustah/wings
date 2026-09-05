package flow

import (
	"context"
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
// Create one with [NewChannel] or [NewBufferedChannel], inside a Run, at a
// point every attempt reaches — the same rule as everything else in a run's
// body. Pass it to threads forked by [Go] or [Map]
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
}

// NewChannel returns an unbuffered channel: a send completes when a receive
// takes it.
func NewChannel[T any](ctx context.Context) *Channel[T] {
	return newChannel[T](ctx, 0)
}

// NewBufferedChannel returns a channel that accepts capacity values before a
// send has to wait for a receive.
func NewBufferedChannel[T any](ctx context.Context, capacity int) *Channel[T] {
	if capacity < 0 {
		capacity = 0
	}
	return newChannel[T](ctx, capacity)
}

func newChannel[T any](ctx context.Context, capacity int) *Channel[T] {
	t := threadFrom(ctx)
	if t == nil {
		// Deliberately a channel that fails on use rather than a nil one: the
		// mistake is worth an error at the point it is made, and returning nil
		// would turn it into a panic somewhere else.
		return &Channel[T]{}
	}
	name := t.newChannelName()
	t.run.declareChannel(name, capacity)
	return &Channel[T]{
		name:  name,
		run:   t.run,
		codec: dswire.ReflectCodec[T]{New: allocator[T]()},
	}
}

// Name is the channel's identity in the history, derived from the thread that
// created it and how many it had created before. Exported for diagnostics.
func (c *Channel[T]) Name() string { return c.name }

// Send puts a value on the channel.
//
// It blocks until the value is taken, or until the buffer has room. A send that
// completed on a previous attempt returns as soon as the value is queued: it is
// already known to have got through, and making a replay wait again for
// something that already happened is how a resumed run deadlocks.
func (c *Channel[T]) Send(ctx context.Context, v T) error {
	t, cs, err := c.bind(ctx)
	if err != nil {
		return err
	}

	data, err := dswire.EncodeRecord(c.codec, v)
	if err != nil {
		return fmt.Errorf("flow: encode value for channel %s: %w", c.name, err)
	}

	seq := t.nextSend(c.name)
	item := cs.put(t.id, seq, data)

	// Consumed before waiting, not after. The event says the send completed,
	// and a replay that waited first would be waiting for a receive that has
	// already been replayed away.
	ev, err := expect[*protos.ChannelSendEvent](t)
	if err != nil {
		return err
	}
	if ev != nil {
		if ev.GetChannel() != c.name || ev.GetSeq() != seq || ev.GetClosed() {
			return continuityf("thread %q previously sent %s#%d at this point, but is now sending %s#%d",
				t.id, ev.GetChannel(), ev.GetSeq(), c.name, seq)
		}
		return t.run.err()
	}

	if err := cs.awaitTaken(ctx, item); err != nil {
		return err
	}
	record(t, &protos.ChannelSendEvent{
		Channel: c.name,
		Seq:     seq,
		Value:   &protos.Data{Serialized: data},
	})
	return t.run.err()
}

// Recv takes the next value off the channel.
//
// The second result is false when the channel is closed and everything sent has
// been taken, exactly as a Go receive reports it. It blocks until there is
// something to take or the channel is closed.
func (c *Channel[T]) Recv(ctx context.Context) (T, bool, error) {
	var zero T

	t, cs, err := c.bind(ctx)
	if err != nil {
		return zero, false, err
	}

	ev, err := expect[*protos.ChannelRecvEvent](t)
	if err != nil {
		return zero, false, err
	}

	if ev != nil {
		if ev.GetChannel() != c.name {
			return zero, false, continuityf("thread %q previously received from %q at this point, but is now receiving from %q",
				t.id, ev.GetChannel(), c.name)
		}
		if ev.GetClosed() {
			return zero, false, t.run.err()
		}
		// The one place replay differs from a live run: wait for THAT item,
		// not for the first one going. Two sends racing produced one order last
		// time and would produce another now, and the run already acted on
		// the first.
		item, err := cs.awaitItem(ctx, ev.GetFromThreadId(), ev.GetFromSeq())
		if err != nil {
			return zero, false, err
		}
		v, err := c.decode(item)
		return v, err == nil, err
	}

	item, err := cs.awaitAny(ctx)
	if err != nil {
		return zero, false, err
	}
	if item == nil {
		record(t, &protos.ChannelRecvEvent{Channel: c.name, Closed: true})
		return zero, false, t.run.err()
	}
	record(t, &protos.ChannelRecvEvent{
		Channel:      c.name,
		FromThreadId: item.from,
		FromSeq:      item.seq,
	})
	v, err := c.decode(item)
	if err != nil {
		return zero, false, err
	}
	return v, true, t.run.err()
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
func (c *Channel[T]) Close(ctx context.Context) error {
	t, cs, err := c.bind(ctx)
	if err != nil {
		return err
	}

	seq := t.nextSend(c.name)
	ev, err := expect[*protos.ChannelSendEvent](t)
	if err != nil {
		return err
	}
	if ev != nil {
		if !ev.GetClosed() || ev.GetChannel() != c.name {
			return continuityf("thread %q previously sent %s#%d at this point, but is now closing %s",
				t.id, ev.GetChannel(), ev.GetSeq(), c.name)
		}
		cs.shut()
		return t.run.err()
	}
	cs.shut()
	record(t, &protos.ChannelSendEvent{Channel: c.name, Seq: seq, Closed: true})
	return t.run.err()
}

// bind resolves the calling thread and this channel's shared state.
func (c *Channel[T]) bind(ctx context.Context) (*threadState, *chanState, error) {
	if c.run == nil {
		return nil, nil, errors.New("flow: this channel was created outside a Run; " +
			"create it inside the run's body, with the context it was given")
	}
	t := threadFrom(ctx)
	if t == nil {
		return nil, nil, fmt.Errorf("flow: channel %s was used outside a run's thread", c.name)
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
}

func newChanState(capacity int) *chanState {
	return &chanState{capacity: capacity, changed: make(chan struct{})}
}

// broadcast wakes everyone waiting. Call with mu held.
func (cs *chanState) broadcast() {
	close(cs.changed)
	cs.changed = make(chan struct{})
}

// put queues a value and reports the item it queued.
func (cs *chanState) put(from string, seq uint64, data []byte) *chanItem {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	// An item already queued under this identity was sent by a previous attempt
	// of the same thread and is being sent again by the replay of it. Reuse it,
	// or a receive naming that identity would find two.
	for _, it := range cs.items {
		if it.from == from && it.seq == seq {
			return it
		}
	}

	pending := 0
	for _, it := range cs.items {
		if !it.taken && !it.buffered {
			pending++
		}
	}
	item := &chanItem{from: from, seq: seq, data: data, buffered: pending < cs.capacity}
	cs.items = append(cs.items, item)
	cs.broadcast()
	return item
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
// the channel had room for it.
func (cs *chanState) awaitTaken(ctx context.Context, item *chanItem) error {
	for {
		cs.mu.Lock()
		if item.taken || item.buffered {
			cs.mu.Unlock()
			return nil
		}
		wait := cs.changed
		cs.mu.Unlock()

		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// awaitItem blocks until one named item is available, then takes it.
//
// Used only on replay, where the receive already happened and the history says
// which item it took.
func (cs *chanState) awaitItem(ctx context.Context, from string, seq uint64) (*chanItem, error) {
	for {
		cs.mu.Lock()
		for _, it := range cs.items {
			if it.from == from && it.seq == seq && !it.taken {
				it.taken = true
				cs.broadcast()
				cs.mu.Unlock()
				return it, nil
			}
		}
		wait := cs.changed
		cs.mu.Unlock()

		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// awaitAny blocks until something can be taken, and returns nil when the
// channel is closed and drained.
func (cs *chanState) awaitAny(ctx context.Context) (*chanItem, error) {
	for {
		cs.mu.Lock()
		for _, it := range cs.items {
			if !it.taken {
				it.taken = true
				cs.broadcast()
				cs.mu.Unlock()
				return it, nil
			}
		}
		if cs.closed {
			cs.mu.Unlock()
			return nil, nil
		}
		wait := cs.changed
		cs.mu.Unlock()

		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

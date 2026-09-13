package flow

import (
	"context"
)

// A shared channel's records live on two durable streams named for its id:
// [ChannelValueStream] carries the writer's values and closes,
// [ChannelConsumeStream] the reader's consume reports and its link marker. flow
// says what a channel is; the [Store] moves the records and the engine carries
// the streams between machines. A channel has one reader and one writer: the
// reader drains the value stream in order the way it drains a local channel — the
// pump mirrors the stream into the queue — and a bounded sender frees its buffer
// as the reader's consume reports come back on the other stream.

// ChannelRetirer is an optional [Store] capability: reclaiming the channels a
// thread created during an in-process call, once the call returns. The call's
// result is recorded and a replay re-inserts it rather than re-entering the call,
// so those channels hold nothing a later replay reads — the same reason a settled
// job's channels are reclaimed. ids are the channels' shared ids ("<run>/<name>").
// Best-effort and asynchronous: the store reclaims each when it is safe to.
type ChannelRetirer interface {
	RetireChannels(ctx context.Context, run string, ids []string)
}

// channelID names a channel of this run to other runs.
func (r *runState) channelID(name string) string { return r.name + "/" + name }

// linkContext is the context the run's channel pumps live on, ended by finish.
func (r *runState) linkContext() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.linkCtx == nil {
		r.linkCtx, r.linkStop = context.WithCancel(context.Background())
	}
	return r.linkCtx
}

// encoding serialises the run's encodes, one at a time, so a channel shared in a
// value is named without racing another thread's encode.
func (r *runState) encoding(encode func() ([]byte, error)) ([]byte, error) {
	r.encMu.Lock()
	defer r.encMu.Unlock()
	return encode()
}

// attach connects this run to a channel another run owns, once. mode is the
// handle's role, deciding which of the channel's streams this run's pumps follow.
func (r *runState) attach(ctx context.Context, id string, capacity int, mode handleMode) (*chanState, error) {
	if cs := r.channel(id); cs != nil {
		return cs, nil
	}
	cs := newChanState(capacity)

	r.mu.Lock()
	if r.channels == nil {
		r.channels = map[string]*chanState{}
	}
	if existing, ok := r.channels[id]; ok {
		r.mu.Unlock()
		return existing, nil
	}
	r.channels[id] = cs
	r.mu.Unlock()

	cs.activate(r.linkContext(), r.store, id, mode)
	return cs, nil
}

// closeLinks stops the pumps this run's channels run. Called when the attempt is over.
func (r *runState) closeLinks() {
	r.mu.Lock()
	stop := r.linkStop
	r.linkStop, r.linkCtx = nil, nil
	r.mu.Unlock()
	if stop != nil {
		stop()
	}
}

// channelPrefetch bounds how many value-carrying items the reader's pump holds in
// memory at once: it stops pulling from the durable stream once this many are
// buffered and unconsumed, and resumes as the run body consumes them. So a reader
// that falls behind leaves the backlog on the stream (disk, retention-bounded)
// rather than mirroring it all into RAM. Independent of channel capacity, which only
// governs whether the sender parks.
const channelPrefetch = 64

// awaitPrefetchRoom blocks until fewer than channelPrefetch value-carrying items are
// buffered, or ctx ends (then it returns false). Only items still holding their bytes
// count — a consumed, claimed, or writer-dropped item has none — so the bound tracks
// resident memory, not queue length. Consuming a value frees a place and wakes it.
func (cs *chanState) awaitPrefetchRoom(ctx context.Context) bool {
	for {
		cs.mu.Lock()
		held := 0
		for _, it := range cs.items {
			if it.data != nil {
				held++
			}
		}
		if held < channelPrefetch {
			cs.mu.Unlock()
			return true
		}
		wait := cs.changed
		cs.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return false
		}
	}
}

// activate makes a channel stream-backed: it remembers the channel's id and store
// and starts the pumps that follow its streams, once. mode is this run's role —
// reader, writer, or both — deciding which streams it follows: a reader follows
// the value stream (values and closes), a writer follows the consume stream (the
// reader's reports that free a bounded buffer), a channel used both ways both.
func (cs *chanState) activate(ctx context.Context, store Store, id string, mode handleMode) {
	cs.mu.Lock()
	if cs.started {
		cs.mu.Unlock()
		return
	}
	cs.started = true
	cs.store = store
	cs.id = id
	cs.mu.Unlock()

	if mode != modeWrite {
		go cs.pumpValues(ctx, store, id)
	}
	if mode != modeRead {
		go cs.pumpConsumes(ctx, store, id)
	}
}

// pumpValues follows the channel's value stream, mirroring its values into the
// queue in order and shutting the channel on a close. It holds only a bounded
// read-ahead in memory; the durable stream keeps the rest until the reader
// catches up. A closing context ends the wait and the pump.
func (cs *chanState) pumpValues(ctx context.Context, store Store, id string) {
	_ = store.Follow(ctx, ChannelValueStream(id), 0, func(ea EventAt) bool {
		it := ea.Event.GetChannelItem()
		if it == nil {
			return true
		}
		switch {
		case it.GetClosed():
			cs.shut()
		case it.GetLink(), it.GetConsumed():
			// A link marker or a consume report has no value and never joins the
			// channel's record; the value stream should carry neither, but skip them.
		default:
			if !cs.awaitPrefetchRoom(ctx) {
				return false
			}
			// Fills a placeholder the writer queued by identity, or queues the value
			// for a reader attached to a channel another run writes.
			cs.put(ctx, it.GetFrom(), it.GetSeq(), it.GetData(), true)
		}
		return true
	})
}

// pumpConsumes follows the channel's consume stream, freeing a bounded sender's
// mirrored place as the reader reports taking each value.
func (cs *chanState) pumpConsumes(ctx context.Context, store Store, id string) {
	_ = store.Follow(ctx, ChannelConsumeStream(id), 0, func(ea EventAt) bool {
		if it := ea.Event.GetChannelItem(); it.GetConsumed() {
			cs.free(it.GetFrom(), it.GetSeq())
		}
		return true
	})
}

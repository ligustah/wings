package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// A channel belongs to the run that created it, and its values live in that
// run's memory. For a channel to reach another run — a workflow's activity on
// another machine, say — the values have to travel, and this file is the seam
// they travel through. flow says what a shared channel is; whatever hosts the
// runs says how the bytes get from one to the other.
//
// A shared channel is a QUEUE, not a rendezvous: a send completes once the
// value is with the host, whatever the channel's declared capacity, since a
// rendezvous across a network is a round trip nobody asked for. And every run
// that receives from it sees every value — threads within one run compete for
// values the way they always did, but two runs each get the whole sequence.
// One run, one receiver, is the ordinary case and is exactly a queue.

// ChannelItem is one value on a shared channel, or its close.
type ChannelItem struct {
	// From is the sender, as "<run>/<thread>", and Seq is that sender's nth
	// send on the channel. Together they identify the item everywhere, which
	// is what lets a replayed receive name the value it took and a copy that
	// arrives twice be dropped.
	From string
	Seq  uint64
	Data []byte
	// Closed marks a close rather than a value. From and Seq are empty.
	Closed bool
}

// ChannelLink is one run's connection to a shared channel: where its sends
// go, and where every run's sends — its own included — arrive from.
type ChannelLink interface {
	// Send hands the host one item this run sent.
	Send(ctx context.Context, item ChannelItem) error
	// Items delivers every item on the channel from its beginning, in the
	// host's order, calling yield for each as it becomes known, and returns
	// when yield returns false or ctx ends. Items this run sent come back
	// through here too.
	Items(ctx context.Context, yield func(ChannelItem) bool) error
	// Close releases the link. The channel itself is unaffected.
	Close() error
}

// ChannelHost is what carries channels between runs. A run given one, with
// [WithChannelHost], can hand its channels to other runs and use channels
// handed to it.
type ChannelHost interface {
	// Link connects the run named run to the channel named id, which is
	// "<owning run>/<channel name>" — the same run, when it is sharing its
	// own. Called once per attempt of a run for each channel it shares or
	// uses.
	Link(ctx context.Context, run, id string) (ChannelLink, error)
}

// WithChannelHost lets this run share channels with other runs. Without one,
// a channel that leaves the run in a call's input is an error at the call.
func WithChannelHost(h ChannelHost) RunOption { return func(o *runOptions) { o.host = h } }

// channelID names a channel of this run to other runs.
func (r *runState) channelID(name string) string { return r.name + "/" + name }

// linkContext is the context the run's links live on: the run's attempt,
// ended by finish.
func (r *runState) linkContext() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.linkCtx == nil {
		r.linkCtx, r.linkStop = context.WithCancel(context.Background())
	}
	return r.linkCtx
}

// export makes one of this run's channels reachable from other runs, if it
// is not already, and returns its id.
//
// What was sent before the channel left the run goes to the host now, taken
// or not: another run receiving from the channel is owed the whole sequence.
func (r *runState) export(ctx context.Context, name string) (string, error) {
	id := r.channelID(name)
	cs := r.channel(name)
	if cs == nil {
		return "", fmt.Errorf("flow: channel %s is not part of this run", name)
	}
	cs.mu.Lock()
	linked := cs.link != nil
	cs.mu.Unlock()
	if linked {
		return id, nil
	}
	if r.host == nil {
		return "", fmt.Errorf("flow: channel %s cannot leave this run: the run has no channel host", name)
	}
	link, err := r.host.Link(ctx, r.name, id)
	if err != nil {
		return "", fmt.Errorf("flow: share channel %s: %w", name, err)
	}

	cs.mu.Lock()
	if cs.link != nil {
		cs.mu.Unlock()
		_ = link.Close()
		return id, nil
	}
	cs.link = link
	items := append([]*chanItem(nil), cs.items...)
	closed := cs.closed
	// A queue from here on: a send still waiting to be taken is complete
	// now, since the value is about to be with the host.
	for _, it := range items {
		it.buffered = true
	}
	cs.broadcast()
	cs.mu.Unlock()

	for _, it := range items {
		if err := link.Send(ctx, ChannelItem{From: it.from, Seq: it.seq, Data: it.data}); err != nil {
			return "", fmt.Errorf("flow: share channel %s: %w", name, err)
		}
	}
	if closed {
		if err := link.Send(ctx, ChannelItem{Closed: true}); err != nil {
			return "", fmt.Errorf("flow: share channel %s: %w", name, err)
		}
	}
	go cs.pump(r.linkContext(), link)
	return id, nil
}

// attach connects this run to a channel another run owns, once.
func (r *runState) attach(ctx context.Context, id string) (*chanState, error) {
	if cs := r.channel(id); cs != nil {
		return cs, nil
	}
	if r.host == nil {
		return nil, fmt.Errorf("flow: channel %s belongs to another run, and this run has no channel host to reach it", id)
	}
	link, err := r.host.Link(ctx, r.name, id)
	if err != nil {
		return nil, fmt.Errorf("flow: reach channel %s: %w", id, err)
	}
	cs := newChanState(0)
	cs.link = link

	r.mu.Lock()
	if r.channels == nil {
		r.channels = map[string]*chanState{}
	}
	if existing, ok := r.channels[id]; ok {
		r.mu.Unlock()
		_ = link.Close()
		return existing, nil
	}
	r.channels[id] = cs
	r.mu.Unlock()

	go cs.pump(r.linkContext(), link)
	return cs, nil
}

// closeLinks ends every link this run holds. Called when the attempt is over.
func (r *runState) closeLinks() {
	r.mu.Lock()
	stop := r.linkStop
	r.linkStop, r.linkCtx = nil, nil
	var links []ChannelLink
	for _, cs := range r.channels {
		cs.mu.Lock()
		if cs.link != nil {
			links = append(links, cs.link)
		}
		cs.mu.Unlock()
	}
	r.mu.Unlock()
	if stop != nil {
		stop()
	}
	for _, l := range links {
		_ = l.Close()
	}
}

// pump delivers what arrives on the link into the channel's local state.
func (cs *chanState) pump(ctx context.Context, link ChannelLink) {
	_ = link.Items(ctx, func(it ChannelItem) bool {
		if it.Closed {
			cs.shut()
			return true
		}
		// Already ours, or already here from an earlier delivery: put drops
		// the copy. Not announced back to the link, where it came from.
		_, _ = cs.put(ctx, it.From, it.Seq, it.Data, false)
		return true
	})
}

// MemChannelHost carries channels between runs in one process, in memory.
// For tests, and for runs that all live in one program.
type MemChannelHost struct {
	mu    sync.Mutex
	chans map[string]*memChannel
}

// NewMemChannelHost returns an empty in-memory host.
func NewMemChannelHost() *MemChannelHost { return &MemChannelHost{chans: map[string]*memChannel{}} }

// Link implements [ChannelHost].
func (h *MemChannelHost) Link(_ context.Context, _, id string) (ChannelLink, error) {
	if id == "" {
		return nil, errors.New("flow: a channel id is required")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := h.chans[id]
	if ch == nil {
		ch = &memChannel{seen: map[string]bool{}, changed: make(chan struct{})}
		h.chans[id] = ch
	}
	return &memLink{ch: ch}, nil
}

type memChannel struct {
	mu      sync.Mutex
	items   []ChannelItem
	seen    map[string]bool
	closed  bool
	changed chan struct{}
}

type memLink struct{ ch *memChannel }

func (l *memLink) Send(_ context.Context, it ChannelItem) error {
	ch := l.ch
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if it.Closed {
		if ch.closed {
			return nil
		}
		ch.closed = true
	} else {
		key := fmt.Sprintf("%s#%d", it.From, it.Seq)
		if ch.seen[key] {
			return nil
		}
		ch.seen[key] = true
	}
	ch.items = append(ch.items, it)
	close(ch.changed)
	ch.changed = make(chan struct{})
	return nil
}

func (l *memLink) Items(ctx context.Context, yield func(ChannelItem) bool) error {
	ch := l.ch
	next := 0
	for {
		ch.mu.Lock()
		items := ch.items[next:]
		wait := ch.changed
		ch.mu.Unlock()
		for _, it := range items {
			next++
			if !yield(it) {
				return nil
			}
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (l *memLink) Close() error { return nil }

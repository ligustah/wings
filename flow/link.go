package flow

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Sharing a channel between runs. flow says what a shared channel is; a
// [ChannelHost] moves the bytes. A shared channel has one reader: the host keeps
// the channel's records in one order, and the reader consumes them in that order
// the way it drains a local channel, so no cross-machine arbitration is needed. A
// sender counts room by the consume reports the host relays back.

// ChannelItem is one record of a shared channel: a value, a close, or the
// reader's report that it consumed a value.
type ChannelItem struct {
	// From is the sender "<run>/<thread>" and Seq its nth send, identifying the
	// value everywhere. On a consume report, From/Seq name the value consumed.
	From string
	Seq  uint64
	Data []byte
	// Closed marks a close rather than a value.
	Closed bool
	// Consumed reports that the single reader took the value named by From/Seq, so
	// a sender counting room by what it has mirrored can free the place. Relayed to
	// every link; a sender applies it to its own queued send.
	Consumed bool
}

// ChannelLink is one run's connection to a shared channel.
type ChannelLink interface {
	// Send hands the host one record this run made: a value, a close, or a want.
	Send(ctx context.Context, item ChannelItem) error
	// Items delivers the channel's record from the beginning in the host's order,
	// calling yield for each, returning when yield returns false or ctx ends.
	Items(ctx context.Context, yield func(ChannelItem) bool) error
	// Close releases the link; the channel is unaffected.
	Close() error
}

// ChannelHost carries channels between runs. A run given one with
// [WithChannelHost] can share its channels and use channels handed to it.
type ChannelHost interface {
	// Link connects run to the channel id ("<owning run>/<channel name>"). Called
	// once per attempt per channel a run shares or uses.
	Link(ctx context.Context, run, id string) (ChannelLink, error)
}

// ChannelValueReader is an optional [ChannelHost] capability: reading back the
// values a channel carried, so a receiver's replay need not keep its own copy of
// what it took — the host's record of the channel is the one copy. A receive on
// a shared channel is recorded by identity alone (from and seq, see
// [ChannelRecvEvent]); replay finds the bytes here. cursor is an opaque position
// in the host's order, zero at the start; a read returns the value records at or
// after it, each with the position to continue from.
type ChannelValueReader interface {
	ChannelValues(ctx context.Context, id string, cursor int64, n int) ([]ChannelValueAt, error)
}

// ChannelValueAt is one value a channel carried, with the cursor to read the
// next from. From and Seq identify it, matching a [ChannelRecvEvent]'s from.
type ChannelValueAt struct {
	From string
	Seq  uint64
	Data []byte
	Next int64
}

// ChannelRetirer is an optional [ChannelHost] capability: reclaiming the channels
// a thread created during an in-process call, once the call returns. The call's
// result is recorded and a replay re-inserts it rather than re-entering the call,
// so those channels hold nothing a later replay reads — the same reason a settled
// job's channels are reclaimed. ids are the channels' shared ids ("<run>/<name>").
// Best-effort and asynchronous: the host reclaims each when it is safe to (a
// shared channel waits for its senders' data to arrive), and a run's end reclaims
// whatever remains.
type ChannelRetirer interface {
	RetireChannels(ctx context.Context, run string, ids []string)
}

// WithChannelHost lets this run share channels with other runs. Without one, a
// channel that leaves the run in a call's input is an error at the call.
func WithChannelHost(h ChannelHost) RunOption { return func(o *runOptions) { o.host = h } }

// channelID names a channel of this run to other runs.
func (r *runState) channelID(name string) string { return r.name + "/" + name }

// linkContext is the context the run's links live on, ended by finish.
func (r *runState) linkContext() context.Context {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.linkCtx == nil {
		r.linkCtx, r.linkStop = context.WithCancel(context.Background())
	}
	return r.linkCtx
}

// export makes one of this run's channels reachable from other runs and returns
// its id. What was sent before the channel left and not yet taken goes to the
// host now — except on a replay, where the host was told the first time and what
// this thread thinks is untaken may be a value another thread has not yet
// replayed taking.
func (r *runState) export(ctx context.Context, name string, replay bool) (string, error) {
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
	var items []*chanItem
	for _, it := range cs.items {
		if !it.taken {
			items = append(items, it)
		}
	}
	closed := cs.closed
	cs.broadcast()
	cs.mu.Unlock()

	if !replay {
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
	}
	go cs.pump(r.linkContext(), link)
	return id, nil
}

// encoding runs encode as thread t's, so a channel in the value is exported on
// t's behalf and a replayed encode exports nothing anew. One encode at a time per
// run, which is how [Channel.MarshalJSON] learns whose encode it is in.
func (r *runState) encoding(t *threadState, encode func() ([]byte, error)) ([]byte, error) {
	r.encMu.Lock()
	defer r.encMu.Unlock()
	r.encoder = t
	defer func() { r.encoder = nil }()
	return encode()
}

// encodingThread reports the thread whose encode is under way, and whether it is
// replaying. Valid only on the goroutine holding encMu.
func (r *runState) encodingThread() (t *threadState, replay bool) {
	if r.encoder == nil {
		return nil, false
	}
	return r.encoder, r.encoder.peek() != nil
}

// attach connects this run to a channel another run owns, once.
func (r *runState) attach(ctx context.Context, id string, capacity int) (*chanState, error) {
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
	cs := newChanState(capacity)
	cs.link = link
	cs.attached = true

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

// pump delivers the channel's record, as it arrives on the link, into local state.
func (cs *chanState) pump(ctx context.Context, link ChannelLink) {
	_ = link.Items(ctx, func(it ChannelItem) bool {
		switch {
		case it.Closed:
			cs.shut()
		case it.Consumed:
			// The reader consumed this value; free the place in a sender's own mirror.
			cs.free(it.From, it.Seq)
		default:
			// put drops a copy already here; not announced back to the link.
			_, _ = cs.put(ctx, it.From, it.Seq, it.Data, false)
		}
		return true
	})
}

// MemChannelHost carries channels between runs in one process, in memory. For
// tests, and for runs that all live in one program.
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
		ch = &memChannel{arbiter: NewArbiter(), changed: make(chan struct{})}
		h.chans[id] = ch
	}
	return &memLink{ch: ch}, nil
}

// ChannelValues implements [ChannelValueReader] over the in-memory record.
// cursor is an index into it; each returned value carries the next index.
func (h *MemChannelHost) ChannelValues(_ context.Context, id string, cursor int64, n int) ([]ChannelValueAt, error) {
	h.mu.Lock()
	ch := h.chans[id]
	h.mu.Unlock()
	if ch == nil {
		return nil, nil
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	var out []ChannelValueAt
	for i := cursor; i < int64(len(ch.items)) && len(out) < n; i++ {
		it := ch.items[i]
		if it.Consumed || it.Closed {
			continue
		}
		out = append(out, ChannelValueAt{From: it.From, Seq: it.Seq, Data: it.Data, Next: i + 1})
	}
	return out, nil
}

type memChannel struct {
	arbiter *Arbiter

	mu      sync.Mutex
	items   []ChannelItem // the record
	changed chan struct{}
}

type memLink struct{ ch *memChannel }

func (l *memLink) Send(_ context.Context, it ChannelItem) error {
	ch := l.ch
	ch.mu.Lock()
	defer ch.mu.Unlock()
	recs := ch.arbiter.Offer(it)
	if len(recs) == 0 {
		return nil
	}
	ch.items = append(ch.items, recs...)
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

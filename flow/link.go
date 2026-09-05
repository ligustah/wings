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
// A shared channel behaves as a channel between threads does: each value is
// taken by ONE receiver, wherever the receivers run, and a send waits while
// the channel is at capacity. Since two receivers on two machines cannot
// agree between themselves, the host agrees for them — a receive is a want
// the host answers with a grant, by the rule in [Arbiter] — and since a
// sender cannot see the other machines' receives, it counts room by what the
// host has told it. A channel's record is, in order, what was sent, what was
// wanted, and what the host granted to whom; a run reads its own past off it
// exactly as another run's.

// ChannelItem is one record of a shared channel: a value, a close, a
// receiver's want, or the host's grant of a value to a want.
type ChannelItem struct {
	// From is the sender, as "<run>/<thread>", and Seq is that sender's nth
	// send on the channel. Together they identify the value everywhere,
	// which is what lets a replayed receive name the value it took and a
	// copy that arrives twice be dropped. On a want, From is the receiver
	// and Seq its nth receive on the channel; on a grant, they name the
	// value granted.
	From string
	Seq  uint64
	Data []byte
	// Closed marks a close rather than a value. From and Seq are empty.
	Closed bool
	// Want marks a receiver asking for a value. Data is empty.
	Want bool
	// To and ToSeq, set on a grant, name the want — the receiver and its
	// nth receive — that the value From/Seq is given to. A receive waits
	// for the grant naming it. Only the host makes grants.
	To    string
	ToSeq uint64
}

// ChannelLink is one run's connection to a shared channel: where what it
// sends and wants goes, and where the channel's record — every run's sends
// and wants, its own included, and the host's grants — arrives from.
type ChannelLink interface {
	// Send hands the host one record this run made: a value, a close, or a
	// want. The host puts it on the channel's record if it is new, and
	// grants what it can; see [Arbiter].
	Send(ctx context.Context, item ChannelItem) error
	// Items delivers the channel's record from its beginning, in the host's
	// order, calling yield for each record as it becomes known, and returns
	// when yield returns false or ctx ends. What this run sent comes back
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
// What was sent before the channel left the run and not yet taken goes to
// the host now: from here on the host says who takes what, and a value still
// here is a value it must know about. What was taken stays taken. And none
// of that when the export is a REPLAY — the thread is re-encoding a value
// its history says it encoded before — since the host was told then, and
// what a replayed thread thinks is untaken may be a value another thread has
// not yet replayed taking.
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

// encoding runs encode as thread t's, so that a channel in the value it
// encodes is exported on t's behalf: a thread replaying an encode it made
// before exports nothing anew. One encode at a time per run, which is how
// [Channel.MarshalJSON], given no context, learns whose encode it is in.
func (r *runState) encoding(t *threadState, encode func() ([]byte, error)) ([]byte, error) {
	r.encMu.Lock()
	defer r.encMu.Unlock()
	r.encoder = t
	defer func() { r.encoder = nil }()
	return encode()
}

// encoder reports the thread whose encode is under way — nil outside one —
// and whether that thread is replaying. Valid only on the encoding
// goroutine, which is the one holding encMu.
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

// pump delivers the channel's record, as it arrives on the link, into the
// channel's local state.
func (cs *chanState) pump(ctx context.Context, link ChannelLink) {
	_ = link.Items(ctx, func(it ChannelItem) bool {
		switch {
		case it.Closed:
			cs.shut()
		case it.To != "":
			cs.grant(it)
		case it.Want:
			// Another run's, or this one's coming back. The grant is what
			// matters, and it follows.
		default:
			// Already ours, or already here from an earlier delivery: put
			// drops the copy. Not announced back to the link, where it came
			// from.
			_, _ = cs.put(ctx, it.From, it.Seq, it.Data, false)
		}
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
		ch = &memChannel{arbiter: NewArbiter(), changed: make(chan struct{})}
		h.chans[id] = ch
	}
	return &memLink{ch: ch}, nil
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

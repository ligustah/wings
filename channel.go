package wings

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
)

// A channel shared between runs on different machines is a durable stream
// relayed through the coordinator (the flow package's ChannelHost is the seam).
// Each run has an outbox per channel — wings.chanout.<job>.<attempt>.<channel> —
// copied home by the same mirror as a recording, but written outside the
// attempt's transaction: a value or want is named by sender and sequence, so a
// duplicate from a replay is dropped, and what it needs is to be seen at once.
// The coordinator's relay merges every outbox into one canonical stream,
// wings.chan.<channel>, by the rule in [flow.Arbiter]. A run receives by reading
// the canonical stream — its own on the coordinator, a pushed copy on a worker.
// Creating an outbox is also the subscription; the canonical stream is never
// dropped, so a restarted coordinator replays receives from it.

const (
	chanoutPrefix = "wings.chanout."
	// chanPrefix is the canonical stream, pushed to workers; not an output
	// family, so a worker's copy is not mirrored back.
	chanPrefix = "wings.chan."

	relayInterval = 500 * time.Millisecond
)

func chanStreamFor(id string) string { return chanPrefix + streamPart(id) }

func outboxFor(run string, attempt int, id string) string {
	return outputName{Prefix: chanoutPrefix, Job: run, Attempt: attempt, Name: id}.String()
}

// --- relay, on the coordinator ---

// relayChannel is the relay's state for one channel: the canonical stream and
// the arbiter that decides what goes on it.
type relayChannel struct {
	stream *dsclient.Stream[flow.ChannelItem]

	mu      sync.Mutex
	arbiter *flow.Arbiter
}

// merge puts a record through the arbiter and appends what it says — the record
// if new, plus any grants — returning what it appended. Nothing for a duplicate.
func (rc *relayChannel) merge(ctx context.Context, it flow.ChannelItem) ([]flow.ChannelItem, error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	recs := rc.arbiter.Offer(it)
	if len(recs) == 0 {
		return nil, nil
	}
	if _, err := rc.stream.Append(ctx, recs); err != nil {
		return nil, err
	}
	return recs, nil
}

type channelRelay struct {
	poke chan struct{}

	mu       sync.Mutex
	channels map[string]*relayChannel // by canonical stream
	tailed   map[string]bool          // outboxes being read
}

func (c *Cluster) startChannelRelay() {
	c.relay = &channelRelay{
		poke:     make(chan struct{}, 1),
		channels: map[string]*relayChannel{},
		tailed:   map[string]bool{},
	}
	c.wg.Go(c.runChannelRelay)
}

func (c *Cluster) pokeRelay() {
	if c.relay == nil {
		return
	}
	select {
	case c.relay.poke <- struct{}{}:
	default:
	}
}

// runChannelRelay finds outboxes on the coordinator's storage and reads each
// into its channel's canonical stream.
func (c *Cluster) runChannelRelay() {
	client, err := c.sharedClient()
	if err != nil {
		return
	}
	t := time.NewTicker(relayInterval)
	defer t.Stop()
	for {
		names, err := client.ListStreams(c.ctx)
		if err == nil {
			for _, name := range names {
				o, ok := parseOutput(name)
				if !ok || o.Prefix != chanoutPrefix || c.wasDropped(name) {
					continue
				}
				c.tailOutbox(client, name, o.Name)
			}
		} else if c.ctx.Err() != nil {
			return
		}
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		case <-c.relay.poke:
		}
	}
}

// relayFor returns the relay's state for a channel, creating the canonical
// stream and replaying what is on it so a restarted coordinator does not append
// what its predecessor did, and grants what the predecessor admitted but never granted.
func (c *Cluster) relayFor(client *dsclient.Client, id string) (*relayChannel, error) {
	canonical := chanStreamFor(id)
	r := c.relay
	r.mu.Lock()
	if rc, ok := r.channels[canonical]; ok {
		r.mu.Unlock()
		return rc, nil
	}
	r.mu.Unlock()

	if err := ensureStream(c.ctx, client, canonical); err != nil {
		return nil, err
	}
	st, err := eventStream[flow.ChannelItem](client, canonical)
	if err != nil {
		return nil, err
	}
	rc := &relayChannel{stream: st, arbiter: flow.NewArbiter()}
	var from int64
	for {
		recs, err := st.Read(c.ctx, from, recordBatch)
		if err != nil {
			return nil, fmt.Errorf("wings: read %s: %w", canonical, err)
		}
		if len(recs) == 0 {
			break
		}
		for _, rec := range recs {
			from = rec.Offset + 1
			rc.arbiter.Restore(rec.Record)
		}
	}
	if owed := rc.arbiter.Grants(); len(owed) > 0 {
		if _, err := st.Append(c.ctx, owed); err != nil {
			return nil, fmt.Errorf("wings: grant what was owed on %s: %w", canonical, err)
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.channels[canonical]; ok {
		return existing, nil
	}
	r.channels[canonical] = rc
	return rc, nil
}

// tailOutbox starts reading one outbox into its channel, once.
func (c *Cluster) tailOutbox(client *dsclient.Client, name, id string) {
	r := c.relay
	r.mu.Lock()
	if r.tailed[name] || c.closed {
		r.mu.Unlock()
		return
	}
	r.tailed[name] = true
	r.mu.Unlock()

	c.wg.Go(func() {
		rc, err := c.relayFor(client, id)
		if err != nil {
			c.log.Warn("wings: cannot relay a shared channel", "channel", id, "err", err)
			return
		}
		var from int64
		for c.ctx.Err() == nil {
			// Re-opened each pass: a handle opened before the mirror's first
			// append binds to the empty stream and never sees later writes — a
			// moved attempt's outbox is exactly that window. A fresh handle sees
			// what is there now.
			st, err := eventStream[flow.ChannelItem](client, name)
			if err != nil {
				if c.ctx.Err() != nil || c.wasDropped(name) {
					return
				}
				if ok, _ := client.StreamExists(c.ctx, name); !ok {
					return
				}
				if pause(c.ctx, time.Second) != nil {
					return
				}
				continue
			}
			readCtx, cancel := context.WithTimeout(c.ctx, followPoll)
			recs, err := st.ReadBlocking(readCtx, from, recordBatch)
			expired := readCtx.Err() != nil
			cancel()
			if err != nil {
				if c.ctx.Err() != nil {
					return
				}
				if !expired {
					if c.wasDropped(name) {
						return
					}
					if ok, _ := client.StreamExists(c.ctx, name); !ok {
						return
					}
					if pause(c.ctx, time.Second) != nil {
						return
					}
				}
				continue
			}
			var merged []flow.ChannelItem
			for _, rec := range recs {
				from = rec.Offset + 1
				appended, err := rc.merge(c.ctx, rec.Record)
				if err != nil {
					if c.ctx.Err() == nil {
						c.log.Warn("wings: cannot relay a shared channel", "channel", id, "err", err)
					}
					return
				}
				merged = append(merged, appended...)
			}
			if len(merged) > 0 {
				c.wakeOnChannel(id, merged)
			}
		}
	})
}

// subscribeChannel starts pushing a channel's canonical stream onto a worker
// that uses it, once. Called by the output mirror when it finds the worker's outbox.
func (c *Cluster) subscribeChannel(workerID, id string) {
	shared, err := c.sharedClient()
	if err != nil {
		return
	}
	c.mu.Lock()
	var w *workerConn
	for _, cand := range c.workers {
		if cand.id == workerID {
			w = cand
		}
	}
	if w == nil || w.client == shared || w.pushes[id] || c.closed {
		c.mu.Unlock()
		return
	}
	if w.pushes == nil {
		w.pushes = map[string]bool{}
	}
	w.pushes[id] = true
	c.mu.Unlock()
	c.pokeRelay()

	canonical := chanStreamFor(id)
	w.wg.Go(func() {
		if err := ensureStream(w.ctx, shared, canonical); err != nil {
			return
		}
		err := w.client.RunMirror(w.ctx, "wings.push."+w.id+"."+streamPart(id), dsclient.MirrorSpec{
			From:   shared,
			Source: canonical,
			Dest:   canonical,
			Create: true,
			Batch:  recordBatch,
		})
		if err != nil && w.ctx.Err() == nil && !errors.Is(err, context.Canceled) {
			c.log.Warn("wings: stopped pushing a shared channel to a worker", "worker", w.id, "channel", id, "err", err)
		}
	})
}

// --- hosts ---

// clusterChannels is the [flow.ChannelHost] for runs on the coordinator.
type clusterChannels struct{ c *Cluster }

func (h clusterChannels) Link(ctx context.Context, run, id string) (flow.ChannelLink, error) {
	client, err := h.c.sharedClient()
	if err != nil {
		return nil, err
	}
	out := outboxFor(run, 0, id)
	if err := ensureStream(ctx, client, out); err != nil {
		return nil, err
	}
	outbox, err := eventStream[flow.ChannelItem](client, out)
	if err != nil {
		return nil, err
	}
	h.c.pokeRelay()
	return &channelLink{
		send: func(ctx context.Context, it flow.ChannelItem) error {
			_, err := outbox.Append(ctx, []flow.ChannelItem{it})
			return err
		},
		client: client,
		in:     chanStreamFor(id),
	}, nil
}

// nodeChannels is the [flow.ChannelHost] for a job's run on a worker.
type nodeChannels struct {
	n   *workerNode
	job *jobState
}

func (h nodeChannels) Link(ctx context.Context, _ string, id string) (flow.ChannelLink, error) {
	out := outboxFor(h.job.id, h.job.attempt, id)
	// Not the attempt's context: a half-made outbox is the next attempt's
	// problem. Created whether or not anything is sent, since it is the subscription.
	if err := ensureStream(context.WithoutCancel(ctx), h.n.client, out); err != nil {
		return nil, err
	}
	outbox, err := eventStream[flow.ChannelItem](h.n.client, out)
	if err != nil {
		return nil, err
	}
	return &channelLink{
		send: func(ctx context.Context, it flow.ChannelItem) error {
			_, err := outbox.Append(ctx, []flow.ChannelItem{it})
			return err
		},
		client: h.n.client,
		in:     chanStreamFor(id),
	}, nil
}

// channelLink is a run's connection to one shared channel: sends go to the run's
// outbox, items come from the canonical stream.
type channelLink struct {
	send   func(context.Context, flow.ChannelItem) error
	client *dsclient.Client
	in     string
}

func (l *channelLink) Send(ctx context.Context, it flow.ChannelItem) error { return l.send(ctx, it) }

func (l *channelLink) Items(ctx context.Context, yield func(flow.ChannelItem) bool) error {
	// The canonical stream appears when the relay has something for it, or the
	// push reaches this worker; until then, look again.
	var st *dsclient.Stream[flow.ChannelItem]
	var from int64
	for ctx.Err() == nil {
		if st == nil {
			ok, err := l.client.StreamExists(ctx, l.in)
			if err != nil || !ok {
				if err := pause(ctx, 200*time.Millisecond); err != nil {
					return err
				}
				continue
			}
			if st, err = eventStream[flow.ChannelItem](l.client, l.in); err != nil {
				return err
			}
		}
		readCtx, cancel := context.WithTimeout(ctx, followPoll)
		recs, err := st.ReadBlocking(readCtx, from, recordBatch)
		expired := readCtx.Err() != nil
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !expired {
				if err := pause(ctx, time.Second); err != nil {
					return err
				}
			}
			continue
		}
		for _, rec := range recs {
			from = rec.Offset + 1
			if !yield(rec.Record) {
				return nil
			}
		}
	}
	return ctx.Err()
}

func (l *channelLink) Close() error { return nil }

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

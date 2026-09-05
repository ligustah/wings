package wings

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
)

// A channel shared between runs on different machines is a durable stream
// relayed through the coordinator. See the flow package's ChannelHost for
// the seam; this is what carries the bytes.
//
// Every run that uses a shared channel has an OUTBOX for it: a stream of
// what that run sent, named for the run and the channel like any other
// output — wings.chanout.<job>.<attempt>.<channel> — so on a worker it is
// written inside the attempt's transaction and copied home by the same
// mirror as a recording. The coordinator's RELAY merges every outbox of a
// channel into one canonical stream, wings.chan.<channel>, in arrival order,
// dropping any item it has seen. A run receives by reading the canonical
// stream: the coordinator its own, a worker a copy the coordinator PUSHES
// onto it.
//
// Nothing asks for the push. A worker that uses a channel creates its outbox
// first, whether or not it ever sends, and the output mirror discovering that
// outbox is what subscribes the worker to the channel. Discovery is live on a
// healthy connection; the listing interval is the fallback.
//
// The canonical stream is never dropped: a coordinator that restarts replays
// its workflow, and the workflow's receives replay from it. A worker's outbox
// goes with the attempt that wrote it, like its other outputs; what an
// abandoned attempt committed to its outbox is merged as soon as the copy
// arrives, well before the job settles and the outbox is dropped.

const (
	// chanoutPrefix is a run's outbox for one shared channel. An output
	// family: parseOutput knows it, the mirror copies it, and it goes with
	// the attempt that wrote it.
	chanoutPrefix = "wings.chanout."
	// chanPrefix is the canonical stream of a shared channel: on the
	// coordinator, and pushed to every worker using the channel. Not an
	// output family — the copy on a worker must not be mirrored back.
	chanPrefix = "wings.chan."

	// relayInterval is how often the relay looks for outboxes it has not
	// seen, between pokes.
	relayInterval = 500 * time.Millisecond
)

// chanStreamFor is a channel's canonical stream, from the sanitised id an
// outbox name carries.
func chanStreamFor(id string) string { return chanPrefix + streamPart(id) }

// outboxFor is one run's outbox for one channel.
func outboxFor(run string, attempt int, id string) string {
	return outputName{Prefix: chanoutPrefix, Job: run, Attempt: attempt, Name: id}.String()
}

func itemKey(it flow.ChannelItem) string { return it.From + "#" + strconv.FormatUint(it.Seq, 10) }

// --- relay, on the coordinator ---

// relayChannel is the relay's state for one channel: the canonical stream
// and what is already on it.
type relayChannel struct {
	stream *dsclient.Stream[flow.ChannelItem]

	mu     sync.Mutex
	seen   map[string]bool
	closed bool
}

// merge appends an item to the canonical stream unless it is already there.
func (rc *relayChannel) merge(ctx context.Context, it flow.ChannelItem) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if it.Closed {
		if rc.closed {
			return nil
		}
	} else if rc.seen[itemKey(it)] {
		return nil
	}
	if _, err := rc.stream.Append(ctx, []flow.ChannelItem{it}); err != nil {
		return err
	}
	if it.Closed {
		rc.closed = true
	} else {
		rc.seen[itemKey(it)] = true
	}
	return nil
}

// channelRelay is the coordinator's side of every shared channel.
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

// pokeRelay asks the relay to look for new outboxes now.
func (c *Cluster) pokeRelay() {
	if c.relay == nil {
		return
	}
	select {
	case c.relay.poke <- struct{}{}:
	default:
	}
}

// runChannelRelay finds outboxes on the coordinator's storage — a worker's
// copied home, or written there by an in-process worker or the workflow —
// and reads each into its channel's canonical stream.
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
// stream and reading what is already on it — a coordinator that restarted
// must not append what its predecessor did.
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
	rc := &relayChannel{stream: st, seen: map[string]bool{}}
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
			if rec.Record.Closed {
				rc.closed = true
			} else {
				rc.seen[itemKey(rec.Record)] = true
			}
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
		st, err := eventStream[flow.ChannelItem](client, name)
		if err != nil {
			return
		}
		var from int64
		for c.ctx.Err() == nil {
			readCtx, cancel := context.WithTimeout(c.ctx, followPoll)
			recs, err := st.ReadBlocking(readCtx, from, recordBatch)
			expired := readCtx.Err() != nil
			cancel()
			if err != nil {
				if c.ctx.Err() != nil {
					return
				}
				if !expired {
					// Gone — dropped with the attempt that wrote it — or
					// unreadable. Either way, done with it.
					if c.wasDropped(name) {
						return
					}
					if ok, _ := client.StreamExists(c.ctx, name); !ok {
						return
					}
					select {
					case <-c.ctx.Done():
						return
					case <-time.After(time.Second):
					}
				}
				continue
			}
			for _, rec := range recs {
				from = rec.Offset + 1
				if err := rc.merge(c.ctx, rec.Record); err != nil {
					if c.ctx.Err() == nil {
						c.log.Warn("wings: cannot relay a shared channel", "channel", id, "err", err)
					}
					return
				}
			}
		}
	})
}

// subscribeChannel starts pushing a channel's canonical stream onto a worker
// that uses it, once. Called by the output mirror when it finds the worker's
// outbox for the channel.
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

// clusterChannels is the [flow.ChannelHost] of runs on the coordinator: the
// workflow, and anything run with [Cluster.Run].
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

// nodeChannels is the [flow.ChannelHost] of a job's run on a worker.
type nodeChannels struct {
	n   *workerNode
	job *jobState
}

func (h nodeChannels) Link(ctx context.Context, _ string, id string) (flow.ChannelLink, error) {
	out := outboxFor(h.job.id, h.job.attempt, id)
	// Not the attempt's context: an outbox half-made when a deadline expires
	// is the next attempt's problem. Created whether or not anything is ever
	// sent, since it is also the subscription.
	if err := ensureStream(context.WithoutCancel(ctx), h.n.client, out); err != nil {
		return nil, err
	}
	outbox, err := eventStream[flow.ChannelItem](h.n.client, out)
	if err != nil {
		return nil, err
	}
	a := h.job.outputs
	return &channelLink{
		send: func(ctx context.Context, it flow.ChannelItem) error {
			return a.append(ctx, outbox, []flow.ChannelItem{it})
		},
		client: h.n.client,
		in:     chanStreamFor(id),
	}, nil
}

// channelLink is a run's connection to one shared channel: sends go to the
// run's outbox, items come from the canonical stream where this run can
// read it.
type channelLink struct {
	send   func(context.Context, flow.ChannelItem) error
	client *dsclient.Client
	in     string
}

func (l *channelLink) Send(ctx context.Context, it flow.ChannelItem) error { return l.send(ctx, it) }

func (l *channelLink) Items(ctx context.Context, yield func(flow.ChannelItem) bool) error {
	// The canonical stream appears when the relay has something for it, or
	// when the push reaches this worker. Until then there is nothing to
	// read, and nothing to do but look again.
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

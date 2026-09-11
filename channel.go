package wings

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
)

// A channel shared between runs on different machines is relayed through the
// coordinator (the flow package's ChannelHost is the seam). Each channel has two
// durable streams named by its id alone — a value stream wings.chanval.<channel>
// carrying the writer's values and closes, and a consume stream
// wings.chancons.<channel> carrying the reader's consume reports. A send writes to
// one of these as part of the writing thread's transaction, so a send's record and
// the event that justifies it commit atomically and come home together under the
// transaction pull (pull.go); a worker thread holds a value until its event so no
// concurrent commit can tear the two ([attemptOutputs.stage]), and a value reaches
// the stream exactly once, so a replay never resends it. A run receives by reading
// the value stream directly — its own copy on the coordinator, a copy pushed to it
// on a worker (subscribeChannel); the coordinator folds the value and consume
// streams into the counts a blocked sender or receiver waits on (foldChannel).
// The streams are named without a job or attempt, so a writer moved to a new
// attempt keeps appending to the same one (fencing stays on the producer id). They
// are dropped when the channel is retired — its creating thread, job, or run has
// finished, so no run will read it again and every send is home (retireCanonicals).

const (
	// chanPrefix is the prefix of a channel's relay key (chanStreamFor) — the name
	// the relay groups its bookkeeping under. No stream of that name exists; the
	// value and consume streams hold the data.
	chanPrefix = "wings.chan."
	// chanvalPrefix and chanconsPrefix name a channel's value stream and consume
	// stream: the writer's values and closes on the one, the reader's consume
	// reports and its Link marker on the other. Both are named by the channel id
	// alone — no job, no attempt — so a writer moved to a new attempt keeps
	// appending to the same stream (fencing stays on the attempt's producer id, not
	// the stream name). Pulled home like any output, so parseOutput knows them.
	chanvalPrefix  = "wings.chanval."
	chanconsPrefix = "wings.chancons."

	relayInterval = 500 * time.Millisecond
)

func chanStreamFor(id string) string { return chanPrefix + streamPart(id) }

// chanValues names a channel's value stream (the writer's values and closes);
// chanConsumes its consume stream (the reader's consume reports). streamPart is
// idempotent, so chanvalPrefix+o.Name equals chanValues(id) for a parsed stream.
func chanValues(id string) string   { return chanvalPrefix + streamPart(id) }
func chanConsumes(id string) string { return chanconsPrefix + streamPart(id) }

// linkStreams names the streams a link's pump follows for its role: a reader
// reads the value stream (values and closes), a writer the consume stream (the
// consume reports that free its bounded buffer), a channel kept whole both. A
// writer that read the value stream would only re-see its own values and never
// learn what the reader took, so its buffer would never free.
func linkStreams(id string, mode flow.LinkMode) []string {
	switch mode {
	case flow.LinkRead:
		return []string{chanValues(id)}
	case flow.LinkWrite:
		return []string{chanConsumes(id)}
	default:
		return []string{chanValues(id), chanConsumes(id)}
	}
}

// lazyStream opens a channel stream on first use, so a Link that may never send
// pays nothing and adds no latency to the path that shares it (which a fork
// waits on) — the stream is made when the first record is sent, off that path.
type lazyStream struct {
	once sync.Once
	open func(context.Context) (*dsclient.Stream[flow.ChannelItem], error)
	st   *dsclient.Stream[flow.ChannelItem]
	err  error
}

func (l *lazyStream) get(ctx context.Context) (*dsclient.Stream[flow.ChannelItem], error) {
	l.once.Do(func() { l.st, l.err = l.open(ctx) })
	return l.st, l.err
}

// valConsLazy returns lazy value and consume streams for a channel on client,
// each created off the critical path when first sent to.
func valConsLazy(client *dsclient.Client, id string) (val, cons *lazyStream) {
	return &lazyStream{open: func(ctx context.Context) (*dsclient.Stream[flow.ChannelItem], error) {
			return openChannelStream(context.WithoutCancel(ctx), client, chanValues(id))
		}}, &lazyStream{open: func(ctx context.Context) (*dsclient.Stream[flow.ChannelItem], error) {
			return openChannelStream(context.WithoutCancel(ctx), client, chanConsumes(id))
		}}
}

// openChannelStream ensures a channel stream exists on client and opens it.
func openChannelStream(ctx context.Context, client *dsclient.Client, name string) (*dsclient.Stream[flow.ChannelItem], error) {
	if err := ensureStream(ctx, client, name); err != nil {
		return nil, err
	}
	return eventStream[flow.ChannelItem](client, name)
}

// pendingSend is a shared-channel value a thread has announced but whose event is
// not yet recorded. A worker thread's producer holds it until that thread's next
// event stages it (see [attemptOutputs.stage], [attemptOutputs.appendEvents]), so
// no commit in between — a heartbeat, an unload — makes a value durable without
// its event, a tear a replay would resend.
type pendingSend struct {
	stream *dsclient.Stream[flow.ChannelItem]
	item   flow.ChannelItem
}

// pushGroup names the mirror that pushes a channel's value stream to one worker.
// A moved receiver's pre-push and its live push share it, so the live push
// resumes where the pre-push stopped rather than copying the stream twice.
func pushGroup(workerID, id string) string { return "wings.push." + workerID + "." + streamPart(id) }

// --- relay, on the coordinator ---

// relayChannel is the relay's counts for one channel — how many values have
// arrived, how many the reader has reported consuming, and whether it is closed —
// folded straight from the channel's value and consume streams (foldChannel), the
// one durable copy of each. A holder blocked on the channel waits on these (see
// [settledOn]); there is nothing else to keep, since the value and consume streams
// are the record and the reader reads the value stream directly.
type relayChannel struct {
	mu       sync.Mutex
	nvalues  uint64
	nconsume uint64
	closed   bool
}

// counts snapshots how many values have arrived, how many the reader has reported
// consuming, and whether the channel is closed — what [settledOn] waits on.
func (rc *relayChannel) counts() (closed bool, values, consumed uint64) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	return rc.closed, rc.nvalues, rc.nconsume
}

// countLocked folds one record into the channel's counts. A Link marker is a
// subscription sign with no value and is not counted. Call with mu held.
func (rc *relayChannel) countLocked(it flow.ChannelItem) {
	switch {
	case it.Link:
	case it.Closed:
		rc.closed = true
	case it.Consumed:
		rc.nconsume++
	default:
		rc.nvalues++
	}
}

type channelRelay struct {
	poke chan struct{}

	mu sync.Mutex
	// channels holds each channel's counts, keyed by chanStreamFor(id) — a stable
	// per-channel key the relay groups its bookkeeping under (no stream of that name
	// exists; the value and consume streams hold the data). A channel appears here
	// as its value or consume stream comes home and is folded, so the retire paths
	// select the channels to reclaim by scanning these keys (createdByThread).
	channels map[string]*relayChannel
	// folded names the value and consume streams whose counts the relay is folding,
	// so each is folded once. Keyed by stream name.
	folded map[string]bool
	// retiredJob records the origin — run and creating thread — of a forked
	// activity whose channels have been retired, so a value or consume stream the
	// relay discovers only after the job settled — too late to be among the folded
	// channels when the retire ran — is still recognized as a retired channel's and
	// dropped rather than folded and left (channelRetired). Keyed by job.
	retiredJob map[string]flow.Origin
	// retiredRun names runs that have finished, so a channel stream under a finished
	// run's prefix that the relay folds only after the run's cleanup is still
	// recognized as dead and dropped (channelRetired). Keyed by run.
	retiredRun map[string]bool
	// consumed is each channel's consume count, kept outside mu so a job unloading
	// on a send can snapshot it while holding the cluster lock without the relay's.
	// Keyed by the channel key (chanStreamFor), value uint64.
	consumed sync.Map
}

func (c *Cluster) startChannelRelay() {
	c.relay = &channelRelay{
		poke:       make(chan struct{}, 1),
		channels:   map[string]*relayChannel{},
		folded:     map[string]bool{},
		retiredJob: map[string]flow.Origin{},
		retiredRun: map[string]bool{},
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

// runChannelRelay finds each channel's value and consume streams on the
// coordinator's storage and folds them into the counts a blocked sender or
// receiver waits on. It also reclaims: a stream whose channel has already been
// retired (its creator gone) is dropped rather than folded — a retire before the
// stream came home, or a pull that re-created a dropped stream once.
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
				if !ok || (o.Prefix != chanvalPrefix && o.Prefix != chanconsPrefix) {
					continue
				}
				if c.wasDropped(name) {
					// Retired for good, but a pull whose destination was chosen before the
					// drop re-created it; re-drop the reappearance (it stays marked dropped,
					// so the pull copies no more to it).
					if err := dropStream(context.WithoutCancel(c.ctx), client, name); err != nil {
						c.log.Warn("wings: could not re-drop a reclaimed channel stream", "stream", name, "err", err)
					}
					continue
				}
				if c.channelRetired(o.Name) {
					// Came home only after its creating thread's retire, too late to be
					// among the folded channels then; retire it now rather than fold and
					// leave it (dropChannelData marks it dropped, so the pull stops).
					c.dropChannelData(chanPrefix + o.Name)
					continue
				}
				// o.Name is the id, streamPart-mangled, which the count key (chanStreamFor)
				// and the stream names (chanValues/chanConsumes) reproduce idempotently.
				c.foldChannel(client, name, o.Name, o.Prefix == chanvalPrefix)
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

// channelRetired reports whether a channel, named by its streamPart-mangled id,
// belongs to a thread or run whose channels have been retired — so a value or
// consume stream discovered after that retire (it came home late) is reclaimed at
// once rather than folded and left. Checked against retired jobs (by creating
// thread) and retired runs (by prefix).
func (c *Cluster) channelRetired(idMangled string) bool {
	r := c.relay
	r.mu.Lock()
	defer r.mu.Unlock()
	key := chanPrefix + idMangled
	for _, origin := range r.retiredJob {
		if createdByThread(key, origin.Run, origin.Thread) {
			return true
		}
	}
	for run := range r.retiredRun {
		if strings.HasPrefix(key, chanPrefix+streamPart(run)+"_") {
			return true
		}
	}
	return false
}

// relayFor returns the relay's counts for a channel, making an empty set on first
// use. Counts are folded from the value and consume streams (foldChannel), so a
// restarted coordinator re-folds to the same totals — each record is on those
// streams exactly once — and nothing here is durable.
func (c *Cluster) relayFor(id string) *relayChannel {
	canonical := chanStreamFor(id)
	r := c.relay
	r.mu.Lock()
	defer r.mu.Unlock()
	if rc, ok := r.channels[canonical]; ok {
		return rc
	}
	rc := &relayChannel{}
	r.channels[canonical] = rc
	return rc
}

// foldChannel folds a channel's value or consume stream into its counts, waking
// waiters as records arrive, once. The stream is the one durable copy — the
// writer's values and closes, or the reader's consumes — so folding it from the
// start gives exact counts and a restart re-folds to the same totals, since each
// record is on it exactly once. Link markers are a subscription sign and skipped.
func (c *Cluster) foldChannel(client *dsclient.Client, name, id string, values bool) {
	r := c.relay
	r.mu.Lock()
	if r.folded[name] || c.closed {
		r.mu.Unlock()
		return
	}
	r.folded[name] = true
	r.mu.Unlock()

	c.wg.Go(func() {
		rc := c.relayFor(id)
		var from int64
		for c.ctx.Err() == nil {
			// Re-opened each pass: a handle opened before the first append binds to the
			// empty stream and never sees later writes — a moved writer's first append
			// to the stable stream is that window.
			st, err := eventStream[flow.ChannelItem](client, name)
			if err != nil {
				if c.ctx.Err() != nil || pause(c.ctx, time.Second) != nil {
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
				if !expired && pause(c.ctx, time.Second) != nil {
					return
				}
				continue
			}
			rc.mu.Lock()
			for _, rec := range recs {
				from = rec.Offset + 1
				if !rec.Record.Link {
					rc.countLocked(rec.Record)
				}
			}
			consumed := rc.nconsume
			rc.mu.Unlock()
			if !values {
				c.relay.consumed.Store(chanStreamFor(id), consumed)
			}
			c.wakeOnChannel(id)
		}
	})
}

// retireRunChannels retires every channel a finished run created. A completed run
// will not resume, so its channels' data — kept otherwise so a resume can replay
// receives — is dead. The run's channels are those whose key falls under its
// prefix (a channel id is "<run>/<thread>.ch<n>", so streamPart maps it to
// "<run>_<thread>_ch<n>"); the "_" after the run guards against a run whose name
// is another's prefix. Selects for [retireCanonicals].
func (c *Cluster) retireRunChannels(run string) {
	r := c.relay
	if r == nil {
		return
	}
	prefix := chanPrefix + streamPart(run) + "_"
	r.mu.Lock()
	r.retiredRun[run] = true
	var cs []string
	for canonical := range r.channels {
		if strings.HasPrefix(canonical, prefix) {
			cs = append(cs, canonical)
		}
	}
	r.mu.Unlock()
	c.retireCanonicals(cs)
}

// createdByThread reports whether key is the relay key of a channel that thread
// created. A channel's id is "<run>/<thread>.ch<n>", so its key is the thread's
// channel prefix followed by the digits of n — and a sub-thread's channel
// ("<thread>.<k>.ch<n>") has a digit, not "ch", after the prefix, so it does not
// match its parent. This is what tells a channel a job created from one it merely
// sends or receives on: only the creator retires it.
func createdByThread(key, run, thread string) bool {
	prefix := chanPrefix + streamPart(run) + "_" + streamPart(thread) + "_ch"
	rest, ok := strings.CutPrefix(key, prefix)
	if !ok || rest == "" {
		return false
	}
	for _, r := range rest {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// retireJobChannels retires the channels a forked activity created. The remote
// activity has returned (its job settled), so its result is recorded and its
// channels' data is dead — a replay re-inserts the result rather than re-entering
// the activity. The channels are those created by the activity's thread, found by
// name among the folded channels (createdByThread). origin is the job's run and
// creating thread; it is remembered so a value or consume stream the relay
// discovers only after this settle — too late to be folded when the retire ran —
// is still recognized as the job's and dropped (channelRetired). Selects for
// [retireCanonicals].
func (c *Cluster) retireJobChannels(job string, origin flow.Origin) {
	r := c.relay
	if r == nil || origin.Run == "" || origin.Thread == "" {
		return
	}
	r.mu.Lock()
	r.retiredJob[job] = origin
	var cs []string
	for canonical := range r.channels {
		if createdByThread(canonical, origin.Run, origin.Thread) {
			cs = append(cs, canonical)
		}
	}
	r.mu.Unlock()
	c.retireCanonicals(cs)
}

// retireChannels retires channels named by their ids, for an in-process activity
// call that created them and has now returned (see [flow.ChannelRetirer]) — the
// same reclaim as a forked activity's, keyed by explicit id because a direct call
// shares its caller's thread rather than getting one of its own. Selects for
// [retireCanonicals].
func (c *Cluster) retireChannels(ids []string) {
	cs := make([]string, 0, len(ids))
	for _, id := range ids {
		cs = append(cs, chanStreamFor(id))
	}
	c.retireCanonicals(cs)
}

// retireCanonicals is the one way the relay retires a channel: drop its value and
// consume streams and forget its counts. A channel is retired only once its
// creator has finished, and by then every send is home — a worker's ride the
// creating thread's transaction and are pulled before the job settles, and the
// creator awaits the threads it shared the channel to — so there is nothing left
// to wait for. Every reclaim — a returned in-process call ([Cluster.retireChannels]),
// a settled forked activity ([Cluster.retireJobChannels]), a finished run
// ([Cluster.retireRunChannels]) — selects its channels and calls this.
func (c *Cluster) retireCanonicals(canonicals []string) {
	for _, canonical := range canonicals {
		c.dropChannelData(canonical)
	}
}

// dropChannelData deletes a retired channel's data — its value and consume
// streams — and forgets the relay's counts for it. Called when the channel's
// creator has finished, so no run will read it again and every send is home. key
// is the channel's relay key (chanStreamFor(id)); no stream of that name exists,
// it only names the relay's bookkeeping.
//
// The value and consume streams are pulled home, so a pull whose destination was
// chosen before the drop can re-create one once; they are marked dropped for good
// so the pull copies no more to them (pulledStream declines a dropped name) and
// the relay's reaper re-drops a reappearance.
func (c *Cluster) dropChannelData(key string) {
	client, err := c.sharedClient()
	if err != nil {
		return
	}
	suffix := strings.TrimPrefix(key, chanPrefix)
	values, consumes := chanvalPrefix+suffix, chanconsPrefix+suffix
	c.markDropped([]string{values, consumes})
	for _, name := range []string{values, consumes} {
		if err := dropStream(context.WithoutCancel(c.ctx), client, name); err != nil {
			c.log.Warn("wings: could not drop a finished run's channel stream", "stream", name, "err", err)
		}
	}
	r := c.relay
	r.mu.Lock()
	delete(r.channels, key)
	r.mu.Unlock()
}

// subscribeChannel starts pushing a channel's value stream onto a worker that
// reads it, once. Called by the output mirror when it finds the worker's consume
// stream, which only a reader creates, so the push never targets the writer.
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

	values := chanValues(id)
	w.wg.Go(func() {
		if err := ensureStream(w.ctx, shared, values); err != nil {
			return
		}
		err := w.client.RunMirror(w.ctx, pushGroup(w.id, id), dsclient.MirrorSpec{
			From:   shared,
			Source: values,
			Dest:   values,
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

func (h clusterChannels) Link(ctx context.Context, run, id string, mode flow.LinkMode) (flow.ChannelLink, error) {
	client, err := h.c.sharedClient()
	if err != nil {
		return nil, err
	}
	h.c.pokeRelay()

	// A send's records go to the channel's stable value or consume stream: the reader
	// reads values off the value stream, backpressure folds consumes off the other.
	valLazy, consLazy := valConsLazy(client, id)
	send := func(ctx context.Context, _ string, it flow.ChannelItem) error {
		// A signal owns no thread and so no transaction; it only ever sends a value,
		// written straight through.
		st, err := valLazy.get(ctx)
		if err != nil {
			return err
		}
		_, err = st.Append(ctx, []flow.ChannelItem{it})
		return err
	}
	if run != flow.SignalSender {
		// A record goes into the writing thread's own transaction (sender names it),
		// so it commits with the event that justifies it and no sibling thread's
		// commit can tear the two apart (see [coordOutputs.stage], flow.Committer).
		send = func(ctx context.Context, sender string, it flow.ChannelItem) error {
			outputs, err := h.c.coordOutputsFor(sender)
			if err != nil {
				return err
			}
			lazy := valLazy
			if it.Consumed {
				lazy = consLazy
			}
			st, err := lazy.get(ctx)
			if err != nil {
				return err
			}
			return outputs.stage(ctx, st, it)
		}
	}
	return &channelLink{
		send:   send,
		client: client,
		in:     linkStreams(id, mode),
	}, nil
}

// nodeChannels is the [flow.ChannelHost] for a job's run on a worker.
type nodeChannels struct {
	n   *workerNode
	job *jobState
}

// threadOrMain is the id of the thread running on ctx, falling back to the main
// thread when ctx is not inside a run body — so a channel write always routes to
// a real producer.
func threadOrMain(ctx context.Context) string {
	if _, thread, ok := flow.Self(ctx); ok {
		return thread
	}
	return flow.MainThread
}

func (h nodeChannels) Link(ctx context.Context, _ string, id string, mode flow.LinkMode) (flow.ChannelLink, error) {
	// The value and consume streams open on first send, off the share path a fork
	// waits on, so linking stays cheap; a worker's channel is learned by the relay
	// from its value or consume stream coming home, not from any per-run stream.
	valLazy, consLazy := valConsLazy(h.n.client, id)
	if mode == flow.LinkRead {
		// A reader's values are the writer's, pushed here from the coordinator's copy
		// of the value stream. The coordinator starts that push when a channel's
		// consume stream appears on a worker, so creating it now — a reader does, a
		// writer never does — announces this worker as the reader to push to, and only
		// the reader, so the writer's own worker is never pushed its own values back.
		// Written through the linking thread's producer so it comes home by the pull.
		mctx := context.WithoutCancel(ctx)
		linker := h.job.txns.For(threadOrMain(ctx))
		cons, err := consLazy.get(mctx)
		if err != nil {
			return nil, err
		}
		if err := linker.append(mctx, cons, []flow.ChannelItem{{Link: true}}); err != nil {
			return nil, err
		}
		if err := linker.commit(mctx); err != nil {
			return nil, err
		}
	}
	return &channelLink{
		// Each record goes into the writing thread's own transaction, so it commits
		// with the event that justifies it and no sibling thread's commit can tear the
		// two apart (pull.go, [attemptOutputs.stage]). A value waits for its send event;
		// a close or consume report rides the event it pairs with. The stream is
		// transactional, so it is pulled, not mirrored.
		send: func(ctx context.Context, _ string, it flow.ChannelItem) error {
			out := h.job.txns.For(threadOrMain(ctx))
			stream := valLazy
			if it.Consumed {
				stream = consLazy
			}
			es, err := stream.get(ctx)
			if err != nil {
				return err
			}
			if it.Closed || it.Consumed {
				return out.append(ctx, es, []flow.ChannelItem{it})
			}
			out.stage(es, it)
			return nil
		},
		client: h.n.client,
		in:     linkStreams(id, mode),
	}, nil
}

// RetireChannels implements [flow.ChannelRetirer]: the run body's in-process
// call that created these channels has returned, so retire them. Done off the
// caller so the call is not held for storage work; the run's end retires whatever
// is left.
func (h clusterChannels) RetireChannels(_ context.Context, _ string, ids []string) {
	h.c.wg.Go(func() { h.c.retireChannels(ids) })
}

// ChannelValues implements [flow.ChannelValueReader] for a run on the
// coordinator, reading values off the channel's value stream on shared storage.
func (h clusterChannels) ChannelValues(ctx context.Context, id string, cursor int64, n int) ([]flow.ChannelValueAt, error) {
	client, err := h.c.sharedClient()
	if err != nil {
		return nil, err
	}
	return channelValues(ctx, client, id, cursor, n)
}

// ChannelValues implements [flow.ChannelValueReader] for a job's run on a
// worker, reading values off the value stream pushed to the worker.
func (h nodeChannels) ChannelValues(ctx context.Context, id string, cursor int64, n int) ([]flow.ChannelValueAt, error) {
	return channelValues(ctx, h.n.client, id, cursor, n)
}

// channelValues reads the values on a channel's value stream at or after cursor,
// which is a stream offset. A replay reads back a received value this way rather
// than keeping its own copy, so the value stream is the one copy. It returns as
// soon as a read yields values, so a replay is not delayed waiting to fill n; it
// blocks only when nothing is there yet — the value stream is pushed to a moved
// receiver's worker and may lag its replay, and the caller knows the value it
// wants was received, so it is still coming — returning empty only if ctx ends. n
// bounds how many values one read gathers ahead into the cache.
func channelValues(ctx context.Context, client *dsclient.Client, id string, cursor int64, n int) ([]flow.ChannelValueAt, error) {
	values := chanValues(id)
	var st *dsclient.Stream[flow.ChannelItem]
	from := cursor
	for {
		if st == nil {
			ok, err := client.StreamExists(ctx, values)
			if err != nil {
				return nil, fmt.Errorf("wings: look for channel stream %s: %w", values, err)
			}
			if !ok {
				if err := pause(ctx, 200*time.Millisecond); err != nil {
					return nil, err
				}
				continue
			}
			if st, err = eventStream[flow.ChannelItem](client, values); err != nil {
				return nil, err
			}
		}
		readCtx, cancel := context.WithTimeout(ctx, followPoll)
		recs, err := st.ReadBlocking(readCtx, from, recordBatch)
		expired := readCtx.Err() != nil
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if expired {
				continue // caught up; the value is still on its way, so wait
			}
			return nil, fmt.Errorf("wings: read channel stream %s at %d: %w", values, from, err)
		}
		var out []flow.ChannelValueAt
		for _, rec := range recs {
			from = rec.Offset + 1
			if it := rec.Record; !it.Consumed && !it.Closed {
				out = append(out, flow.ChannelValueAt{From: it.From, Seq: it.Seq, Data: it.Data, Next: from})
				if len(out) >= n {
					break
				}
			}
		}
		// A consume report or a close carries no value; keep reading rather than hand
		// the caller an empty result it would read as the value being gone.
		if len(out) > 0 {
			return out, nil
		}
	}
}

// channelLink is a run's connection to one shared channel: sends go to the
// channel's value or consume stream, and items come from the streams in names —
// a reader follows the value stream, a writer the consume stream (for the
// consume reports that free its buffer), a channel kept whole both.
type channelLink struct {
	send   func(ctx context.Context, sender string, it flow.ChannelItem) error
	client *dsclient.Client
	in     []string
}

func (l *channelLink) Send(ctx context.Context, sender string, it flow.ChannelItem) error {
	return l.send(ctx, sender, it)
}

func (l *channelLink) Items(ctx context.Context, yield func(flow.ChannelItem) bool) error {
	if len(l.in) == 1 {
		return l.follow(ctx, l.in[0], yield)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	// The streams are independent, so their follows run concurrently; serialise the
	// yields into the one pump, and end every follow once one asks to stop.
	shared := func(it flow.ChannelItem) bool {
		mu.Lock()
		defer mu.Unlock()
		if ctx.Err() != nil {
			return false
		}
		if !yield(it) {
			cancel()
			return false
		}
		return true
	}
	var wg sync.WaitGroup
	for _, name := range l.in {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = l.follow(ctx, name, shared)
		}()
	}
	wg.Wait()
	return ctx.Err()
}

// follow delivers one stream's records to yield in order, from the start. The
// stream appears once the writer first sends, or the push reaches this worker;
// until then, look again.
func (l *channelLink) follow(ctx context.Context, name string, yield func(flow.ChannelItem) bool) error {
	var st *dsclient.Stream[flow.ChannelItem]
	var from int64
	for ctx.Err() == nil {
		if st == nil {
			ok, err := l.client.StreamExists(ctx, name)
			if err != nil || !ok {
				if err := pause(ctx, 200*time.Millisecond); err != nil {
					return err
				}
				continue
			}
			if st, err = eventStream[flow.ChannelItem](l.client, name); err != nil {
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

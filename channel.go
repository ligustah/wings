package wings

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// A channel shared between runs on different machines is relayed through the
// coordinator (the flow package's ChannelHost is the seam). Each channel has two
// durable streams named by its id alone — a value stream wings.chanval.<channel>
// carrying the writer's values and closes, and a consume stream
// wings.chancons.<channel> carrying the reader's consume reports. A send writes to
// one of these as part of the writing thread's transaction, so a send's record and
// the event that justifies it commit atomically and come home together under the
// transaction pull (pull.go). The append never commits before the event that
// justifies it, so no commit takes a value home without its event and a value
// reaches the stream exactly once, so a replay never resends it. A run receives by reading
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
	// the stream name). Pulled home like any output, so parseOutput knows them. They
	// defer to flow's names so a channel's streams have one name everywhere.
	chanvalPrefix  = flow.ChannelValuePrefix
	chanconsPrefix = flow.ChannelConsumePrefix

	relayInterval = 500 * time.Millisecond
)

func chanStreamFor(id string) string { return chanPrefix + streamPart(id) }

// chanValues names a channel's value stream (the writer's values and closes).
// streamPart is idempotent, so chanvalPrefix+o.Name equals chanValues(id) for a
// parsed stream.
func chanValues(id string) string { return chanvalPrefix + streamPart(id) }

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
func (rc *relayChannel) countLocked(it *protos.ChannelItem) {
	switch {
	case it == nil, it.GetLink():
	case it.GetClosed():
		rc.closed = true
	case it.GetConsumed():
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
	// folding cancels each stream's fold goroutine, so a retired channel's fold is
	// stopped rather than left spinning on the dropped stream. Keyed by stream name.
	folding map[string]context.CancelFunc
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
		folding:    map[string]context.CancelFunc{},
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
			var present []string
			for _, name := range names {
				o, ok := parseOutput(name)
				if !ok || (o.Prefix != chanvalPrefix && o.Prefix != chanconsPrefix) {
					continue
				}
				// A channel stream still on storage: its run's and job's tombstones must
				// stay, since a late one under them may yet be discovered here.
				present = append(present, chanPrefix+o.Name)
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
				// and the value stream name (chanValues) reproduce idempotently.
				c.foldChannel(client, name, o.Name, o.Prefix == chanvalPrefix)
			}
			// A complete listing: evict the retire tombstones of runs and jobs whose
			// streams are all gone, so retiredRun/retiredJob do not grow for the
			// coordinator's life.
			c.evictQuiescentTombstones(present)
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

// evictQuiescentTombstones drops the retire tombstones of runs and jobs none of
// whose channel streams are still on storage. A tombstone (retiredRun,
// retiredJob) exists only to recognize a stream that comes home after its creator
// retired (channelRetired). Once the relay has discovered that stream and dropped
// it, it is gone from a listing, and no later one can arrive — the creator's pulls
// all finished before it retired, so nothing re-creates its streams — leaving the
// tombstone dead weight. present is every channel stream this sweep listed;
// retaining a tombstone whose streams still appear keeps recognizing them, and
// dropping the rest bounds the maps over a long-lived coordinator.
func (c *Cluster) evictQuiescentTombstones(present []string) {
	r := c.relay
	r.mu.Lock()
	defer r.mu.Unlock()
	for run := range r.retiredRun {
		prefix := chanPrefix + streamPart(run) + "_"
		if !anyHasPrefix(present, prefix) {
			delete(r.retiredRun, run)
		}
	}
	for job, origin := range r.retiredJob {
		if !anyCreatedByThread(present, origin) {
			delete(r.retiredJob, job)
		}
	}
}

func anyHasPrefix(keys []string, prefix string) bool {
	for _, k := range keys {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

func anyCreatedByThread(keys []string, origin flow.Origin) bool {
	for _, k := range keys {
		if createdByThread(k, origin.Run, origin.Thread) {
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
	// A per-fold context so dropChannelData can stop this goroutine when the channel
	// is retired; without it the fold spun on the deleted stream for the cluster's
	// life, one leaked goroutine per channel ever folded.
	fctx, cancel := context.WithCancel(c.ctx)
	if r.folding == nil {
		r.folding = map[string]context.CancelFunc{}
	}
	r.folding[name] = cancel
	r.mu.Unlock()

	c.wg.Go(func() {
		// The consume fold owns this channel's consumed count; drop it as the fold ends
		// so a retired channel leaves none behind (the value fold's delete is a no-op).
		defer c.relay.consumed.Delete(chanStreamFor(id))
		rc := c.relayFor(id)
		var from int64
		for fctx.Err() == nil {
			// Re-opened each pass: a handle opened before the first append binds to the
			// empty stream and never sees later writes — a moved writer's first append
			// to the stable stream is that window.
			st, err := eventStream[*protos.Event](client, name)
			if err != nil {
				if fctx.Err() != nil || pause(fctx, time.Second) != nil {
					return
				}
				continue
			}
			readCtx, cancel := context.WithTimeout(fctx, followPoll)
			recs, err := st.ReadBlocking(readCtx, from, recordBatch)
			expired := readCtx.Err() != nil
			cancel()
			if err != nil {
				if fctx.Err() != nil {
					return
				}
				if !expired && pause(fctx, time.Second) != nil {
					return
				}
				continue
			}
			rc.mu.Lock()
			for _, rec := range recs {
				from = rec.Offset + 1
				rc.countLocked(rec.Record.GetChannelItem())
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
	// Stop pushing this channel to any worker: the stream is gone, so the mirror
	// would only spin against a deleted stream for the worker's life.
	c.mu.Lock()
	for _, w := range c.workers {
		if cancel := w.pushes[suffix]; cancel != nil {
			cancel()
			delete(w.pushes, suffix)
		}
	}
	c.mu.Unlock()
	r := c.relay
	r.mu.Lock()
	for _, name := range []string{values, consumes} {
		if cancel := r.folding[name]; cancel != nil {
			cancel()
			delete(r.folding, name)
		}
		delete(r.folded, name)
	}
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
	part := streamPart(id)
	c.mu.Lock()
	var w *workerConn
	for _, cand := range c.workers {
		if cand.id == workerID {
			w = cand
		}
	}
	if w == nil || w.client == shared || w.pushes[part] != nil || c.closed {
		c.mu.Unlock()
		return
	}
	if w.pushes == nil {
		w.pushes = map[string]context.CancelFunc{}
	}
	// A per-push context under the worker's, so dropChannelData can stop this one
	// channel's push when it retires without ending the worker's others.
	pctx, pcancel := context.WithCancel(w.ctx)
	w.pushes[part] = pcancel
	c.mu.Unlock()
	c.pokeRelay()

	values := chanValues(id)
	w.wg.Go(func() {
		if err := ensureStream(pctx, shared, values); err != nil {
			return
		}
		err := w.client.RunMirror(pctx, pushGroup(w.id, id), dsclient.MirrorSpec{
			From:   shared,
			Source: values,
			Dest:   values,
			Create: true,
			Batch:  recordBatch,
		})
		if err != nil && pctx.Err() == nil && !errors.Is(err, context.Canceled) {
			c.log.Warn("wings: stopped pushing a shared channel to a worker", "worker", w.id, "channel", id, "err", err)
		}
	})
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


func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

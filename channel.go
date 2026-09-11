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

// A channel shared between runs on different machines is a durable stream
// relayed through the coordinator (the flow package's ChannelHost is the seam).
// Each run has an outbox per channel — wings.chanout.<job>.<attempt>.<channel> —
// written as part of the attempt's transaction, so a send's record comes home
// with the history that produced it, together, under the transaction pull
// (pull.go); each thread has its own transactional producer ([coordOutputs],
// [attemptOutputs]), so a send's record and its event commit atomically and
// isolated from the run's other threads — a worker thread holds a value until its
// event so no concurrent commit can tear the two ([attemptOutputs.stage]) — and a
// value reaches an outbox exactly once, so a replay never resends it. The
// coordinator's relay merges every outbox into one canonical stream,
// wings.chan.<channel>, admitting each record once and advancing the outbox's read
// position in the same commit (a durable, restart-idempotent merge cursor), with
// no dedup — history is the record. A run receives by reading the canonical
// stream — its own on the coordinator, a pushed copy on a worker.
// A run marks its outbox with a Link record when it subscribes, so even a pure
// receiver that sends nothing leaves an outbox the relay can see; the canonical
// stream is never dropped, so a restarted coordinator replays receives from it.

const (
	chanoutPrefix = "wings.chanout."
	// chanPrefix is the canonical stream, pushed to workers; not an output
	// family, so a worker's copy is not mirrored back.
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
// chanConsumes its consume stream (the reader's consume reports). Both are
// derivable from a channel's outbox name too, since streamPart is idempotent:
// chanvalPrefix+o.Name equals chanValues(id) for an outbox of id.
func chanValues(id string) string   { return chanvalPrefix + streamPart(id) }
func chanConsumes(id string) string { return chanconsPrefix + streamPart(id) }

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

// pushGroup names the mirror that pushes a channel's canonical stream to one
// worker. A moved receiver's pre-push and its live push share it, so the live
// push resumes where the pre-push stopped rather than copying the stream twice.
func pushGroup(workerID, id string) string { return "wings.push." + workerID + "." + streamPart(id) }

func outboxFor(run string, attempt int, id string) string {
	return outputName{Prefix: chanoutPrefix, Job: run, Attempt: attempt, Name: id}.String()
}

// --- relay, on the coordinator ---

// relayChannel is the relay's state for one channel: the canonical stream, a
// producer that merges outboxes into it transactionally (so each outbox's read
// position advances in the same commit as the records it yielded — a durable,
// restart-idempotent merge cursor), and the counts a holder waits on, folded from
// what is on the canonical. Per-thread transactional sends (see
// [coordOutputs.stage], [attemptOutputs.buffer]) mean a value reaches an outbox
// exactly once and a replay never resends it, so the merge admits every record
// rather than deduping — history is the record, which is why there is no ledger.
type relayChannel struct {
	stream   *dsclient.Stream[flow.ChannelItem]
	producer dsclient.Producer

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

// countLocked folds one merged record into the channel's counts. A Link marker is
// a subscription sign with no value and is not counted (nor put on the canonical).
// Call with mu held.
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

// mergeGroup names an outbox's durable read position on the canonical, under which
// the merge transaction stages how far the outbox has been consumed.
func mergeGroup(outbox string) string { return "wings.merge." + outbox }

// canonProducerID names the producer the relay merges a channel's outboxes into
// its canonical stream with.
func canonProducerID(id string) string { return "wings.merge." + streamPart(id) }

type channelRelay struct {
	poke chan struct{}

	mu       sync.Mutex
	channels map[string]*relayChannel // by canonical stream
	// creating serializes first-time setup of a canonical's relayChannel, one lock
	// per canonical: opening the merge producer twice under the same id fences the
	// first, so only one goroutine may open it. Keyed by canonical stream name.
	creating map[string]*sync.Mutex
	tailed   map[string]bool // outboxes being read
	// folded names the value and consume streams whose counts the relay is folding,
	// so each is folded once. Keyed by stream name.
	folded map[string]bool
	// finalJob names jobs that have settled and whose output is fully home, so
	// their outboxes are complete: once its tail has merged the last of one into
	// the canonical stream it drops it and stops. Keyed by streamPart(job).
	finalJob map[string]bool
	// outboxJobs names jobs the relay has ever tailed an outbox for, so a settled
	// job with no shared channel is not chased. Keyed by streamPart(job).
	outboxJobs map[string]bool
	// feeders counts the outboxes still feeding each canonical stream — the
	// coordinator's own plus one per forked sender — so the canonical is dropped
	// only when the last of them is gone. Keyed by canonical stream name.
	feeders map[string]int
	// canonByRun names the canonical streams of a run's channels, so the run's
	// completion can retire whatever is left. Keyed by run name.
	canonByRun map[string]map[string]bool
	// canonByJob names the canonical streams a placed job created, observed as its
	// outboxes are found, so the job's settle retires exactly them. Keyed by job.
	canonByJob map[string]map[string]bool
	// doneCanon names canonical streams whose owning run has completed, so the
	// last feeder to leave drops them. Keyed by canonical stream name.
	doneCanon map[string]bool
	// tailStop cancels a tailed outbox's reads, so retiring its channel wakes the
	// tail to drop it at once rather than after its next poll. Keyed by outbox name.
	tailStop map[string]context.CancelFunc
	// consumed is each channel's consume count, kept outside mu so a job unloading
	// on a send can snapshot it while holding the cluster lock without the relay's.
	// Keyed by canonical stream name, value uint64.
	consumed sync.Map
}

func (c *Cluster) startChannelRelay() {
	c.relay = &channelRelay{
		poke:       make(chan struct{}, 1),
		channels:   map[string]*relayChannel{},
		creating:   map[string]*sync.Mutex{},
		tailed:     map[string]bool{},
		folded:     map[string]bool{},
		finalJob:   map[string]bool{},
		outboxJobs: map[string]bool{},
		feeders:    map[string]int{},
		canonByRun: map[string]map[string]bool{},
		canonByJob: map[string]map[string]bool{},
		doneCanon:  map[string]bool{},
		tailStop:   map[string]context.CancelFunc{},
	}
	c.wg.Go(c.runChannelRelay)
}

// noteRunChannel records that canonical belongs to run, so the run's completion
// can retire it. Called from the coordinator's channel host, which alone has the
// run and the channel id unmangled.
func (r *channelRelay) noteRunChannel(run, canonical string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byRun := r.canonByRun[run]
	if byRun == nil {
		byRun = map[string]bool{}
		r.canonByRun[run] = byRun
	}
	byRun[canonical] = true
}

// noteJobChannel records that canonical was created by a placed job, so the
// job's settle retires exactly the channels it created — the same reclaim scope
// a returned in-process call gets, keyed by what the coordinator observed rather
// than inferred from the stream's name.
func (r *channelRelay) noteJobChannel(job, canonical string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byJob := r.canonByJob[job]
	if byJob == nil {
		byJob = map[string]bool{}
		r.canonByJob[job] = byJob
	}
	byJob[canonical] = true
}

func (r *channelRelay) jobFinal(job string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.finalJob[job]
}

// canonDone reports that a channel has been retired: its owning activity or run
// has finished, so no more will be sent on it and every outbox feeding it is
// complete. The run's own outbox (chanout.<run>.0.<id>) settles no job, so this
// is the only thing that ends it before the run does.
func (r *channelRelay) canonDone(canonical string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.doneCanon[canonical]
}

// hadOutbox reports whether the relay has ever tailed an outbox for a job, so
// only such a job's settle chases its channel outboxes.
func (c *Cluster) hadOutbox(job string) bool {
	if c.relay == nil {
		return false
	}
	c.relay.mu.Lock()
	defer c.relay.mu.Unlock()
	return c.relay.outboxJobs[job]
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
				if !ok || c.wasDropped(name) {
					continue
				}
				switch o.Prefix {
				case chanoutPrefix:
					c.tailOutbox(client, name, o.Name)
				case chanvalPrefix, chanconsPrefix:
					// Counts are folded straight off the value and consume streams: the
					// writer's values and closes, the reader's consumes. o.Name is the
					// id, streamPart-mangled, which the count key (chanStreamFor) and the
					// stream names (chanValues/chanConsumes) reproduce idempotently.
					c.foldChannel(client, name, o.Name, o.Prefix == chanvalPrefix)
				}
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
// stream and replaying what is on it so a restarted coordinator does not re-admit
// what its predecessor already wrote.
func (c *Cluster) relayFor(client *dsclient.Client, id string) (*relayChannel, error) {
	canonical := chanStreamFor(id)
	r := c.relay
	r.mu.Lock()
	if rc, ok := r.channels[canonical]; ok {
		r.mu.Unlock()
		return rc, nil
	}
	cm := r.creating[canonical]
	if cm == nil {
		cm = &sync.Mutex{}
		r.creating[canonical] = cm
	}
	r.mu.Unlock()

	// One creator per canonical: opening the merge producer is what bumps its epoch,
	// so two concurrent tails opening it would fence each other and stall the merge.
	cm.Lock()
	defer cm.Unlock()
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
	p, err := client.Producer(c.ctx, canonProducerID(id))
	if err != nil {
		return nil, fmt.Errorf("wings: open a producer for %s: %w", canonical, err)
	}
	// Counts are folded from the value and consume streams (foldChannel), not from
	// the canonical, so the relayChannel starts empty and the merge only keeps the
	// canonical for an outbox's drain and drop on settle. The merge cursors
	// (GetOffset) still resume each outbox where its predecessor left off.
	rc := &relayChannel{stream: st, producer: p}

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
	o, _ := parseOutput(name)
	job := o.Job

	r := c.relay
	tailCtx, tailStop := context.WithCancel(c.ctx)
	r.mu.Lock()
	if r.tailed[name] || c.closed {
		r.mu.Unlock()
		tailStop()
		return
	}
	r.tailed[name] = true
	r.outboxJobs[job] = true
	r.feeders[chanStreamFor(id)]++
	r.tailStop[name] = tailStop
	r.mu.Unlock()

	// A worker-created channel is known to the coordinator only by this outbox;
	// attribute its canonical stream to the job's run (so the run's end retires
	// whatever is left) and, when this job created it rather than merely sending or
	// receiving on it, to the job (so the job's settle retires exactly what it
	// created). The coordinator's own channels are attributed in
	// clusterChannels.Link instead; runOfJob names a live placed job, so it skips
	// them. Resolved off the lock to keep c.mu after r.mu.
	if run, thread, ok := c.runOfJob(job); ok {
		canonical := chanStreamFor(id)
		r.noteRunChannel(run, canonical)
		if createdByThread(canonical, run, thread) {
			r.noteJobChannel(job, canonical)
		}
	}

	c.wg.Go(func() {
		defer c.forgetTail(name, tailStop)
		rc, err := c.relayFor(client, id)
		if err != nil {
			c.log.Warn("wings: cannot relay a shared channel", "channel", id, "err", err)
			return
		}
		// Resume where this outbox was last merged, so a restarted coordinator does
		// not re-admit what its predecessor already put on the canonical.
		pos, _, err := client.GetOffset(c.ctx, chanStreamFor(id), mergeGroup(name))
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			c.log.Warn("wings: cannot read a channel merge cursor", "channel", id, "err", err)
			return
		}
		from := int64(pos)
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
			readCtx, cancel := context.WithTimeout(tailCtx, followPoll)
			recs, err := st.ReadBlocking(readCtx, from, recordBatch)
			// A wake (tailCtx ended while c.ctx lives) means the channel was retired.
			woke := tailCtx.Err() != nil && c.ctx.Err() == nil
			expired := readCtx.Err() != nil && !woke
			cancel()
			if err != nil && !woke {
				if c.ctx.Err() != nil {
					return
				}
				if expired {
					// Caught up (nothing new before the poll timed out). Everything
					// this outbox holds is merged into the canonical stream now, so
					// drop it once nothing more will be written to it: the job has
					// settled and its output is home (jobFinal), or the channel has
					// been retired (canonDone) — the wake below is the prompt path for
					// that, this the backstop for a tail registered after the retire.
					// The canonical stream stays for a resume to replay from.
					if c.relay.jobFinal(job) || c.relay.canonDone(chanStreamFor(id)) {
						c.dropOutbox(name)
						return
					}
					continue
				}
				if c.wasDropped(name) {
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
			if from, err = c.mergeOutbox(rc, id, name, from, recs); err != nil {
				if c.ctx.Err() == nil {
					c.log.Warn("wings: cannot relay a shared channel", "channel", id, "err", err)
				}
				return
			}
			if woke {
				// Retired: drain what the outbox still holds into the canonical, so a
				// value the run's own thread sent is not lost, then drop it.
				for {
					rest, err := st.Read(c.ctx, from, recordBatch)
					if err != nil || len(rest) == 0 {
						break
					}
					if from, err = c.mergeOutbox(rc, id, name, from, rest); err != nil {
						break
					}
				}
				c.dropOutbox(name)
				return
			}
		}
	})
}

// mergeOutbox appends an outbox batch to the canonical stream and advances the
// outbox's durable read position in the same transaction — so a restart resumes
// exactly where it committed, never re-admitting a record — then wakes receivers.
// Link markers are a subscription sign only and are not forwarded or counted.
func (c *Cluster) mergeOutbox(rc *relayChannel, id, outbox string, from int64, recs []dsclient.OffsetRecord[flow.ChannelItem]) (int64, error) {
	items := make([]flow.ChannelItem, 0, len(recs))
	for _, rec := range recs {
		from = rec.Offset + 1
		if !rec.Record.Link {
			items = append(items, rec.Record)
		}
	}
	if len(items) == 0 {
		// Only subscription markers: nothing to put on the canonical, and the cursor
		// advances durably the next time a real record is merged (a marker re-read on
		// restart is skipped again).
		return from, nil
	}
	canonical := chanStreamFor(id)

	rc.mu.Lock()
	tx, err := rc.producer.BeginTimeout(c.ctx, rc.producer.TransactionTimeout())
	if err != nil {
		rc.mu.Unlock()
		return from, fmt.Errorf("wings: begin merge of %s: %w", canonical, err)
	}
	if _, err := dsclient.Output(tx, rc.stream).Append(c.ctx, items); err != nil {
		_ = tx.Abort(context.WithoutCancel(c.ctx))
		rc.mu.Unlock()
		return from, fmt.Errorf("wings: merge into %s: %w", canonical, err)
	}
	if err := tx.StageOffset(c.ctx, canonical, mergeGroup(outbox), uint64(from)); err != nil {
		_ = tx.Abort(context.WithoutCancel(c.ctx))
		rc.mu.Unlock()
		return from, fmt.Errorf("wings: record merge cursor of %s: %w", outbox, err)
	}
	if err := tx.Commit(c.ctx); err != nil {
		rc.mu.Unlock()
		return from, fmt.Errorf("wings: commit merge of %s: %w", canonical, err)
	}
	rc.mu.Unlock()
	return from, nil
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
		rc, err := c.relayFor(client, id)
		if err != nil {
			if c.ctx.Err() == nil {
				c.log.Warn("wings: cannot fold a shared channel's counts", "channel", id, "err", err)
			}
			return
		}
		var from int64
		for c.ctx.Err() == nil {
			// Re-opened each pass, as the outbox tail is: a handle opened before the
			// first append binds to the empty stream and never sees later writes — a
			// moved writer's first append to the stable stream is that window.
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

// forgetTail drops a tail's cancel registration as it exits and releases the
// context, whether it left on its own or was woken to retire.
func (c *Cluster) forgetTail(name string, stop context.CancelFunc) {
	r := c.relay
	r.mu.Lock()
	if r.tailStop[name] != nil {
		delete(r.tailStop, name)
	}
	r.mu.Unlock()
	stop()
}

// finishChannels marks a settled job's shared-channel outboxes for the relay to
// drop, but only once the job's output is fully home, so the relay has all of
// each outbox to merge into the canonical stream before it goes. In-process work
// writes straight to shared storage, so there is nothing to wait for. Called off
// the settle path for a job the relay has tailed an outbox for.
func (c *Cluster) finishChannels(job string, w *workerConn) {
	if c.relay == nil {
		return
	}
	client, err := c.sharedClient()
	if err != nil {
		return
	}
	if w != nil && w.client != client {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(c.ctx), outputDrain)
		caught := false
		for {
			// The outbox comes home by the transaction pull now (pull.go), so wait
			// on the pulled level rather than the mirror: the coordinator's copy of
			// the job's streams must be as complete as the worker's before the relay
			// drops the outbox.
			done, err := c.pulledLevel(ctx, w, job)
			if err != nil {
				break // cannot confirm the copy is home
			}
			if done {
				caught = true
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(outputPoll):
				continue
			}
			break
		}
		cancel()
		if !caught {
			// Leave the outbox rather than risk dropping sends still on their way
			// home; a resume still reads them, and this only forgoes the cleanup.
			return
		}
	}
	r := c.relay
	r.mu.Lock()
	r.finalJob[streamPart(job)] = true
	r.mu.Unlock()
	c.pokeRelay()
}

// dropOutbox deletes a settled channel's outbox and lets its tail stop. tailed
// keeps the name, so no discovery pass starts a fresh tail on the gone stream.
// Dropping it drops the canonical stream too once this was its last feeder and
// the owning run has finished (dropRetiredCanonical).
func (c *Cluster) dropOutbox(name string) {
	client, err := c.sharedClient()
	if err != nil {
		return
	}
	c.markDropped([]string{name})
	if err := dropStream(context.WithoutCancel(c.ctx), client, name); err != nil {
		c.log.Warn("wings: could not drop a settled channel outbox", "stream", name, "err", err)
	}
	c.unmarkDropped([]string{name})

	o, _ := parseOutput(name)
	canonical := chanPrefix + o.Name
	r := c.relay
	r.mu.Lock()
	if r.feeders[canonical] > 0 {
		r.feeders[canonical]--
	}
	retire := r.feeders[canonical] == 0 && r.doneCanon[canonical]
	r.mu.Unlock()
	if retire {
		c.dropCanonical(canonical)
	}
}

// retireRunChannels marks a finished run's canonical streams for retirement and
// drops any whose feeding outboxes are already gone; the rest go as their last
// feeder's outbox is dropped (dropOutbox). A completed run will not resume, so
// its canonical streams — kept otherwise so a resume can replay receives — are
// dead. Covers the run's coordinator-created channels (noteRunChannel).
func (c *Cluster) retireRunChannels(run string) {
	r := c.relay
	if r == nil {
		return
	}
	r.mu.Lock()
	cs := make([]string, 0, len(r.canonByRun[run]))
	for canonical := range r.canonByRun[run] {
		cs = append(cs, canonical)
	}
	delete(r.canonByRun, run)
	r.mu.Unlock()
	c.retireCanonicals(cs)
}

// createdByThread reports whether canonical is the stream of a channel that
// thread created. A channel's id is "<run>/<thread>.ch<n>", so its stream name
// is the thread's channel prefix followed by the digits of n — and a sub-thread's
// channel ("<thread>.<k>.ch<n>") has a digit, not "ch", after the prefix, so it
// does not match its parent. This is what tells a channel a job created from one
// it merely sends or receives on: only the creator retires it.
func createdByThread(canonical, run, thread string) bool {
	prefix := chanPrefix + streamPart(run) + "_" + streamPart(thread) + "_ch"
	rest, ok := strings.CutPrefix(canonical, prefix)
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
// the activity. The channels are those observed to be created under the job
// (noteJobChannel, gated by createdByThread), the same reclaim a returned
// in-process call gets by explicit id. Selects for [retireCanonicals].
func (c *Cluster) retireJobChannels(job string) {
	r := c.relay
	if r == nil {
		return
	}
	r.mu.Lock()
	cs := make([]string, 0, len(r.canonByJob[job]))
	for canonical := range r.canonByJob[job] {
		cs = append(cs, canonical)
	}
	delete(r.canonByJob, job)
	r.mu.Unlock()
	c.retireCanonicals(cs)
}

// retireChannels retires channels named by their ids, for an in-process activity
// call that created them and has now returned (see [flow.ChannelRetirer]) — the
// same reclaim as a forked activity's, keyed by explicit id because a direct call
// shares its caller's thread rather than getting one of its own. Selects for
// [retireCanonicals].
func (c *Cluster) retireChannels(run string, ids []string) {
	r := c.relay
	if r == nil {
		return
	}
	r.mu.Lock()
	byRun := r.canonByRun[run]
	var cs []string
	for _, id := range ids {
		if canonical := chanStreamFor(id); byRun[canonical] {
			cs = append(cs, canonical)
		}
	}
	r.mu.Unlock()
	c.retireCanonicals(cs)
}

// retireCanonicals is the one way the relay retires a canonical stream: mark each
// done and drop those with no feeding outbox left; the rest go as their last
// feeder's outbox is dropped (dropOutbox), so a channel still fed by a live sender
// waits for its data to arrive home. Every reclaim — a returned in-process call
// ([Cluster.retireChannels]), a settled forked activity ([Cluster.retireJobChannels]),
// a finished run ([Cluster.retireRunChannels]) — selects its channels and calls this.
func (c *Cluster) retireCanonicals(canonicals []string) {
	r := c.relay
	if r == nil || len(canonicals) == 0 {
		return
	}
	r.mu.Lock()
	retired := make(map[string]bool, len(canonicals))
	var drop []string
	var wake []context.CancelFunc
	for _, canonical := range canonicals {
		r.doneCanon[canonical] = true
		retired[canonical] = true
		if r.feeders[canonical] == 0 {
			drop = append(drop, canonical)
		}
	}
	// Wake the tails feeding a retired channel so they drop their outboxes at once,
	// rather than after their next poll; the last one gone drops the canonical.
	for name, stop := range r.tailStop {
		if o, ok := parseOutput(name); ok && retired[chanPrefix+o.Name] {
			wake = append(wake, stop)
		}
	}
	r.mu.Unlock()
	for _, stop := range wake {
		stop()
	}
	for _, canonical := range drop {
		c.dropCanonical(canonical)
	}
}

// dropCanonical deletes one canonical stream and forgets the relay's state for
// it. Called when the channel's activity has finished and its last outbox is gone.
func (c *Cluster) dropCanonical(canonical string) {
	client, err := c.sharedClient()
	if err != nil {
		return
	}
	c.markDropped([]string{canonical})
	if err := dropStream(context.WithoutCancel(c.ctx), client, canonical); err != nil {
		c.log.Warn("wings: could not drop a finished run's channel stream", "stream", canonical, "err", err)
	}
	c.unmarkDropped([]string{canonical})
	r := c.relay
	r.mu.Lock()
	delete(r.channels, canonical)
	delete(r.feeders, canonical)
	delete(r.doneCanon, canonical)
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

func (h clusterChannels) Link(ctx context.Context, run, id string, _ flow.LinkMode) (flow.ChannelLink, error) {
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
	// The run owns this channel; record it so the run's completion can retire its
	// canonical stream. Only here is the run paired with the unmangled id.
	if h.c.relay != nil {
		h.c.relay.noteRunChannel(run, chanStreamFor(id))
	}
	h.c.pokeRelay()

	// Alongside the outbox (still merged into the canonical stream the reader reads),
	// a send's records also go to the channel's stable value or consume stream, which
	// the reader and backpressure move onto next.
	valLazy, consLazy := valConsLazy(client, id)
	send := func(ctx context.Context, _ string, it flow.ChannelItem) error {
		// A signal owns no thread and so no transaction; it only ever sends a value,
		// written straight through to both streams.
		st, err := valLazy.get(ctx)
		if err != nil {
			return err
		}
		if _, err := outbox.Append(ctx, []flow.ChannelItem{it}); err != nil {
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
			if err := outputs.stage(ctx, outbox, it); err != nil {
				return err
			}
			return outputs.stage(ctx, st, it)
		}
	}
	return &channelLink{
		send:   send,
		client: client,
		in:     chanValues(id),
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
	// A worker's outbox comes home inside a thread's transactions (pull.go), not by a
	// stream copy, so an outbox that never has a record written to it — a pure
	// creator's or receiver's — produces no transaction and the coordinator never
	// learns it exists, and so never attributes or reclaims its channel. Write one
	// marker through the linking thread's producer, committed now, so every outbox is
	// pulled and the relay sees it; the relay drops the marker from the record.
	// A send's records go to the channel's stable value or consume stream — the
	// reader reads values off the value stream, backpressure folds consumes off the
	// consume stream — alongside the outbox the relay still merges (for the counts
	// backpressure reads until it moves off too). The streams open on first send,
	// off the share path a fork waits on, so linking stays cheap.
	valLazy, consLazy := valConsLazy(h.n.client, id)
	mctx := context.WithoutCancel(ctx)
	linker := h.job.txns.For(threadOrMain(ctx))
	if err := linker.append(mctx, outbox, []flow.ChannelItem{{Link: true}}); err != nil {
		return nil, err
	}
	if mode == flow.LinkRead {
		// A reader's values are the writer's, pushed here from the coordinator's copy
		// of the value stream. The coordinator starts that push when a channel's
		// consume stream appears on a worker, so creating it now — a reader does, a
		// writer never does — announces this worker as the reader to push to, and only
		// the reader, so the writer's own worker is never pushed its own values back.
		cons, err := consLazy.get(mctx)
		if err != nil {
			return nil, err
		}
		if err := linker.append(mctx, cons, []flow.ChannelItem{{Link: true}}); err != nil {
			return nil, err
		}
	}
	if err := linker.commit(mctx); err != nil {
		return nil, err
	}
	return &channelLink{
		// Each record goes into the writing thread's own transaction, so it commits
		// with the event that justifies it and no sibling thread's commit can tear the
		// two apart (pull.go, [attemptOutputs.stage]). A value waits for its send event;
		// a close or consume report rides the event it pairs with. The whole outbox is
		// transactional, so it is pulled, not mirrored.
		send: func(ctx context.Context, _ string, it flow.ChannelItem) error {
			out := h.job.txns.For(threadOrMain(ctx))
			extra := valLazy
			if it.Consumed {
				extra = consLazy
			}
			es, err := extra.get(ctx)
			if err != nil {
				return err
			}
			if it.Closed || it.Consumed {
				if err := out.append(ctx, outbox, []flow.ChannelItem{it}); err != nil {
					return err
				}
				return out.append(ctx, es, []flow.ChannelItem{it})
			}
			out.stage(outbox, it)
			out.stage(es, it)
			return nil
		},
		client: h.n.client,
		in:     chanValues(id),
	}, nil
}

// RetireChannels implements [flow.ChannelRetirer]: the run body's in-process
// call that created these channels has returned, so retire them. Done off the
// caller so the call is not held for storage work; the run's end retires whatever
// is left.
func (h clusterChannels) RetireChannels(_ context.Context, run string, ids []string) {
	h.c.wg.Go(func() { h.c.retireChannels(run, ids) })
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

// channelLink is a run's connection to one shared channel: sends go to the run's
// outbox, items come from the canonical stream.
type channelLink struct {
	send   func(ctx context.Context, sender string, it flow.ChannelItem) error
	client *dsclient.Client
	in     string
}

func (l *channelLink) Send(ctx context.Context, sender string, it flow.ChannelItem) error {
	return l.send(ctx, sender, it)
}

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

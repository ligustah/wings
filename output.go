package wings

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow/protos"
)

// Getting a job's streams off a worker onto the coordinator, and back when the
// job moves. Everything an attempt writes — history, recordings, and shared-channel
// outboxes — it writes inside its transaction, so it all comes home as the
// attempt's transactions (pull.go); a [dsclient.MirrorSet] only watches worker
// catalogs, to subscribe a worker to a channel when its outbox appears, and makes
// the copies the other way onto the worker a retry runs on (hydrate). No
// bookkeeping: where a copy has reached lives at the destination, re-derived from
// stream names.

const (
	// outputSet names the mirror's consumer group, so a restarted coordinator
	// resumes its copies rather than remaking them.
	outputSet = "wings.outputs"

	// outputDiscover is the fallback fleet re-list interval; the set otherwise
	// follows each worker's catalog live.
	outputDiscover = 5 * time.Minute

	// outputDrain bounds how long taking work off a worker waits for its copy to
	// catch up.
	outputDrain = 15 * time.Second

	// outputPoll is how often that wait looks again.
	outputPoll = 50 * time.Millisecond

	// outputWait bounds how long a reader waits for output still on its way
	// behind the handle that named it.
	outputWait = 2 * time.Minute

	// outputAppend bounds one append to a worker's own storage — a net under a
	// broker that has stopped answering.
	outputAppend = 30 * time.Second
)

const (
	// recordingPrefix is a job's event log. See recording.go.
	recordingPrefix = "wings.replay."
	// priorPrefix holds a previous attempt's log on the worker running the next
	// one, out of reach of the mirror carrying that worker's output the other
	// way — two mirrors on one destination fence each other.
	priorPrefix = "wings.prior."
)

// outputName is one output's stream name, taken apart. The name carries whose
// it is, which attempt wrote it, and what the job called it, so no register of
// outputs is needed.
type outputName struct {
	Prefix  string
	Job     string
	Attempt int
	Name    string
}

func (o outputName) String() string {
	if o.Prefix == chanvalPrefix || o.Prefix == chanconsPrefix {
		// A channel stream is named by its id alone: no job, no attempt.
		return o.Prefix + streamPart(o.Name)
	}
	return o.Prefix + streamPart(o.Job) + "." + strconv.Itoa(o.Attempt) + "." + streamPart(o.Name)
}

// in returns the same output under another prefix.
func (o outputName) in(prefix string) outputName { o.Prefix = prefix; return o }

// parseOutput takes a stream name apart and reports whether it is one of ours.
func parseOutput(stream string) (outputName, bool) {
	for _, prefix := range []string{chanvalPrefix, chanconsPrefix} {
		if rest, ok := strings.CutPrefix(stream, prefix); ok {
			// Stable channel stream: the id alone, with no job or attempt, so the
			// job/attempt filters (dropOutputsOf, per-job pulledLevel) skip it — it is
			// shared across a channel's life and must not be dropped on a move.
			return outputName{Prefix: prefix, Name: rest}, true
		}
	}
	for _, prefix := range []string{recordingPrefix, historyPrefix, priorPrefix} {
		rest, ok := strings.CutPrefix(stream, prefix)
		if !ok {
			continue
		}
		parts := strings.Split(rest, ".")
		if len(parts) != 3 {
			return outputName{}, false
		}
		attempt, err := strconv.Atoi(parts[1])
		if err != nil {
			return outputName{}, false
		}
		return outputName{Prefix: prefix, Job: parts[0], Attempt: attempt, Name: parts[2]}, true
	}
	return outputName{}, false
}

// streamPart maps everything outside [A-Za-z0-9_-] to an underscore, so a
// caller's output name is safe to join with dots.
func streamPart(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// jobOutput stands up a stream for [Record] on the worker the coordinator will
// keep it from.
func jobOutput(ctx context.Context, prefix, name string) (*dsclient.Client, string, error) {
	if name == "" {
		return nil, "", errors.New("wings: this needs a name")
	}
	j := jobFrom(ctx)
	if j == nil {
		return nil, "", errors.New("wings: this was called outside a work function; " +
			"it belongs to a job, and there is no job here")
	}
	stream := outputName{Prefix: prefix, Job: j.id, Attempt: j.attempt, Name: name}.String()
	client, err := j.node.declareOutput(ctx, stream, name)
	if err != nil {
		return nil, "", err
	}
	return client, stream, nil
}

// ensureStream creates a stream if it is not already there.
func ensureStream(ctx context.Context, client *dsclient.Client, name string) error {
	ok, err := client.StreamExists(ctx, name)
	if err != nil {
		return fmt.Errorf("wings: check %s: %w", name, err)
	}
	if ok {
		return nil
	}
	if err := client.CreateStream(ctx, name, streamConfig()); err != nil {
		return fmt.Errorf("wings: create %s: %w", name, err)
	}
	return nil
}

// awaitStream waits for a stream that is still expected to arrive — the handle
// travels ahead of what it names. expect is false for a dead attempt's handle,
// whose output will never arrive.
func awaitStream(ctx context.Context, client *dsclient.Client, name string, expect bool) error {
	deadline := ctx
	if expect {
		var cancel context.CancelFunc
		deadline, cancel = context.WithTimeout(ctx, outputWait)
		defer cancel()
	}
	for {
		ok, err := client.StreamExists(deadline, name)
		if err == nil && ok {
			return nil
		}
		if err != nil && ctx.Err() != nil {
			return fmt.Errorf("wings: check %s: %w", name, err)
		}
		if !expect {
			return fmt.Errorf("wings: %s is not here; it was discarded, or it never arrived", name)
		}
		select {
		case <-deadline.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("wings: %s has not arrived after %s; the worker that wrote "+
				"it may have gone before its output was copied", name, outputWait)
		case <-time.After(outputPoll):
		}
	}
}

// startOutputMirror watches the fleet's catalogs so a worker's outbox, when it
// appears, subscribes that worker to the channel's canonical stream. It copies
// nothing home — everything a job writes comes by the transaction pull (pull.go) —
// but the pull cannot see a pure receiver's outbox, which writes no record, so the
// subscription is noticed here instead.
func (c *Cluster) startOutputMirror() error {
	client, err := c.sharedClient()
	if err != nil {
		return err
	}
	h, err := client.NewMirrorSet(outputSet, dsclient.MirrorSet{
		LiveSources: func(context.Context) ([]dsclient.MirrorSource, error) {
			return c.mirrorSources(client), nil
		},
		Create:   true,
		Discover: outputDiscover,
		Select: func(cand dsclient.MirrorCandidate) (dsclient.MirrorTarget, error) {
			// Everything a job writes is written inside the attempt's transaction and
			// comes home by the transaction pull (pull.go), so the set copies nothing.
			// It watches worker catalogs for one thing the pull cannot drive: a reader
			// needs the writer's values pushed to its worker, and a reader — only a
			// reader, never the writer — creates the channel's consume stream as it
			// links. So the value stream is pushed to a worker when its consume stream
			// appears here; pushing it to the writer's own worker, which appends those
			// same values, would be a mirror loop.
			if o, ok := parseOutput(cand.Stream); ok && o.Prefix == chanconsPrefix && !c.wasDropped(cand.Stream) {
				c.subscribeChannel(cand.Source, o.Name)
			}
			return dsclient.MirrorTarget{}, dsclient.ErrSkipStream
		},
		OnStreamError: func(cand dsclient.MirrorCandidate, err error) error {
			if c.ctx.Err() == nil && !errors.Is(err, context.Canceled) {
				c.log.Warn("wings: keeping worker output", "worker", cand.Source,
					"stream", cand.Stream, "err", err)
			}
			return nil // one worker's trouble is not the fleet stopping
		},
		OnDiscoveryDegraded: func(source string, err error) {
			// Falling back to polling still works, but silently adds minutes of
			// lag on a fleet that retires idle machines — worth a line.
			c.log.Warn("wings: cannot follow a worker's new streams; falling back to polling",
				"worker", source, "every", outputDiscover, "err", err)
		},
	})
	if err != nil {
		return fmt.Errorf("wings: set up output storage: %w", err)
	}
	c.outputs = h

	c.wg.Go(func() {
		if err := h.Run(c.ctx); err != nil && c.ctx.Err() == nil {
			c.log.Error("wings: stopped keeping output", "err", err)
		}
	})
	return nil
}

// mirrorSources is the fleet as the mirror should see it now. An in-process
// worker is left out: its writes already are the coordinator's copy.
func (c *Cluster) mirrorSources(shared *dsclient.Client) []dsclient.MirrorSource {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []dsclient.MirrorSource
	for _, w := range c.workers {
		if w.client == shared {
			continue
		}
		out = append(out, dsclient.MirrorSource{Name: w.id, Client: w.client})
	}
	return out
}

// pokeOutputs tells the mirror the fleet changed, so a just-arrived machine is
// copied at once rather than after a discovery interval. Pokes coalesce.
func (c *Cluster) pokeOutputs() {
	if c.outputs != nil {
		c.outputs.Poke()
	}
}

// hydrate copies a job's earlier recordings from the coordinator onto the worker
// about to run it again — a moved job reads its predecessor's output through its
// own storage. One resumable pass: a stream already copied copies nothing.
func (c *Cluster) hydrate(ctx context.Context, w *workerConn, priors []Recording) error {
	if len(priors) == 0 {
		return nil
	}
	client, err := c.sharedClient()
	if err != nil {
		return err
	}
	if w.client == client {
		return nil // one engine; already where it needs to be
	}

	want := make(map[string]string, len(priors))
	for _, rec := range priors {
		o, ok := parseOutput(rec.ID)
		if !ok {
			continue
		}
		want[o.in(recordingPrefix).String()] = rec.ID
	}
	if len(want) == 0 {
		return nil
	}

	return w.client.RunMirrorSet(ctx, outputSet, dsclient.MirrorSet{
		Sources:          []dsclient.MirrorSource{{Name: "coordinator", Client: client}},
		Create:           true,
		StopWhenCaughtUp: true,
		Batch:            recordBatch,
		Select: func(cand dsclient.MirrorCandidate) (dsclient.MirrorTarget, error) {
			dest, ok := want[cand.Stream]
			if !ok {
				return dsclient.MirrorTarget{}, dsclient.ErrSkipStream
			}
			return dsclient.MirrorTarget{Name: dest}, nil
		},
	})
}

// hydrateHistory copies a job's last history onto the worker about to run its
// next attempt, under that attempt's name, so the retry replays to where its
// predecessor got and carries on under its own name.
func (c *Cluster) hydrateHistory(ctx context.Context, w *workerConn, job jobEnvelope) error {
	client, err := c.sharedClient()
	if err != nil {
		return err
	}
	attempt, ok, err := c.lastAttempt(ctx, job.ID, job.Attempt)
	if err != nil || !ok {
		return err
	}
	sources, err := historyStreams(ctx, client, job.ID, attempt)
	if err != nil {
		return err
	}
	// Each thread's history is its own stream; copy every one, keeping its thread
	// (the Name component) under the new attempt so the retry replays each thread.
	for _, source := range sources {
		o, _ := parseOutput(source)
		dest := outputName{Prefix: historyPrefix, Job: o.Job, Attempt: job.Attempt, Name: o.Name}.String()
		if err := w.client.RunMirror(ctx, "wings.hydrate."+dest, dsclient.MirrorSpec{
			From:             client,
			Source:           source,
			Dest:             dest,
			Create:           true,
			StopWhenCaughtUp: true,
			Batch:            recordBatch,
		}); err != nil {
			return err
		}
	}
	return nil
}

// hydrateChannels copies the canonical streams a moved job receives from onto the
// worker about to run its next attempt, before it is dispatched, so the replay
// reads its recorded receives from a local copy — rather than waiting for the
// push to be discovered once the job links its outbox, a wait that can outlast a
// heartbeat and move the job again. The catch-up copy is awaited on the same
// consumer group the live push uses, so the push later resumes from it rather than
// re-copying; subscribeChannel then keeps delivering new values. Nothing is copied
// into the job's own storage: the canonical stream stays the one durable copy, and
// the worker's copy is transient, as for any worker that uses the channel. The
// channels are read from the job's last history, where each receive names its own.
func (c *Cluster) hydrateChannels(ctx context.Context, w *workerConn, job jobEnvelope) error {
	client, err := c.sharedClient()
	if err != nil {
		return err
	}
	if w.client == client {
		return nil // one broker; the worker already reads the coordinator's canonical
	}
	ids, err := c.receivedChannels(ctx, client, job)
	if err != nil || len(ids) == 0 {
		return err
	}
	for _, id := range ids {
		values := chanValues(id)
		ok, err := client.StreamExists(ctx, values)
		if err != nil {
			return fmt.Errorf("wings: look for channel stream %s: %w", values, err)
		}
		if !ok {
			continue // nothing was put on it, so nothing to replay
		}
		if err := ensureStream(ctx, w.client, values); err != nil {
			return err
		}
		if err := w.client.RunMirror(ctx, pushGroup(w.id, id), dsclient.MirrorSpec{
			From:             client,
			Source:           values,
			Dest:             values,
			Create:           true,
			StopWhenCaughtUp: true,
			Batch:            recordBatch,
		}); err != nil {
			return fmt.Errorf("wings: pre-push channel %s to worker %s: %w", id, w.id, err)
		}
		// Keep delivering new values on the same group, which resumes where the
		// catch-up stopped.
		c.subscribeChannel(w.id, id)
	}
	return nil
}

// receivedChannels are the ids of the shared channels a job's last history shows
// it received from, each canonical stream named the way its receives name it.
// Every thread's history is scanned, since any thread of the run may have received.
func (c *Cluster) receivedChannels(ctx context.Context, client *dsclient.Client, job jobEnvelope) ([]string, error) {
	attempt, ok, err := c.lastAttempt(ctx, job.ID, job.Attempt)
	if err != nil || !ok {
		return nil, err
	}
	sources, err := historyStreams(ctx, client, job.ID, attempt)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var ids []string
	for _, source := range sources {
		st, err := eventStream[*protos.Event](client, source)
		if err != nil {
			return nil, err
		}
		for from := int64(0); ; {
			recs, err := st.Read(ctx, from, recordBatch)
			if err != nil {
				return nil, fmt.Errorf("wings: read history %s to find its channels: %w", source, err)
			}
			if len(recs) == 0 {
				break
			}
			for _, r := range recs {
				from = r.Offset + 1
				rv := r.Record.GetChannelRecv()
				if rv == nil || rv.GetChannel() == "" {
					continue
				}
				// A channel received from another run is recorded by its full id; one this
				// run created is recorded by its local name, qualified here as its
				// canonical stream is.
				id := rv.GetChannel()
				if !strings.Contains(id, "/") {
					id = job.Run + "/" + id
				}
				if !seen[id] {
					seen[id] = true
					ids = append(ids, id)
				}
			}
		}
	}
	return ids, nil
}

// lastAttempt is the highest attempt of job with any history on the coordinator,
// before the one given (any attempt when before is negative); ok is false when
// the job has none home yet.
func (c *Cluster) lastAttempt(ctx context.Context, job string, before int) (int, bool, error) {
	client, err := c.sharedClient()
	if err != nil {
		return 0, false, err
	}
	names, err := client.ListStreams(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("wings: look for the history of job %s: %w", job, err)
	}
	last, found := -1, false
	for _, name := range names {
		o, ok := parseOutput(name)
		if !ok || o.Prefix != historyPrefix || o.Job != streamPart(job) || (before >= 0 && o.Attempt >= before) {
			continue
		}
		if o.Attempt > last {
			last, found = o.Attempt, true
		}
	}
	return last, found, nil
}

// historyStreams are the names of every thread's history stream for one attempt
// of a job, on the coordinator.
func historyStreams(ctx context.Context, client *dsclient.Client, job string, attempt int) ([]string, error) {
	names, err := client.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("wings: look for the history of job %s: %w", job, err)
	}
	var out []string
	for _, name := range names {
		o, ok := parseOutput(name)
		if !ok || o.Prefix != historyPrefix || o.Job != streamPart(job) || o.Attempt != attempt {
			continue
		}
		out = append(out, name)
	}
	return out, nil
}

// drainOutputs waits, bounded and best-effort, for the coordinator's copies of
// what a worker holds to be as complete as the worker's own, before it stops
// being readable. Everything a job writes comes home by the transaction pull
// (pull.go), so this waits on the pulled level. Both callers have already stopped
// sending the worker work.
func (c *Cluster) drainOutputs(ctx context.Context, from *workerConn, job string) {
	client, err := c.sharedClient()
	if err != nil || from == nil || from.client == client {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, outputDrain)
	defer cancel()

	for {
		level, err := c.pulledLevel(ctx, from, job)
		if err != nil || level {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(outputPoll):
		}
	}
}

// priorsOf finds a job's earlier attempts' logs on the coordinator, oldest
// first, each named as the worker about to run the retry will find it: under the
// prior namespace for a worker with its own storage, or in place in process.
func (c *Cluster) priorsOf(ctx context.Context, w *workerConn, job string) ([]Recording, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	lands := priorPrefix
	if w.client == client {
		lands = recordingPrefix
	}
	names, err := client.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("wings: look for what job %s recorded: %w", job, err)
	}
	var out []Recording
	for _, name := range names {
		o, ok := parseOutput(name)
		if !ok || o.Prefix != recordingPrefix || o.Job != streamPart(job) {
			continue
		}
		out = append(out, Recording{
			Name:    o.Name,
			ID:      o.in(lands).String(),
			Attempt: o.Attempt,
		})
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Attempt < out[j-1].Attempt; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, nil
}

// dropOutputsOf deletes what a job's abandoned attempts wrote, on both sides;
// the kept attempt's output stays until the caller discards it. Order matters:
// decline the stream, forget the copy, then delete source-first, or a running
// copy would put a deleted destination back. writers says which worker ran each
// attempt, so only that worker is asked to delete its stream.
func (c *Cluster) dropOutputsOf(job string, keep int, writers map[int]*workerConn) {
	client, err := c.sharedClient()
	if err != nil {
		return
	}
	ctx := context.WithoutCancel(c.ctx)
	// Let the coordinator's copy of the kept attempt catch up before deleting any
	// of the job's streams on its worker: deleting one mid-transaction fails the
	// pull for the whole worker, which would strand other jobs' output on it. The
	// kept attempt's worker is the one still pulling; abandoned ones are drained or
	// dead.
	c.drainOutputs(ctx, writers[keep], job)
	names, err := client.ListStreams(ctx)
	if err != nil {
		c.log.Warn("wings: could not look for abandoned output", "job", job, "err", err)
		return
	}
	var stale []outputName
	for _, name := range names {
		o, ok := parseOutput(name)
		if !ok || o.Job != streamPart(job) {
			continue
		}
		// The kept attempt's recordings stay; its history is for a next attempt
		// that will not come, so it goes too — unless RetainHistory keeps it, so
		// the inspector can still show a moved thread's execution.
		if o.Attempt == keep && (o.Prefix != historyPrefix || c.cfg.RetainHistory) {
			continue
		}
		stale = append(stale, o)
	}
	if len(stale) == 0 {
		return
	}
	staleNames := make([]string, len(stale))
	for i, o := range stale {
		staleNames[i] = o.String()
	}

	c.markDropped(staleNames)
	defer c.unmarkDropped(staleNames)

	fleet := c.fleet()
	reachable := func(w *workerConn) bool {
		return w != nil && w.client != client && slices.Contains(fleet, w)
	}
	for _, o := range stale {
		name := o.String()
		if w := writers[o.Attempt]; reachable(w) {
			if c.outputs != nil {
				c.outputs.Forget(w.id, name)
			}
			if err := dropStream(ctx, w.client, name); err != nil {
				c.log.Warn("wings: could not discard abandoned output on a worker",
					"worker", w.id, "stream", name, "err", err)
			}
		}
		// A recording was also copied as a prior onto every later attempt's worker.
		if o.Prefix == recordingPrefix {
			prior := o.in(priorPrefix).String()
			for attempt, w := range writers {
				if attempt <= o.Attempt || !reachable(w) {
					continue
				}
				if err := dropStream(ctx, w.client, prior); err != nil {
					c.log.Warn("wings: could not discard a prior on a worker",
						"worker", w.id, "stream", prior, "err", err)
				}
			}
		}
		if err := dropStream(ctx, client, name); err != nil {
			c.log.Warn("wings: could not discard abandoned output", "stream", name, "err", err)
		}
	}
}

// fleet is a snapshot of the workers, for use without the lock.
func (c *Cluster) fleet() []*workerConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*workerConn(nil), c.workers...)
}

// markDropped records a stream as deleted so no discovery pass restarts its
// copy; wasDropped reports it, unmarkDropped clears it once the stream is gone.
func (c *Cluster) markDropped(names []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dropped == nil {
		c.dropped = map[string]bool{}
	}
	for _, n := range names {
		c.dropped[n] = true
	}
}

func (c *Cluster) unmarkDropped(names []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range names {
		delete(c.dropped, n)
	}
}

func (c *Cluster) wasDropped(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropped[name]
}

// dropStream deletes a stream. Deleting one already gone is not an error.
func dropStream(ctx context.Context, client *dsclient.Client, name string) error {
	ok, err := client.StreamExists(ctx, name)
	if err != nil {
		return fmt.Errorf("wings: check %s: %w", name, err)
	}
	if !ok {
		return nil
	}
	if err := client.DeleteStream(ctx, name); err != nil {
		return fmt.Errorf("wings: discard %s: %w", name, err)
	}
	return nil
}

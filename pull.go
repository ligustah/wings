package wings

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/ligustah/commitlog/compress"
	streams "github.com/ligustah/durable_streams"
	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"
)

// The coordinator copies what an attempt commits (its history, recordings, byte
// streams) transaction by transaction, applied whole and in commit order, so a
// retry is never handed a history ahead of the recording it names. The engine
// does this: a worker's broker publishes finished transactions, the coordinator
// pulls them per worker under a durable cursor, exactly once. A shared channel's
// outbox is the exception — written outside a transaction, it comes by the
// stream mirror (output.go).

// pullSource keys a worker's subscription cursor; the worker id is stable across
// a coordinator restart.
func pullSource(workerID string) string { return "wings.worker." + workerID }

// pull keeps the coordinator's copy of what one worker's attempts commit, until
// the worker is gone. In-process workers share the engine and have nothing to copy.
//
// dest.Pull blocks until it errs or the context ends; a broker that wedges on a
// transaction it can neither finish nor abandon leaves it blocked without ever
// returning to be retried. So it runs on its own goroutine and this one watches
// the coordinator's progress against the worker's committed offsets: a pull that
// stays behind while applying nothing is reconnected, and if that does not clear
// it, the worker is treated as lost so its jobs move rather than the run hanging.
func (c *Cluster) pull(w *workerConn) {
	if w.remote == nil || c.engine == nil {
		return
	}
	dest := c.engine.Coordinator()
	t := time.NewTicker(pullWatchInterval)
	defer t.Stop()

	var watch pullWatch
	for w.ctx.Err() == nil && !w.dead.Load() {
		pullCtx, cancel := context.WithCancel(w.ctx)
		done := make(chan error, 1)
		go func() {
			done <- dest.Pull(pullCtx, streams.PullOptions{
				Source:   pullSource(w.id),
				Producer: "wings.pull." + w.id,
				Open: func(ctx context.Context, from int64) (streams.TransactionSource, error) {
					// Resolved each open: the connection is the cluster's to replace.
					broker := w.remote.Conn()
					if broker == nil {
						var err error
						if broker, err = w.remote.Cluster().Any(); err != nil {
							return nil, err
						}
					}
					return broker.SubscribeTransactions(ctx, "wings.coordinator", from)
				},
				// One incarnation per writer: a moved job is a new attempt under a
				// new id, so no writer is superseded under the same name.
				Epoch:   func(string) (uint16, error) { return 1, nil },
				Only:    c.pullWanted,
				Stream:  c.pulledStream,
				Options: c.pulledOptions,
			})
		}()

		var (
			err         error
			interrupted bool
		)
	watching:
		for {
			select {
			case err = <-done:
				cancel()
				break watching
			case <-w.ctx.Done():
				cancel()
				<-done
				return
			case now := <-t.C:
				ours, behind, perr := c.pullStatus(w.ctx, w, "")
				if perr != nil {
					continue // a reading we could not take is not a stall
				}
				reconnect, lost := watch.observe(behind, ours, now)
				if lost {
					c.log.Error("wings: coordinator's pull of a worker is wedged; treating the worker as lost",
						"worker", w.id, "stalled_for", pullFaultGrace)
					c.logStuckPull(w)
					cancel()
					<-done
					c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: "pull wedged"})
					w.dead.Store(true)
					c.redispatchFrom(w)
					return
				}
				if reconnect {
					c.log.Warn("wings: coordinator's pull of a worker has made no progress; reconnecting",
						"worker", w.id, "stalled_for", pullInterruptGrace)
					c.logStuckPull(w)
					interrupted = true
					cancel()
					err = <-done
					break watching
				}
			}
		}

		if w.ctx.Err() != nil {
			return
		}
		if interrupted {
			continue // reopen at once; the stall clock keeps running across it
		}
		if errors.Is(err, context.Canceled) {
			return
		}
		if errors.Is(err, streams.ErrIncompleteTransaction) {
			// Fatal by design: a transaction that cannot be copied whole would
			// leave an undetectable hole. Said once, not every second.
			c.log.Error("wings: cannot keep a worker's transactions; a transaction cannot be copied whole",
				"worker", w.id, "err", err)
			return
		}
		c.log.Debug("wings: stopped keeping a worker's transactions; resuming", "worker", w.id, "err", err)
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// pullWatch tracks whether the coordinator's pull of a worker is making
// progress. It escalates only while the coordinator is behind the worker's own
// committed offsets and the total it has applied is not climbing: a pull that is
// merely slow still advances, resetting the clock, so paging never trips it.
type pullWatch struct {
	applied     int64     // the total committed offset last seen on the coordinator
	stalledFrom time.Time // when the current behind-and-frozen stretch began; zero if none
	reconnected bool      // a reconnect has already been asked for this stretch
}

// observe folds one progress reading into the watch and says what to do:
// reconnect the pull once it has been frozen while behind for pullInterruptGrace,
// then declare the worker lost if it stays frozen through pullFaultGrace.
func (pw *pullWatch) observe(behind bool, applied int64, now time.Time) (reconnect, lost bool) {
	if !behind || applied > pw.applied {
		pw.applied = applied
		pw.stalledFrom = time.Time{}
		pw.reconnected = false
		return false, false
	}
	if pw.stalledFrom.IsZero() {
		pw.stalledFrom = now
		return false, false
	}
	stalled := now.Sub(pw.stalledFrom)
	switch {
	case stalled >= pullFaultGrace:
		return false, true
	case stalled >= pullInterruptGrace && !pw.reconnected:
		pw.reconnected = true
		return true, false
	}
	return false, false
}

// pullWanted declines only an attempt the job has already moved past — those
// streams are discarded on the worker, so their transactions arrive with records
// gone, which the engine cannot copy whole. The current attempt, and anything
// not yet known or already settled, is wanted, since a job's last transaction is
// pulled after it finishes.
func (c *Cluster) pullWanted(workload string) bool {
	rest, ok := strings.CutPrefix(workload, attemptWorkloadPrefix)
	if !ok {
		return true
	}
	// The id is streamPart(job).streamPart(thread).attempt (a thread per producer);
	// both parts are dot-free, so the job is the first segment and the attempt the
	// last. A legacy two-part id (job.attempt) still parses: the job is first, the
	// attempt last.
	parts := strings.Split(rest, ".")
	if len(parts) < 2 {
		return true
	}
	attempt, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		return true
	}
	part := parts[0]
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, p := range c.pending {
		if streamPart(id) == part {
			return attempt >= p.job.Attempt
		}
	}
	return true
}

// pulledStream says where a pulled record goes on the coordinator: the same name
// for a job's output, a shared-channel outbox among it; nowhere for plumbing, a
// prior (put there by the coordinator), or a stream the job has finished with. An
// outbox is pulled like any other output now that a send writes it inside the
// attempt's transaction, so the record and the history that justified it come
// home together.
func (c *Cluster) pulledStream(sourceLog string) (string, bool) {
	name, _, ok := streams.SplitPartitionLogName(sourceLog)
	if !ok {
		name = sourceLog
	}
	// A thread's log is pulled home under its own name, but is not a parseOutput
	// stream, so a settled job's history drop leaves it (see dropOutputsOf) and the
	// lines outlive the history.
	if strings.HasPrefix(name, logPrefix) {
		if c.wasDropped(name) {
			return "", false
		}
		return name, true
	}
	o, ok := parseOutput(name)
	if !ok {
		return "", false
	}
	switch {
	case o.Prefix == priorPrefix:
		return "", false
	case c.wasDropped(name):
		return "", false
	}
	return name, true
}

// pulledOptions opens every pulled destination with the cluster's storage
// settings, so the coordinator's copy — an attempt's history among the largest
// streams — is stored like every stream wings creates itself: the v3 block
// layout, and the cluster's codec when compression is on. The puller is the sole
// creator of these copies and every wings creator uses the same settings, so
// stating them here settles the stream's storage with nothing to disagree over.
func (c *Cluster) pulledOptions(string) []streams.StreamOption {
	opts := []streams.StreamOption{streams.WithBlockFormat(streamBlockFormat)}
	if streamCompression != dswire.CompressionNone {
		opts = append(opts, streams.WithCompression(compress.Codec(streamCompression)))
	}
	return opts
}

// pulledLevel reports whether the coordinator's copies of a job's outputs (or
// every job's, when job is empty) are as complete as the worker's own.
func (c *Cluster) pulledLevel(ctx context.Context, w *workerConn, job string) (bool, error) {
	_, behind, err := c.pullStatus(ctx, w, job)
	if err != nil {
		return false, err
	}
	return !behind, nil
}

// pullStatus reports the coordinator's copy of a job's outputs (every job's,
// when job is empty) against the worker's own: behind is true when any copy is
// short of the worker, and applied is the total committed offset the coordinator
// holds — a figure that only climbs as the pull applies more, so a frozen one
// while behind is a wedged pull rather than a slow one.
func (c *Cluster) pullStatus(ctx context.Context, w *workerConn, job string) (applied int64, behind bool, err error) {
	client, err := c.sharedClient()
	if err != nil {
		return 0, false, err
	}
	names, err := w.client.ListStreams(ctx)
	if err != nil {
		return 0, false, err
	}
	for _, name := range names {
		o, ok := parseOutput(name)
		if !ok || o.Prefix == priorPrefix || (job != "" && o.Job != streamPart(job)) {
			continue
		}
		theirs, err := committedThrough(ctx, w.client, name)
		if err != nil {
			return 0, false, err
		}
		if theirs < 0 {
			continue
		}
		ours, err := committedThrough(ctx, client, name)
		if err != nil {
			return 0, false, err
		}
		if ours > 0 {
			applied += ours
		}
		if ours < theirs {
			behind = true
		}
	}
	return applied, behind, nil
}

// logStuckPull records which of a worker's output streams the coordinator is
// behind on, for a pull the watch judged stuck: a coordinator offset of -1 while
// the worker holds records is a stream wings deleted out from under an in-flight
// pull transaction, the join key against the broker's undecidable-participant
// warning.
func (c *Cluster) logStuckPull(w *workerConn) {
	client, err := c.sharedClient()
	if err != nil {
		return
	}
	names, err := w.client.ListStreams(w.ctx)
	if err != nil {
		return
	}
	for _, name := range names {
		if o, ok := parseOutput(name); !ok || o.Prefix == priorPrefix {
			continue
		}
		theirs, terr := committedThrough(w.ctx, w.client, name)
		ours, oerr := committedThrough(w.ctx, client, name)
		if terr != nil || oerr != nil || ours >= theirs {
			continue
		}
		c.log.Warn("wings: coordinator is behind a worker on a stream",
			"worker", w.id, "stream", name, "worker_committed", theirs,
			"coordinator_committed", ours, "wings_dropped", c.wasDropped(name))
	}
}

// committedThrough is the offset of a stream's last committed record, or -1 when
// it is empty or absent.
func committedThrough(ctx context.Context, client *dsclient.Client, name string) (int64, error) {
	ok, err := client.StreamExists(ctx, name)
	if err != nil || !ok {
		return -1, err
	}
	st, err := client.OpenStream[[]byte](name, dsclient.WithCodec[[]byte](dswire.RawCodec{}))
	if err != nil {
		return -1, err
	}
	info, err := st.Info(ctx)
	if err != nil {
		return -1, err
	}
	return info.NewestCommitted, nil
}

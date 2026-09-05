package wings

import (
	"context"
	"errors"
	"time"

	streams "github.com/ligustah/durable_streams"
	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"
)

// Everything an attempt writes on its worker — its history, its recordings,
// its files — goes into one transaction, committed at the points that mean
// something (see attempt.go). The coordinator's copy of it is made the same
// way: one transaction at the source is one transaction at the destination,
// applied whole or not at all, in the order they were committed. So what a
// retry is handed is never a history a step ahead of the recording it names,
// and never a file whose last chunk landed before the event that says it
// was written.
//
// The engine does this. A worker's broker publishes its finished
// transactions, with their records, off its transaction log; the
// coordinator's engine PULLS them, subscribed per worker, and applies each
// under its own producer with a durable cursor and a per-writer mark, so a
// coordinator that restarts resumes where it left off and a transaction
// delivered twice is applied once. The worker keeps a transaction until the
// coordinator says it is durable here, and no longer.
//
// A worker's outbox for a shared channel is the one thing an attempt writes
// OUTSIDE a transaction — it has to be seen at once, not at the next commit
// point — and it comes home by the stream mirror in output.go, which is what
// carried everything before the engine could carry transactions.

// pullSource names one worker's subscription at the coordinator. It keys the
// cursor, so it is the worker's id: stable across a coordinator restart that
// finds the worker again, and never two workers' at once.
func pullSource(workerID string) string { return "wings.worker." + workerID }

// pull keeps the coordinator's copy of what one worker's attempts commit,
// until the worker is gone. One loop per worker with a broker of its own; an
// in-process worker shares the coordinator's engine and has nothing to copy.
func (c *Cluster) pull(w *workerConn) {
	if w.remote == nil || c.engine == nil {
		return
	}
	dest := c.engine.Coordinator()
	for w.ctx.Err() == nil {
		err := dest.Pull(w.ctx, streams.PullOptions{
			Source:   pullSource(w.id),
			Producer: "wings.pull." + w.id,
			Open: func(ctx context.Context, from int64) (streams.TransactionSource, error) {
				// Resolved on every open rather than once: the connection
				// behind a remote client is the cluster's to replace.
				broker := w.remote.Conn()
				if broker == nil {
					var err error
					if broker, err = w.remote.Cluster().Any(); err != nil {
						return nil, err
					}
				}
				return broker.SubscribeTransactions(ctx, "wings.coordinator", from)
			},
			// One incarnation per writer: an attempt's transactional id is
			// its own, and a job moved is a new attempt under a new id, so
			// no writer is ever superseded by another under the same name.
			Epoch:  func(string) (uint16, error) { return 1, nil },
			Stream: c.pulledStream,
		})
		if w.ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return
		}
		if errors.Is(err, streams.ErrIncompleteTransaction) {
			// Fatal by the engine's design: a transaction it cannot copy
			// whole is one it will not copy at all, and skipping it would
			// leave a hole nothing downstream can detect. Said loudly, once,
			// rather than every second.
			c.log.Error("wings: cannot keep a worker's transactions; a transaction cannot be copied whole",
				"worker", w.id, "err", err)
			return
		}
		// A connection dropped, a broker restarting: the cursor is durable,
		// so resuming costs nothing but the look.
		c.log.Debug("wings: stopped keeping a worker's transactions; resuming", "worker", w.id, "err", err)
		select {
		case <-w.ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

// pulledStream says where a record of a worker's transaction goes here: the
// same name, for what a job wrote; nowhere, for what is not the job's output
// or is a copy of the coordinator's own.
func (c *Cluster) pulledStream(sourceLog string) (string, bool) {
	name, _, ok := streams.SplitPartitionLogName(sourceLog)
	if !ok {
		name = sourceLog
	}
	o, ok := parseOutput(name)
	if !ok {
		// The worker's own plumbing, or a channel pushed onto it: nothing
		// the coordinator wants a copy of.
		return "", false
	}
	switch {
	case o.Prefix == priorPrefix:
		// Put there BY the coordinator, for a retry. Forwarding it back
		// would be a copy of a copy.
		return "", false
	case o.Prefix == chanoutPrefix:
		// Not written in a transaction, so never here; declined all the
		// same, so that if it ever were it would still come the one way.
		return "", false
	case c.wasDropped(name):
		// A stream this job has finished with: a late transaction of an
		// abandoned attempt, arriving after the job settled.
		return "", false
	}
	return name, true
}

// pulledLevel reports whether the coordinator's copies of a job's outputs on
// a worker are as complete as the worker's own — every stream of every
// attempt of the job, or of every job when job is empty. What the worker has
// committed and the coordinator has not yet applied is the gap.
func (c *Cluster) pulledLevel(ctx context.Context, w *workerConn, job string) (bool, error) {
	client, err := c.sharedClient()
	if err != nil {
		return false, err
	}
	names, err := w.client.ListStreams(ctx)
	if err != nil {
		return false, err
	}
	for _, name := range names {
		o, ok := parseOutput(name)
		if !ok || o.Prefix == priorPrefix || o.Prefix == chanoutPrefix || (job != "" && o.Job != streamPart(job)) {
			continue
		}
		theirs, err := committedThrough(ctx, w.client, name)
		if err != nil {
			return false, err
		}
		if theirs < 0 {
			continue
		}
		ours, err := committedThrough(ctx, client, name)
		if err != nil {
			return false, err
		}
		if ours < theirs {
			return false, nil
		}
	}
	return true, nil
}

// committedThrough is the offset of the last committed record of a stream,
// or -1 when the stream is empty or not there.
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

package wings

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

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
			Epoch:  func(string) (uint16, error) { return 1, nil },
			Only:   c.pullWanted,
			Stream: c.pulledStream,
		})
		if w.ctx.Err() != nil || errors.Is(err, context.Canceled) {
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
	i := strings.LastIndexByte(rest, '.')
	if i < 0 {
		return true
	}
	attempt, err := strconv.Atoi(rest[i+1:])
	if err != nil {
		return true
	}
	part := rest[:i]
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

// pulledLevel reports whether the coordinator's copies of a job's outputs (or
// every job's, when job is empty) are as complete as the worker's own.
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
		if !ok || o.Prefix == priorPrefix || (job != "" && o.Job != streamPart(job)) {
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

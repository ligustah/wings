package wings

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

const (
	// lineageReachWait bounds waiting for an ancestor's history to reach the fork
	// of the next thread on a lineage; a read of the coordinator's own engine can
	// come back short of durable records while a kill's churn settles.
	lineageReachWait = 5 * time.Second
	lineageReachPoll = 50 * time.Millisecond
)

// rootKnown reports whether a root is one a worker could start from.
func rootKnown(r flow.Root) bool { return r.Workflow != "" || r.Function != "" }

// unknownRoot reports whether an error is a worker saying it does not hold the
// code a lineage's root names. Matched on content: the value does not survive
// the trip.
func unknownRoot(err error) bool {
	return err != nil && strings.Contains(err.Error(), flow.ErrUnknownRoot.Error())
}

// lineageOfJob is the root and lineage of the thread a job runs, from which
// a thread of run code it forks is reached by one more id.
func lineageOfJob(job jobEnvelope) (flow.Root, []string) {
	if len(job.Lineage) > 0 {
		return job.Root, job.Lineage
	}
	return flow.Root{Function: job.Func, Input: job.Payload}, []string{threadOf(job)}
}

// hydrateLineage puts a lineage job's ancestors' histories on the worker about
// to run it, in the attempt's history stream, unless that stream is already
// there.
func (c *Cluster) hydrateLineage(ctx context.Context, w *workerConn, job jobEnvelope) error {
	if len(job.Lineage) < 2 {
		return nil
	}
	dest := historyName(job.ID, job.Attempt)
	ok, err := w.client.StreamExists(ctx, dest)
	if err != nil {
		return fmt.Errorf("wings: look for the history of job %s on worker %s: %w", job.ID, w.id, err)
	}
	if ok {
		return nil
	}
	var events []*protos.Event
	run := runOf(job)
	for i, id := range job.Lineage[:len(job.Lineage)-1] {
		evs, err := c.historyReaching(ctx, run, id, job.Lineage[i+1])
		if err != nil {
			return err
		}
		events = append(events, evs...)
	}
	if err := ensureStream(ctx, w.client, dest); err != nil {
		return err
	}
	st, err := eventStream[*protos.Event](w.client, dest)
	if err != nil {
		return err
	}
	// In a transaction: the coordinator builds the attempt's history from the
	// worker's transactions (pull.go), so records written outside one are lost.
	producer, err := w.client.Producer(ctx, "wings.lineage."+dest)
	if err != nil {
		return fmt.Errorf("wings: put the ancestors of job %s on worker %s: %w", job.ID, w.id, err)
	}
	tx, err := producer.Begin(ctx)
	if err != nil {
		return fmt.Errorf("wings: put the ancestors of job %s on worker %s: %w", job.ID, w.id, err)
	}
	if _, err := dsclient.Output(tx, st).Append(ctx, events); err != nil {
		_ = tx.Abort(ctx)
		return fmt.Errorf("wings: put the ancestors of job %s on worker %s: %w", job.ID, w.id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("wings: put the ancestors of job %s on worker %s: %w", job.ID, w.id, err)
	}
	return nil
}

// historyReaching reads a thread's history, re-reading until it records the fork
// of child or lineageReachWait runs out — then ships the best it has, so the
// replay reports which fork it could not reach.
func (c *Cluster) historyReaching(ctx context.Context, run, id, child string) ([]*protos.Event, error) {
	deadline := time.Now().Add(lineageReachWait)
	for {
		evs, err := c.threadHistory(ctx, run, id)
		if err != nil {
			return nil, err
		}
		if forkReaches(evs, child) {
			return evs, nil
		}
		if !time.Now().Before(deadline) {
			if len(evs) == 0 {
				return nil, fmt.Errorf("wings: thread %s of run %s has no history to replay", id, run)
			}
			return evs, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(lineageReachPoll):
		}
	}
}

// forkReaches reports whether a history records a fork whose child is the given
// thread id.
func forkReaches(events []*protos.Event, child string) bool {
	for _, e := range events {
		if f := e.GetFork(); f != nil && f.GetThreadId() == child {
			return true
		}
	}
	return false
}

// threadHistory is the coordinator's copy of one thread's history: the last
// history of the job the thread is, or the run's own store when it is not a job.
func (c *Cluster) threadHistory(ctx context.Context, run, thread string) ([]*protos.Event, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	var jobID string
	c.mu.Lock()
	if thread == "main" && isJobRun(run) {
		jobID = jobOfRun(run)
	} else {
		key := flow.Origin{Run: run, Thread: thread}.Key()
		if p := c.byOrigin[key]; p != nil {
			jobID = p.job.ID
		} else {
			// No live job: the one it last ran as still finds its history. See ranAs.
			jobID = c.ranAs[key]
		}
	}
	c.mu.Unlock()
	if jobID == "" {
		// A thread the coordinator ran itself keeps its history in the store.
		return flow.NewStore(client).Events(ctx, run, thread)
	}
	name, err := c.lastHistory(ctx, jobID, -1)
	if err != nil {
		return nil, err
	}
	if name == "" {
		// The job's history was dropped once its thread was joined; fall back to
		// the store.
		return flow.NewStore(client).Events(ctx, run, thread)
	}
	st, err := eventStream[*protos.Event](client, name)
	if err != nil {
		return nil, err
	}
	var events []*protos.Event
	var from int64
	for {
		recs, err := st.Read(ctx, from, recordBatch)
		if err != nil {
			return nil, fmt.Errorf("wings: read history %s: %w", name, err)
		}
		if len(recs) == 0 {
			return events, nil
		}
		for _, r := range recs {
			if r.Record.GetThreadId() == thread {
				events = append(events, r.Record)
			}
			from = r.Offset + 1
		}
	}
}

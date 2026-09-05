package wings

import (
	"context"
	"fmt"
	"strings"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// A thread of run code — one a workflow or a work function forks with
// [flow.Context.Spawn] — is a closure, and a closure cannot be sent to a
// worker. What can be sent is the way to it: every worker holds the run's
// code, and the closure is what the code arrives at after replaying the
// thread's ancestors to the fork that made it. So the coordinator sends such
// a thread as a job with no function and a LINEAGE — the path of thread ids
// from a thread the worker can start by name down to the thread itself —
// and puts the ancestors' histories on the worker first, in the stream the
// attempt's own history goes on. The worker runs it with [flow.RunLineage],
// which replays each ancestor from that stream to the fork of the next and
// runs the last for real. See flow/lineage.go for what a replay is and is
// not.
//
// The root is what the run was started as: the workflow, by name, for a
// thread of the workflow; the function on its input for a thread a work
// function forked, since a work function is itself a thread of run code
// that a worker starts by name. A run started with a bare body under
// [Cluster.Run] has no root a worker could start from, and its threads of
// run code stay on the coordinator.
//
// An ancestor's history is wherever that ancestor is: the coordinator's own
// store for the workflow's threads, and the coordinator's copy of the job's
// last history for a thread that is a job — which has the fork in it, since
// the fork is what the coordinator read to dispatch this thread at all.

// rootKnown reports whether a root is one a worker could start from.
func rootKnown(r flow.Root) bool { return r.Workflow != "" || r.Function != "" }

// unknownRoot reports whether an error is a worker saying it does not hold
// the code a lineage's root names. Matched on content: the value does not
// survive the trip.
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

// hydrateLineage puts the histories of a lineage job's ancestors onto the
// worker about to run it, in the attempt's history stream, unless that
// stream is already there: a retry has its predecessor's history copied
// under its name by hydrateHistory, and the ancestors came with that.
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
	for _, id := range job.Lineage[:len(job.Lineage)-1] {
		evs, err := c.threadHistory(ctx, runOf(job), id)
		if err != nil {
			return err
		}
		if len(evs) == 0 {
			return fmt.Errorf("wings: thread %s of run %s has no history to replay", id, runOf(job))
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
	if _, err := st.Append(ctx, events); err != nil {
		return fmt.Errorf("wings: put the ancestors of job %s on worker %s: %w", job.ID, w.id, err)
	}
	return nil
}

// threadHistory is the coordinator's copy of one thread's history: the last
// history copied home of the job the thread is, or the run's own on the
// coordinator when the thread is not a job.
func (c *Cluster) threadHistory(ctx context.Context, run, thread string) ([]*protos.Event, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	var p *pendingJob
	if thread == "main" && isJobRun(run) {
		p = c.pending[jobOfRun(run)]
	} else {
		p = c.byOrigin[flow.Origin{Run: run, Thread: thread}.Key()]
	}
	c.mu.Unlock()
	if p == nil {
		return flow.NewStore(client).Events(ctx, run, thread)
	}
	name, err := c.lastHistory(ctx, p.job.ID, -1)
	if err != nil || name == "" {
		return nil, err
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

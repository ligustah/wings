package wings

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ligustah/wings/flow"
)

// recoveredJob is what the journal says about one job left outstanding. Its
// input is not recorded there; the workflow's replay brings it at the fork,
// which rejoins the job by origin. Until then the job is incomplete: waitable,
// but not dispatchable.
type recoveredJob struct {
	job    jobEnvelope
	origin flow.Origin
	worker string         // where its current attempt was sent, or empty
	ran    map[int]string // where each attempt was sent
	yield  *yieldEnvelope
	held   bool
}

// recoverJobs rebuilds the jobs previous coordinators left outstanding from the
// journal and attaches them to the workers still running them. Call before any
// worker is adopted, so an arriving result finds its job.
func (c *Cluster) recoverJobs(ctx context.Context, workers []*workerConn) error {
	left, err := c.outstandingInJournal(ctx)
	if err != nil {
		return err
	}
	if len(left) == 0 {
		return nil
	}
	settled, err := c.mirroredResults(ctx)
	if err != nil {
		return err
	}
	byID := map[string]*workerConn{}
	for _, w := range workers {
		byID[w.id] = w
	}

	now := time.Now()
	var entries []journalEntry
	c.mu.Lock()
	for _, r := range left {
		p := &pendingJob{
			job:        r.job,
			done:       make(chan struct{}),
			bounds:     boundsOf(r.job.Func),
			origin:     r.origin,
			placed:     !r.held || r.worker != "",
			yield:      r.yield,
			ran:        map[int]*workerConn{},
			recovered:  true,
			incomplete: true,
		}
		for attempt, id := range r.ran {
			if w := byID[id]; w != nil {
				p.ran[attempt] = w
			}
		}
		where := "held: nothing to run it on"
		switch {
		case r.yield != nil:
			where = r.yield.describe()
		case r.worker != "":
			if w := byID[r.worker]; w != nil {
				p.worker = w
				p.since, p.started, p.beat = now, now, now
				c.charge(w)
				where = "still on worker " + w.id
			} else {
				where = "held: worker " + r.worker + " is gone"
			}
		}
		if res, ok := settled[p.job.ID]; ok {
			// Finished before the predecessor died; its result is on the mirror.
			if p.worker != nil {
				c.release(p.worker)
				p.worker = nil
			}
			p.yield = nil
			p.settle(res)
			where = "finished; its result is kept for the replay"
		}
		c.pending[p.job.ID] = p
		if key := p.origin.Key(); key != "" {
			c.byOrigin[key] = p
		}
		entries = append(entries, journalEntry{
			Kind: journalRecovered, Job: p.job.ID, Func: p.job.Func,
			Worker: workerID(p.worker), Attempt: p.job.Attempt, Err: where,
		}.from(p.origin))
	}
	// Nested depends on the parent being recovered, so it is set once all are.
	for _, p := range c.pending {
		if p.recovered {
			p.job.Nested = c.parentJobLocked(p.origin) != nil
			if p.worker != nil {
				c.followHistory(p, p.job.Attempt)
			}
		}
	}
	c.mu.Unlock()

	for _, e := range entries {
		c.journal.record(e)
	}
	c.log.Info("wings: recovered the jobs a previous coordinator left outstanding", "jobs", len(left))
	return nil
}

// outstandingInJournal reads the journal for jobs previous coordinators
// dispatched and never saw settle, in the order first seen.
func (c *Cluster) outstandingInJournal(ctx context.Context) ([]*recoveredJob, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	s, err := client.OpenStream[journalEntry](journalStream)
	if err != nil {
		return nil, fmt.Errorf("wings: open %s: %w", journalStream, err)
	}
	jobs := map[string]*recoveredJob{}
	var order []string
	for from := int64(0); ; {
		recs, err := s.Read(ctx, from, 512)
		if err != nil {
			return nil, fmt.Errorf("wings: read %s at %d: %w", journalStream, from, err)
		}
		if len(recs) == 0 {
			break
		}
		for _, rec := range recs {
			from = rec.Offset + 1
			e := rec.Record
			if e.Job == "" || e.Epoch == c.epoch {
				continue
			}
			r := jobs[e.Job]
			if r == nil {
				r = &recoveredJob{ran: map[int]string{}}
				r.job.ID = e.Job
				r.origin = flow.Origin{Run: e.Run, Thread: e.Thread, Step: e.Step}
				r.job.Run, r.job.Thread = e.Run, e.Thread
				jobs[e.Job] = r
				order = append(order, e.Job)
			}
			if e.Func != "" {
				r.job.Func = e.Func
			}
			switch e.Kind {
			case journalSubmitted, journalRedispatch, journalRecovered:
				r.job.Attempt = e.Attempt
				r.worker, r.held, r.yield = e.Worker, e.Worker == "", nil
				if e.Worker != "" {
					r.ran[e.Attempt] = e.Worker
				}
			case journalHeld:
				r.job.Attempt = e.Attempt
				r.worker, r.held, r.yield = "", true, nil
			case journalYielded:
				r.job.Attempt = e.Attempt
				r.worker, r.held, r.yield = "", false, e.Yield
			case journalCompleted, journalFailed:
				delete(jobs, e.Job)
			}
		}
	}
	var out []*recoveredJob
	for _, id := range order {
		// A job has a function, or a thread (run-code reached by lineage); an
		// entry with neither is not one the journal can describe.
		if r, ok := jobs[id]; ok && (r.job.Func != "" || r.job.Thread != "") {
			out = append(out, r)
		}
	}
	return out, nil
}

// mirroredResults reads every result previous coordinators mirrored, by job. A
// mirrored result means the job is over.
func (c *Cluster) mirroredResults(ctx context.Context) (map[string]resultEnvelope, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	names, err := client.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("wings: list streams: %w", err)
	}
	out := map[string]resultEnvelope{}
	for _, name := range names {
		if !strings.HasPrefix(name, mirrorPrefix) {
			continue
		}
		s, err := client.OpenStream[mirroredResult](name)
		if err != nil {
			return nil, fmt.Errorf("wings: open %s: %w", name, err)
		}
		for from := int64(0); ; {
			recs, err := s.Read(ctx, from, 512)
			if err != nil {
				return nil, fmt.Errorf("wings: read %s at %d: %w", name, from, err)
			}
			if len(recs) == 0 {
				break
			}
			for _, rec := range recs {
				from = rec.Offset + 1
				if rec.Record.Result.Yield == nil {
					out[rec.Record.Result.ID] = rec.Record.Result
				}
			}
		}
	}
	return out, nil
}

// complete gives a recovered job the envelope the replay forked it with, and
// reports whether it now needs placing. Call with mu held.
func (p *pendingJob) complete(job jobEnvelope) (place bool) {
	if !p.incomplete {
		return false
	}
	p.job.Func, p.job.Payload, p.job.Nested = job.Func, job.Payload, job.Nested
	p.job.Root, p.job.Lineage = job.Root, job.Lineage
	p.incomplete = false
	return p.worker == nil && p.yield == nil && !p.finished()
}

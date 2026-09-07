package wings

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ligustah/wings/flow"
)

// A thread parked longer than unloadAfter is unloaded: its attempt ends, its
// history commits, and the coordinator holds the job off every worker until what
// it waited for happens, then dispatches it afresh to replay to where it
// stopped. A long sleep is unloaded at once.

// unloadAfter is how long a thread may be parked before its attempt is unloaded.
// A variable for tests.
var unloadAfter = time.Minute

// unloadable says which waits are worth unloading: those the coordinator can see
// the end of. A short sleep ends on its own and is left alone.
func unloadable(w flow.Wait) bool {
	switch w.On {
	case flow.WaitJoin, flow.WaitRecv, flow.WaitSend:
		return true
	}
	return false
}

// unloadError is the cause an unloaded attempt is cancelled with, so the result
// names the wait rather than "context canceled".
type unloadError struct{ wait flow.Wait }

func (e *unloadError) Error() string {
	return fmt.Sprintf("wings: unloaded while waiting on %s", e.wait.On)
}

// unload ends a job's attempt because a thread of it has waited too long; the
// attempt commits what it has and reports a yield.
func (n *workerNode) unload(job jobEnvelope, w flow.Wait) {
	key := attemptKey(job.ID, job.Attempt)
	n.runMu.Lock()
	cancel, ok := n.running[key]
	n.runMu.Unlock()
	if !ok {
		return
	}
	n.log.Info("wings: unloading a job that has been waiting", "job", job.ID, "attempt", job.Attempt,
		"thread", w.Thread, "on", w.On)
	cancel(&unloadError{wait: w})
}

// yieldOf is the yield an attempt's error amounts to — a suspension (when to run
// again) or an unload (what it waited for) — or nil for an error that is an
// answer.
func yieldOf(ctx context.Context, err error) *yieldEnvelope {
	if ok, until := flow.IsSuspended(err); ok {
		return &yieldEnvelope{Until: until}
	}
	if u, ok := errors.AsType[*unloadError](context.Cause(ctx)); ok && errors.Is(err, context.Canceled) {
		return &yieldEnvelope{Wait: u.wait.On, Channel: u.wait.Channel, Seq: u.wait.Seq}
	}
	return nil
}

func (y *yieldEnvelope) describe() string {
	switch {
	case !y.Until.IsZero():
		return "sleeping until " + y.Until.UTC().Format(time.RFC3339)
	case y.Channel != "":
		return fmt.Sprintf("waiting on %s from channel %s", y.Wait, y.Channel)
	default:
		return "waiting on " + y.Wait
	}
}

// yieldLocked takes a job off its worker at the job's own request; its outputs
// stay for the next worker. Call with mu held.
func (c *Cluster) yieldLocked(p *pendingJob, y *yieldEnvelope) {
	c.unblockLocked(p)
	c.release(p.worker)
	p.worker = nil
	p.since, p.started, p.beat = time.Time{}, time.Time{}, time.Time{}
	p.yield = y
	c.log.Info("wings: a job yielded its worker", "job", p.job.ID, "fn", p.job.Func, "why", y.describe())
}

// wake dispatches a yielded job again because what it waited for happened; this
// does not count against the job's attempts.
func (c *Cluster) wake(p *pendingJob, why string) {
	c.mu.Lock()
	if cur, still := c.pending[p.job.ID]; !still || cur != p || p.yield == nil {
		c.mu.Unlock()
		return
	}
	p.yield = nil
	c.mu.Unlock()
	c.log.Info("wings: waking a job", "job", p.job.ID, "fn", p.job.Func, "why", why)
	c.move(p, why, false)
}

// yieldSettled reports whether a channel wait was already satisfied when the
// yield arrived — the grant or close landed while the attempt was unloading, so
// woke nobody. Such a job must be woken at once.
func (c *Cluster) yieldSettled(p *pendingJob, y *yieldEnvelope) bool {
	if y.Channel == "" || c.relay == nil {
		return false
	}
	c.relay.mu.Lock()
	rc := c.relay.channels[chanStreamFor(y.Channel)]
	c.relay.mu.Unlock()
	if rc == nil {
		return false
	}
	thread := runOf(p.job) + "/" + threadOf(p.job)
	return rc.arbiter.Settled(flow.ChannelItem{Want: y.Wait == flow.WaitRecv, From: thread, Seq: y.Seq})
}

// wakeOnChannel wakes the yielded jobs a channel record is for: a receive by the
// grant naming its thread or by the close, a send by any grant (a value taken is
// room made). A want wakes nobody.
func (c *Cluster) wakeOnChannel(id string, recs []flow.ChannelItem) {
	canonical := chanStreamFor(id)
	var due []*pendingJob
	c.mu.Lock()
	for _, p := range c.pending {
		if p.yield == nil || p.yield.Channel == "" || chanStreamFor(p.yield.Channel) != canonical {
			continue
		}
		thread := runOf(p.job) + "/" + threadOf(p.job)
		for _, rec := range recs {
			granted, closed := rec.To != "", rec.Closed
			if p.yield.Wait == flow.WaitRecv && (closed || (granted && rec.To == thread)) ||
				p.yield.Wait == flow.WaitSend && granted {
				due = append(due, p)
				break
			}
		}
	}
	c.mu.Unlock()
	for _, p := range due {
		c.wake(p, "something arrived on channel "+id)
	}
}

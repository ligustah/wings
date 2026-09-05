package wings

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ligustah/wings/flow"
)

// A thread that waits gives its slot up (slots.go) and keeps its place in
// memory, which is right for a wait of seconds and wrong for one of hours: a
// worker full of threads waiting for tomorrow is a worker whose memory is
// spoken for by nothing that is happening. So a wait that lasts is UNLOADED —
// the attempt is ended where it stands, its history committed, and the job
// handed back to the coordinator with a note of what it was waiting for —
// and a sleep past flow.ShortSleep is unloaded at once, since the thread
// itself said how long it would be. The coordinator keeps the job, off every
// worker, until the condition holds: the deadline passes, the thread it was
// joining finishes, an item arrives on the channel it was receiving from. Then
// it is dispatched afresh, to whichever worker is least loaded then, with
// its history put there first, and it replays to where it stopped and finds
// what it was waiting for.
//
// The heuristic is a threshold, unloadAfter, and nothing cleverer: below it
// the thread stays where it is, above it the thread's place is worth more
// than the replay. The worker chooses; the coordinator only schedules.

// unloadAfter is how long a thread may be parked before its attempt is
// unloaded. A variable for tests.
var unloadAfter = time.Minute

// unloadable says which waits are worth unloading: those with something the
// coordinator can see the end of. A short sleep ends on its own, within
// flow.ShortSleep, and is left alone.
func unloadable(w flow.Wait) bool {
	switch w.On {
	case flow.WaitJoin, flow.WaitRecv, flow.WaitSend:
		return true
	}
	return false
}

// unloadError is the cause an attempt is cancelled with when it is unloaded,
// so the result can say so rather than "context canceled".
type unloadError struct{ wait flow.Wait }

func (e *unloadError) Error() string {
	return fmt.Sprintf("wings: unloaded while waiting on %s", e.wait.On)
}

// unload ends a job's attempt because a thread of it has been waiting for
// too long. The attempt commits what it has and reports a yield.
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

// yieldOf is the yield an attempt's error amounts to, if it does: a
// suspension, which says when to run the thread again, or an unload, which
// says what it was waiting for. Nil for an error that is an answer.
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

// --- coordinator ---

// yieldLocked takes a job off its worker at the job's own request. The
// attempt's outputs stay where they are, to be put on the next worker when
// the job is dispatched again, and dropped with the rest when it settles.
// Call with mu held.
func (c *Cluster) yieldLocked(p *pendingJob, y *yieldEnvelope) {
	c.unblockLocked(p)
	c.release(p.worker)
	p.worker = nil
	p.since, p.started, p.beat = time.Time{}, time.Time{}, time.Time{}
	p.yield = y
	c.log.Info("wings: a job yielded its worker", "job", p.job.ID, "fn", p.job.Func, "why", y.describe())
}

// wake dispatches a yielded job again, because what it was waiting for has
// happened. A move for that reason is not a suspicion and does not count
// against the job.
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

// yieldSettled reports whether a job's wait on a channel was over by the
// time the yield arrived: the grant, or the close, reached the record while
// the attempt was being unloaded, and so woke nobody. Such a job is woken at
// once, or it would wait for news that has already come.
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

// wakeOnChannel wakes the yielded jobs that what has just gone on a
// channel's record is for. A receive is woken by the grant that names its
// thread, or by the close; a send by any grant, since a value taken is room
// made — and one woken for room that another sender took replays to the
// same wait and parks again, which costs a replay and nothing else. A want
// wakes nobody: the receive's own want, come round again, is not news.
//
// Matched by canonical stream: the relay knows a channel by the name its
// outbox carried, which is the id made safe for a stream name, and the
// yield carries the id itself.
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

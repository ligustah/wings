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

// unloads says whether a thread parked on w should be unloaded from this worker.
// In p2p a channel-blocked thread stays loaded: the worker's own stream
// subscription wakes it directly, and the coordinator cannot follow a worker-led
// channel stream to drive the wake without stalling its own commits. Only the
// joins, whose end the coordinator sees regardless, are unloaded there.
func (n *workerNode) unloads(w flow.Wait) bool {
	if !unloadable(w) {
		return false
	}
	if n.p2p && (w.On == flow.WaitRecv || w.On == flow.WaitSend) {
		return false
	}
	return true
}

// p2pEvictAfter is how long a p2p channel receiver may be parked before the
// worker evicts its footprint and reloads it in place when the value arrives
// (worker.go). Zero disables eviction, so a waiter stays loaded (v0.22.1); the
// production trigger arrives with preemption. A variable for tests.
var p2pEvictAfter time.Duration

// evicts says whether the worker itself should evict a thread parked on w and
// reload it locally, rather than either staying loaded or unloading to the
// coordinator. Only p2p channel receivers, and only once armed: the worker holds
// a node-local watch and owns the wake, so the coordinator never follows the
// stream. See [workerNode.evict] and [workerNode.runJob].
func (n *workerNode) evicts(w flow.Wait) bool {
	return n.p2p && p2pEvictAfter > 0 && w.On == flow.WaitRecv
}

// unloadError is the cause an unloaded attempt is cancelled with, so the result
// names the wait rather than "context canceled".
type unloadError struct{ wait flow.Wait }

func (e *unloadError) Error() string {
	return fmt.Sprintf("wings: unloaded while waiting on %s", e.wait.On)
}

// evictError is the cause an evicted attempt is cancelled with. Unlike an unload
// it is not handed to the coordinator: the worker reloads the job in place when
// the channel it names has a value (worker.go).
type evictError struct{ wait flow.Wait }

func (e *evictError) Error() string {
	return fmt.Sprintf("wings: evicted while waiting on %s from channel %s", e.wait.On, e.wait.Channel)
}

// preemptError is the cause a preempted attempt is cancelled with. Like an
// eviction the worker reloads it in place, but there is nothing to wait for: it
// just gets back in line for a slot (worker.go).
type preemptError struct{}

func (e *preemptError) Error() string { return "wings: preempted to give another thread a turn" }

// preempt ends a running attempt so the worker can reload it after another thread
// has had the slot. Cooperative: the attempt notices the cancellation at its next
// context-aware step, commits what it has, and is reloaded from there.
func (n *workerNode) preempt(job string, attempt int) {
	key := attemptKey(job, attempt)
	n.runMu.Lock()
	cancel, ok := n.running[key]
	n.runMu.Unlock()
	if !ok {
		return
	}
	n.log.Info("wings: preempting a running thread to give another a turn", "job", job, "attempt", attempt)
	cancel(&preemptError{})
}

// preemptedOf reports whether an attempt was preempted; the worker reloads such a
// job in place once it can take a slot again.
func preemptedOf(ctx context.Context, err error) bool {
	_, ok := errors.AsType[*preemptError](context.Cause(ctx))
	return ok && errors.Is(err, context.Canceled)
}

// evict ends a job's attempt so the worker can reload it when its channel wakes;
// the attempt commits what it has, as an unload does, but the worker keeps the
// job and drives the reload itself.
func (n *workerNode) evict(job jobEnvelope, w flow.Wait) {
	key := attemptKey(job.ID, job.Attempt)
	n.runMu.Lock()
	cancel, ok := n.running[key]
	n.runMu.Unlock()
	if !ok {
		return
	}
	n.log.Info("wings: evicting a channel-waiter to reload it in place", "job", job.ID, "attempt", job.Attempt,
		"thread", w.Thread, "channel", w.Channel)
	cancel(&evictError{wait: w})
}

// evictedWaitOf reports the wait an attempt was evicted on, if it was; the worker
// reloads such a job rather than handing its yield to the coordinator.
func evictedWaitOf(ctx context.Context, err error) (flow.Wait, bool) {
	if e, ok := errors.AsType[*evictError](context.Cause(ctx)); ok && errors.Is(err, context.Canceled) {
		return e.wait, true
	}
	return flow.Wait{}, false
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
	// An eviction the worker could not reload (its context ended, or the channel
	// went away) falls back to a yield the coordinator holds, like an unload.
	if e, ok := errors.AsType[*evictError](context.Cause(ctx)); ok && errors.Is(err, context.Canceled) {
		return &yieldEnvelope{Wait: e.wait.On, Channel: e.wait.Channel, Seq: e.wait.Seq}
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
	if y.Wait == flow.WaitSend {
		p.consumeBase = c.channelConsumed(y.Channel)
	}
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
	c.move(p, why, false, false)
}

// yieldSettled reports whether a channel wait was already satisfied when the
// yield arrived — the value, consume, or close landed while the attempt was
// unloading, so woke nobody. Such a job must be woken at once.
func (c *Cluster) yieldSettled(p *pendingJob, y *yieldEnvelope) bool {
	if y.Channel == "" {
		return false
	}
	rc := c.relayChannel(y.Channel)
	if rc == nil {
		return false
	}
	return settledOn(rc, y.Wait, y.Seq, p.consumeBase)
}

// settledOn reports whether a channel wait can proceed from the relay's counts: a
// receive once a value has arrived at its sequence (a single reader takes values
// in arrival order) or the channel closed; a send once a consume has freed room
// beyond what the sender had already seen when it parked, or the channel closed.
func settledOn(rc *relayChannel, wait string, seq, consumeBase uint64) bool {
	closed, values, consumed := rc.counts()
	if closed {
		return true
	}
	switch wait {
	case flow.WaitRecv:
		return values > seq
	case flow.WaitSend:
		return consumed > consumeBase
	}
	return false
}

// channelConsumed is how many values the reader has reported consuming on a
// channel, or zero for one the relay has no record of yet. Read from a lock-free
// snapshot so a job unloading under the cluster lock need not take the relay's.
func (c *Cluster) channelConsumed(id string) uint64 {
	if c.relay == nil {
		return 0
	}
	if v, ok := c.relay.consumed.Load(chanStreamFor(id)); ok {
		return v.(uint64)
	}
	return 0
}

func (c *Cluster) relayChannel(id string) *relayChannel {
	if c.relay == nil {
		return nil
	}
	c.relay.mu.Lock()
	defer c.relay.mu.Unlock()
	return c.relay.channels[chanStreamFor(id)]
}

// wakeOnChannel wakes the yielded jobs a channel record is for: a receive when a
// value it can take has arrived or the channel closed, a send when a consume has
// freed room or the channel closed.
func (c *Cluster) wakeOnChannel(id string) {
	rc := c.relayChannel(id)
	if rc == nil {
		return
	}
	canonical := chanStreamFor(id)
	var due []*pendingJob
	c.mu.Lock()
	for _, p := range c.pending {
		if p.yield == nil || p.yield.Channel == "" || chanStreamFor(p.yield.Channel) != canonical {
			continue
		}
		if settledOn(rc, p.yield.Wait, p.yield.Seq, p.consumeBase) {
			due = append(due, p)
		}
	}
	c.mu.Unlock()
	for _, p := range due {
		c.wake(p, "something arrived on channel "+id)
	}
}

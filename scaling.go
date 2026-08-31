package wings

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// Scaling turns the worker count into something wings manages rather than
// something you choose.
//
// It is deliberately provider-independent: the decision is made from the queue
// alone, and carrying it out goes through the same per-target path that brought
// the first workers up. So the same configuration adds a goroutine, a child
// process, or a cloud VM, and a policy tuned locally means the same thing in
// production.
//
// The zero value is off, and [Config.Workers] governs instead.
type Scaling struct {
	// Min is the floor, held even when there is no work at all. Below 1 there
	// would be nothing to send the first job to, so 0 is read as 1.
	Min int
	// Max is the ceiling, and enabling autoscaling means setting it. It is a
	// spend limit as much as a capacity one: with a cloud target every worker
	// above the floor is a machine being billed.
	Max int

	// JobsPerWorker is how much backlog one worker is expected to carry.
	// Workers wanted is outstanding jobs divided by this, so 1 means a worker
	// per queued job and 10 means a worker per ten. Defaults to 1.
	JobsPerWorker int

	// IdleTimeout is how long a worker must have had nothing to do before it is
	// torn down. Defaults to 60s.
	//
	// The right value is dominated by what a replacement COSTS: a goroutine is
	// free to recreate and a cloud VM is minutes of boot plus an upload, so a
	// timeout that looks thrifty on the local target can leave a remote cluster
	// permanently rebuilding itself.
	IdleTimeout time.Duration

	// Interval is how often the policy is evaluated. Defaults to 2s.
	Interval time.Duration

	// MaxStep bounds how many workers one decision may add. Defaults to Max,
	// i.e. unbounded within the ceiling. Lower it when provisioning is slow or
	// rate-limited and a burst of queued work should not become a burst of
	// simultaneous machine creations.
	MaxStep int
}

func (s Scaling) enabled() bool { return s.Max > 0 }

func (s Scaling) validate() error {
	if !s.enabled() {
		// Nothing to check: with Max unset the whole struct is inert, and
		// rejecting a stray field would be rejecting a zero value.
		return nil
	}
	if s.Min < 0 {
		return fmt.Errorf("wings: Scaling.Min is %d; it cannot be negative", s.Min)
	}
	if s.Max < s.Min {
		return fmt.Errorf("wings: Scaling.Max (%d) is below Scaling.Min (%d)", s.Max, s.Min)
	}
	if s.JobsPerWorker < 0 {
		return fmt.Errorf("wings: Scaling.JobsPerWorker is %d; it cannot be negative", s.JobsPerWorker)
	}
	return nil
}

// withDefaults fills the blanks. Applied once at Start so the loop never has to
// ask whether a field was set.
func (s Scaling) withDefaults() Scaling {
	if !s.enabled() {
		return s
	}
	if s.Min < 1 {
		s.Min = 1
	}
	if s.Max < s.Min {
		s.Max = s.Min
	}
	if s.JobsPerWorker < 1 {
		s.JobsPerWorker = 1
	}
	if s.IdleTimeout <= 0 {
		s.IdleTimeout = 60 * time.Second
	}
	if s.Interval <= 0 {
		s.Interval = 2 * time.Second
	}
	if s.MaxStep < 1 {
		s.MaxStep = s.Max
	}
	return s
}

// initialWorkers is how many to start with: the floor when autoscaling, and
// whatever was configured otherwise.
func (s Scaling) initialWorkers(configured int) int {
	if !s.enabled() {
		return configured
	}
	return s.Min
}

// want is how many workers the given backlog calls for, clamped to the bounds.
func (s Scaling) want(outstanding int) int {
	// Round up: with JobsPerWorker=10, nine queued jobs still need a worker.
	n := (outstanding + s.JobsPerWorker - 1) / s.JobsPerWorker
	return min(max(n, s.Min), s.Max)
}

// autoscale evaluates the policy on a timer until the cluster stops.
//
// One goroutine, and every decision is carried out synchronously inside it.
// That matters most for the slowest target: provisioning can take minutes, and
// a loop that fired again while the last decision was still in flight would
// answer the same backlog by creating the same machines twice.
func (c *Cluster) autoscale() {
	defer c.wg.Done()

	s := c.cfg.Scaling
	t := time.NewTicker(s.Interval)
	defer t.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
		}
		c.scaleOnce()
	}
}

func (c *Cluster) scaleOnce() {
	s := c.cfg.Scaling

	c.reapDead()

	now := time.Now()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	outstanding := len(c.pending)
	live, idle := 0, []*workerConn(nil)
	for _, w := range c.workers {
		if !w.available() {
			continue
		}
		live++
		if w.inflight == 0 && !w.idleSince.IsZero() && now.Sub(w.idleSince) >= s.IdleTimeout {
			idle = append(idle, w)
		}
	}
	c.mu.Unlock()

	switch want := s.want(outstanding); {
	case want > live:
		c.scaleUp(min(want-live, s.MaxStep), outstanding)
	case want < live:
		c.scaleDown(live-want, idle)
	}
}

func (c *Cluster) scaleUp(n, outstanding int) {
	if n <= 0 {
		return
	}
	c.log.Info("wings: scaling up", "add", n, "outstanding", outstanding)

	workers, err := c.launch(c.ctx, n)
	if err != nil {
		// Not fatal. The cluster keeps running at its current size and the next
		// tick tries again; a quota refusal or a slow zone should cost
		// throughput, not the run.
		c.log.Error("wings: scale up failed", "add", n, "err", err)
		return
	}
	for _, w := range workers {
		c.adopt(w)
	}
}

// scaleDown retires up to n workers that have been idle long enough.
//
// Draining is marked under the same lock that assigns work, which is what makes
// this safe: a worker cannot be chosen for a job between being found idle and
// being taken out of service, so nothing is ever sent to a worker that is
// already closing.
func (c *Cluster) scaleDown(n int, idle []*workerConn) {
	if n <= 0 || len(idle) == 0 {
		return
	}

	var retire []*workerConn
	c.mu.Lock()
	for _, w := range idle {
		if len(retire) >= n {
			break
		}
		// Re-checked under the lock: this list was gathered earlier, and a job
		// may have landed on one of them since.
		if w.inflight != 0 || w.draining || w.dead.Load() {
			continue
		}
		w.draining = true
		retire = append(retire, w)
	}
	if len(retire) > 0 {
		c.workers = slices.DeleteFunc(c.workers, func(w *workerConn) bool {
			return slices.Contains(retire, w)
		})
	}
	c.mu.Unlock()

	for _, w := range retire {
		// Tell the tail goroutine this was deliberate, so the read error that
		// closing causes is not reported as a lost worker and does not trigger
		// a redispatch of jobs that do not exist.
		w.dead.Store(true)
		c.log.Info("wings: retiring idle worker", "worker", w.id)
		c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: "retired while idle"})
		if err := c.releaseWorker(context.WithoutCancel(c.ctx), w); err != nil {
			c.log.Error("wings: retiring worker", "worker", w.id, "err", err)
		}
	}
}

// reapDead releases workers that died on their own.
//
// Their tail goroutine has already redispatched what they owed; this is what
// releases the machine, so that a cloud instance whose worker crashed stops
// being billed rather than lingering until Stop.
func (c *Cluster) reapDead() {
	var reaped []*workerConn

	c.mu.Lock()
	c.workers = slices.DeleteFunc(c.workers, func(w *workerConn) bool {
		if w.dead.Load() && w.inflight == 0 {
			reaped = append(reaped, w)
			return true
		}
		return false
	})
	c.mu.Unlock()

	for _, w := range reaped {
		c.log.Info("wings: releasing dead worker", "worker", w.id)
		c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: "died"})
		if err := c.releaseWorker(context.WithoutCancel(c.ctx), w); err != nil {
			c.log.Error("wings: releasing dead worker", "worker", w.id, "err", err)
		}
	}
}

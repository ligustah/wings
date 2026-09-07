package wings

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// Scaling makes the worker count follow the queue, from the queue alone, so the
// same policy adds a goroutine, a child process, or a cloud VM. The zero value
// is a fixed fleet of [Config.Workers] — the same policy with Min and Max equal,
// which is still maintained: a dead or preempted worker is replaced.
type Scaling struct {
	// Min is the floor, held even with no work. 0 is read as 1.
	Min int
	// Max is the ceiling; setting it enables autoscaling. With a cloud target it
	// is a spend limit.
	Max int

	// JobsPerWorker is how much backlog one worker should carry: workers wanted
	// is outstanding jobs divided by this. Defaults to [Config.Concurrency], else 1.
	JobsPerWorker int

	// IdleTimeout is how long a worker must be idle before it is retired.
	// Defaults to 60s. Weigh it against what a replacement costs to boot.
	IdleTimeout time.Duration

	// Interval is how often the policy is evaluated. Defaults to 2s.
	Interval time.Duration

	// MaxStep bounds how many workers one decision may add. Defaults to Max.
	// Lower it when provisioning is slow or rate-limited.
	MaxStep int
}

func (s Scaling) enabled() bool { return s.Max > 0 }

// fixed reports whether the fleet holds one size.
func (s Scaling) fixed() bool { return s.Min == s.Max }

// fixedFleet is the policy a plain worker count means: n workers, kept at n.
func fixedFleet(n int) Scaling { return Scaling{Min: n, Max: n} }

func (s Scaling) validate() error {
	if !s.enabled() {
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

// withDefaults fills the blanks, once at Start. concurrency is
// [Config.Concurrency], the default for JobsPerWorker.
func (s Scaling) withDefaults(concurrency int) Scaling {
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
		s.JobsPerWorker = max(concurrency, 1)
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

// want is how many workers the backlog calls for, rounded up and clamped to the
// bounds.
func (s Scaling) want(outstanding int) int {
	n := (outstanding + s.JobsPerWorker - 1) / s.JobsPerWorker
	return min(max(n, s.Min), s.Max)
}

// autoscale evaluates the policy on a timer until the cluster stops. One
// goroutine, each decision carried out synchronously, so a slow provision cannot
// answer the same backlog twice.
func (c *Cluster) autoscale() {
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
		// Not fatal: the next tick tries again.
		c.log.Error("wings: scale up failed", "add", n, "err", err)
		return
	}
	for _, w := range workers {
		c.adopt(w)
	}
}

// scaleDown retires up to n idle workers. Draining is marked under the lock that
// assigns work, so nothing is sent to a worker that is already closing.
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
		// Re-checked under the lock: a job may have landed since.
		if w.inflight != 0 || w.draining || w.dead.Load() {
			continue
		}
		w.draining = true
		retire = append(retire, w)
	}
	c.mu.Unlock()

	// Still in c.workers, so their output is still being mirrored while they
	// drain; marked draining, so nothing new is sent.
	for _, w := range retire {
		c.drainOutputs(context.WithoutCancel(c.ctx), w, "")
	}

	c.mu.Lock()
	c.workers = slices.DeleteFunc(c.workers, func(w *workerConn) bool {
		return slices.Contains(retire, w)
	})
	c.mu.Unlock()
	// So the mirror drops them now rather than failing against a closing client.
	c.pokeOutputs()

	for _, w := range retire {
		// Deliberate, so the closing read error is not read as a lost worker.
		w.dead.Store(true)
		c.log.Info("wings: retiring idle worker", "worker", w.id)
		c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: "retired while idle"})
		if err := c.releaseWorker(context.WithoutCancel(c.ctx), w); err != nil {
			c.log.Error("wings: retiring worker", "worker", w.id, "err", err)
		}
	}
}

// reapDead releases workers that died on their own, so a crashed worker's
// machine stops billing rather than lingering until Stop. Driven by the watchdog
// so it runs even with a fixed fleet.
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
	if len(reaped) > 0 {
		c.pokeOutputs()
	}

	for _, w := range reaped {
		c.log.Info("wings: releasing dead worker", "worker", w.id)
		c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: "died"})
		if err := c.releaseWorker(context.WithoutCancel(c.ctx), w); err != nil {
			c.log.Error("wings: releasing dead worker", "worker", w.id, "err", err)
		}
	}
}

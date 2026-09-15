package wings

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ligustah/wings/flow"
)

// A slot is held by a running thread, not a job: a thread that waits gives its
// slot up and takes one again when the wait ends, so a worker's load is what it
// is running rather than what it holds.

// slots is a pool of a worker's running slots. A freed slot goes first to a
// thread resuming a wait, then to one not yet started.
type slots struct {
	mu     sync.Mutex
	free   int
	urgent []chan struct{}
	normal []chan struct{}
}

func newSlots(n int) *slots { return &slots{free: n} }

// contended reports whether a thread is waiting for a slot — the signal that a
// running thread's slice is worth preempting.
func (s *slots) contended() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.urgent) > 0 || len(s.normal) > 0
}

// acquire takes a slot, waiting for one if none is free. urgent puts the
// caller ahead of every non-urgent waiter.
func (s *slots) acquire(ctx context.Context, urgent bool) error {
	s.mu.Lock()
	if s.free > 0 {
		s.free--
		s.mu.Unlock()
		return nil
	}
	ch := make(chan struct{}, 1)
	if urgent {
		s.urgent = append(s.urgent, ch)
	} else {
		s.normal = append(s.normal, ch)
	}
	s.mu.Unlock()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		s.urgent = without(s.urgent, ch)
		s.normal = without(s.normal, ch)
		s.mu.Unlock()
		select {
		case <-ch:
			// Handed a slot as we left; give it back.
			s.release()
		default:
		}
		return ctx.Err()
	}
}

// release gives a slot up: to the first urgent waiter, else the first other
// waiter, else back to the pool.
func (s *slots) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.urgent) > 0 {
		s.urgent[0] <- struct{}{}
		s.urgent = s.urgent[1:]
		return
	}
	if len(s.normal) > 0 {
		s.normal[0] <- struct{}{}
		s.normal = s.normal[1:]
		return
	}
	s.free++
}

func without(waiters []chan struct{}, ch chan struct{}) []chan struct{} {
	for i, w := range waiters {
		if w == ch {
			return append(waiters[:i], waiters[i+1:]...)
		}
	}
	return waiters
}

// parkReport is how long a thread must be parked before the coordinator is told.
var parkReport = 100 * time.Millisecond

// jobSlot is the one slot an attempt holds, shared by every in-process thread of
// it: the slot tracks whether any of them is running.
type jobSlot struct {
	n   *workerNode
	job jobEnvelope

	mu   sync.Mutex
	held bool
	// blocked is how many of the job's threads are parked and reported to
	// the coordinator; the job is reported woken when the last resumes.
	blocked int
	// evicting is set while the worker is evicting this job to reload it in place
	// (yield.go), so the park's resume does not report a wake the worker will drive
	// itself — the coordinator sees Evicted, then Woke on the reload, not a wake now.
	evicting atomic.Bool
	// preemptTimer fires while a thread holds the slot running, to preempt it after
	// preemptAfter and let another have a turn (yield.go). Stopped when it parks or
	// finishes, so only running time counts. Guarded by mu.
	preemptTimer *time.Timer
	// checkpoint is the latest progress the attempt has committed, so a reload in
	// place resumes from there rather than restarting. Guarded by mu.
	checkpoint []byte
}

func (s *jobSlot) setCheckpoint(b []byte) {
	s.mu.Lock()
	s.checkpoint = b
	s.mu.Unlock()
}

func (s *jobSlot) lastCheckpoint() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkpoint
}

func (s *jobSlot) take(ctx context.Context, urgent bool) error {
	s.mu.Lock()
	if s.held {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	if err := s.n.slots.acquire(ctx, urgent); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held {
		// Another thread of the job took one meanwhile; one is enough.
		s.n.slots.release()
		return nil
	}
	s.held = true
	if s.n.preemptAfter > 0 {
		s.preemptTimer = time.AfterFunc(s.n.preemptAfter, s.maybePreempt)
	}
	return nil
}

// maybePreempt preempts this thread once it has held its slot a full slice, but
// only while another thread is waiting for a slot — an idle worker does not churn.
// With no waiter it waits another slice and checks again.
func (s *jobSlot) maybePreempt() {
	s.mu.Lock()
	held := s.held
	s.mu.Unlock()
	if !held {
		return
	}
	if s.n.slots.contended() {
		s.n.preempt(s.job.ID, s.job.Attempt)
		return
	}
	s.mu.Lock()
	if s.held {
		s.preemptTimer = time.AfterFunc(s.n.preemptAfter, s.maybePreempt)
	}
	s.mu.Unlock()
}

func (s *jobSlot) give() {
	s.mu.Lock()
	held := s.held
	s.held = false
	if s.preemptTimer != nil {
		s.preemptTimer.Stop()
		s.preemptTimer = nil
	}
	s.mu.Unlock()
	if held {
		s.n.slots.release()
	}
}

// Park implements [flow.Parker]: the thread gives the slot up and takes one back
// (ahead of new work) when its wait ends. A lasting wait is reported to the
// coordinator; a longer one has the attempt unloaded (yield.go).
func (s *jobSlot) Park(ctx context.Context, w flow.Wait) func(context.Context) error {
	s.give()
	reported := make(chan struct{})
	timer := time.AfterFunc(parkReport, func() {
		s.mu.Lock()
		s.blocked++
		first := s.blocked == 1
		s.mu.Unlock()
		if first {
			s.beat(ctx, beatEnvelope{Job: s.job.ID, Attempt: s.job.Attempt, Wait: w.On})
		}
		close(reported)
	})
	var unload *time.Timer
	switch {
	case s.n.unloads(w):
		unload = time.AfterFunc(unloadAfter, func() { s.n.unload(s.job, w) })
	case s.n.evicts(w):
		unload = time.AfterFunc(p2pEvictAfter, func() {
			s.evicting.Store(true)
			s.n.evict(s.job, w)
		})
	}
	return func(ctx context.Context) error {
		if unload != nil {
			unload.Stop()
		}
		if !timer.Stop() {
			<-reported
			s.mu.Lock()
			s.blocked--
			last := s.blocked == 0
			s.mu.Unlock()
			// An eviction reports no wake here: the worker keeps the job and drives
			// its reload, telling the coordinator Evicted now and Woke then.
			if last && !s.evicting.Load() {
				s.beat(ctx, beatEnvelope{Job: s.job.ID, Attempt: s.job.Attempt, Woke: true})
			}
		}
		return s.take(ctx, true)
	}
}

func (s *jobSlot) beat(ctx context.Context, b beatEnvelope) {
	if err := s.n.sendBeat(ctx, b); err != nil {
		s.n.log.Debug("wings: could not report a thread's wait", "job", s.job.ID, "err", err)
	}
}

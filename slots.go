package wings

import (
	"context"
	"sync"
	"time"

	"github.com/ligustah/wings/flow"
)

// A worker runs as many threads at once as its concurrency says, and a
// thread that is waiting — for a thread it forked, for a channel, for the
// clock — is not running. So a slot is held by a RUNNING thread, not by a
// job: a thread that waits gives its slot up, and takes one again when the
// wait is over, ahead of any new work the worker has queued. That is what
// lets a worker with one slot run a job and the thread that job is waiting
// on, and what keeps a worker full of blocked threads from being a worker
// that does nothing.
//
// The coordinator is told about waits that last: a thread parked for longer
// than parkReport is reported blocked, which exempts its job from the
// heartbeat bound — it is silent because it is waiting — and stops counting
// it towards the worker's load, so the worker is given more to do. A short
// wait is not worth a round trip and stays on the worker.

// slots is a pool of a worker's running slots.
//
// A slot given up is handed straight to whoever is waiting for one, and a
// thread resuming a wait is served before a thread that has not started:
// finishing what is under way is worth more than beginning something new,
// and a resumed thread is usually one another thread is waiting on.
type slots struct {
	mu     sync.Mutex
	free   int
	urgent []chan struct{}
	normal []chan struct{}
}

func newSlots(n int) *slots { return &slots{free: n} }

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
			// Handed a slot in the same instant. Not wanted any more.
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

// parkReport is how long a thread must be parked before the coordinator is
// told. Most waits are shorter, and a report is a round trip.
var parkReport = 100 * time.Millisecond

// jobSlot is the slot one job's attempt holds, shared by every thread of the
// attempt running in this process.
//
// One slot per job rather than per thread: the threads a job forks in-process
// with Spawn run on goroutines of their own but count as the job, as they
// always have. What the slot tracks is whether ANY of them is running — a
// park by one thread frees the slot when it was held, and a resume by any
// takes it back if nobody has meanwhile.
type jobSlot struct {
	n   *workerNode
	job jobEnvelope

	mu   sync.Mutex
	held bool
	// blocked is how many of the job's threads are parked and reported to
	// the coordinator; the job is reported woken when the last resumes.
	blocked int
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
		// Another of the job's threads took one meanwhile; one is enough.
		s.n.slots.release()
		return nil
	}
	s.held = true
	return nil
}

func (s *jobSlot) give() {
	s.mu.Lock()
	held := s.held
	s.held = false
	s.mu.Unlock()
	if held {
		s.n.slots.release()
	}
}

// Park is the [flow.Parker]: the thread gives the job's slot up, and takes
// one back — ahead of new work — when its wait is over. A wait that lasts
// is reported to the coordinator, and its end too.
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
	return func(ctx context.Context) error {
		if !timer.Stop() {
			<-reported
			s.mu.Lock()
			s.blocked--
			last := s.blocked == 0
			s.mu.Unlock()
			if last {
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

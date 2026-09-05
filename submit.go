package wings

import (
	"context"
	"errors"
)

// Putting jobs onto a worker's queue, many at a time.
//
// A Map over a large input dispatches every element at once, each from a
// goroutine of its own, and each used to be one append to the worker's jobs
// stream: one round trip through the engine, or over the network, per job.
// Those goroutines all arrive within microseconds of each other, so the
// submissions that queue up while one append is in flight are sent together in
// the next. A single call sees no batching and pays no waiting: the batcher
// takes what is already there and never holds a job back for company.

const (
	// submitBatch is the most jobs one append carries.
	submitBatch = 256
	// submitBytes bounds the payload in one append. A job's payload is the
	// caller's, and a batch of large ones would otherwise be a message the
	// transport will not carry.
	submitBytes = 8 << 20
)

// submission is one job on its way to a worker's queue, and where to say
// whether it got there.
type submission struct {
	job  jobEnvelope
	done chan error
}

// send puts one job on a worker's queue and waits for it to be there.
//
// The wait is on the worker rather than on the caller's context: once queued
// the job is going to be appended, and a caller told otherwise would credit the
// worker back for a job that is about to run there.
func (c *Cluster) send(ctx context.Context, w *workerConn, job jobEnvelope) error {
	s := submission{job: job, done: make(chan error, 1)}
	select {
	case w.submits <- s:
	case <-w.ctx.Done():
		return errors.New("wings: worker is gone")
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-s.done:
		return err
	case <-w.ctx.Done():
		return errors.New("wings: worker is gone")
	}
}

// submitter is one worker's batcher. It runs until the worker stops, and
// answers everything still waiting when it does.
func (c *Cluster) submitter(w *workerConn) {
	defer func() {
		// Whoever was still waiting is told; nothing is left blocked on a
		// worker that has stopped taking anything.
		for {
			select {
			case s := <-w.submits:
				s.done <- errors.New("wings: worker is gone")
			default:
				return
			}
		}
	}()

	batch := make([]submission, 0, submitBatch)
	jobs := make([]jobEnvelope, 0, submitBatch)
	nested := make([]jobEnvelope, 0, submitBatch)
	for {
		var first submission
		select {
		case first = <-w.submits:
		case <-w.ctx.Done():
			return
		}
		batch = append(batch[:0], first)
		size := len(first.job.Payload)

		// Everything that arrived meanwhile, without waiting for more.
		for len(batch) < submitBatch && size < submitBytes {
			select {
			case s := <-w.submits:
				batch = append(batch, s)
				size += len(s.job.Payload)
				continue
			default:
			}
			break
		}

		// Two queues, one batcher: what a job called goes on the nested queue,
		// which the worker reads without waiting for a slot on the other.
		jobs, nested := jobs[:0], nested[:0]
		for _, s := range batch {
			if s.job.Nested {
				nested = append(nested, s.job)
			} else {
				jobs = append(jobs, s.job)
			}
		}
		var err error
		if len(jobs) > 0 {
			_, err = w.jobs.Append(w.ctx, jobs)
		}
		if len(nested) > 0 {
			if _, nerr := w.nested.Append(w.ctx, nested); nerr != nil && err == nil {
				err = nerr
			}
		}
		w.appends.Add(1)
		for _, s := range batch {
			s.done <- err
		}
	}
}

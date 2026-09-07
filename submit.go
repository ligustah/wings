package wings

import (
	"context"
	"errors"
)

// One worker's job submissions are batched into shared appends: whatever queued
// while the last append was in flight goes together in the next. No job is held
// back for company.

const (
	// submitBatch is the most jobs one append carries.
	submitBatch = 256
	// submitBytes bounds one append's payload, below the transport's message limit.
	submitBytes = 8 << 20
)

// submission is one job on its way to a worker's queue, and where to say
// whether it got there.
type submission struct {
	job  jobEnvelope
	done chan error
}

// send puts one job on a worker's queue and waits for it to be there. Once
// queued the wait follows the worker, not the caller's context: the job is going
// to be appended regardless.
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

// submitter is one worker's batcher. It runs until the worker stops, failing
// everything still waiting when it does.
func (c *Cluster) submitter(w *workerConn) {
	defer func() {
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

		// Whatever else is already queued, without waiting for more.
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

		// Nested jobs go on their own queue, which the worker reads without
		// waiting for a slot.
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

package wings

import (
	"fmt"
	"slices"
	"time"
)

// Moving queued work to a worker that arrived after it was queued.
//
// A job is placed on the least loaded worker AT SUBMIT, and from then on it is
// that worker's: its queue is a stream, and the worker takes from it in order.
// That is what makes a worker's death survivable — the queue is still there —
// but it also means a worker that arrives later, or frees up sooner, finds
// nothing addressed to it. A Map over sixty jobs onto two workers put thirty on
// each; when one of them was preempted and replaced, the replacement sat idle
// while the survivor worked through both halves, because every job had already
// been given a home.
//
// So the watchdog looks, once a tick, for exactly that shape: a worker with
// jobs waiting unstarted on its queue while another has clearly less to do,
// and moves the waiting ones over until the two are within a job of each
// other. The move is the same move a lost worker's jobs get — a new attempt on
// a new worker, and a word to the old one to skip it when its turn comes —
// except that it does not count against the job, since nothing ran.

// rebalance evens out queued work between workers. Runs on the watchdog.
func (c *Cluster) rebalance(now time.Time) {
	type move struct {
		p        *pendingJob
		from, to *workerConn
	}
	var moves []move

	c.mu.Lock()
	// What could be moved: jobs on a worker that has not begun them, and has
	// had a tick to. Deepest in the queue first, since the job appended last
	// is the one furthest from being taken up — moving the head of a queue
	// races the worker for it.
	queued := map[*workerConn][]*pendingJob{}
	for _, p := range c.pending {
		if p.worker == nil || !p.started.IsZero() || now.Sub(p.since) < rebalanceAfter {
			continue
		}
		queued[p.worker] = append(queued[p.worker], p)
	}
	for _, q := range queued {
		slices.SortFunc(q, func(a, b *pendingJob) int { return b.since.Compare(a.since) })
	}

	// Greedy, on a copy of the loads: take from the fullest worker that has
	// something movable and give to the emptiest, while the two are more than
	// a job apart. The counts are what the picker uses, so the destination is
	// the one the move will choose for itself.
	load := map[*workerConn]int{}
	for _, w := range c.workers {
		if w.available() {
			load[w] = w.inflight
		}
	}
	for len(moves) < rebalanceStep {
		var from, to *workerConn
		for _, w := range c.workers {
			n, ok := load[w]
			if !ok {
				continue
			}
			if len(queued[w]) > 0 && (from == nil || n > load[from]) {
				from = w
			}
			if to == nil || n < load[to] {
				to = w
			}
		}
		if from == nil || to == nil || from == to || load[from]-load[to] < 2 {
			break
		}
		p := queued[from][0]
		queued[from] = queued[from][1:]
		load[from]--
		load[to]++
		moves = append(moves, move{p: p, from: from, to: to})
	}
	c.mu.Unlock()

	if len(moves) == 0 {
		return
	}
	c.log.Info("wings: moving queued jobs to workers with less to do", "jobs", len(moves),
		"from", moves[0].from.id, "to", moves[0].to.id)
	for _, m := range moves {
		c.move(m.p, fmt.Sprintf("rebalanced: queued on %s while %s had less to do", m.from.id, m.to.id), false)
	}
}

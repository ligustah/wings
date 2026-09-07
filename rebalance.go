package wings

import (
	"fmt"
	"slices"
	"time"
)

// rebalance moves jobs waiting unstarted on one worker's queue to a worker with
// less to do, so a worker that joined after submit is not left idle. Runs on the
// watchdog; a moved job does not count against its attempts.
func (c *Cluster) rebalance(now time.Time) {
	type move struct {
		p        *pendingJob
		from, to *workerConn
	}
	var moves []move

	c.mu.Lock()
	// Movable: jobs unstarted for at least a tick. Deepest in the queue first —
	// moving the head races the worker for it.
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

	// Greedy on a copy of the loads: fullest movable worker to emptiest, while
	// the two are more than a job apart.
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

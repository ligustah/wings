package wings

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// A work function is a run, and a run makes calls. Those calls are the
// cluster's to place like any other, and this file is how one gets from the
// worker that made it to the coordinator and its answer back.
//
// There is no request message. The run's history already says what it
// called and where: a CallEvent without a ReturnEvent after it is a call in
// flight. The coordinator has a copy of every attempt's history — the same
// copy a retry is handed — so it reads calls out of it, dispatches each with
// the origin the event names, and sends the answer to the worker on the
// control stream. Making a call is therefore a COMMIT POINT on the worker:
// the event has to be visible to travel, which also means the run's state up
// to the call is consistent before the call runs.
//
// The origin is what makes a move safe. The run is named for the JOB, not
// the attempt, so a retry that replays a call presents the same run, thread
// and step, and the coordinator recognises the call it is already running —
// or already answered, since answers are kept until the job settles — rather
// than dispatching it again.
//
// On the worker, calls arrive on a queue of their own and run without a
// place in the batch. The job queue is taken in batches of the worker's
// concurrency, and a batch is not over until every job in it is; a call
// queued behind the batch that is waiting for it would wait forever.

// jobRunName is the run a job executes as. Stable across attempts, because it
// is what the job's calls are recorded against.
func jobRunName(job string) string { return "job:" + job }

// isJobRun reports whether a run name is a job's, and jobOfRun says which.
func isJobRun(run string) bool   { return strings.HasPrefix(run, "job:") }
func jobOfRun(run string) string { return strings.TrimPrefix(run, "job:") }
func callKey(thread string, step uint64) string {
	return thread + "#" + strconv.FormatUint(step, 10)
}

// followPoll is how often a history follower looks up from its read to see
// whether the attempt it follows is still the one running.
const followPoll = 5 * time.Second

// --- coordinator ---

// followHistory starts reading one attempt's history for the calls it makes.
// Once per attempt, from the first beat that says the attempt is running.
// Call with mu held.
func (c *Cluster) followHistory(p *pendingJob, attempt int) {
	if p.followed[attempt] || c.closed {
		return
	}
	if p.followed == nil {
		p.followed = map[int]bool{}
	}
	p.followed[attempt] = true
	c.wg.Go(func() { c.follow(p, attempt) })
}

// follow reads an attempt's history as it is copied home and dispatches the
// calls in it that have no return yet.
//
// Calls are dispatched after each batch rather than per event, so a history
// hydrated onto a retry — which arrives as one long batch, calls and their
// returns together — dispatches nothing that was already answered.
func (c *Cluster) follow(p *pendingJob, attempt int) {
	// The engine is up before any worker can beat; a cluster without one is
	// a test feeding beats by hand, and has no history to read.
	client := c.shared
	if client == nil {
		return
	}
	name := historyName(p.job.ID, attempt)
	current := func() bool {
		if c.ctx.Err() != nil || p.finished() {
			return false
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		return p.job.Attempt == attempt
	}
	wait := func(d time.Duration) {
		select {
		case <-c.ctx.Done():
		case <-p.done:
		case <-time.After(d):
		}
	}

	var (
		st         *dsclient.Stream[*protos.Event]
		from       int64
		open       = map[string]*protos.CallEvent{}
		dispatched = map[string]bool{}
	)
	for current() {
		if st == nil {
			// Not there yet: the attempt has recorded nothing worth a stream,
			// or the copy has not caught up. Either way the answer is to look
			// again, not to give up.
			ok, err := client.StreamExists(c.ctx, name)
			if err != nil || !ok {
				wait(time.Second)
				continue
			}
			if st, err = eventStream[*protos.Event](client, name); err != nil {
				c.log.Warn("wings: cannot follow a job's history for its calls", "job", p.job.ID, "err", err)
				return
			}
		}
		readCtx, cancel := context.WithTimeout(c.ctx, followPoll)
		recs, err := st.ReadBlocking(readCtx, from, recordBatch)
		expired := readCtx.Err() != nil
		cancel()
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			if !expired {
				wait(time.Second)
			}
			continue
		}
		for _, r := range recs {
			from = r.Offset + 1
			ev := r.Record
			switch e := protos.UnpackEventPayload(ev).(type) {
			case *protos.CallEvent:
				open[callKey(ev.GetThreadId(), ev.GetSerial())] = e
			case *protos.ReturnEvent:
				delete(open, callKey(ev.GetThreadId(), e.GetCallSerial()))
			}
		}
		for key, call := range open {
			delete(open, key)
			if dispatched[key] {
				continue
			}
			dispatched[key] = true
			thread, step, _ := strings.Cut(key, "#")
			n, _ := strconv.ParseUint(step, 10, 64)
			c.wg.Go(func() { c.dispatchNested(p, attempt, thread, n, call) })
		}
	}
}

// dispatchNested runs one call a job made and sends the answer to whichever
// attempt of the job is running when it comes.
//
// An answer already kept for this call is sent straight back: that is a
// retry replaying a call its predecessor had answered. A call still in flight
// is rejoined by its origin, the same way a workflow's is.
func (c *Cluster) dispatchNested(p *pendingJob, attempt int, thread string, step uint64, call *protos.CallEvent) {
	key := callKey(thread, step)

	c.mu.Lock()
	if res, ok := p.answers[key]; ok {
		w, a := p.worker, p.job.Attempt
		c.mu.Unlock()
		c.answerOn(w, p.job.ID, a, thread, step, res)
		return
	}
	p.children++
	c.mu.Unlock()

	// Bounded by the parent: a job that settles — finishes, fails, is given
	// up on — without waiting for a call it made leaves nobody wanting the
	// answer, and the last waiter giving up is what stops the work.
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	go func() {
		select {
		case <-p.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	origin := flow.Origin{Run: jobRunName(p.job.ID), Thread: thread, Step: step, Attempt: uint64(attempt)}
	child, err := c.submit(flow.WithOrigin(ctx, origin), call.GetName(), call.GetParams().GetSerialized())
	var res resultEnvelope
	if err == nil {
		res, err = c.await(ctx, child)
	}

	c.mu.Lock()
	p.children--
	if kept, ok := p.answers[key]; ok {
		// Recorded when the child settled, which is the authoritative copy.
		res = kept
	} else if err == nil {
		c.keepAnswerLocked(p, key, res)
	}
	w, a := p.worker, p.job.Attempt
	gone := p.finished()
	c.mu.Unlock()

	if gone || w == nil {
		return
	}
	if err != nil {
		if ctx.Err() != nil {
			return // the parent is gone, or the cluster is
		}
		res = resultEnvelope{Error: err.Error()}
	}
	c.answerOn(w, p.job.ID, a, thread, step, res)
}

// keepAnswerLocked records the outcome of a call a job made, against the job.
// Call with mu held.
func (c *Cluster) keepAnswerLocked(p *pendingJob, key string, res resultEnvelope) {
	if p.answers == nil {
		p.answers = map[string]resultEnvelope{}
	}
	if _, ok := p.answers[key]; !ok {
		p.answers[key] = res
	}
}

// noteSettledLocked is called for every job that settles, with mu held: one
// that is a call made by another job has its answer kept against that job
// here, at the moment it stops being outstanding, so a retry of the parent
// that asks in the same instant finds the answer rather than a gap.
func (c *Cluster) noteSettledLocked(child *pendingJob, res resultEnvelope) {
	if !isJobRun(child.origin.Run) {
		return
	}
	parent, ok := c.pending[jobOfRun(child.origin.Run)]
	if !ok {
		return
	}
	c.keepAnswerLocked(parent, callKey(child.origin.Thread, child.origin.Step), res)
}

// answerOn sends the outcome of a call to the worker running the attempt
// that made it. Best effort like stopOn: a worker that is gone has its
// attempt moved, and the retry asks again.
func (c *Cluster) answerOn(w *workerConn, job string, attempt int, thread string, step uint64, res resultEnvelope) {
	if w == nil || w.control == nil || w.dead.Load() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), 10*time.Second)
	defer cancel()
	env := controlEnvelope{Job: job, Attempt: attempt, Answer: &answerEnvelope{
		Thread: thread, Step: step, Payload: res.Payload, Error: res.Error,
	}}
	if _, err := w.control.Append(ctx, []controlEnvelope{env}); err != nil {
		c.log.Debug("wings: could not answer a job's call", "worker", w.id, "job", job, "err", err)
	}
}

// finished reports whether the job has an outcome.
func (p *pendingJob) finished() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// --- worker ---

// answerBox is where one call's answer is left for the attempt waiting on it.
// Room for one: a second copy of the same answer is not news.
type answerBox struct{ ch chan answerEnvelope }

// box finds or makes the box for one call of one attempt. Call with runMu
// held.
func (n *workerNode) boxLocked(attempt, call string) *answerBox {
	if n.answers == nil {
		n.answers = map[string]map[string]*answerBox{}
	}
	boxes := n.answers[attempt]
	if boxes == nil {
		boxes = map[string]*answerBox{}
		n.answers[attempt] = boxes
	}
	b := boxes[call]
	if b == nil {
		b = &answerBox{ch: make(chan answerEnvelope, 1)}
		boxes[call] = b
	}
	return b
}

// answer takes an answer off the control stream to the attempt waiting for
// it. One for an attempt not running here is dropped: it was for one that has
// finished or moved on, and the coordinator answers a retry afresh.
func (n *workerNode) answer(c controlEnvelope) {
	key := attemptKey(c.Job, c.Attempt)
	n.runMu.Lock()
	defer n.runMu.Unlock()
	if _, running := n.running[key]; !running {
		return
	}
	select {
	case n.boxLocked(key, callKey(c.Answer.Thread, c.Answer.Step)).ch <- *c.Answer:
	default:
	}
}

// nestedExecutor is the [flow.Executor] a job's run makes its calls through:
// commit, so the call is in the history the coordinator reads, and wait for
// the answer.
type nestedExecutor struct {
	n   *workerNode
	job *jobState
}

func (e nestedExecutor) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	o := flow.OriginFrom(ctx)
	if o.Zero() {
		return nil, errors.New("wings: " + name + " was called on a worker outside its job's run")
	}
	// A direct call runs here. It blocks its caller either way, so sending
	// it to another worker would move the CPU while this one's slot sat
	// idle, and pay a commit and a round trip for it. Recorded and replayed
	// like any call. A fan-out — Go, Map — is the cluster's to place.
	if !o.Forked {
		return flow.Local().Invoke(ctx, name, payload)
	}
	attempt := attemptKey(e.job.id, e.job.attempt)
	e.n.runMu.Lock()
	box := e.n.boxLocked(attempt, callKey(o.Thread, o.Step))
	e.n.runMu.Unlock()

	// The call event is in the open transaction. Committing is what makes
	// this a request: the coordinator reads the history's copy.
	if err := e.job.outputs.commit(ctx); err != nil {
		return nil, err
	}
	select {
	case a := <-box.ch:
		if a.Error != "" {
			return nil, errors.New(a.Error)
		}
		return a.Payload, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// serveNested runs the calls other jobs made, as they arrive, each on a
// goroutine of its own. No batch and no bound: what bounds a worker's work is
// its job queue, and a call is work a job on that queue is already waiting
// for. Results go on the same result stream the batch loop writes, one at a
// time.
func (n *workerNode) serveNested(ctx context.Context) {
	info, err := n.nested.Info(ctx)
	if err != nil {
		if ctx.Err() == nil {
			n.log.Warn("wings: cannot read the calls other jobs make", "err", err)
		}
		return
	}
	from := max(info.Oldest, 0)

	for ctx.Err() == nil {
		readCtx, cancel := context.WithTimeout(ctx, pollInterval)
		recs, err := n.nested.ReadBlocking(readCtx, from, 64)
		expired := readCtx.Err() != nil
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if !expired {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
			continue
		}
		for _, r := range recs {
			from = r.Offset + 1
			go n.runNested(ctx, r.Record)
		}
	}
}

func (n *workerNode) runNested(ctx context.Context, job jobEnvelope) {
	res := n.runOne(ctx, job)
	// The batch loop's rule, kept: a worker whose machine is being taken back
	// reports nothing, since the coordinator has moved the job and an error
	// from here could arrive as its answer.
	if n.leaving.Load() {
		return
	}
	if _, err := n.out.Append(context.WithoutCancel(ctx), []resultEnvelope{res}); err != nil && ctx.Err() == nil {
		n.log.Error("wings: could not deliver the result of a call", "job", job.ID, "err", err)
	}
}

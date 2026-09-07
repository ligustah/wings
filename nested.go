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

// Threads forked by a running job. There is no request message: the run's
// history already records a fork (a ForkEvent with no matching JoinEvent is a
// thread in flight), so the coordinator reads forks out of its copy of the
// history, dispatches each with the thread as its origin, and returns the result
// on the control stream. Forking is therefore a commit point on the worker. The
// origin makes a move safe: a retry replays the same fork under the same run and
// thread, so the coordinator rejoins the thread rather than dispatching it again.

// jobRunName is the run a bare call executes as on the worker; isJobRun and
// jobOfRun recognise and decode it.
func jobRunName(job string) string { return "job:" + job }

func isJobRun(run string) bool   { return strings.HasPrefix(run, "job:") }
func jobOfRun(run string) string { return strings.TrimPrefix(run, "job:") }

func runOf(job jobEnvelope) string {
	if job.Run != "" {
		return job.Run
	}
	return jobRunName(job.ID)
}

func threadOf(job jobEnvelope) string {
	if job.Thread != "" {
		return job.Thread
	}
	return "main"
}

// parentThread is the thread that forked one; threads are named "<parent>.<n>".
func parentThread(thread string) (string, bool) {
	i := strings.LastIndexByte(thread, '.')
	if i < 0 {
		return "", false
	}
	return thread[:i], true
}

// parentJobLocked is the job running the thread that forked the one o names, or
// nil. Call with mu held.
func (c *Cluster) parentJobLocked(o flow.Origin) *pendingJob {
	if o.Zero() {
		return nil
	}
	parent, ok := parentThread(o.Thread)
	if !ok {
		return nil
	}
	if parent == "main" && isJobRun(o.Run) {
		return c.pending[jobOfRun(o.Run)]
	}
	return c.byOrigin[flow.Origin{Run: o.Run, Thread: parent}.Key()]
}

func callKey(thread string, step uint64) string {
	return thread + "#" + strconv.FormatUint(step, 10)
}

// forkedCall is one thread a job forked and has not joined: a function and
// input, or the lineage that reaches a thread of run code.
type forkedCall struct {
	fn      string
	input   []byte
	root    flow.Root
	lineage []string
}

func (f forkedCall) job() jobEnvelope {
	return jobEnvelope{Func: f.fn, Payload: f.input, Root: f.root, Lineage: f.lineage}
}

const (
	followPoll = 5 * time.Second
	followLook = 50 * time.Millisecond
)

// --- coordinator ---

// followHistory starts reading one attempt's history for the threads it forks,
// once per attempt. Call with mu held.
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
// threads forked in it that have no join yet. Dispatch is per batch, not per
// event, so a hydrated history (forks and joins together) dispatches nothing
// already answered.
func (c *Cluster) follow(p *pendingJob, attempt int) {
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

	// A thread of run code carries its ancestors' histories too; their forks
	// are the ancestors', dispatched elsewhere. Only the job's own thread and
	// threads named under it are its. See lineage.go.
	own := threadOf(p.job)
	owned := func(id string) bool { return id == own || strings.HasPrefix(id, own+".") }

	var (
		st         *dsclient.Stream[*protos.Event]
		from       int64
		open       = map[string]forkedCall{}
		dispatched = map[string]bool{}
	)
	for current() {
		if st == nil {
			ok, err := client.StreamExists(c.ctx, name)
			if err != nil {
				wait(time.Second)
				continue
			}
			if !ok {
				wait(followLook)
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
			if !owned(ev.GetThreadId()) {
				continue
			}
			switch e := protos.UnpackEventPayload(ev).(type) {
			case *protos.ForkEvent:
				call := forkedCall{fn: e.GetFunction(), input: e.GetInput().GetSerialized()}
				if call.fn == "" {
					root, lineage := lineageOfJob(p.job)
					call.root, call.lineage = root, append(lineage, e.GetThreadId())
				}
				open[callKey(e.GetThreadId(), 0)] = call
			case *protos.JoinEvent:
				delete(open, callKey(e.GetThreadId(), 0))
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

// dispatchNested runs one thread a job forked and sends the result to whichever
// attempt of the job is running when it comes. An answer already kept is a retry
// replaying a fork its predecessor had joined; a thread in flight is rejoined by
// its origin.
func (c *Cluster) dispatchNested(p *pendingJob, attempt int, thread string, step uint64, call forkedCall) {
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

	// Bounded by the parent: once it settles nobody wants the answer.
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	go func() {
		select {
		case <-p.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	origin := flow.Origin{Run: runOf(p.job), Thread: thread, Step: step, Attempt: uint64(attempt)}
	child, err := c.submitJob(flow.WithOrigin(ctx, origin), call.job())
	if err != nil && ctx.Err() == nil {
		c.log.Warn("wings: could not start a thread a job forked", "job", p.job.ID, "thread", thread, "err", err)
	}
	var res resultEnvelope
	if err == nil {
		res, err = c.await(ctx, child)
	}

	c.mu.Lock()
	p.children--
	if kept, ok := p.answers[key]; ok {
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
			return
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

// noteSettledLocked keeps a settling job's result against its parent job, so a
// retry of the parent that asks in the same instant finds it. Returns the parent
// when it is waiting off every worker for the thread, for the caller to wake.
// Call with mu held.
func (c *Cluster) noteSettledLocked(child *pendingJob, res resultEnvelope) *pendingJob {
	parent := c.parentJobLocked(child.origin)
	if parent == nil {
		return nil
	}
	c.keepAnswerLocked(parent, callKey(child.origin.Thread, child.origin.Step), res)
	if parent.yield != nil && parent.yield.Wait == flow.WaitJoin {
		return parent
	}
	return nil
}

// answerOn sends the outcome of a call to the worker running the attempt that
// made it. Best effort: a gone worker has its attempt moved, and the retry asks again.
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

func (p *pendingJob) finished() bool {
	select {
	case <-p.done:
		return true
	default:
		return false
	}
}

// --- worker ---

// answerBox holds one call's answer for the attempt waiting on it. Room for one.
type answerBox struct{ ch chan answerEnvelope }

// boxLocked finds or makes the box for one call of one attempt. Call with runMu held.
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

// answer routes a control-stream answer to the attempt waiting for it, dropping
// one for an attempt not running here.
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

// nestedPlacer is the [flow.Placer] a job's run forks threads through: commit so
// the fork is in the history the coordinator reads, then wait for the result.
type nestedPlacer struct {
	n   *workerNode
	job *jobState
}

func (e nestedPlacer) Place(ctx context.Context, th flow.Thread, body func(flow.Context) ([]byte, error)) ([]byte, error) {
	if th.Fn == "" {
		if err := flow.Share(ctx); err != nil {
			return nil, err
		}
	}
	out, err := e.place(ctx, th)
	if err != nil && th.Fn == "" && unknownRoot(err) {
		// No worker holds the code; run it here.
		return flow.InProcess().Place(ctx, th, body)
	}
	return out, err
}

func (e nestedPlacer) place(ctx context.Context, th flow.Thread) ([]byte, error) {
	attempt := attemptKey(e.job.id, e.job.attempt)
	e.n.runMu.Lock()
	box := e.n.boxLocked(attempt, callKey(th.ID, 0))
	e.n.runMu.Unlock()

	// Committing the open transaction is what makes this a request.
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

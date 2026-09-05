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

// A work function is a run, and a run forks threads. Those threads are the
// cluster's to place like any other, and this file is how one gets from the
// worker that forked it to the coordinator and its result back.
//
// There is no request message. The run's history already says what it
// forked: a ForkEvent naming a function, without a JoinEvent for that thread
// after it, is a thread in flight. The coordinator has a copy of every
// attempt's history — the same copy a retry is handed — so it reads forks
// out of it, dispatches each as a job with the thread as its origin, and
// sends the result to the worker on the control stream. Forking is
// therefore a COMMIT POINT on the worker: the event has to be visible to
// travel, which also means the run's state up to the fork is consistent
// before the thread runs. A function the job calls directly is not any of
// this: it runs on the worker, on the calling thread, recorded and replayed
// like any call.
//
// The origin is what makes a move safe. A job is a thread of a run — the
// run the workflow is, and the thread its fork named — and a retry that
// replays a fork presents the same run and thread, so the coordinator
// recognises the thread it is already running — or already answered, since
// answers are kept until the job settles — rather than dispatching it again.
// Which job a thread's parent is follows from the thread's name, since a
// thread is named under its parent.
//
// On the worker, threads forked by jobs arrive on a queue of their own,
// served like the job queue: continuously, each on a goroutine, in a
// running slot. The job waiting for one has given its slot up — see
// slots.go — so on a worker with one slot the thread still runs.

// jobRunName is the run a bare call executes as on the worker: one made on
// [Cluster.Bind], which belongs to no run of its own. Stable across attempts,
// because it is what the threads the job forks are recorded against.
func jobRunName(job string) string { return "job:" + job }

// isJobRun reports whether a run name is one made up for a bare call's job,
// and jobOfRun says which job.
func isJobRun(run string) bool   { return strings.HasPrefix(run, "job:") }
func jobOfRun(run string) string { return strings.TrimPrefix(run, "job:") }

// runOf is the run a job's thread belongs to, on the coordinator and the
// worker alike: the run it was forked from, or the one made up for a bare
// call.
func runOf(job jobEnvelope) string {
	if job.Run != "" {
		return job.Run
	}
	return jobRunName(job.ID)
}

// threadOf is the thread a job runs as: the one it was forked as, or main
// for a bare call, which is a run of its own.
func threadOf(job jobEnvelope) string {
	if job.Thread != "" {
		return job.Thread
	}
	return "main"
}

// parentThread is the thread that forked one, by its name: a thread is
// named "<parent>.<n>". main has none.
func parentThread(thread string) (string, bool) {
	i := strings.LastIndexByte(thread, '.')
	if i < 0 {
		return "", false
	}
	return thread[:i], true
}

// parentJobLocked is the job running the thread that forked the one an
// origin names, or nil when that thread is not a job — the workflow's own
// main, or nothing at all. Call with mu held.
//
// A bare call's job is the main thread of a run made up for it, and is not
// indexed by any origin; its children find it by the run's name instead.
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

// forkedCall is one thread a job forked and has not joined: the function it
// runs and the input, or — for a thread of run code — the lineage that
// reaches it, from the job's own root.
type forkedCall struct {
	fn      string
	input   []byte
	root    flow.Root
	lineage []string
}

// job is the thread as a job to send.
func (f forkedCall) job() jobEnvelope {
	return jobEnvelope{Func: f.fn, Payload: f.input, Root: f.root, Lineage: f.lineage}
}

// followPoll is how often a history follower looks up from its read to see
// whether the attempt it follows is still the one running; followLook is
// how often it looks for the history's copy before the copy exists.
const (
	followPoll = 5 * time.Second
	followLook = 50 * time.Millisecond
)

// --- coordinator ---

// followHistory starts reading one attempt's history for the threads it forks.
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
// threads forked in it that have no join yet.
//
// Threads are dispatched after each batch rather than per event, so a history
// hydrated onto a retry — which arrives as one long batch, forks and their
// joins together — dispatches nothing that was already answered.
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

	// A thread of run code has its ancestors' histories in its stream too,
	// put there for the replay that reaches it — see lineage.go — and the
	// forks in those are the ancestors', already dispatched from wherever
	// the ancestors are. Only what the job's own threads fork is its.
	replayed := map[string]bool{}
	for _, id := range p.job.Lineage[:max(len(p.job.Lineage)-1, 0)] {
		replayed[id] = true
	}

	var (
		st         *dsclient.Stream[*protos.Event]
		from       int64
		open       = map[string]forkedCall{}
		dispatched = map[string]bool{}
	)
	for current() {
		if st == nil {
			// Not there yet: the attempt has recorded nothing worth a stream,
			// or the copy has not caught up. Either way the answer is to look
			// again, not to give up — and soon, since a fork is the first
			// thing many attempts record and the job is silent while it
			// waits for the answer: a bound on silence shorter than the
			// look would move the job for asking.
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
			if replayed[ev.GetThreadId()] {
				continue
			}
			switch e := protos.UnpackEventPayload(ev).(type) {
			case *protos.ForkEvent:
				call := forkedCall{fn: e.GetFunction(), input: e.GetInput().GetSerialized()}
				if call.fn == "" {
					// Run code: reached by the job's own lineage, one thread
					// longer. See lineage.go.
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

// dispatchNested runs one thread a job forked and sends the result to
// whichever attempt of the job is running when it comes.
//
// An answer already kept for this thread is sent straight back: that is a
// retry replaying a fork its predecessor had joined. A thread still in flight
// is rejoined by its origin, the same way a workflow's is.
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
	origin := flow.Origin{Run: runOf(p.job), Thread: thread, Step: step, Attempt: uint64(attempt)}
	child, err := c.submitJob(flow.WithOrigin(ctx, origin), call.job())
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
// that is a thread forked by another job has its result kept against that
// job here, at the moment it stops being outstanding, so a retry of the
// parent that asks in the same instant finds the answer rather than a gap.
// It returns the parent when the parent is off every worker waiting for a
// thread to finish, which this may be; the caller wakes it once the lock is
// dropped.
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

// nestedPlacer is the [flow.Placer] a job's run forks its threads through:
// commit, so the fork is in the history the coordinator reads, and wait for
// the result. A thread of run code goes the same way, by its lineage — the
// coordinator works that out from the job's — after every channel of the run
// is shared, since the thread may use any of them.
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
		// No worker holds the code, and this one does: the coordinator's
		// answer says so, and the thread runs here after all.
		return flow.InProcess().Place(ctx, th, body)
	}
	return out, err
}

func (e nestedPlacer) place(ctx context.Context, th flow.Thread) ([]byte, error) {
	attempt := attemptKey(e.job.id, e.job.attempt)
	e.n.runMu.Lock()
	box := e.n.boxLocked(attempt, callKey(th.ID, 0))
	e.n.runMu.Unlock()

	// The fork event is in the open transaction. Committing is what makes
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

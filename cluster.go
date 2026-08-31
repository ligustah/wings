package wings

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ligustah/durable_streams/broker/embed"
	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"
)

// Cluster is a set of running workers and the means to call work functions on
// them. Create one with [Start].
type Cluster struct {
	cfg Config
	log *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dir    string
	tmpDir string

	// shared is the cluster's own embedded durable-streams instance: no broker,
	// no listener, no port. Every cluster has one, because the coordinator keeps
	// its journal there whatever the target is.
	//
	// The in-process target then runs EVERYTHING on it — every worker's stream
	// pair as well — so coordinator and workers meet on one engine instead of
	// one per worker. One rather than many because a worker that is a goroutine
	// is not a machine: giving each its own meant its own directory, its own
	// locks and its own shutdown, and retiring one meant closing an engine out
	// from under whatever still referenced it.
	shared     *dsclient.Client
	sharedOnce sync.Once
	sharedErr  error
	sharedStop func() error

	// journal is the coordinator's own durable record, on that same instance.
	// It is the one thing the coordinator keeps for itself rather than for a
	// worker, which is why it is here and not on a workerConn.
	journal *journal

	// mu guards workers, pending, closed, and every workerConn field that
	// changes after construction (inflight, draining, idleSince).
	//
	// One lock rather than per-field atomics because the load-bearing operation
	// is a COMPOUND one: choosing a worker and charging a job to it must be
	// indivisible against the scaler deciding that same worker is idle and
	// removing it. Atomics make each half safe and the pair still wrong.
	mu      sync.Mutex
	workers []*workerConn
	pending map[string]*pendingJob
	closed  bool

	nextID  atomic.Uint64
	nextSeq atomic.Uint64
}

// workerConn is one worker as the coordinator sees it: a connection to its
// broker, its two streams, and whatever resource has to be released when it
// goes away.
type workerConn struct {
	id      string
	client  *dsclient.Client
	jobs    *dsclient.Stream[jobEnvelope]
	results *dsclient.Stream[resultEnvelope]

	// ownsClient is false for an in-process worker, whose backend the worker
	// node itself closes. Closing it twice takes the broker down under the half
	// of the process still using it.
	ownsClient bool

	node    *workerNode // in-process only; shares the cluster engine, owns nothing
	proc    *os.Process // local-process only
	machine Machine     // remote only

	// ctx bounds every goroutine belonging to THIS worker -- its result tail,
	// and in process its run loop too -- and stop ends them. wg is how close
	// waits for them.
	//
	// Per worker rather than per cluster because a worker can now be retired
	// while the cluster runs on. A goroutine still reading from a worker whose
	// resources are being released is the bug this prevents, and autoscaling is
	// what made it reachable.
	ctx  context.Context
	stop context.CancelFunc
	wg   sync.WaitGroup

	// Guarded by Cluster.mu.
	inflight  int
	draining  bool
	idleSince time.Time

	// dead is set by the worker's own tail goroutine, which does not hold
	// Cluster.mu, so it stays atomic.
	dead atomic.Bool
}

// available reports whether new work may be sent here. Call with Cluster.mu.
func (w *workerConn) available() bool { return !w.draining && !w.dead.Load() }

type pendingJob struct {
	job    jobEnvelope
	worker *workerConn
	done   chan resultEnvelope
}

// Start brings up the workers described by cfg.
//
// IN A WORKER PROCESS THIS NEVER RETURNS. A binary launched by wings serves
// work until it is shut down, so everything after this call in your main is
// coordinator-only by construction. See the package doc.
func Start(ctx context.Context, cfg Config) (*Cluster, error) {
	log := cfg.logger()

	if isWorkerProcess() {
		err := runWorkerProcess(ctx, log)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("wings: worker exiting", "err", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	if len(registry) == 0 {
		return nil, errors.New("wings: no work functions defined; call wings.Define in a package-scope var")
	}
	if err := cfg.Scaling.validate(); err != nil {
		return nil, err
	}
	// Normalised once, here, so nothing downstream has to ask whether a field
	// was set — the scaling loop reads its policy as given.
	cfg.Scaling = cfg.Scaling.withDefaults()

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &Cluster{
		cfg:     cfg,
		log:     log,
		ctx:     runCtx,
		cancel:  cancel,
		pending: map[string]*pendingJob{},
	}

	c.dir = cfg.Dir
	if c.dir == "" {
		var err error
		if c.dir, err = os.MkdirTemp("", "wings-*"); err != nil {
			cancel()
			return nil, fmt.Errorf("wings: data dir: %w", err)
		}
		c.tmpDir = c.dir
	}

	// Opened before any worker exists, so the record starts at the beginning of
	// the run rather than at the first thing that happened to succeed.
	//
	// Every target gets one, including the remote one: the coordinator's account
	// of its own decisions is local by definition, and a cluster whose machines
	// have all been destroyed is exactly when you want it.
	client, err := c.sharedClient()
	if err != nil {
		cancel()
		c.cleanupDir()
		return nil, err
	}
	if c.journal, err = openJournal(ctx, client, log); err != nil {
		cancel()
		_ = c.closeShared()
		c.cleanupDir()
		return nil, err
	}

	n := cfg.Scaling.initialWorkers(cfg.workers())
	workers, err := c.launch(ctx, n)
	if err != nil {
		cancel()
		c.journal.close()
		_ = c.closeShared()
		c.cleanupDir()
		return nil, err
	}
	for _, w := range workers {
		c.adopt(w)
	}

	if cfg.Scaling.enabled() {
		c.wg.Add(1)
		go c.autoscale()
		log.Info("wings: cluster ready", "workers", len(workers), "target", cfg.Target.kind,
			"autoscale", fmt.Sprintf("%d..%d", cfg.Scaling.Min, cfg.Scaling.Max))
	} else {
		log.Info("wings: cluster ready", "workers", len(workers), "target", cfg.Target.kind)
	}
	return c, nil
}

// adopt puts a freshly launched worker into service and starts tailing it.
func (c *Cluster) adopt(w *workerConn) {
	c.mu.Lock()
	w.idleSince = time.Now()
	c.workers = append(c.workers, w)
	c.mu.Unlock()

	c.journal.record(journalEntry{Kind: journalWorkerUp, Worker: w.id})

	c.wg.Add(1)
	w.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer w.wg.Done()
		c.tail(w)
	}()
}

// launch brings up n workers for the configured target.
func (c *Cluster) launch(ctx context.Context, n int) ([]*workerConn, error) {
	switch c.cfg.Target.kind {
	case targetInProcess:
		return c.launchInProcess(ctx, n)
	case targetLocalProcess:
		return c.launchLocalProcess(ctx, n)
	case targetRemote:
		return c.launchRemote(ctx, n)
	default:
		return nil, fmt.Errorf("wings: unknown target %d", c.cfg.Target.kind)
	}
}

// workerID mints a name unique for the life of the cluster. Reusing an index
// after a worker is torn down would make two workers share a transactional id
// on the broker, which fences the live one.
func (c *Cluster) workerID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, c.nextSeq.Add(1)-1)
}

// connect wires a worker's streams onto a backend the coordinator can reach.
// The one function every target funnels through, and the reason a remote worker
// needs no code of its own here.
func (c *Cluster) connect(id string, client *dsclient.Client, owns bool) (*workerConn, error) {
	w := &workerConn{id: id, client: client, ownsClient: owns}
	w.ctx, w.stop = context.WithCancel(c.ctx)

	var err error
	if w.jobs, err = client.OpenStream[jobEnvelope](jobStreamFor(id)); err != nil {
		return nil, fmt.Errorf("wings: open %s on worker %s: %w", jobStreamFor(id), id, err)
	}
	if w.results, err = client.OpenStream[resultEnvelope](resultStreamFor(id)); err != nil {
		return nil, fmt.Errorf("wings: open %s on worker %s: %w", resultStreamFor(id), id, err)
	}
	return w, nil
}

// connectBackend is connect for a backend the coordinator owns outright — the
// out-of-process targets, where the client exists only to talk to this one
// worker and dies with it.
func (c *Cluster) connectBackend(id string, backend dswire.Backend) (*workerConn, error) {
	return c.connect(id, dsclient.Wrap(backend), true)
}

// sharedClient is the cluster's own embedded durable-streams instance, created
// on first use and torn down by [Cluster.Stop].
//
// Only the in-process target reaches for it, and it is what makes that target
// honest: coordinator and workers are the same process, so there is nothing to
// serve over a socket and nothing to dial — they open the same streams on the
// same engine. Above this line the coordinator sees a *dsclient.Client either
// way, which is why one connect serves every target.
func (c *Cluster) sharedClient() (*dsclient.Client, error) {
	c.sharedOnce.Do(func() {
		dir := filepath.Join(c.dir, "engine")
		b, err := embed.StartInProcess(embed.InProcessConfig{Dir: dir, Logger: streamLogger(c.log)})
		if err != nil {
			c.sharedErr = fmt.Errorf("wings: start embedded streams in %s: %w", dir, err)
			return
		}
		c.shared = dsclient.Wrap(b.Client())
		c.sharedStop = b.Close
	})
	return c.shared, c.sharedErr
}

// closeShared releases the embedded instance. Called once, after every worker
// that reads through it is gone.
func (c *Cluster) closeShared() error {
	if c.sharedStop == nil {
		return nil
	}
	// The client wraps the engine's own backend, which Close also releases, so
	// only one of them may do it.
	err := c.sharedStop()
	c.sharedStop, c.shared = nil, nil
	return err
}

// tail delivers a worker's results until the cluster stops or the worker dies.
func (c *Cluster) tail(w *workerConn) {
	var from int64
	for {
		recs, err := w.results.ReadBlocking(w.ctx, from, 256)
		if err != nil {
			if w.ctx.Err() != nil {
				return
			}
			if w.dead.Load() {
				return // already retired deliberately
			}
			c.log.Error("wings: lost worker", "worker", w.id, "err", err)
			c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: err.Error()})
			w.dead.Store(true)
			c.redispatchFrom(w)
			return
		}
		for _, r := range recs {
			from = r.Offset + 1
			c.deliver(r.Record)
		}
	}
}

// deliver hands one result to whoever is waiting for it.
//
// An unknown id is normal rather than alarming: a worker's stream survives the
// coordinator that wrote to it, so a restart against a persistent Dir replays
// results nobody is waiting for any more.
func (c *Cluster) deliver(res resultEnvelope) {
	c.mu.Lock()
	p, ok := c.pending[res.ID]
	if ok {
		delete(c.pending, res.ID)
		c.release(p.worker)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	c.journal.record(journalEntry{
		Kind: journalCompleted, Job: res.ID, Func: p.job.Func,
		Worker: p.worker.id, Attempt: p.job.Attempt, Err: res.Error,
	})
	select {
	case p.done <- res:
	default:
	}
}

// release credits a finished job back to its worker. Call with mu held.
func (c *Cluster) release(w *workerConn) {
	if w.inflight > 0 {
		w.inflight--
	}
	if w.inflight == 0 {
		w.idleSince = time.Now()
	}
}

// charge assigns a job to a worker. Call with mu held.
func (c *Cluster) charge(w *workerConn) {
	w.inflight++
	w.idleSince = time.Time{}
}

// redispatchFrom re-sends everything a dead worker still owed us.
//
// This is where at-least-once is actually paid for: the job may have completed
// on the dead worker and died with its result, or never have run at all, and
// nothing here can tell those apart.
func (c *Cluster) redispatchFrom(dead *workerConn) {
	c.mu.Lock()
	var orphans []*pendingJob
	for _, p := range c.pending {
		if p.worker == dead {
			orphans = append(orphans, p)
		}
	}
	c.mu.Unlock()

	if len(orphans) == 0 {
		return
	}
	c.log.Warn("wings: redispatching jobs from lost worker", "worker", dead.id, "jobs", len(orphans))

	for _, p := range orphans {
		job := p.job
		job.Attempt++

		c.mu.Lock()
		w := c.pick()
		if w == nil {
			c.mu.Unlock()
			c.failPending(p, fmt.Errorf("wings: worker %s was lost and no live worker remains", dead.id))
			continue
		}
		if cur, still := c.pending[job.ID]; !still || cur != p {
			c.mu.Unlock()
			continue
		}
		p.worker = w
		p.job = job
		c.charge(w)
		c.mu.Unlock()

		c.journal.record(journalEntry{
			Kind: journalRedispatch, Job: job.ID, Func: job.Func,
			Worker: w.id, Attempt: job.Attempt,
		})

		if _, err := w.jobs.Append(c.ctx, []jobEnvelope{job}); err != nil {
			c.mu.Lock()
			c.release(w)
			c.mu.Unlock()
			c.failPending(p, fmt.Errorf("wings: redispatch to worker %s: %w", w.id, err))
		}
	}
}

func (c *Cluster) failPending(p *pendingJob, err error) {
	c.mu.Lock()
	cur, ok := c.pending[p.job.ID]
	if ok && cur == p {
		delete(c.pending, p.job.ID)
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	c.journal.record(journalEntry{
		Kind: journalFailed, Job: p.job.ID, Func: p.job.Func,
		Attempt: p.job.Attempt, Err: err.Error(),
	})
	select {
	case p.done <- resultEnvelope{ID: p.job.ID, Error: err.Error()}:
	default:
	}
}

// pick chooses the available worker with the least work outstanding.
// Call with mu held.
func (c *Cluster) pick() *workerConn {
	var best *workerConn
	for _, w := range c.workers {
		if !w.available() {
			continue
		}
		if best == nil || w.inflight < best.inflight {
			best = w
		}
	}
	return best
}

// submit places one job and returns a handle to its outcome.
func (c *Cluster) submit(ctx context.Context, fnName string, payload []byte) (*pendingJob, error) {
	job := jobEnvelope{
		ID:      strconv.FormatUint(c.nextID.Add(1), 36),
		Func:    fnName,
		Payload: payload,
	}
	p := &pendingJob{job: job, done: make(chan resultEnvelope, 1)}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("wings: cluster is stopped")
	}
	w := c.pick()
	if w == nil {
		c.mu.Unlock()
		return nil, errors.New("wings: no live workers")
	}
	p.worker = w
	c.pending[job.ID] = p
	c.charge(w)
	c.mu.Unlock()

	if _, err := w.jobs.Append(ctx, []jobEnvelope{job}); err != nil {
		c.mu.Lock()
		delete(c.pending, job.ID)
		c.release(w)
		c.mu.Unlock()
		return nil, fmt.Errorf("wings: submit to worker %s: %w", w.id, err)
	}
	c.journal.record(journalEntry{
		Kind: journalSubmitted, Job: job.ID, Func: job.Func, Worker: w.id,
	})
	return p, nil
}

// await blocks for one job's outcome.
func (c *Cluster) await(ctx context.Context, p *pendingJob) (resultEnvelope, error) {
	select {
	case res := <-p.done:
		return res, nil
	case <-ctx.Done():
		c.mu.Lock()
		if cur, ok := c.pending[p.job.ID]; ok && cur == p {
			delete(c.pending, p.job.ID)
			c.release(p.worker)
		}
		c.mu.Unlock()
		return resultEnvelope{}, ctx.Err()
	case <-c.ctx.Done():
		return resultEnvelope{}, errors.New("wings: cluster stopped while waiting for a result")
	}
}

// Workers reports how many workers are currently in service. Useful when
// autoscaling is on and the count is not something the caller chose.
func (c *Cluster) Workers() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, w := range c.workers {
		if w.available() {
			n++
		}
	}
	return n
}

// Outstanding reports how many jobs have been submitted and not yet answered.
// This is the quantity autoscaling reacts to.
func (c *Cluster) Outstanding() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// Stop shuts the workers down and releases everything the cluster provisioned.
//
// Machines are destroyed. That is the point of provisioning them, but it means
// a cluster you forgot to Stop is one you are still paying for — call it in a
// defer.
func (c *Cluster) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	workers := c.workers
	c.workers = nil
	c.mu.Unlock()

	c.journal.record(journalEntry{Kind: journalClusterStop})

	c.cancel()
	c.wg.Wait()

	var errs []error
	for _, w := range workers {
		errs = append(errs, w.close(ctx))
	}
	// The journal writes through the shared instance, so it must be drained
	// before that instance goes away — and it is drained last, so the entries
	// for the workers just closed are in it.
	c.journal.close()
	// Last: the in-process workers read through it, and every one of them has
	// now stopped.
	errs = append(errs, c.closeShared())
	c.cleanupDir()
	return errors.Join(errs...)
}

func (c *Cluster) cleanupDir() {
	if c.tmpDir != "" {
		_ = os.RemoveAll(c.tmpDir)
	}
}

func (w *workerConn) close(ctx context.Context) error {
	// Every goroutine of this worker's must be gone before anything it reads
	// through is released.
	if w.stop != nil {
		w.stop()
	}
	w.wg.Wait()

	var errs []error
	if w.ownsClient && w.client != nil {
		errs = append(errs, w.client.Close())
	}
	if w.proc != nil {
		// Kill's error is deliberately dropped. The process may already be gone
		// — killed by a test, preempted, or crashed — and on Windows killing an
		// exited-but-unreaped process fails with "Access is denied", which would
		// turn every ordinary shutdown after a worker loss into a Stop error.
		// Wait is what actually establishes it is gone, and it returns
		// immediately for a process that already exited.
		_ = w.proc.Kill()
		_, _ = w.proc.Wait()
	}
	if w.machine != nil {
		errs = append(errs, w.machine.Close(ctx))
	}
	return errors.Join(errs...)
}

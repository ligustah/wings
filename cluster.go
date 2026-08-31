package wings

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

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

	workers []*workerConn
	tmpDir  string

	mu      sync.Mutex
	pending map[string]*pendingJob
	closed  bool

	nextID atomic.Uint64
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
	// node itself closes. Closing it twice takes the broker down under the
	// half of the process still using it.
	ownsClient bool

	node    *workerNode // in-process only
	proc    *os.Process // local-process only
	machine Machine     // remote only

	inflight atomic.Int64
	dead     atomic.Bool
}

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

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &Cluster{
		cfg:     cfg,
		log:     log,
		ctx:     runCtx,
		cancel:  cancel,
		pending: map[string]*pendingJob{},
	}

	dir := cfg.Dir
	if dir == "" {
		var err error
		if dir, err = os.MkdirTemp("", "wings-*"); err != nil {
			cancel()
			return nil, fmt.Errorf("wings: data dir: %w", err)
		}
		c.tmpDir = dir
	}

	workers, err := c.launch(ctx, dir)
	if err != nil {
		cancel()
		c.cleanupDir()
		return nil, err
	}
	c.workers = workers

	for _, w := range workers {
		c.wg.Add(1)
		go c.tail(w)
	}
	log.Info("wings: cluster ready", "workers", len(workers), "target", cfg.Target.kind)
	return c, nil
}

// launch dispatches to the per-target bring-up. Everything after this point
// treats the three the same.
func (c *Cluster) launch(ctx context.Context, dir string) ([]*workerConn, error) {
	n := c.cfg.workers()
	switch c.cfg.Target.kind {
	case targetInProcess:
		return c.launchInProcess(ctx, dir, n)
	case targetLocalProcess:
		return c.launchLocalProcess(ctx, dir, n)
	case targetRemote:
		return c.launchRemote(ctx, n)
	default:
		return nil, fmt.Errorf("wings: unknown target %d", c.cfg.Target.kind)
	}
}

// connect wires a worker's streams onto a backend the coordinator can reach.
// The one function every target funnels through, and the reason a remote worker
// needs no code of its own here.
func (c *Cluster) connect(id string, backend dswire.Backend, owns bool) (*workerConn, error) {
	client := dsclient.Wrap(backend)
	w := &workerConn{id: id, client: client, ownsClient: owns}

	var err error
	if w.jobs, err = client.OpenStream[jobEnvelope](jobStream); err != nil {
		return nil, fmt.Errorf("wings: open %s on worker %s: %w", jobStream, id, err)
	}
	if w.results, err = client.OpenStream[resultEnvelope](resultStream); err != nil {
		return nil, fmt.Errorf("wings: open %s on worker %s: %w", resultStream, id, err)
	}
	return w, nil
}

// tail delivers a worker's results until the cluster stops or the worker dies.
func (c *Cluster) tail(w *workerConn) {
	defer c.wg.Done()

	var from int64
	for {
		recs, err := w.results.ReadBlocking(c.ctx, from, 256)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			c.log.Error("wings: lost worker", "worker", w.id, "err", err)
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
	}
	c.mu.Unlock()
	if !ok {
		return
	}
	p.worker.inflight.Add(-1)
	select {
	case p.done <- res:
	default:
	}
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
		w := c.pick()
		if w == nil {
			c.failPending(p, fmt.Errorf("wings: worker %s was lost and no live worker remains", dead.id))
			continue
		}
		job := p.job
		job.Attempt++

		c.mu.Lock()
		p.worker = w
		p.job = job
		still := c.pending[job.ID] == p
		c.mu.Unlock()
		if !still {
			continue
		}

		w.inflight.Add(1)
		if _, err := w.jobs.Append(c.ctx, []jobEnvelope{job}); err != nil {
			w.inflight.Add(-1)
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
	select {
	case p.done <- resultEnvelope{ID: p.job.ID, Error: err.Error()}:
	default:
	}
}

// pick chooses the live worker with the least work outstanding.
func (c *Cluster) pick() *workerConn {
	var best *workerConn
	var bestN int64
	for _, w := range c.workers {
		if w.dead.Load() {
			continue
		}
		n := w.inflight.Load()
		if best == nil || n < bestN {
			best, bestN = w, n
		}
	}
	return best
}

// submit places one job and returns a channel carrying its outcome.
func (c *Cluster) submit(ctx context.Context, fnName string, payload []byte) (*pendingJob, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("wings: cluster is stopped")
	}
	c.mu.Unlock()

	w := c.pick()
	if w == nil {
		return nil, errors.New("wings: no live workers")
	}

	job := jobEnvelope{
		ID:      strconv.FormatUint(c.nextID.Add(1), 36),
		Func:    fnName,
		Payload: payload,
	}
	p := &pendingJob{job: job, worker: w, done: make(chan resultEnvelope, 1)}

	c.mu.Lock()
	c.pending[job.ID] = p
	c.mu.Unlock()

	w.inflight.Add(1)
	if _, err := w.jobs.Append(ctx, []jobEnvelope{job}); err != nil {
		w.inflight.Add(-1)
		c.mu.Lock()
		delete(c.pending, job.ID)
		c.mu.Unlock()
		return nil, fmt.Errorf("wings: submit to worker %s: %w", w.id, err)
	}
	return p, nil
}

// await blocks for one job's outcome.
func (c *Cluster) await(ctx context.Context, p *pendingJob) (resultEnvelope, error) {
	select {
	case res := <-p.done:
		return res, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, p.job.ID)
		c.mu.Unlock()
		p.worker.inflight.Add(-1)
		return resultEnvelope{}, ctx.Err()
	case <-c.ctx.Done():
		return resultEnvelope{}, errors.New("wings: cluster stopped while waiting for a result")
	}
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
	c.mu.Unlock()

	c.cancel()
	c.wg.Wait()

	var errs []error
	for _, w := range c.workers {
		errs = append(errs, w.close(ctx))
	}
	c.cleanupDir()
	return errors.Join(errs...)
}

func (c *Cluster) cleanupDir() {
	if c.tmpDir != "" {
		_ = os.RemoveAll(c.tmpDir)
	}
}

func (w *workerConn) close(ctx context.Context) error {
	var errs []error
	if w.ownsClient && w.client != nil {
		errs = append(errs, w.client.Close())
	}
	if w.node != nil {
		errs = append(errs, w.node.close())
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

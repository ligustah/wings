package wings

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ligustah/durable_streams/broker/client/dsremote"
	"github.com/ligustah/durable_streams/broker/embed"
	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ligustah/wings/flow"
)

// Cluster is a set of running workers and the means to call work functions on
// them. Create one with [Start].
type Cluster struct {
	cfg Config
	log *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	dir string

	// shared is the cluster's own embedded durable-streams instance (no broker,
	// no port), where the coordinator keeps its journal whatever the target. The
	// in-process target runs every worker's streams on it too.
	shared     *dsclient.Client
	sharedOnce sync.Once
	sharedErr  error
	sharedStop func() error
	// engine is the instance itself, for what only the engine can do: pull
	// a worker's transactions. See pull.go.
	engine *embed.InProcess
	// engineSrv serves the engine on loopback so worker child processes can dial
	// it, for the shared-broker local target. Nil otherwise.
	engineOnce sync.Once
	engineErr  error
	engineSrv  *grpc.Server
	engineAddr string

	// journal is the coordinator's own durable record, on that same instance.
	// It is the one thing the coordinator keeps for itself rather than for a
	// worker, which is why it is here and not on a workerConn.
	journal *journal

	// machines is the write-ahead record of provisioned machines, so a
	// coordinator that restarts knows what it left running.
	machines *machineLog

	// outputs is one mirror over the whole fleet, keeping a copy of everything
	// jobs write. See output.go. Set once before any worker can run a job, and
	// only read after, so it needs no lock.
	outputs *dsclient.MirrorSetHandle
	// relay merges what runs send on shared channels. See channel.go.
	relay *channelRelay

	// dropped names the output streams being deleted right now, so the mirror
	// declines them rather than starting a copy of something on its way out.
	// Guarded by mu.
	dropped map[string]bool

	// mu guards workers, pending, closed and the mutable workerConn fields. One
	// lock, not per-field atomics, because choosing a worker and charging a job
	// to it must be indivisible against the scaler retiring that same worker.
	mu      sync.Mutex
	workers []*workerConn
	pending map[string]*pendingJob
	// byOrigin indexes outstanding jobs by the workflow call they belong to, so
	// a replayed call rejoins its job rather than starting a second.
	byOrigin map[string]*pendingJob
	// ranAs remembers, per workflow call, the last job that ran it, kept past the
	// job being forgotten: a thread of run code dispatched later needs that
	// ancestor's history to replay through. In memory only.
	ranAs  map[string]string
	closed bool

	// epoch identifies this coordinator run and is part of every name it mints,
	// so a restarted coordinator never reuses a name whose durable stream already
	// has a read position. Deliberately not stored — its purpose is to differ.
	epoch string

	nextID  atomic.Uint64
	nextSeq atomic.Uint64
}

// workerConn is one worker as the coordinator sees it: a connection to its
// broker, its two streams, and whatever resource has to be released when it
// goes away.
type workerConn struct {
	id     string
	client *dsclient.Client
	// remote reaches the worker's own broker for its finished transactions,
	// which the client above cannot. Nil in process. See pull.go.
	remote  *dsremote.Client
	jobs    *dsclient.Stream[jobEnvelope]
	results *dsclient.Stream[resultEnvelope]
	// control carries the coordinator's word about a job already here: stop it,
	// or here is a call's answer.
	control *dsclient.Stream[controlEnvelope]
	// nested is the queue of calls made by jobs, taken off regardless of the job
	// queue's depth. See nested.go.
	nested *dsclient.Stream[jobEnvelope]

	// beats is progress from jobs still running here, read on its own goroutine
	// since it concerns unfinished jobs.
	beats *dsclient.Stream[beatEnvelope]
	// beatsFrom is where following beats begins: the stream's end before this
	// worker could be given anything.
	beatsFrom int64

	// ownsClient is false for an in-process worker, whose backend the node
	// closes; closing it twice takes the broker down under the rest of the process.
	ownsClient bool

	node    *workerNode // in-process only; shares the cluster engine, owns nothing
	proc    *os.Process // local-process only
	dir     string      // local-process only: the child's broker directory
	machine Machine     // remote only

	// lease is the machine's identity in the coordinator's own record. Empty
	// for a worker that is not a machine.
	lease string

	// exited closes when an observable worker (a child process) has stopped,
	// separating "gone" from merely "unreachable" — a dropped remote connection
	// waits out the reconnect window instead. Nil when liveness cannot be observed.
	exited chan struct{}

	// mirror copies this worker's results onto the coordinator's own durable
	// streams, and holds the offset to resume reading from.
	mirror *mirror

	// submits is the way onto this worker's queue (submit.go): jobs arriving
	// together go in one append, counted by appends.
	submits chan submission
	appends atomic.Int64

	// pushes names the shared channels being copied onto this worker. Guarded by Cluster.mu.
	pushes map[string]bool

	// ctx bounds every goroutine of this worker, so retiring one while the
	// cluster runs on releases its resources without a goroutine still reading
	// them. wg is how close waits for them.
	ctx  context.Context
	stop context.CancelFunc
	wg   sync.WaitGroup

	// Guarded by Cluster.mu. blocked is how many inflight jobs have a thread
	// waiting; inflight minus blocked is the worker's load.
	inflight  int
	blocked   int
	draining  bool
	idleSince time.Time

	// dead is set by the worker's tail goroutine, which does not hold Cluster.mu.
	dead atomic.Bool
}

// available reports whether new work may be sent here. Call with Cluster.mu.
func (w *workerConn) available() bool { return !w.draining && !w.dead.Load() }

// load is how many jobs are running here, as opposed to waiting. Call with
// Cluster.mu.
func (w *workerConn) load() int { return w.inflight - w.blocked }

// hasExited reports whether this worker is observably gone, as opposed to
// merely unreachable.
func (w *workerConn) hasExited() bool {
	if w.exited == nil {
		return false
	}
	select {
	case <-w.exited:
		return true
	default:
		return false
	}
}

type pendingJob struct {
	job    jobEnvelope
	worker *workerConn
	// placed says the job has been put on a worker at least once; one held for
	// want of a worker has never run, so placing it later is not a retry.
	placed bool
	// ran is which worker each attempt was sent to, so the one worker holding an
	// abandoned attempt's output is the one asked to delete it.
	ran map[int]*workerConn

	// done closes once when the job has an outcome; a channel, not a value, since
	// a retried workflow can rejoin a call and a job may have several waiters.
	done chan struct{}
	res  resultEnvelope
	once sync.Once
	// waiters counts who is still interested; the last to give up abandons the job.
	waiters int
	// origin is the larger work this job is a step of, kept so a redispatch or
	// failure records against the right run after the caller's context is gone.
	origin flow.Origin

	// bounds declared on the function, read once at submit.
	bounds flow.Bounds

	// The calls this job makes when it is a run (nested.go): followed is which
	// attempts' histories are being read, children the calls not yet answered,
	// answers every answer by call position so a replayed call is not re-run.
	followed map[int]bool
	children int
	answers  map[string]resultEnvelope
	// blocked says the worker reported a thread of this job waiting.
	blocked bool
	// yield is set while the job is off every worker by its own choice. See yield.go.
	yield *yieldEnvelope
	// recovered says a restarted coordinator took this job from its journal;
	// incomplete says nothing has forked it in this process yet, so it has no input.
	recovered  bool
	incomplete bool
	// since is when the current attempt was dispatched, started when the worker
	// began it, beat when it last reported progress. The work's bounds run from
	// started, not since, so time queued is not charged to the work.
	since   time.Time
	started time.Time
	beat    time.Time
	// checkpoint is the last progress reported, handed to the next attempt.
	checkpoint []byte
}

// overdue reports whether a job has run out of time and which bound it hit, so
// each gets its own remedy. A job not yet begun is measured against the start
// bound alone. Call with mu held.
func (p *pendingJob) overdue(now time.Time) (stuck bool, tooSlow bool) {
	if p.started.IsZero() {
		if p.bounds.Start > 0 && !p.since.IsZero() && now.Sub(p.since) > p.bounds.Start {
			stuck = true
		}
		return stuck, false
	}
	if p.bounds.Timeout > 0 && now.Sub(p.started) > p.bounds.Timeout {
		tooSlow = true
	}
	// A job waiting on a thread it forked or on a channel is not stuck; its total
	// bound still runs.
	if p.bounds.Heartbeat > 0 && p.children == 0 && !p.blocked {
		last := p.beat
		if last.IsZero() {
			last = p.started
		}
		if now.Sub(last) > p.bounds.Heartbeat {
			stuck = true
		}
	}
	return stuck, tooSlow
}

// Start brings up the workers described by cfg. In a worker process it never
// returns: the binary serves work until shutdown, so code after it is
// coordinator-only. See the package doc.
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

	if err := cfg.Scaling.validate(); err != nil {
		return nil, err
	}
	// A fixed worker count is a policy of that many kept at that many, so a lost
	// machine is replaced rather than left short until Stop.
	if !cfg.Scaling.enabled() {
		cfg.Scaling = fixedFleet(cfg.workers())
	}
	cfg.Scaling = cfg.Scaling.withDefaults(cfg.Concurrency)

	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &Cluster{
		cfg:      cfg,
		log:      log,
		ctx:      runCtx,
		cancel:   cancel,
		pending:  map[string]*pendingJob{},
		byOrigin: map[string]*pendingJob{},
		ranAs:    map[string]string{},
		epoch:    newEpoch(),
	}

	c.dir = cfg.Dir
	if c.dir == "" {
		c.dir = defaultDataDir
	}
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		cancel()
		return nil, fmt.Errorf("wings: data dir: %w", err)
	}

	// Opened before any worker exists, so the record starts at the run's start.
	client, err := c.sharedClient()
	if err != nil {
		cancel()
		return nil, err
	}
	if c.journal, err = openJournal(ctx, client, log, c.epoch); err != nil {
		cancel()
		_ = c.closeShared()
		return nil, err
	}
	if c.machines, err = c.openMachineLog(ctx); err != nil {
		cancel()
		c.journal.close()
		_ = c.closeShared()
		return nil, err
	}

	c.journal.record(journalEntry{Kind: journalClusterStart})

	// fail undoes everything brought up so far — goroutines, workers (released so
	// their leases close), then the record and engine — on one path.
	fail := func(err error, workers []*workerConn) (*Cluster, error) {
		cancel()
		release := context.WithoutCancel(ctx)
		for _, w := range workers {
			_ = c.releaseWorker(release, w)
		}
		c.wg.Wait()
		c.journal.close()
		_ = c.closeShared()
		return nil, err
	}

	c.startChannelRelay()
	if err := c.startOutputMirror(); err != nil {
		return fail(err, nil)
	}

	// Before provisioning: recover whatever a previous coordinator left running
	// and still billing, so those machines count towards the number wanted and a
	// restart does not double the cluster.
	workers, err := c.reattach(ctx)
	if err != nil {
		return fail(err, nil)
	}
	// And its outstanding jobs, before any worker is read, so an arriving result
	// finds its job.
	if err := c.recoverJobs(ctx, workers); err != nil {
		return fail(err, workers)
	}

	n := cfg.Scaling.initialWorkers(cfg.workers()) - len(workers)
	if n > 0 {
		fresh, err := c.launch(ctx, n)
		if err != nil {
			return fail(err, workers)
		}
		workers = append(workers, fresh...)
	}
	for _, w := range workers {
		c.adopt(w)
	}

	c.wg.Go(c.watchdog)
	c.wg.Go(c.autoscale)
	if cfg.Scaling.fixed() {
		log.Info("wings: cluster ready", "workers", len(workers), "target", cfg.Target.kind)
	} else {
		log.Info("wings: cluster ready", "workers", len(workers), "target", cfg.Target.kind,
			"autoscale", fmt.Sprintf("%d..%d", cfg.Scaling.Min, cfg.Scaling.Max))
	}
	return c, nil
}

// releaseWorker closes a worker and its machine's lease together — anything
// taking a worker out of service goes through here. The lease closes only once
// the machine is gone, so a crash between the two errs towards looking for a
// machine that no longer exists rather than one that bills unwatched.
func (c *Cluster) releaseWorker(ctx context.Context, w *workerConn) error {
	err := w.close(ctx)
	c.dropWorkerStreams(ctx, w)
	if w.lease == "" {
		return err
	}
	return errors.Join(err, c.machines.write(ctx, machineRecord{
		Kind: machineReleased, Lease: w.lease, Worker: w.id,
	}))
}

// dropWorkerStreams removes what a worker that is not coming back left on the
// coordinator's storage. Streams are named per worker and names are never reused,
// so on a persistent Dir they would otherwise accumulate across autoscale cycles.
// Best effort: what cannot be removed is a leak, not a failure to stop.
func (c *Cluster) dropWorkerStreams(ctx context.Context, w *workerConn) {
	client, err := c.sharedClient()
	if err != nil {
		return
	}
	names := []string{mirrorStreamFor(w.id)}
	if w.client == client {
		names = append(names, jobStreamFor(w.id), resultStreamFor(w.id), beatStreamFor(w.id), controlStreamFor(w.id), nestedStreamFor(w.id))
	}
	for _, name := range names {
		if err := dropStream(ctx, client, name); err != nil {
			c.log.Warn("wings: could not remove a gone worker's stream", "worker", w.id, "stream", name, "err", err)
		}
	}
	if w.dir != "" {
		if err := os.RemoveAll(w.dir); err != nil {
			c.log.Warn("wings: could not remove a gone worker's directory", "worker", w.id, "dir", w.dir, "err", err)
		}
	}
}

// adopt puts a freshly launched worker into service and starts tailing it.
func (c *Cluster) adopt(w *workerConn) {
	// Beats are followed from the stream's end: older ones concern jobs no
	// longer outstanding. Measured now, while nothing can be sent here yet.
	if info, err := w.beats.Info(c.ctx); err != nil {
		if c.ctx.Err() == nil {
			c.log.Warn("wings: cannot find the end of a worker's heartbeats; following from the start",
				"worker", w.id, "err", err)
		}
	} else {
		w.beatsFrom = info.Newest + 1
	}

	c.mu.Lock()
	w.idleSince = time.Now()
	c.workers = append(c.workers, w)
	c.mu.Unlock()

	c.journal.record(journalEntry{Kind: journalWorkerUp, Worker: w.id})

	// Counted twice: on the worker so close waits for its loops, on the cluster
	// so Stop waits for all of them.
	loop := func(run func(*workerConn)) {
		c.wg.Add(1)
		w.wg.Go(func() {
			defer c.wg.Done()
			run(w)
		})
	}
	loop(c.tail)
	loop(c.tailBeats)
	loop(c.submitter)
	loop(c.pull)

	// So the mirror keeps this worker's output at once, not after a discovery pass.
	c.pokeOutputs()

	c.placeHeld()
}

// placeHeld sends jobs that were waiting for a worker, now that one has arrived.
func (c *Cluster) placeHeld() {
	var held []*pendingJob
	c.mu.Lock()
	for _, p := range c.pending {
		// Skip a job waiting by choice (yield), or with nothing to send or do.
		if p.worker == nil && p.yield == nil && !p.incomplete && !p.finished() {
			held = append(held, p)
		}
	}
	c.mu.Unlock()
	if len(held) == 0 {
		return
	}
	c.log.Info("wings: a worker arrived; placing the jobs that were waiting for one", "jobs", len(held))
	for _, p := range held {
		c.moveJob(p, "a worker arrived")
	}
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

// workerID mints a name unique for the cluster's life; a reused name would make
// two workers share a transactional id and fence the live one.
func (c *Cluster) workerID(prefix string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, c.epoch, c.nextSeq.Add(1)-1)
}

// connect wires a worker's streams onto a backend the coordinator can reach —
// the one function every target funnels through.
func (c *Cluster) connect(id string, client *dsclient.Client, owns bool) (*workerConn, error) {
	w := &workerConn{id: id, client: client, ownsClient: owns,
		submits: make(chan submission, submitBatch)}
	w.ctx, w.stop = context.WithCancel(c.ctx)

	var err error
	if w.jobs, err = client.OpenStream[jobEnvelope](jobStreamFor(id)); err != nil {
		return nil, fmt.Errorf("wings: open %s on worker %s: %w", jobStreamFor(id), id, err)
	}
	if w.results, err = client.OpenStream[resultEnvelope](resultStreamFor(id)); err != nil {
		return nil, fmt.Errorf("wings: open %s on worker %s: %w", resultStreamFor(id), id, err)
	}
	if w.beats, err = client.OpenStream[beatEnvelope](beatStreamFor(id)); err != nil {
		return nil, fmt.Errorf("wings: open %s on worker %s: %w", beatStreamFor(id), id, err)
	}
	if w.control, err = client.OpenStream[controlEnvelope](controlStreamFor(id)); err != nil {
		return nil, fmt.Errorf("wings: open %s on worker %s: %w", controlStreamFor(id), id, err)
	}
	if w.nested, err = client.OpenStream[jobEnvelope](nestedStreamFor(id)); err != nil {
		return nil, fmt.Errorf("wings: open %s on worker %s: %w", nestedStreamFor(id), id, err)
	}
	if w.mirror, err = c.openMirror(c.ctx, id); err != nil {
		return nil, err
	}
	return w, nil
}

// connectBackend is connect for a backend the coordinator owns outright — the
// out-of-process targets, whose client dies with the worker.
func (c *Cluster) connectBackend(id string, backend dswire.Backend) (*workerConn, error) {
	w, err := c.connect(id, dsclient.Wrap(backend), true)
	if err != nil {
		return nil, err
	}
	if r, ok := backend.(*dsremote.Client); ok {
		w.remote = r
	}
	return w, nil
}

// boundsOf is what a function declared about itself, or nothing for one this
// binary does not define.
func boundsOf(name string) flow.Bounds {
	b, _ := flow.BoundsOf(name)
	return b
}

// sharedClient is the cluster's own embedded durable-streams instance — home to
// the journal, the machine record and the copies of what jobs write — created on
// first use and torn down by [Cluster.Stop]. The in-process target runs its
// workers on it too.
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
		c.engine = b
	})
	return c.shared, c.sharedErr
}

// serveEngine serves the coordinator's engine on loopback so worker child
// processes can dial it, for the shared-broker local target. Served once; the
// address is stable for the cluster's life.
func (c *Cluster) serveEngine() (string, error) {
	if _, err := c.sharedClient(); err != nil {
		return "", err
	}
	c.engineOnce.Do(func() {
		srv, lis, err := serveBroker(c.engine.Service(), "127.0.0.1:0", c.log)
		if err != nil {
			c.engineErr = err
			return
		}
		c.engineSrv, c.engineAddr = srv, lis.Addr().String()
	})
	return c.engineAddr, c.engineErr
}

// closeShared releases the embedded instance, once every worker that reads
// through it is gone.
func (c *Cluster) closeShared() error {
	if c.sharedStop == nil {
		return nil
	}
	// The client wraps the engine's backend, which Close also releases; only one may.
	err := c.sharedStop()
	c.sharedStop, c.shared = nil, nil
	return err
}

// tail mirrors a worker's results onto the coordinator's streams and delivers
// them, until the cluster stops or the worker is genuinely gone — a transient
// read failure is not death, since the worker's queue survives a dropped connection.
func (c *Cluster) tail(w *workerConn) {
	from := w.mirror.next

	var (
		trouble  time.Time // when the current run of failures began
		attempts int
		// batch is how many results one read asks for; halved when a batch of
		// legal results is together too large to carry.
		batch = resultBatch
	)

	for {
		if w.ctx.Err() != nil || w.dead.Load() {
			return
		}

		// Checked before the read: a wedged connection hangs rather than fails,
		// so a window enforced only after a read is one a hung read never reaches.
		if !trouble.IsZero() && time.Since(trouble) >= c.cfg.reconnect() {
			c.log.Error("wings: worker did not come back", "worker", w.id, "after", time.Since(trouble))
			c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: "unreachable"})
			w.dead.Store(true)
			c.redispatchFrom(w)
			return
		}

		// Every read is bounded: a broken connection can hang forever, and while
		// in trouble the bound is the remaining window so retries cannot outlast it.
		var (
			recs []dsclient.OffsetRecord[resultEnvelope]
			err  error
		)
		if trouble.IsZero() {
			readCtx, cancel := context.WithTimeout(w.ctx, pollInterval)
			recs, err = w.results.ReadBlocking(readCtx, from, batch)
			expired := readCtx.Err() != nil
			cancel()
			// Our own poll expiring on a healthy worker is just an idle interval,
			// judged by the context since gRPC's error does not wrap it.
			if err != nil && expired && w.ctx.Err() == nil {
				continue
			}
		} else {
			// A worker in trouble is asked a non-blocking question, so a recovered
			// one with nothing to deliver is still seen to be back.
			limit := min(c.cfg.reconnect()-time.Since(trouble), 5*time.Second)
			readCtx, cancel := context.WithTimeout(w.ctx, limit)
			recs, err = w.results.Read(readCtx, from, batch)
			cancel()
		}

		if err != nil {
			if w.ctx.Err() != nil {
				return
			}
			if w.dead.Load() {
				return // already retired deliberately
			}

			// A too-large message is neither a broken link nor a dead worker:
			// usually a batch of legal results, so ask for fewer.
			if status.Code(err) == codes.ResourceExhausted {
				if batch > 1 {
					batch /= 2
					continue
				}
				// A single record that cannot be carried: nothing decodes, so
				// nothing says which job it was. Skipped; it stays outstanding
				// until its own bound settles it.
				c.log.Error("wings: skipping a result too large for the connection to carry",
					"worker", w.id, "offset", from, "err", err,
					"hint", "the worker that produced it was built from different source; "+
						"output this size belongs on a flow.Channel, streamed rather than returned")
				from++
				continue
			}

			// A worker we can see has exited is dead now; no reason to wait out
			// the reconnect window.
			if w.hasExited() {
				c.log.Error("wings: worker exited", "worker", w.id, "err", err)
				c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: "process exited"})
				w.dead.Store(true)
				c.redispatchFrom(w)
				return
			}

			if trouble.IsZero() {
				trouble, attempts = time.Now(), 0
				c.log.Warn("wings: lost contact with worker, retrying",
					"worker", w.id, "err", err, "giving_up_after", c.cfg.reconnect())
			}
			// The reconnect window is for a dropped network connection; a machine
			// the cloud confirms is gone is not coming back, so do not wait it out.
			if c.machineGone(w) {
				c.log.Error("wings: worker's machine is gone", "worker", w.id, "after", time.Since(trouble))
				c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: "machine is gone"})
				w.dead.Store(true)
				c.redispatchFrom(w)
				return
			}
			attempts++
			if !sleepCtx(w.ctx, reconnectBackoff(attempts)) {
				return
			}
			continue
		}

		if !trouble.IsZero() {
			c.log.Info("wings: worker is back", "worker", w.id, "after", time.Since(trouble))
			trouble, attempts = time.Time{}, 0
		}
		// Grown back after a read that fit, so a run of large results is not
		// paid for by a permanently timid batch.
		batch = min(batch*2, resultBatch)

		for _, r := range recs {
			// Mirrored before delivery, so a result a caller saw completed is
			// never one a recovery would see outstanding.
			if err := w.mirror.append(w.ctx, r.Offset, r.Record); err != nil {
				if w.ctx.Err() != nil {
					return
				}
				// Coordinator storage failing is not the worker's fault and a
				// re-read would not fix it: report, but still deliver.
				c.log.Error("wings: could not mirror result", "worker", w.id, "err", err)
			}
			from = r.Offset + 1
			c.deliver(r.Record)
		}
	}
}

// machineGone asks a worker's machine, if it can be asked, whether it has ceased
// to exist. Only a definite answer counts; an unanswered question is not gone.
func (c *Cluster) machineGone(w *workerConn) bool {
	p, ok := w.machine.(Prober)
	if !ok {
		return false
	}
	ctx, cancel := context.WithTimeout(w.ctx, 10*time.Second)
	defer cancel()
	alive, err := p.Alive(ctx)
	if err != nil {
		c.log.Debug("wings: could not ask whether a worker's machine is alive", "worker", w.id, "err", err)
		return false
	}
	return !alive
}

// reconnectBackoff grows to a ceiling, so a long outage costs a handful of
// attempts rather than thousands.
func reconnectBackoff(attempt int) time.Duration {
	d := time.Duration(attempt) * 500 * time.Millisecond
	return min(d, 5*time.Second)
}

// sleepCtx waits, and reports false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// deliver hands one result to whoever is waiting for it. An unknown id (a
// restart replaying old results) and a result from a moved-away attempt are both
// ignored: the latter's outputs are being deleted, so delivering it would hand
// the caller handles to streams on their way out.
func (c *Cluster) deliver(res resultEnvelope) {
	c.mu.Lock()
	p, ok := c.pending[res.ID]
	if ok && res.Attempt != p.job.Attempt {
		c.mu.Unlock()
		c.log.Info("wings: ignoring a result from an attempt that was moved away",
			"job", res.ID, "attempt", res.Attempt, "current", p.job.Attempt)
		return
	}
	if ok && res.Yield != nil {
		c.yieldLocked(p, res.Yield)
		c.mu.Unlock()
		c.journal.record(journalEntry{
			Kind: journalYielded, Job: p.job.ID, Func: p.job.Func,
			Attempt: p.job.Attempt, Err: res.Yield.describe(), Yield: res.Yield,
		}.from(p.origin))
		if c.yieldSettled(p, res.Yield) {
			c.wake(p, "what it was waiting for had already arrived")
		}
		return
	}
	var wake *pendingJob
	if ok {
		wake = c.noteSettledLocked(p, res)
		if p.recovered && p.waiters == 0 {
			// Recovered and not yet re-forked: the result is kept, unclaimed,
			// for the fork that will, and taken off its worker.
			c.unblockLocked(p)
			c.release(p.worker)
			p.worker = nil
		} else {
			c.forget(p)
			c.release(p.worker)
		}
	}
	c.mu.Unlock()
	if wake != nil {
		c.wake(wake, "the thread it was waiting for finished")
	}
	if !ok {
		return
	}
	c.journal.record(journalEntry{
		Kind: journalCompleted, Job: res.ID, Func: p.job.Func,
		Worker: workerID(p.worker), Attempt: p.job.Attempt, Err: res.Error,
	}.from(p.origin))
	p.settle(res)
}

// unblockLocked takes back a job's report that it is waiting: it woke, or
// it is leaving the worker that said so. Call with mu held.
func (c *Cluster) unblockLocked(p *pendingJob) {
	if !p.blocked {
		return
	}
	p.blocked = false
	if p.worker != nil && p.worker.blocked > 0 {
		p.worker.blocked--
	}
}

// forget takes a job out of the outstanding set. Call with mu held.
func (c *Cluster) forget(p *pendingJob) {
	if cur, ok := c.pending[p.job.ID]; !ok || cur != p {
		return
	}
	c.unblockLocked(p)
	delete(c.pending, p.job.ID)
	// Abandoned attempts left outputs nobody holds a handle to; drop them. Not
	// while stopping: this can run on a caller's uncounted goroutine, and closed
	// is set under this lock before Stop waits, so seeing it clear means the add
	// to the wait group lands first.
	if p.job.Attempt > 0 && !c.closed {
		job, keep := p.job.ID, p.job.Attempt
		writers := maps.Clone(p.ran)
		c.wg.Go(func() {
			c.dropOutputsOf(job, keep, writers)
		})
	}
	if key := p.origin.Key(); key != "" {
		if cur, ok := c.byOrigin[key]; ok && cur == p {
			delete(c.byOrigin, key)
			// A later thread of run code descending from this call replays
			// through its history; ranAs remembers which job to ask. See threadHistory.
			c.ranAs[key] = p.job.ID
		}
	}
}

// settle publishes a job's outcome to everyone waiting on it, exactly once.
func (p *pendingJob) settle(res resultEnvelope) {
	p.once.Do(func() {
		p.res = res
		close(p.done)
	})
}

// workerID names the worker a job was on, or nothing when it never reached one.
func workerID(w *workerConn) string {
	if w == nil {
		return ""
	}
	return w.id
}

// release credits a finished job back to its worker. Call with mu held. A nil
// worker is expected: a job briefly has none while moveJob fails it.
func (c *Cluster) release(w *workerConn) {
	if w == nil {
		return
	}
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

// redispatchFrom re-sends everything a dead worker still owed us. This is where
// at-least-once is paid for: a job may have finished on the dead worker and died
// with its result, and nothing here can tell that from one that never ran.
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
		c.moveJob(p, fmt.Sprintf("worker %s was lost", dead.id))
	}
}

// moveJob sends one outstanding job to a different worker. The job keeps its id,
// gains an attempt, and carries its last checkpoint so a long job does not
// restart from nothing. The old worker is credited back, or it would never be
// reaped and its machine would bill on until the cluster stopped.
func (c *Cluster) moveJob(p *pendingJob, why string) { c.move(p, why, true) }

// move is moveJob, with a say in whether the move counts against the job's
// attempt budget. A move on suspicion is counted (so a job that kills every
// worker is eventually given up on); a move for balance, of a job that has not
// started, is not.
func (c *Cluster) move(p *pendingJob, why string, counted bool) {
	c.mu.Lock()
	if cur, still := c.pending[p.job.ID]; !still || cur != p {
		c.mu.Unlock()
		return
	}
	c.unblockLocked(p)
	from, left := p.worker, p.job.Attempt
	if p.incomplete {
		// Recovered from the journal, which has no input: nothing to send until
		// the replay forks it again. Held until then.
		if from != nil {
			c.release(from)
		}
		p.worker = nil
		p.since, p.started, p.beat = time.Time{}, time.Time{}, time.Time{}
		job := p.job
		c.mu.Unlock()
		c.log.Warn("wings: a recovered job has nothing to run it on; holding it until it is forked again",
			"job", job.ID, "fn", job.Func, "why", why)
		c.journal.record(journalEntry{
			Kind: journalHeld, Job: job.ID, Func: job.Func, Attempt: left, Err: why,
		}.from(p.origin))
		c.stopOn(from, job.ID, left, why)
		return
	}
	if counted && p.placed && p.job.Attempt+1 >= c.cfg.attempts() {
		if from != nil {
			c.release(from)
			p.worker = nil
		}
		c.mu.Unlock()
		c.stopOn(from, p.job.ID, left, why)
		c.failPending(p, fmt.Errorf("wings: %s gave up after %d attempts; last: %s",
			p.job.Func, c.cfg.attempts(), why))
		return
	}
	w := c.pickBut(from)
	if w == nil {
		// Nowhere to send it now. The fleet is kept at size, so a replacement is
		// coming; the job waits with no worker and its clocks stopped until adopt
		// places it, rather than failing on the spot.
		if from != nil {
			c.release(from)
		}
		p.worker = nil
		p.since, p.started, p.beat = time.Time{}, time.Time{}, time.Time{}
		// Copied under the lock: adopt may place this job (rewriting p.job)
		// before the lines below run.
		job := p.job
		c.mu.Unlock()
		c.log.Warn("wings: no live worker for a job; holding it until one arrives",
			"job", job.ID, "fn", job.Func, "why", why)
		c.journal.record(journalEntry{
			Kind: journalHeld, Job: job.ID, Func: job.Func, Attempt: left, Err: why,
		}.from(p.origin))
		c.stopOn(from, job.ID, left, why)
		return
	}
	if from != nil {
		c.release(from)
	}
	job := p.job
	// A held job placed for the first time is not on its second attempt: nothing ran.
	if p.placed {
		job.Attempt++
	}
	p.placed = true
	job.Checkpoint = p.checkpoint
	p.job = job
	p.worker = w
	if p.ran == nil {
		p.ran = map[int]*workerConn{}
	}
	p.ran[job.Attempt] = w
	p.since = time.Now()
	p.started = time.Time{}
	p.beat = time.Time{}
	c.charge(w)
	c.mu.Unlock()

	c.journal.record(journalEntry{
		Kind: journalRedispatch, Job: job.ID, Func: job.Func,
		Worker: w.id, Attempt: job.Attempt, Err: why,
	}.from(p.origin))

	// Off the watchdog's goroutine (copying a large recording must not stall a
	// sweep), but on the wait group so it does not outlive Stop and run against a
	// closed engine.
	c.wg.Go(func() {
		// Stop the abandoned attempt so its slot is not lost, but only after its
		// outputs are level below: stopping first would cut that copy short.
		defer c.stopOn(from, job.ID, left, why)
		// What abandoned attempts recorded reaches the retry only through here;
		// their handles never left in a result. Wait for the old worker's writes
		// to reach the coordinator first, or the retry resumes from less than survived.
		c.drainOutputs(c.ctx, from, job.ID)

		priors, err := c.priorsOf(c.ctx, w, job.ID)
		if err != nil {
			c.log.Warn("wings: could not look for what a job recorded",
				"job", job.ID, "err", err)
		}
		job.Priors = priors
		if err := c.hydrate(c.ctx, w, job.Priors); err != nil {
			// Not fatal: a retry that cannot read its predecessor's recordings
			// starts over, which is slow but correct.
			c.log.Warn("wings: could not give a retry its predecessor's recordings",
				"job", job.ID, "worker", w.id, "err", err)
			job.Priors = nil
		}
		if err := c.hydrateHistory(c.ctx, w, job); err != nil {
			// Same trade: a retry without its history starts from the top.
			c.log.Warn("wings: could not give a retry its predecessor's history",
				"job", job.ID, "worker", w.id, "err", err)
		}
		if err := c.hydrateLineage(c.ctx, w, job); err != nil {
			// Not the same trade: a thread of run code without its ancestors
			// cannot run at all, so the attempt fails and the next is tried elsewhere.
			c.log.Warn("wings: could not give a thread of run code its ancestors' histories",
				"job", job.ID, "worker", w.id, "err", err)
		}
		if err := c.send(c.ctx, w, job); err != nil {
			c.log.Warn("wings: could not hand a moved job to a worker; moving it again",
				"job", job.ID, "worker", w.id, "err", err)
			c.move(p, fmt.Sprintf("could not hand it to %s: %v", w.id, err), true)
		}
	})
}

// onBeat records that a job is still alive, and where it has got to. A beat for
// an unknown id, or from a superseded attempt, is ignored: crediting the latter
// would reset the retry's clock and could roll its checkpoint back.
func (c *Cluster) onBeat(b beatEnvelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pending[b.Job]
	if !ok || b.Attempt != p.job.Attempt {
		return
	}
	now := time.Now()
	// Any beat marks the job started: the first report can go missing, and a
	// visibly progressing job must not stay exempt from its bounds.
	if p.started.IsZero() {
		p.started = now
	}
	// The attempt is running, so its history is being written: follow it for
	// the calls it makes. Once per attempt, from whichever beat comes first.
	c.followHistory(p, b.Attempt)
	if b.Started {
		// The one report that is not progress. Leaving beat alone keeps the
		// heartbeat clock honest: it runs from started until the job actually
		// says something.
		return
	}
	p.beat = now
	if b.Wait != "" && !p.blocked {
		p.blocked = true
		if p.worker != nil {
			p.worker.blocked++
		}
	}
	if b.Woke {
		c.unblockLocked(p)
	}
	if len(b.Checkpoint) > 0 {
		p.checkpoint = b.Checkpoint
	}
}

// watchdog moves jobs that have gone quiet and fails ones that have run too
// long.
//
// One goroutine for the whole cluster rather than a timer per job: the
// interesting quantity is a deadline that has already passed, and scanning a
// map of outstanding jobs once a second costs nothing next to the work they
// represent.
func (c *Cluster) watchdog() {
	t := time.NewTicker(watchdogInterval)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case now := <-t.C:
			c.sweep(now)
			c.rebalance(now)
			c.reapDead()
		}
	}
}

func (c *Cluster) sweep(now time.Time) {
	var (
		stuck []*pendingJob
		whys  []string
		slow  []*pendingJob
		due   []*pendingJob
	)

	c.mu.Lock()
	for _, p := range c.pending {
		if p.finished() {
			continue // settled, kept for a fork that has not come yet
		}
		if p.yield != nil {
			if !p.yield.Until.IsZero() && !now.Before(p.yield.Until) {
				due = append(due, p)
			}
			continue
		}
		isStuck, isSlow := p.overdue(now)
		switch {
		case isSlow:
			// Before isStuck: a job over its total bound is failed, not moved to
			// spend the same time again elsewhere.
			slow = append(slow, p)
		case isStuck:
			// The reason is named under the lock, since which bound it hit
			// depends on state the lock guards.
			why := fmt.Sprintf("no heartbeat for %s", p.bounds.Heartbeat)
			if p.started.IsZero() {
				why = fmt.Sprintf("not started within %s", p.bounds.Start)
			}
			stuck = append(stuck, p)
			whys = append(whys, why)
		}
	}
	c.mu.Unlock()

	for _, p := range slow {
		c.log.Warn("wings: job exceeded its timeout", "job", p.job.ID, "fn", p.job.Func,
			"timeout", p.bounds.Timeout)
		c.failPending(p, fmt.Errorf("wings: %s exceeded its %s timeout", p.job.Func, p.bounds.Timeout))
	}
	for i, p := range stuck {
		c.log.Warn("wings: job is overdue, moving it", "job", p.job.ID,
			"fn", p.job.Func, "worker", workerID(p.worker), "why", whys[i])
		c.moveJob(p, whys[i])
	}
	for _, p := range due {
		c.wake(p, "its sleep is over")
	}
}

func (c *Cluster) failPending(p *pendingJob, err error) {
	c.mu.Lock()
	cur, ok := c.pending[p.job.ID]
	var wake *pendingJob
	if ok && cur == p {
		wake = c.noteSettledLocked(p, resultEnvelope{ID: p.job.ID, Error: err.Error()})
		c.forget(p)
	}
	w, attempt := p.worker, p.job.Attempt
	c.mu.Unlock()
	if !ok || cur != p {
		return
	}
	if wake != nil {
		c.wake(wake, "the thread it was waiting for failed")
	}
	// A job failed for taking too long is usually still running it; tell the
	// worker to stop. Off the watchdog's goroutine, on the wait group.
	c.wg.Go(func() {
		c.stopOn(w, p.job.ID, attempt, err.Error())
	})
	c.journal.record(journalEntry{
		Kind: journalFailed, Job: p.job.ID, Func: p.job.Func,
		Worker: workerID(p.worker), Attempt: p.job.Attempt, Err: err.Error(),
	}.from(p.origin))
	p.settle(resultEnvelope{ID: p.job.ID, Error: err.Error()})
}

// stopOn tells a worker to stop an attempt nobody wants the answer to. Best
// effort: a worker that cannot hear runs the attempt out and its result is
// dropped. When it works it reclaims the slot the attempt was holding.
func (c *Cluster) stopOn(w *workerConn, job string, attempt int, why string) {
	if w == nil || w.control == nil || w.dead.Load() {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), 10*time.Second)
	defer cancel()
	if _, err := w.control.Append(ctx, []controlEnvelope{{Job: job, Attempt: attempt, Why: why}}); err != nil {
		c.log.Debug("wings: could not tell a worker to stop a job", "worker", w.id, "job", job, "err", err)
	}
}

// pick chooses the available worker with the least work outstanding.
// Call with mu held.
func (c *Cluster) pick() *workerConn { return c.pickBut(nil) }

// pickBut is pick, preferring anywhere but one worker — a job is usually moved
// because of where it was. The avoided worker is still used when it is the only one.
func (c *Cluster) pickBut(avoid *workerConn) *workerConn {
	var best, fallback *workerConn
	for _, w := range c.workers {
		if !w.available() {
			continue
		}
		if w == avoid {
			if fallback == nil || w.load() < fallback.load() {
				fallback = w
			}
			continue
		}
		if best == nil || w.load() < best.load() {
			best = w
		}
	}
	if best != nil {
		return best
	}
	return fallback
}

// submit places one job and returns a handle to its outcome.
func (c *Cluster) submit(ctx context.Context, fnName string, payload []byte) (*pendingJob, error) {
	return c.submitJob(ctx, jobEnvelope{Func: fnName, Payload: payload})
}

// submitJob is submit for a job already described: a function on its input, or a
// thread of run code by its lineage. Everything but what the job does is filled in here.
func (c *Cluster) submitJob(ctx context.Context, job jobEnvelope) (*pendingJob, error) {
	job.ID = c.epoch + "-" + strconv.FormatUint(c.nextID.Add(1), 36)
	// The fleet's capacity travels with the job, so a thread that fans out reads
	// the cluster's parallelism, not one worker's.
	job.Capacity = c.maxParallelism()
	p := &pendingJob{
		job:    job,
		done:   make(chan struct{}),
		bounds: boundsOf(job.Func),
		origin: flow.OriginFrom(ctx),
	}
	job.Run, job.Thread = p.origin.Run, p.origin.Thread

	key := p.origin.Key()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("wings: cluster is stopped")
	}
	job.Nested = c.parentJobLocked(p.origin) != nil
	p.job = job
	// A replayed call whose predecessor had not finished rejoins that job rather
	// than dispatching a second copy of the same work.
	if key != "" {
		if live, ok := c.byOrigin[key]; ok {
			live.waiters++
			// A recovered job has been held waiting for exactly this fork, which
			// carries its input.
			place := live.complete(job)
			c.mu.Unlock()
			c.journal.record(journalEntry{
				Kind: journalAttached, Job: live.job.ID, Func: live.job.Func,
				Worker: workerID(live.worker), Attempt: live.job.Attempt,
			}.from(live.origin))
			if place {
				c.moveJob(live, "recovered with nothing to run it on, and now forked again")
			}
			return live, nil
		}
	}
	w := c.pick()
	if w == nil {
		// No worker right now. The fleet is kept at size, so one is coming; the
		// job is held until adopt places it, bounded by the caller's context,
		// rather than failed outright.
		p.waiters = 1
		p.ran = map[int]*workerConn{}
		c.pending[job.ID] = p
		if key != "" {
			c.byOrigin[key] = p
		}
		c.mu.Unlock()
		c.log.Warn("wings: no live worker for a new job; holding it until one arrives",
			"job", job.ID, "fn", job.Func)
		c.journal.record(journalEntry{
			Kind: journalHeld, Job: job.ID, Func: job.Func, Err: "no live worker",
		}.from(p.origin))
		return p, nil
	}
	p.worker = w
	p.placed = true
	p.ran = map[int]*workerConn{job.Attempt: w}
	p.since = time.Now()
	p.waiters = 1
	c.pending[job.ID] = p
	if key != "" {
		c.byOrigin[key] = p
	}
	c.charge(w)
	c.mu.Unlock()

	// A thread of run code is reached through its ancestors, whose histories the
	// worker must have before the job: see lineage.go.
	err := c.hydrateLineage(ctx, w, job)
	if err == nil {
		err = c.send(ctx, w, job)
	}
	if err != nil {
		// The worker is failing, not the job: it was picked a moment ago and no
		// longer answers. Move the job rather than fail it, but counted, so a job
		// nothing can ever be handed to fails rather than loops.
		c.log.Warn("wings: could not hand a job to a worker; moving it",
			"job", job.ID, "worker", w.id, "err", err)
		c.move(p, fmt.Sprintf("could not hand it to %s: %v", w.id, err), true)
		return p, nil
	}
	c.journal.record(journalEntry{
		Kind: journalSubmitted, Job: job.ID, Func: job.Func, Worker: w.id,
	}.from(p.origin))
	return p, nil
}

// await blocks for one job's outcome.
func (c *Cluster) await(ctx context.Context, p *pendingJob) (resultEnvelope, error) {
	select {
	case <-p.done:
		c.mu.Lock()
		p.waiters--
		if p.recovered && p.waiters <= 0 {
			// Kept unclaimed until now. See deliver.
			c.forget(p)
		}
		c.mu.Unlock()
		return p.res, nil

	case <-ctx.Done():
		// Abandoned only by the last caller still interested: a retried workflow
		// can have one attempt give up while the next has already rejoined.
		c.mu.Lock()
		p.waiters--
		if p.waiters <= 0 {
			if cur, ok := c.pending[p.job.ID]; ok && cur == p {
				c.forget(p)
				c.release(p.worker)
				// Just credited back a job the worker may still be running; tell
				// it to stop. Off this goroutine, and not once Stop is waiting.
				if w := p.worker; w != nil && !c.closed {
					job, attempt := p.job.ID, p.job.Attempt
					c.wg.Go(func() {
						c.stopOn(w, job, attempt, "its caller gave up")
					})
				}
			}
		}
		c.mu.Unlock()
		return resultEnvelope{}, ctx.Err()

	case <-c.ctx.Done():
		c.mu.Lock()
		p.waiters--
		c.mu.Unlock()
		// A context error, not the job's answer: to the waiting run this is an
		// interruption, to be resumed by the next coordinator.
		return resultEnvelope{}, fmt.Errorf("wings: cluster stopped while waiting for a result: %w", context.Canceled)
	}
}

// Workers reports how many workers are currently in service.
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

// Stop shuts the workers down and releases everything the cluster provisioned,
// destroying any machines it created. Call it in a defer: an un-stopped cluster
// keeps billing.
func (c *Cluster) Stop(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	// A copy: a worker that dies from here on is deleted from c.workers in place.
	workers := slices.Clone(c.workers)
	c.mu.Unlock()

	c.journal.record(journalEntry{Kind: journalClusterStop})

	// Drain each worker's outputs before the mirror is cancelled and its machine
	// destroyed, or a persistent Dir would keep only the front of a file whose
	// handle promised the whole of it. The workers must still be in c.workers
	// here, since that is the fleet the mirror answers for. All at once, or a
	// fleet's worth of bounded drains in a row would make Stop slow.
	var drains sync.WaitGroup
	for _, w := range workers {
		drains.Go(func() {
			c.drainOutputs(ctx, w, "")
		})
	}
	drains.Wait()

	c.mu.Lock()
	c.workers = nil
	c.mu.Unlock()

	c.cancel()
	c.wg.Wait()

	// All at once: releasing a cloud machine can take most of a minute, and a
	// whole fleet in sequence would make Stop a long, still-billing wait.
	errs := make([]error, len(workers))
	var releases sync.WaitGroup
	for i, w := range workers {
		releases.Go(func() {
			errs[i] = c.releaseWorker(ctx, w)
		})
	}
	releases.Wait()
	// The journal writes through the shared instance, so drain it before that
	// instance goes away, and after the workers so their closing entries are in it.
	c.journal.close()
	// After the workers that dialed it are gone, stop serving the engine, then
	// release it last — the in-process workers read through it.
	if c.engineSrv != nil {
		c.engineSrv.GracefulStop()
	}
	errs = append(errs, c.closeShared())
	return errors.Join(errs...)
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
		// Kill's error is dropped: the process may already be gone, and on Windows
		// killing an exited-but-unreaped process fails, which would make every
		// shutdown after a worker loss a Stop error.
		_ = w.proc.Kill()
		// The watcher goroutine owns Wait; wait for it, since two concurrent Waits
		// on one process is undefined.
		if w.exited != nil {
			select {
			case <-w.exited:
			case <-time.After(10 * time.Second):
			}
		}
	}
	if w.machine != nil {
		errs = append(errs, w.machine.Close(ctx))
	}
	return errors.Join(errs...)
}

// tailBeats follows one worker's progress reports. Beats are read from the end
// of the stream (only the latest checkpoint is wanted), from the offset adopt
// captured before the worker could be given any job — captured here instead, the
// first beats of a job dispatched in between would fall behind the start and be missed.
func (c *Cluster) tailBeats(w *workerConn) {
	from := w.beatsFrom

	for {
		if w.ctx.Err() != nil || w.dead.Load() {
			return
		}
		readCtx, cancel := context.WithTimeout(w.ctx, pollInterval)
		recs, err := w.beats.ReadBlocking(readCtx, from, 256)
		expired := readCtx.Err() != nil
		cancel()
		if err != nil {
			if w.ctx.Err() != nil {
				return
			}
			// Nothing here declares a worker dead: that is the result tail's job,
			// and two goroutines racing to the same verdict only muddy it.
			if !expired {
				select {
				case <-w.ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
			continue
		}
		for _, r := range recs {
			from = r.Offset + 1
			if r.Record.Leaving {
				// The worker itself saying its machine is being taken back — not a
				// guess. Same path as a death: what it owed is moved, the machine released.
				c.log.Warn("wings: worker is leaving; its machine is being taken back", "worker", w.id)
				c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: "preempted"})
				w.dead.Store(true)
				c.redispatchFrom(w)
				return
			}
			c.onBeat(r.Record)
		}
	}
}

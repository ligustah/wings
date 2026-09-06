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
	// engine is the instance itself, for what only the engine can do: pull
	// a worker's transactions. See pull.go.
	engine *embed.InProcess

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
	// byOrigin indexes outstanding jobs by the workflow call they belong to,
	// so a workflow attempt that replays a call finds the one its predecessor
	// was making rather than starting a second.
	byOrigin map[string]*pendingJob
	// ranAs remembers, per workflow call, the last job that ran it, kept past
	// the job being forgotten. A thread of run code dispatched later — a
	// descendant spawned or redispatched after an ancestor's job is already
	// gone — needs that ancestor's history to replay through, and the history
	// outlives the job (it is named by job id, and forget keeps the last
	// attempt). byOrigin does not outlive it, so without this the lookup falls
	// to the coordinator's own store, where a thread that ran as a job never
	// wrote. In memory only: a restarted coordinator rebuilds outstanding jobs
	// from its journal, and a call whose job was already done needs no rerun.
	ranAs  map[string]string
	closed bool

	// epoch identifies THIS run of the coordinator, and is part of every name
	// it mints.
	//
	// Names outlive the process that chose them: a worker's mirror stream is
	// named after the worker and sits on a persistent Dir, and a job id appears
	// in results that are still on a worker's queue. A counter that restarts at
	// zero therefore hands a new worker a name whose stream already has a read
	// position — so its results are skipped as already seen, and every job sent
	// to it hangs. Reattachment made that reachable within one process, since
	// recovered workers keep the names they were started with.
	//
	// Deliberately NOT stored. Its whole purpose is to differ from last time,
	// and a value read back from disk is the one thing that cannot.
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
	// remote is the connection to the worker's own broker, for what a worker
	// with an engine of its own publishes and the client above cannot
	// reach: its finished transactions. Nil for an in-process worker. See
	// pull.go.
	remote  *dsremote.Client
	jobs    *dsclient.Stream[jobEnvelope]
	results *dsclient.Stream[resultEnvelope]
	// control is the coordinator's word to this worker about a job already
	// on it: stop this one, nobody wants the answer.
	control *dsclient.Stream[controlEnvelope]
	// nested is the queue of calls made by jobs, which the worker takes off
	// without regard to how full its job queue is. See nested.go.
	nested *dsclient.Stream[jobEnvelope]

	// beats is progress reported by jobs still running here. Read on its own
	// goroutine rather than with results, because it says something about a
	// job that has NOT finished and waiting for the result stream to produce
	// would defeat the purpose.
	beats *dsclient.Stream[beatEnvelope]
	// beatsFrom is where following beats begins: the end of the stream as it
	// was before this worker could be given anything to beat about.
	beatsFrom int64

	// ownsClient is false for an in-process worker, whose backend the worker
	// node itself closes. Closing it twice takes the broker down under the half
	// of the process still using it.
	ownsClient bool

	node    *workerNode // in-process only; shares the cluster engine, owns nothing
	proc    *os.Process // local-process only
	dir     string      // local-process only: the child's broker directory
	machine Machine     // remote only

	// lease is the machine's identity in the coordinator's own record. Empty
	// for a worker that is not a machine.
	lease string

	// exited closes when a worker we can actually observe has stopped.
	//
	// It is what separates "gone" from "unreachable", and the distinction is the
	// whole reason retrying is safe. A child process is a fact: we started it,
	// we can wait on it, and once it has exited no amount of patience brings it
	// back. A remote machine across a dropped connection is not a fact — it is
	// probably fine — so that case waits out the reconnect window instead. Nil
	// for a worker whose liveness cannot be observed directly.
	exited chan struct{}

	// mirror copies this worker's results onto the coordinator's own durable
	// streams, and holds the offset to resume reading from.
	mirror *mirror

	// submits is the way onto this worker's queue. See submit.go: jobs that
	// arrive together go in one append, and appends counts how many there
	// were.
	submits chan submission
	appends atomic.Int64

	// pushes names the shared channels being copied onto this worker.
	// Guarded by Cluster.mu.
	pushes map[string]bool

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

	// Guarded by Cluster.mu. blocked is how many of the inflight jobs have
	// reported a thread waiting; what the worker is running is the
	// difference, and that is its load.
	inflight  int
	blocked   int
	draining  bool
	idleSince time.Time

	// dead is set by the worker's own tail goroutine, which does not hold
	// Cluster.mu, so it stays atomic.
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
	// placed says the job has been put on some worker at least once. A job
	// that could not be — every worker was gone — is held, with no worker,
	// until one arrives, and placing it then is not a retry: it never ran.
	//
	// Guarded by Cluster.mu.
	placed bool
	// ran is which worker each attempt was sent to, by attempt number. What an
	// abandoned attempt wrote is on the worker that ran it and nowhere else, so
	// this is the one worker to delete it from when the job settles — rather
	// than asking every worker in the fleet whether it holds each stream.
	//
	// Guarded by Cluster.mu.
	ran map[int]*workerConn

	// done closes once, when the job has an outcome, and res is that outcome.
	//
	// A closed channel rather than a value on one because a job can now have
	// more than one waiter: a workflow that is retried rejoins the call its
	// previous attempt was making instead of dispatching a second copy of it,
	// and a value channel delivers to exactly one of them.
	done chan struct{}
	res  resultEnvelope
	once sync.Once
	// waiters counts who is still interested. A caller that gives up abandons
	// the job only when it was the last one — the point of rejoining is that
	// the work carries on.
	//
	// Guarded by Cluster.mu.
	waiters int
	// origin is what larger piece of work this job is a step of, kept so a
	// redispatch or a failure can be recorded against the same run as the
	// submit — the caller's context is long gone by then.
	origin flow.Origin

	// bounds are those declared on the function. Read once at submit so the
	// watchdog does not go through the registry per job per tick.
	bounds flow.Bounds

	// The calls this job makes, when it is a run that makes them. See
	// nested.go. followed says which attempts' histories are being read for
	// calls; children counts the calls dispatched and not yet answered; and
	// answers keeps every answer by the call's position, so an attempt that
	// replays a call already answered is handed the answer again rather than
	// having the work done twice.
	//
	// All guarded by Cluster.mu.
	followed map[int]bool
	children int
	answers  map[string]resultEnvelope
	// blocked says the job's worker reported a thread of it waiting, and
	// has not yet reported it woken. Guarded by Cluster.mu.
	blocked bool
	// yield is set while the job is off every worker by its own choice,
	// waiting for what would bring it back. See yield.go. Guarded by
	// Cluster.mu.
	yield *yieldEnvelope
	// recovered says a restarted coordinator took this job back from its
	// journal rather than dispatching it (recover.go), and incomplete that
	// nothing has yet forked it in this process: the envelope has no input,
	// so it cannot be sent anywhere. Both guarded by Cluster.mu.
	recovered  bool
	incomplete bool
	// since is when the current attempt was dispatched, started when the
	// worker reported beginning it, and beat when it last reported progress.
	//
	// The bounds on the work run from started, not since: a job can sit behind
	// others on a busy worker for longer than its own timeout, and none of that
	// is time the work took. Until started is set neither bound applies, and
	// only the start bound does. Zero beat means it has not beaten yet, which
	// is why the heartbeat clock then runs from started: a function that
	// declares a heartbeat timeout and never beats must be caught, not
	// exempted.
	//
	// Guarded by Cluster.mu.
	since   time.Time
	started time.Time
	beat    time.Time
	// checkpoint is the last progress reported, and is handed to the next
	// attempt so it resumes rather than starting over.
	checkpoint []byte
}

// overdue reports whether a job has run out of time, and why. Call with mu
// held.
//
// The two bounds mean different things and get different remedies, so this
// answers with which one was hit rather than with a bare yes.
//
// A job the worker has not yet begun is measured against the start bound
// alone. Its timeout and heartbeat bounds are about the work, and charging them
// for a queue the job is waiting in would fail a quick job for being behind a
// slow one — or move it, to the back of another queue, until it ran out of
// attempts having never once run.
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
	// A job waiting on a thread it forked is quiet for as long as the thread
	// takes, and is not stuck: the coordinator itself is running what it is
	// waiting for. Nor is one whose worker says it is waiting — on a channel,
	// on the clock. Its total bound still runs.
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

	if err := cfg.Scaling.validate(); err != nil {
		return nil, err
	}
	// A plain worker count IS a policy: that many, kept at that many. One
	// loop maintains the fleet whichever way it was asked for, so a fixed
	// fleet that loses a machine to a preemption gets it back rather than
	// running short until Stop.
	if !cfg.Scaling.enabled() {
		cfg.Scaling = fixedFleet(cfg.workers())
	}
	// Normalised once, here, so nothing downstream has to ask whether a field
	// was set — the scaling loop reads its policy as given.
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
	if c.journal, err = openJournal(ctx, client, log, c.epoch); err != nil {
		cancel()
		_ = c.closeShared()
		c.cleanupDir()
		return nil, err
	}
	if c.machines, err = c.openMachineLog(ctx); err != nil {
		cancel()
		c.journal.close()
		_ = c.closeShared()
		c.cleanupDir()
		return nil, err
	}

	c.journal.record(journalEntry{Kind: journalClusterStart})

	// fail undoes everything above and everything between here and a
	// successful return: whatever goroutines have started, whatever workers
	// were brought up — released properly, so a machine that was destroyed
	// has its lease closed — and then the record and the engine. One path
	// rather than one per failure, because the path that was written by hand
	// for a late failure was the one that leaked.
	fail := func(err error, workers []*workerConn) (*Cluster, error) {
		cancel()
		release := context.WithoutCancel(ctx)
		for _, w := range workers {
			_ = c.releaseWorker(release, w)
		}
		c.wg.Wait()
		c.journal.close()
		_ = c.closeShared()
		c.cleanupDir()
		return nil, err
	}

	// Before any worker exists. The mirror reads the fleet afresh on every
	// pass, so it has nothing to wait for — and a failure here costs nothing,
	// where a failure after the machines were up used to cost the machines.
	c.startChannelRelay()
	if err := c.startOutputMirror(); err != nil {
		return fail(err, nil)
	}

	// Before provisioning anything: whatever a previous coordinator left
	// running is still billing, and is either put back to work or destroyed.
	// Doing this first also means the machines it recovers count towards the
	// number wanted, so a restart does not double the cluster.
	//
	// Only reachable with a persistent Dir. With a temporary one the record is
	// created fresh and empty every time, which is correct: nothing was left.
	workers, err := c.reattach(ctx)
	if err != nil {
		return fail(err, nil)
	}
	// And whatever it left running on them, before any of them is read:
	// a result that arrives must find its job.
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

// releaseWorker closes a worker and closes its machine's lease.
//
// The two belong together, and they did not used to: a worker retired by the
// scaler or reaped after dying was closed without the record being told, so its
// lease stayed open and every future start went hunting for a machine that had
// been destroyed on purpose. Anything that takes a worker out of service goes
// through here.
//
// The lease is closed only once the machine is actually gone, so a crash
// between the two leaves it open and the next start looks for it — which is the
// safe direction to be wrong in. A machine that is looked for and not found
// costs one API call; one that is never looked for bills forever.
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
// coordinator's storage.
//
// A worker's streams are named after it, and a name is never reused, so on a
// persistent Dir every worker that was ever retired, reaped or stopped left its
// mirror behind — and an in-process worker its queue, results and beats too,
// since those are on the shared engine — and every autoscale cycle minted more.
// A local worker's broker directory is the same leak on disk.
//
// Best effort: a copy that could not be removed is a leak, not a worker that
// failed to stop, and the caller is releasing a machine.
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
	// Beats are followed from the END of the stream: whatever a previous
	// coordinator's jobs reported is about jobs no longer outstanding. The end
	// is measured now, while nothing can be sent to this worker yet. Newest is
	// the log end, and the beat stream is not transactional — nothing appends
	// to it inside a transaction — so the record after it is the next one
	// anybody will write.
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

	// Each loop is counted twice: on the worker, so closing it can wait for
	// its own loops, and on the cluster, so Stop can wait for all of them.
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

	// A machine the mirror has not been told about yet. It will find this one on
	// its own eventually, and eventually is a long time to be writing output
	// nothing is keeping.
	c.pokeOutputs()

	c.placeHeld()
}

// placeHeld sends the jobs that were waiting for a worker to the fleet as it
// is now. Called when a worker arrives, which is the only time the answer to
// "is there anywhere to send this" changes from no to yes.
func (c *Cluster) placeHeld() {
	var held []*pendingJob
	c.mu.Lock()
	for _, p := range c.pending {
		// Not one that is off every worker by choice: that one is waiting
		// for something other than a worker. Nor one with nothing to send
		// yet, or nothing left to do.
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

// workerID mints a name unique for the life of the cluster. Reusing an index
// after a worker is torn down would make two workers share a transactional id
// on the broker, which fences the live one.
func (c *Cluster) workerID(prefix string) string {
	return fmt.Sprintf("%s-%s-%d", prefix, c.epoch, c.nextSeq.Add(1)-1)
}

// connect wires a worker's streams onto a backend the coordinator can reach.
// The one function every target funnels through, and the reason a remote worker
// needs no code of its own here.
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
// out-of-process targets, where the client exists only to talk to this one
// worker and dies with it.
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

// sharedClient is the cluster's own embedded durable-streams instance, created
// on first use and torn down by [Cluster.Stop].
//
// Every target uses it: the journal, the machine record and the copies of
// what jobs write all live here, whatever a worker is. The in-process target
// goes further and runs its workers on it too, which is what makes that target
// honest: coordinator and workers are the same process, so there is nothing to
// serve over a socket and nothing to dial — they open the same streams on the
// same engine. Above this line the coordinator sees a *dsclient.Client either
// way, which is why one connect serves every target.
// boundsOf is what a function declared about itself, or nothing for one this
// binary does not define — the worker will refuse that job and say so.
func boundsOf(name string) flow.Bounds {
	b, _ := flow.BoundsOf(name)
	return b
}

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

// tail mirrors a worker's results onto the coordinator's own streams and
// delivers them, until the cluster stops or the worker is genuinely gone.
//
// "Genuinely" is the change from a version that treated any error as death. A
// transient read failure — a five-second network blip, a broker restarting — is
// indistinguishable at this line from a machine that burned down, and giving up
// on the first one meant redispatching the jobs of a healthy worker and then
// destroying the VM that was still holding them. The worker's queue survives a
// dropped connection by design; throwing the worker away was the coordinator
// declining to use that.
func (c *Cluster) tail(w *workerConn) {
	from := w.mirror.next

	var (
		trouble  time.Time // when the current run of failures began
		attempts int
		// batch is how many results one read asks for. Every result is under
		// maxResult on its own, but a read returns many, and a batch of large
		// ones can together exceed what one message carries. That is not a
		// dead worker; it is a read that asked for too much.
		batch = resultBatch
	)

	for {
		if w.ctx.Err() != nil || w.dead.Load() {
			return
		}

		// Checked BEFORE the read, not only after it. A wedged connection does
		// not fail — it hangs — so a window enforced only on the way out of a
		// read is a window a hung read never reaches. This is the difference
		// between giving up in the configured time and never giving up at all.
		if !trouble.IsZero() && time.Since(trouble) >= c.cfg.reconnect() {
			c.log.Error("wings: worker did not come back", "worker", w.id, "after", time.Since(trouble))
			c.journal.record(journalEntry{Kind: journalWorkerGone, Worker: w.id, Err: "unreachable"})
			w.dead.Store(true)
			c.redispatchFrom(w)
			return
		}

		// EVERY read is bounded, including the healthy one. A broken connection
		// does not always fail: a read issued on one can simply never return,
		// and an unbounded read there is a worker that is neither delivering
		// results nor being given up on — the worst of both. While in trouble
		// the bound is the remaining window, so retries cannot outlast it.
		var (
			recs []dsclient.OffsetRecord[resultEnvelope]
			err  error
		)
		if trouble.IsZero() {
			readCtx, cancel := context.WithTimeout(w.ctx, pollInterval)
			recs, err = w.results.ReadBlocking(readCtx, from, batch)
			expired := readCtx.Err() != nil
			cancel()
			// Our own poll expiring on a healthy worker is not news: it means
			// nothing was produced in that interval, which is what an idle
			// worker looks like. Judged by the context, not the error: over
			// gRPC the error is a status that does not wrap the context's, and
			// a worker with nothing to say for thirty seconds was being taken
			// for one that had gone quiet.
			if err != nil && expired && w.ctx.Err() == nil {
				continue
			}
		} else {
			// A worker in trouble is asked a question it can answer at once. A
			// blocking read on an idle worker times out however healthy the
			// link is, so one that had recovered but had no result to deliver
			// could never be seen to be back; a plain read of whatever is
			// there returns immediately on a live link, empty or not.
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

			// A message too large is not a link that might come back, and it
			// is not a dead worker either. Nearly always it is a batch of
			// legal results that is too much at once, so ask for fewer. Only
			// a single record that cannot be carried is a genuine oversized
			// result — which a worker built from this source never sends — and
			// even that costs the one job, not the worker holding it and the
			// machine under it, which used to be declared dead and destroyed.
			if status.Code(err) == codes.ResourceExhausted {
				if batch > 1 {
					batch /= 2
					continue
				}
				// Nothing can be decoded, so nothing says which job it was. It
				// stays outstanding until its own bound settles it; the record
				// is left where it is, since a resume would only skip it again.
				c.log.Error("wings: skipping a result too large for the connection to carry",
					"worker", w.id, "offset", from, "err", err,
					"hint", "the worker that produced it was built from different source; "+
						"a result this size belongs in a wings.Artifact")
				from++
				continue
			}

			// A worker we can see has exited is dead now, not in two minutes.
			// Waiting out the window for it would leave its jobs unredispatched
			// for no reason at all.
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
			// A cloud that can say the machine is gone is not waited for.
			// The window exists because a dropped connection is usually the
			// network; a preempted or deleted instance is not coming back,
			// and its jobs would sit unmoved for the whole of it.
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
		// Grown back after a read that fit, so a run of large results costs a
		// few smaller reads rather than a permanently timid one.
		batch = min(batch*2, resultBatch)

		for _, r := range recs {
			// Written down before it is handed over, so a result a caller saw
			// completed is never one a recovery would see outstanding.
			if err := w.mirror.append(w.ctx, r.Offset, r.Record); err != nil {
				if w.ctx.Err() != nil {
					return
				}
				// The coordinator's own storage failing is not the worker's
				// fault and retrying the read would not fix it, so this is
				// reported and the result still delivered: losing the record is
				// bad, losing the work as well is worse.
				c.log.Error("wings: could not mirror result", "worker", w.id, "err", err)
			}
			from = r.Offset + 1
			c.deliver(r.Record)
		}
	}
}

// machineGone asks a worker's machine, if it is one that can be asked, whether
// it has ceased to exist. Only a definite no counts: an unanswered question is
// the ordinary reconnect wait.
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

// deliver hands one result to whoever is waiting for it.
//
// An unknown id is normal rather than alarming: a worker's stream survives the
// coordinator that wrote to it, so a restart against a persistent Dir replays
// results nobody is waiting for any more.
//
// So is a result from an attempt that was moved away. The job was taken off
// that worker on the suspicion it was stuck, and a worker that was merely slow
// finishes anyway. Its answer is not the answer: the retry is the attempt the
// job now is, its worker was already credited back when the job left it, and
// its outputs are exactly what the coordinator deletes once the job settles.
// Delivering it would hand the caller handles to streams on their way out.
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
			// Recovered, and nothing has forked it again yet: the result is
			// kept, unclaimed, for the fork that will. Off its worker,
			// which has no more to do with it.
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
	// Every attempt but the one that produced the result wrote something
	// nobody holds a handle to. Only worth looking when there WAS an earlier
	// attempt, which is rare.
	//
	// Not once the cluster is stopping. This can be reached from a caller's
	// own goroutine — one giving up on a result — which is not counted in the
	// wait group, and adding to a group that Stop may already be waiting on
	// is a misuse. closed is set under this same lock before Stop waits, so
	// seeing it clear here means the add lands first.
	if p.job.Attempt > 0 && !c.closed {
		job, keep := p.job.ID, p.job.Attempt
		writers := maps.Clone(p.ran)
		// On the cluster's wait group, so Stop does not close the storage this
		// is deleting through while it is still deleting.
		c.wg.Go(func() {
			c.dropOutputsOf(job, keep, writers)
		})
	}
	if key := p.origin.Key(); key != "" {
		if cur, ok := c.byOrigin[key]; ok && cur == p {
			delete(c.byOrigin, key)
			// Remember which job last ran this call, so a thread of run code
			// that descends from it and is placed after this can still be
			// given its history to replay through. See threadHistory and ranAs.
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

// release credits a finished job back to its worker. Call with mu held.
//
// A job briefly has no worker: moveJob credits the old one and clears it
// before dropping the lock to fail the job, and a caller giving up on that
// job in between finds it still pending and releases whatever it holds. That
// worker was already credited, so there is nothing to do — and there used to
// be a nil dereference instead.
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
		c.moveJob(p, fmt.Sprintf("worker %s was lost", dead.id))
	}
}

// moveJob sends one outstanding job to a different worker.
//
// The job keeps its id and gains an attempt, and it carries whatever
// checkpoint the last attempt reported — so a long job that was most of the way
// through does not start from nothing. That is what makes moving one affordable
// enough to do on suspicion rather than only on certainty.
//
// The worker it came from is credited back. It has to be: a worker is reaped
// only once nothing is outstanding on it, so a dead worker whose jobs were
// moved away without this was never reaped, and its machine billed on until the
// cluster stopped — which is precisely the case reaping exists for.
func (c *Cluster) moveJob(p *pendingJob, why string) { c.move(p, why, true) }

// move is moveJob, with a say in whether the move counts against the job.
//
// A move on suspicion — a worker lost, a job gone quiet — is an attempt that
// may have run, and counted so that a job which kills every worker it lands
// on is eventually given up on. A move for balance is of a job that has not
// started, from a queue it was merely waiting on, and counting that would
// have a job fail for having been moved to where it could run sooner.
func (c *Cluster) move(p *pendingJob, why string, counted bool) {
	c.mu.Lock()
	if cur, still := c.pending[p.job.ID]; !still || cur != p {
		c.mu.Unlock()
		return
	}
	c.unblockLocked(p)
	from, left := p.worker, p.job.Attempt
	if p.incomplete {
		// Recovered from the journal, which has no input: there is nothing
		// to send until the replay forks it again. Held until then.
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
	// At-least-once has no natural end, and a job that kills whatever worker it
	// lands on would be moved forever while the caller waited on a cluster that
	// merely looked busy.
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
		// Nowhere to send it, for now. The fleet is kept at its size, so a
		// replacement is on its way, and this job waits for it — held with
		// no worker, its clocks stopped, until adopt places it. It used to
		// be failed on the spot, which made every preemption of the last
		// machine a failed run.
		if from != nil {
			c.release(from)
		}
		p.worker = nil
		p.since, p.started, p.beat = time.Time{}, time.Time{}, time.Time{}
		// Copied under the lock: the worker this waits for may arrive and
		// place it before the lines below run, and placing rewrites p.job.
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
	// A held job being placed for the first time is not on its second
	// attempt: nothing ran.
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

	// Off the watchdog's goroutine: a retry carrying a large recording has to
	// have it put on the new worker first, and a sweep must not wait on a copy.
	//
	// On the cluster's wait group, so Stop waits for it. Every caller of this
	// is itself counted, so the count cannot be zero here — and left uncounted
	// it would outlive Stop and finish its move against a journal and an
	// engine that had already been closed.
	c.wg.Go(func() {
		// The attempt being left behind may well still be running — a worker
		// that was merely slow finishes anyway — and its answer is not the
		// answer. Stop it, so the slot it holds is not lost to an attempt
		// nobody will read. After the copy of what it wrote is level, since
		// stopping it first would cut that short.
		defer c.stopOn(from, job.ID, left, why)
		// What the abandoned attempts recorded goes with the job. Their handles
		// never left — a handle only ever leaves in a result, and an attempt
		// that was moved produced none — so this is the only way the work they
		// did reaches the attempt that has to redo it.
		// What the old worker recorded may not have reached the coordinator
		// yet; a job can stall sooner than the mirror looks. Wait for it before
		// deciding what the retry gets, or the retry resumes from less than
		// actually survived.
		c.drainOutputs(c.ctx, from, job.ID)

		priors, err := c.priorsOf(c.ctx, w, job.ID)
		if err != nil {
			c.log.Warn("wings: could not look for what a job recorded",
				"job", job.ID, "err", err)
		}
		job.Priors = priors
		if err := c.hydrate(c.ctx, w, job.Priors); err != nil {
			// Not fatal. A retry that cannot read what its predecessor wrote
			// starts from the beginning, which is slow but correct; refusing to
			// run it at all is neither.
			c.log.Warn("wings: could not give a retry its predecessor's recordings",
				"job", job.ID, "worker", w.id, "err", err)
			job.Priors = nil
		}
		if err := c.hydrateHistory(c.ctx, w, job); err != nil {
			// The same trade: a retry without its history starts the function
			// from the top, which is at-least-once doing what it says.
			c.log.Warn("wings: could not give a retry its predecessor's history",
				"job", job.ID, "worker", w.id, "err", err)
		}
		if err := c.hydrateLineage(c.ctx, w, job); err != nil {
			// Not the same trade: a thread of run code without its ancestors
			// cannot be reached at all. The attempt fails on the worker, and
			// the next is tried elsewhere.
			c.log.Warn("wings: could not give a thread of run code its ancestors' histories",
				"job", job.ID, "worker", w.id, "err", err)
		}
		if err := c.send(c.ctx, w, job); err != nil {
			// As at submit: the worker's failing, not the job's.
			c.log.Warn("wings: could not hand a moved job to a worker; moving it again",
				"job", job.ID, "worker", w.id, "err", err)
			c.move(p, fmt.Sprintf("could not hand it to %s: %v", w.id, err), true)
		}
	})
}

// onBeat records that a job is still alive, and where it has got to.
//
// A beat for an id nobody is waiting for is ordinary rather than alarming: the
// job may have just finished, or been moved elsewhere, and the worker's report
// was already in flight. So is one from an attempt the job has moved on from:
// the worker it was left on may wake and report, and that report says nothing
// about the attempt now running — crediting it would reset the retry's clock
// and could replace its checkpoint with an older one.
func (c *Cluster) onBeat(b beatEnvelope) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pending[b.Job]
	if !ok || b.Attempt != p.job.Attempt {
		return
	}
	now := time.Now()
	// Any beat says the job is running, not only the one that says so: a
	// worker's first report can go missing like any other, and a job that is
	// visibly making progress must not stay exempt from its bounds for it.
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
			// Checked first. A job that has blown its total bound is over
			// whether or not it was also quiet, and moving it would only spend
			// the same time again somewhere else.
			slow = append(slow, p)
		case isStuck:
			// Named under the lock, since which bound it was depends on state
			// only the lock guards. The two are different complaints: one is
			// about a job that went quiet, the other about a worker that never
			// began it.
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
	// A job failed for taking too long is usually still taking it. The worker
	// enforces the same bound itself, but only the bound it knows. Off this
	// goroutine, which is the watchdog's: every caller of this is counted in
	// the group, so the count cannot be zero here.
	c.wg.Go(func() {
		c.stopOn(w, p.job.ID, attempt, err.Error())
	})
	c.journal.record(journalEntry{
		Kind: journalFailed, Job: p.job.ID, Func: p.job.Func,
		Worker: workerID(p.worker), Attempt: p.job.Attempt, Err: err.Error(),
	}.from(p.origin))
	p.settle(resultEnvelope{ID: p.job.ID, Error: err.Error()})
}

// stopOn tells a worker to stop an attempt nobody wants the answer to.
//
// Best effort, and cheap to be wrong about: a worker that is gone, or one too
// old to read the control stream, simply runs the attempt to its end as it
// always did, and the result is dropped as it always was. What this buys when
// it works is the slot — a worker credited back for a job it is still running
// is a worker the picker overloads and the scaler may retire mid-job.
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

// pickBut is pick, preferring anywhere but one worker.
//
// A job being moved is usually being moved BECAUSE of where it was — a machine
// that stopped answering, or one whose clock says it is stuck — and sending it
// straight back there wastes the whole timeout again. The old worker is still
// the answer when it is the only one, since a retry on a busy worker beats no
// retry at all.
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

// submitJob is submit for a job already described: a function on its input,
// or a thread of run code by its lineage. Everything but what the job does
// is filled in here.
func (c *Cluster) submitJob(ctx context.Context, job jobEnvelope) (*pendingJob, error) {
	job.ID = c.epoch + "-" + strconv.FormatUint(c.nextID.Add(1), 36)
	p := &pendingJob{
		job:    job,
		done:   make(chan struct{}),
		bounds: boundsOf(job.Func),
		// Read off the context rather than passed in: only a workflow sets it,
		// and threading a parameter nobody else supplies through every caller
		// would make the ordinary case pay for the special one.
		origin: flow.OriginFrom(ctx),
	}
	// The job is the thread the origin names, and the worker runs it as
	// that. Where it goes on the worker follows from what it is, not from a
	// parameter: a thread whose parent is itself a job on a worker is nested.
	job.Run, job.Thread = p.origin.Run, p.origin.Thread

	key := p.origin.Key()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("wings: cluster is stopped")
	}
	job.Nested = c.parentJobLocked(p.origin) != nil
	p.job = job
	// A workflow attempt that replays a call its predecessor had not finished
	// rejoins that job instead of dispatching a second one. Two copies of an
	// hour of work would be a waste on their own; worse, the copy starts from
	// nothing while the original is most of the way through, holding the
	// checkpoint and the steps that make it cheap to move.
	if key != "" {
		if live, ok := c.byOrigin[key]; ok {
			live.waiters++
			// A job a restarted coordinator recovered has been waiting for
			// exactly this: the fork that carries its input.
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
		// No worker right now — every machine gone at once, or the fleet
		// still being replaced. The fleet is kept at its size, so one is
		// coming; the job is held until adopt places it, and the caller's
		// own context bounds the wait. Refusing here made a call that landed
		// in the gap between a preemption and its replacement fail outright.
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

	// A thread of run code is reached through its ancestors, whose
	// histories the worker must have before the job: see lineage.go.
	err := c.hydrateLineage(ctx, w, job)
	if err == nil {
		err = c.send(ctx, w, job)
	}
	if err != nil {
		// The worker, not the work: it was picked a moment ago and does not
		// answer now — dying, as a rule, its jobs about to be moved off it
		// — so this job is moved off it too, rather than failed, which
		// made a fork that landed on a machine in its last second the
		// thread's own failure: one its parent, waiting on a channel the
		// thread was to feed, never saw. Counted against the job, so that
		// one nothing can be handed to fails rather than loops.
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
		// Abandoned only by the LAST caller still interested. A workflow that
		// is retried has one attempt giving up while the next has already
		// rejoined, and dropping the job there would throw away the work the
		// retry is counting on.
		c.mu.Lock()
		p.waiters--
		if p.waiters <= 0 {
			if cur, ok := c.pending[p.job.ID]; ok && cur == p {
				c.forget(p)
				c.release(p.worker)
				// The worker was just credited back for a job it is still
				// running. Tell it to stop, off this goroutine — the caller is
				// leaving — and only if Stop is not already waiting on the
				// group, which closed says under this same lock.
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
		// A context error, because that is what it is to the run waiting:
		// an interruption, to be resumed by the next coordinator, and not
		// the job's answer.
		return resultEnvelope{}, fmt.Errorf("wings: cluster stopped while waiting for a result: %w", context.Canceled)
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
	// A copy, not the slice: a worker that dies from here on is deleted
	// from c.workers in place, which clears the slot it left at the end of
	// the array this would otherwise still be reading.
	workers := slices.Clone(c.workers)
	c.mu.Unlock()

	c.journal.record(journalEntry{Kind: journalClusterStop})

	// Before the mirror is cancelled: what a job wrote in its last moments may
	// still be on its way. Its result arrived and its caller has the handle,
	// and the machine that holds the rest is about to be destroyed. Cancelling
	// first left a persistent Dir holding the front of a file whose handle
	// promised the whole of it. All at once, since each is bounded on its own
	// and a fleet's worth of bounds in a row would be a long Stop.
	//
	// The workers are still in c.workers for this, and must be: the fleet the
	// mirror follows IS c.workers, and a worker taken out of it has its copies
	// cancelled and stops being a source the mirror will answer for. Nothing
	// new reaches them meanwhile — closed is set, so submit refuses — and the
	// scaler declines to run on a closing cluster.
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

	// All at once. Releasing a cloud machine is a delete the API takes most
	// of a minute to confirm, and one after another made Stop on a fleet of
	// sixteen a ten-minute wait — every one of them billing until its turn.
	errs := make([]error, len(workers))
	var releases sync.WaitGroup
	for i, w := range workers {
		releases.Go(func() {
			errs[i] = c.releaseWorker(ctx, w)
		})
	}
	releases.Wait()
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
		_ = w.proc.Kill()
		// The watcher goroutine owns Wait, so this waits for IT rather than
		// calling Wait a second time: two concurrent waits on one process is not
		// something os/exec promises anything about.
		if w.exited != nil {
			select {
			case <-w.exited:
			case <-time.After(10 * time.Second):
				// Bounded rather than indefinite. A process that will not die
				// should cost a leaked handle, not a shutdown that never ends.
			}
		}
	}
	if w.machine != nil {
		errs = append(errs, w.machine.Close(ctx))
	}
	return errors.Join(errs...)
}

// tailBeats follows one worker's progress reports.
//
// A loop of its own rather than a second case in tail: results and beats are
// different streams with different meanings, and a beat is only useful while the
// job it describes is still running — waiting for the result stream to produce
// something before noticing one would defeat the point entirely.
//
// Beats are read from the END of the stream, not from the beginning. Whatever a
// previous coordinator's jobs reported is about jobs that are no longer
// outstanding, and a checkpoint is a position rather than a record: only the
// latest is ever wanted, and old ones name jobs nobody is waiting for.
//
// Where the end is was found by adopt, before the worker could be given a job:
// found here, after, the first beats of a job dispatched in between were behind
// the starting point and never read. A job whose start was missed that way had
// no clock running on it, and one that then went quiet was never moved — the
// step-replay test hung on exactly that under a loaded suite.
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
			// Nothing here declares a worker dead. That is the result tail's
			// job, and it has the reconnect window and the exit signal to do it
			// with; two goroutines racing to reach the same verdict would only
			// make the verdict harder to reason about.
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
				// The one verdict this loop does reach: not a guess about a
				// silent worker, but the worker itself saying its machine is
				// being taken back. Same path as a death, so what it owed is
				// moved and the machine released.
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

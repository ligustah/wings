package wings

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ligustah/durable_streams/broker/embed"
	"github.com/ligustah/durable_streams/broker/protos"
	"github.com/ligustah/durable_streams/dsclient"
	"google.golang.org/grpc"

	"github.com/ligustah/wings/flow"
)

// workerNode drains one worker's job streams. It owns no broker: given a
// *dsclient.Client and a name it declares and consumes its streams, so the same
// type serves an in-process goroutine and a worker process with its own broker.
type workerNode struct {
	id          string
	concurrency int
	timeout     time.Duration
	log         *slog.Logger

	// slots bounds how many threads run here at once. See slots.go.
	slots *slots

	client  *dsclient.Client
	jobs    *dsclient.Stream[jobEnvelope]
	out     *dsclient.Stream[resultEnvelope]
	beats   *dsclient.Stream[beatEnvelope]
	control *dsclient.Stream[controlEnvelope]
	nested  *dsclient.Stream[jobEnvelope]

	// answers holds outcomes of calls made by attempts running here, by attempt
	// then call position (see nested.go). Guarded by runMu with running.
	answers map[string]map[string]*answerBox

	runMu sync.Mutex
	// running is how to stop each attempt in flight, keyed by job and attempt so
	// a cancellation cannot stop the attempt that replaced a moved one here.
	running map[string]context.CancelCauseFunc
	// leaving is set once the machine is being taken back; nothing new runs after it.
	leaving atomic.Bool
	// stopped names attempts the coordinator stopped before they started here, so
	// one moved off this queue is refused rather than run when its turn comes.
	stopped map[string]string

	// open names streams already stood up for a job, so opening the same name
	// twice is refused. Names carry the attempt, so a retry back on this worker
	// does not collide with its predecessor.
	openMu sync.Mutex
	open   map[string]bool
}

// newWorkerNode declares a worker's streams on client and returns its loop.
func newWorkerNode(ctx context.Context, client *dsclient.Client, id string, concurrency int, timeout time.Duration, log *slog.Logger) (*workerNode, error) {
	if log == nil {
		log = slog.Default()
	}
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}
	n := &workerNode{
		id:          id,
		concurrency: concurrency,
		timeout:     timeout,
		log:         log.With("worker", id),
		client:      client,
		slots:       newSlots(concurrency),
	}
	if err := n.declareStreams(ctx); err != nil {
		return nil, err
	}
	return n, nil
}

// declareStreams creates this worker's streams if they are not already there.
func (n *workerNode) declareStreams(ctx context.Context) error {
	for _, name := range []string{jobStreamFor(n.id), resultStreamFor(n.id), beatStreamFor(n.id), controlStreamFor(n.id), nestedStreamFor(n.id)} {
		ok, err := n.client.StreamExists(ctx, name)
		if err != nil {
			return fmt.Errorf("wings: check stream %s: %w", name, err)
		}
		if ok {
			continue
		}
		if err := n.client.CreateStream(ctx, name, nil); err != nil {
			return fmt.Errorf("wings: create stream %s: %w", name, err)
		}
	}
	var err error
	if n.jobs, err = n.client.OpenStream[jobEnvelope](jobStreamFor(n.id)); err != nil {
		return fmt.Errorf("wings: open %s: %w", jobStreamFor(n.id), err)
	}
	if n.out, err = n.client.OpenStream[resultEnvelope](resultStreamFor(n.id)); err != nil {
		return fmt.Errorf("wings: open %s: %w", resultStreamFor(n.id), err)
	}
	if n.beats, err = n.client.OpenStream[beatEnvelope](beatStreamFor(n.id)); err != nil {
		return fmt.Errorf("wings: open %s: %w", beatStreamFor(n.id), err)
	}
	if n.control, err = n.client.OpenStream[controlEnvelope](controlStreamFor(n.id)); err != nil {
		return fmt.Errorf("wings: open %s: %w", controlStreamFor(n.id), err)
	}
	if n.nested, err = n.client.OpenStream[jobEnvelope](nestedStreamFor(n.id)); err != nil {
		return fmt.Errorf("wings: open %s: %w", nestedStreamFor(n.id), err)
	}
	return nil
}

// declareOutput stands a stream up on this worker for one attempt to write. The
// coordinator's mirror finds it by name; one stream per output, so a large one
// cannot hold up another's records.
func (n *workerNode) declareOutput(ctx context.Context, stream, name string) (*dsclient.Client, error) {
	n.openMu.Lock()
	if n.open == nil {
		n.open = map[string]bool{}
	}
	if n.open[stream] {
		n.openMu.Unlock()
		return nil, fmt.Errorf("wings: this attempt already opened %q", name)
	}
	n.open[stream] = true
	n.openMu.Unlock()

	// Not ctx: a deadline here would leave a half-made stream for the next attempt.
	if err := ensureStream(context.WithoutCancel(ctx), n.client, stream); err != nil {
		return nil, err
	}
	return n.client, nil
}

// progressOf is one attempt's [flow.Progress]: heartbeats and finished steps go
// on this worker's beat stream tagged with the job and attempt.
type progressOf struct {
	n       *workerNode
	job     string
	attempt int
	outputs *attemptOutputs
}

// Heartbeat commits what the attempt has written before reporting progress, so a
// checkpoint never claims more than a retry can be handed.
func (p progressOf) Heartbeat(ctx context.Context, checkpoint []byte) error {
	if err := p.outputs.commit(ctx); err != nil {
		return err
	}
	return p.n.sendBeat(ctx, beatEnvelope{Job: p.job, Attempt: p.attempt, Checkpoint: checkpoint})
}

// sendBeat publishes one progress report, outside the attempt's transaction so
// it is visible while the job runs, and off a background context so a job on its
// dying deadline still delivers its last checkpoint.
func (n *workerNode) sendBeat(ctx context.Context, b beatEnvelope) error {
	if _, err := n.beats.Append(context.WithoutCancel(ctx), []beatEnvelope{b}); err != nil {
		return fmt.Errorf("wings: send progress for job %s: %w", b.Job, err)
	}
	return nil
}

// run serves the worker's job and nested queues until ctx is cancelled or the
// machine is taken back. Each queue is read continuously; each job runs on its
// own goroutine, taking a slot when running and yielding it while waiting (slots.go).
func (n *workerNode) run(ctx context.Context) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go n.tailControl(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { n.serve(ctx, n.jobs, false) })
	wg.Go(func() { n.serve(ctx, n.nested, true) })
	wg.Wait()
	return nil
}

// tailControl follows the coordinator's word about jobs already here — answers
// and cancellations — from the end of the stream, since older records concern
// attempts long gone.
func (n *workerNode) tailControl(ctx context.Context) {
	info, err := n.control.Info(ctx)
	if err != nil {
		if ctx.Err() == nil {
			n.log.Warn("wings: cannot follow the coordinator's cancellations", "err", err)
		}
		return
	}
	from := info.Newest + 1

	for ctx.Err() == nil {
		readCtx, cancel := context.WithTimeout(ctx, pollInterval)
		recs, err := n.control.ReadBlocking(readCtx, from, 64)
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
			if r.Record.Answer != nil {
				n.answer(r.Record)
				continue
			}
			n.stopAttempt(r.Record)
		}
	}
}

// watchPreemption polls the URL the machine gave for the cloud's decision to
// take it back, and calls leave once. A transient error is not a preemption and
// is retried. See [PreemptionURLEnv].
func (n *workerNode) watchPreemption(ctx context.Context, url string) {
	client := &http.Client{Timeout: 2 * time.Minute}
	for ctx.Err() == nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			n.log.Warn("wings: preemption URL is unusable", "url", url, "err", err)
			return
		}
		req.Header.Set("Metadata-Flavor", "Google")
		req.Header.Set("X-Wings-Worker", n.id)
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			n.log.Debug("wings: could not ask about preemption", "err", err)
			sleepCtx(ctx, 5*time.Second)
			continue
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64))
		resp.Body.Close()
		if strings.EqualFold(strings.TrimSpace(string(body)), "TRUE") {
			n.leave()
			return
		}
		sleepCtx(ctx, 2*time.Second)
	}
}

// leave tells the coordinator this worker is going so its jobs move now, refuses
// new ones, and ends the attempts running here.
func (n *workerNode) leave() {
	n.log.Warn("wings: this machine is being taken back; handing its work over")
	n.leaving.Store(true)
	if _, err := n.beats.Append(context.Background(), []beatEnvelope{{Leaving: true}}); err != nil {
		n.log.Error("wings: could not tell the coordinator this worker is leaving", "err", err)
	}
	n.runMu.Lock()
	cancels := make([]context.CancelCauseFunc, 0, len(n.running))
	for _, cancel := range n.running {
		cancels = append(cancels, cancel)
	}
	n.runMu.Unlock()
	cause := errors.New("wings: this worker's machine is being taken back")
	for _, cancel := range cancels {
		cancel(cause)
	}
}

// attemptKey names one attempt of one job.
func attemptKey(job string, attempt int) string { return job + "/" + strconv.Itoa(attempt) }

// startAttempt registers an attempt as running and returns the context to run it
// under, which a coordinator cancellation ends. An attempt stopped before it
// started is cancelled at once.
func (n *workerNode) startAttempt(ctx context.Context, job jobEnvelope) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(ctx)
	key := attemptKey(job.ID, job.Attempt)
	n.runMu.Lock()
	if n.running == nil {
		n.running = map[string]context.CancelCauseFunc{}
	}
	n.running[key] = cancel
	why, early := n.stopped[key]
	delete(n.stopped, key)
	n.runMu.Unlock()
	if early {
		n.log.Info("wings: not starting a job the coordinator already stopped", "job", job.ID, "attempt", job.Attempt, "why", why)
		cancel(fmt.Errorf("wings: the coordinator stopped this job: %s", why))
	}
	return ctx, func() {
		n.runMu.Lock()
		delete(n.running, key)
		delete(n.answers, key)
		n.runMu.Unlock()
		cancel(nil)
	}
}

// stopAttempt ends an attempt the coordinator no longer wants, or marks it so it
// is refused if it reaches the queue later.
func (n *workerNode) stopAttempt(c controlEnvelope) {
	why := c.Why
	if why == "" {
		why = "nobody is waiting for it"
	}
	key := attemptKey(c.Job, c.Attempt)
	n.runMu.Lock()
	cancel, ok := n.running[key]
	if !ok {
		if n.stopped == nil {
			n.stopped = map[string]string{}
		}
		n.stopped[key] = why
	}
	n.runMu.Unlock()
	if !ok {
		return
	}
	n.log.Info("wings: stopping a job the coordinator no longer wants", "job", c.Job, "attempt", c.Attempt, "why", why)
	cancel(fmt.Errorf("wings: the coordinator stopped this job: %s", why))
}

// serve runs the jobs on one queue as they arrive, each on its own goroutine.
// The loop ends when the machine is taken back: nothing is reported or taken
// after that, since a late result could beat the coordinator's move.
func (n *workerNode) serve(ctx context.Context, queue *dsclient.Stream[jobEnvelope], nested bool) {
	info, err := queue.Info(ctx)
	if err != nil {
		if ctx.Err() == nil {
			n.log.Warn("wings: cannot read the worker's queue", "err", err)
		}
		return
	}
	from := max(info.Oldest, 0)

	for ctx.Err() == nil && !n.leaving.Load() {
		readCtx, cancel := context.WithTimeout(ctx, pollInterval)
		recs, err := queue.ReadBlocking(readCtx, from, 64)
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
			go n.runJob(ctx, r.Record)
		}
	}
}

// runJob runs one job in a slot and reports its result.
func (n *workerNode) runJob(ctx context.Context, job jobEnvelope) {
	slot := &jobSlot{n: n, job: job}
	if err := slot.take(ctx, false); err != nil {
		return
	}
	res := n.runOne(ctx, job, slot)
	slot.give()
	if n.leaving.Load() {
		return
	}
	if _, err := n.out.Append(context.WithoutCancel(ctx), []resultEnvelope{res}); err != nil && ctx.Err() == nil {
		n.log.Error("wings: could not deliver a result", "job", job.ID, "err", err)
	}
}

func (n *workerNode) runOne(ctx context.Context, job jobEnvelope, slot *jobSlot) (res resultEnvelope) {
	res.ID = job.ID
	res.Attempt = job.Attempt

	if n.leaving.Load() {
		res.Error = "wings: this worker's machine is being taken back; the job was not started"
		return res
	}

	// The function's own bound wins over the cluster default; a function this
	// worker lacks is left to Execute to refuse.
	timeout := n.timeout
	if bounds, ok := flow.BoundsOf(job.Func); ok && bounds.Timeout > 0 {
		timeout = bounds.Timeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	// Stoppable from the coordinator: a job nobody waits on is not worth its slot.
	ctx, finish := n.startAttempt(ctx, job)
	defer finish()

	// Progress and the last attempt's checkpoint, installed for every job since
	// Heartbeat is always allowed and the checkpoint is what makes a redispatch cheap.
	outputs := newAttemptOutputs(n, job)
	ctx = flow.WithProgress(ctx, progressOf{n, job.ID, job.Attempt, outputs}, flow.Resume{
		Attempt: job.Attempt, Checkpoint: job.Checkpoint,
	})
	state := &jobState{id: job.ID, attempt: job.Attempt, priors: job.Priors, node: n, outputs: outputs}
	ctx = withJob(ctx, state)
	// The fleet's parallelism, so a thread fanning out here sizes to the fleet;
	// absent from an older coordinator or a bare call, flow falls back to this process.
	if job.Capacity > 0 {
		ctx = flow.WithMaxParallelism(ctx, job.Capacity)
	}
	// A thread this job forks is the cluster's to place (nested.go); a waiting
	// thread yields the job's slot (slots.go).
	placer := nestedPlacer{n: n, job: state}
	// From now, not from when the job was appended, so time queued behind others
	// is not counted against the work.
	if err := n.sendBeat(ctx, beatEnvelope{Job: job.ID, Attempt: job.Attempt, Started: true}); err != nil {
		n.log.Warn("wings: could not report a job as started", "job", job.ID, "err", err)
	}

	// A panicking work function costs one job, not the worker.
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 8192)
			buf = buf[:runtime.Stack(buf, false)]
			n.log.Error("wings: work function panicked", "fn", job.Func, "job", job.ID, "panic", r)
			res = resultEnvelope{ID: job.ID, Attempt: job.Attempt,
				Error: fmt.Sprintf("wings: %s panicked: %v\n\n%s", job.Func, r, buf)}
		}
	}()

	// Run as the thread it is, from the coordinator's copy of the last attempt's
	// history, so a retry replays what its predecessor did. One attempt per
	// dispatch: whether and where to retry is the coordinator's call. A thread of
	// run code is reached by replaying its ancestors (lineage.go).
	runOpts := []flow.RunOption{
		flow.WithStore(&historyStore{a: outputs, name: historyName(job.ID, job.Attempt)}),
		flow.WithPlacer(placer), flow.WithParker(slot), flow.WithChannelHost(nodeChannels{n: n, job: state}),
		flow.Once(),
	}
	var payload []byte
	var err error
	if len(job.Lineage) > 0 {
		payload, err = flow.RunLineage(ctx, runOf(job), job.Root, job.Lineage, runOpts...)
	} else {
		payload, err = flow.RunThread(ctx, runOf(job), threadOf(job), job.Func, job.Payload, runOpts...)
	}
	// Commit what the attempt wrote before its answer leaves; a failed commit is
	// the attempt failing.
	if cerr := outputs.finish(ctx); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		// A yield, not an answer: run again later, and when. See yield.go.
		if y := yieldOf(ctx, err); y != nil {
			res.Yield = y
			return res
		}
		res.Error = err.Error()
		// Name the function and bound rather than a bare "deadline exceeded"; the
		// parent's own cancellation (a shutdown) is excluded.
		if timeout > 0 && errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil {
			res.Error = fmt.Sprintf("wings: %s exceeded its %s timeout", job.Func, timeout)
		}
		// Likewise a coordinator stop, rather than "context canceled".
		if cause := context.Cause(ctx); cause != nil && errors.Is(err, context.Canceled) && cause != context.Canceled {
			res.Error = cause.Error()
		}
		return res
	}
	// Refused here, where the job pays, rather than as an uncarryable read on the
	// coordinator that looks like a dropped connection.
	if len(payload) > maxResult {
		res.Error = fmt.Sprintf("wings: the result of %s is %d bytes, more than a result may be (%d); "+
			"stream output this size over a flow.Channel rather than returning it", job.Func, len(payload), maxResult)
		return res
	}
	res.Payload = payload
	return res
}

// servedBroker is a broker a worker process stands up for itself, with the gRPC
// server that lets the coordinator in. In-process targets need none.
type servedBroker struct {
	broker *embed.InProcess
	client *dsclient.Client
	srv    *grpc.Server
	lis    net.Listener
}

func startServedBroker(dir, listen string, log *slog.Logger) (*servedBroker, error) {
	b, err := embed.StartInProcess(embed.InProcessConfig{Dir: dir, Logger: streamLogger(log)})
	if err != nil {
		return nil, fmt.Errorf("wings: start broker: %w", err)
	}
	srv, lis, err := serveBroker(b.Service(), listen, log)
	if err != nil {
		_ = b.Close()
		return nil, err
	}
	return &servedBroker{broker: b, client: dsclient.Wrap(b.Client()), srv: srv, lis: lis}, nil
}

// serveBroker stands up a gRPC server for a durable-streams service on listen,
// so another process can reach it. Used for a worker's own broker and, in
// shared-broker mode, for the coordinator's engine.
func serveBroker(service protos.DurableStreamsServer, listen string, log *slog.Logger) (*grpc.Server, net.Listener, error) {
	lis, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, nil, fmt.Errorf("wings: listen on %s: %w", listen, err)
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessage),
		grpc.MaxSendMsgSize(maxMessage),
	)
	protos.RegisterDurableStreamsServer(srv, service)
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("wings: broker server stopped", "err", err)
		}
	}()
	return srv, lis, nil
}

func (s *servedBroker) addr() string { return s.lis.Addr().String() }

// workerStore is where a worker keeps its streams: the coordinator's engine,
// dialed over WINGS_BROKER in shared-broker mode and holding no data of its own,
// or a served broker of its own otherwise. It returns a client, the address to
// announce, and a close.
func workerStore(log *slog.Logger) (*dsclient.Client, string, func(), error) {
	if broker := os.Getenv(envBroker); broker != "" {
		backend, err := dialWorker(broker)
		if err != nil {
			return nil, "", nil, fmt.Errorf("wings: dial shared broker %s: %w", broker, err)
		}
		return dsclient.Wrap(backend), broker, func() { _ = backend.Close() }, nil
	}

	dir := os.Getenv(envDir)
	if dir == "" {
		// Only when started by hand: the coordinator always sets WINGS_DIR.
		dir = defaultWorkerDataDir
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, "", nil, fmt.Errorf("wings: worker data dir: %w", err)
	}
	listen := os.Getenv(envListen)
	if listen == "" {
		listen = "127.0.0.1:0"
	}
	b, err := startServedBroker(dir, listen, log)
	if err != nil {
		return nil, "", nil, err
	}
	return b.client, b.addr(), func() { _ = b.close() }, nil
}

func (s *servedBroker) close() error {
	if s == nil {
		return nil
	}
	if s.srv != nil {
		s.srv.GracefulStop()
	}
	// The client wraps the broker's backend, which Close also releases, so only
	// one of them may do it.
	return s.broker.Close()
}

// isWorkerProcess reports whether wings started this process as a worker.
func isWorkerProcess() bool { return os.Getenv(envMode) == modeWorker }

// runWorkerProcess is what a worker binary does instead of returning from
// [Start]: it serves until the process is killed.
func runWorkerProcess(ctx context.Context, log *slog.Logger) error {
	concurrency, _ := strconv.Atoi(os.Getenv(envConcurrency))
	jobTimeout, _ := time.ParseDuration(os.Getenv(envJobTimeout))
	id := os.Getenv(envWorkerID)
	if id == "" {
		id = "worker"
	}

	// A shared broker: dial the coordinator's engine instead of running one, so
	// this worker keeps no data of its own. Otherwise stand up a served broker.
	client, addr, closeStore, err := workerStore(log)
	if err != nil {
		return err
	}
	defer closeStore()

	n, err := newWorkerNode(ctx, client, id, concurrency, jobTimeout, log)
	if err != nil {
		return err
	}

	if url := os.Getenv(PreemptionURLEnv); url != "" {
		go n.watchPreemption(ctx, url)
	}

	// The parent reads this to know the worker is up, so it must be first on
	// stdout and flushed before anything blocks.
	fmt.Fprintln(os.Stdout, readyPrefix+addr)

	n.log.Info("wings: worker serving", "addr", addr, "concurrency", n.concurrency)
	err = n.run(ctx)
	if n.leaving.Load() {
		// Taken back: stay up until the machine goes, so the coordinator can
		// finish copying what the jobs here wrote.
		n.log.Info("wings: no longer taking work; serving what was written here until the machine goes")
		<-ctx.Done()
		return nil
	}
	return err
}

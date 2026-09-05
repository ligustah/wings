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

// workerNode is the loop that drains one worker's job stream.
//
// It owns NO broker. Given a *dsclient.Client and a name, it declares its pair
// of streams and consumes them — so the same type serves a goroutine sharing
// one in-process engine with a dozen others, and a worker process that spun up
// a broker of its own. What differs between the targets is which client it is
// handed, and nothing else.
type workerNode struct {
	id          string
	concurrency int
	timeout     time.Duration
	log         *slog.Logger

	client  *dsclient.Client
	jobs    *dsclient.Stream[jobEnvelope]
	out     *dsclient.Stream[resultEnvelope]
	beats   *dsclient.Stream[beatEnvelope]
	control *dsclient.Stream[controlEnvelope]
	nested  *dsclient.Stream[jobEnvelope]

	// answers are the outcomes of calls made by attempts running here, by
	// attempt and then by the call's position. See nested.go. Guarded by
	// runMu, with running, since an answer is only kept for an attempt that
	// is.
	answers map[string]map[string]*answerBox

	// running is how to stop each attempt in flight here, keyed by job and
	// attempt. A cancellation names both, so one for an attempt already moved
	// away cannot stop the one that replaced it on this same worker.
	runMu   sync.Mutex
	running map[string]context.CancelCauseFunc
	// leaving is set once the machine is being taken back. Nothing new is run
	// after it: a job taken off the queue is answered with an error instead,
	// and the coordinator, told, has already moved it elsewhere.
	leaving atomic.Bool
	// stopped names attempts the coordinator stopped BEFORE they started here:
	// a job moved for waiting too long on this queue is still on this queue,
	// and would otherwise run in full when its turn came. Cleared when the
	// attempt is taken off the queue and refused on the spot.
	stopped map[string]string

	// open names the streams this worker has already stood up for a job, so one
	// attempt opening the same name twice is refused rather than silently
	// producing two under one handle. The names carry the attempt, so a retry
	// that lands back on this same worker opens a new one rather than colliding
	// with what its predecessor left here.
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
	}
	if err := n.declareStreams(ctx); err != nil {
		return nil, err
	}
	return n, nil
}

// declareStreams creates this worker's job and result streams if they are not
// already there. StreamExists is node-local, which is the right question for
// both an embedded engine and a single-node broker.
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

// declareOutput stands a stream up on this worker for one attempt of one job to
// write.
//
// Nothing announces it. The coordinator's mirror finds it by listing this
// worker, and the name says which job and which attempt it belongs to — so
// there is no register here to be out of date with what is actually on disk.
//
// One stream per output, so a job writing a gigabyte of video cannot hold up
// another job's events behind it.
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

	// Deliberately not ctx: a job whose deadline expires between here and the
	// first record would leave a stream half-made, and the next attempt looking
	// at it.
	if err := ensureStream(context.WithoutCancel(ctx), n.client, stream); err != nil {
		return nil, err
	}
	return n.client, nil
}

// progressOf is one running attempt's [flow.Progress]: its heartbeats and
// finished steps go on this worker's beat stream, tagged with the job and
// attempt they are about.
type progressOf struct {
	n       *workerNode
	job     string
	attempt int
	outputs *attemptOutputs
}

// A report of progress is a commit point. What the attempt has written is
// committed BEFORE the coordinator is told how far it got, so a checkpoint
// never claims more than a retry can be handed: a lost beat costs a resume
// from slightly earlier, and a failed commit sends no beat at all.
func (p progressOf) Heartbeat(ctx context.Context, checkpoint []byte) error {
	if err := p.outputs.commit(ctx); err != nil {
		return err
	}
	return p.n.sendBeat(ctx, beatEnvelope{Job: p.job, Attempt: p.attempt, Checkpoint: checkpoint})
}

func (p progressOf) Step(ctx context.Context, step flow.StepRecord) error {
	if err := p.outputs.commit(ctx); err != nil {
		return err
	}
	return p.n.sendBeat(ctx, beatEnvelope{Job: p.job, Attempt: p.attempt, Step: &step})
}

// sendBeat publishes one progress report.
//
// Outside the processor's transaction on purpose: a heartbeat is only useful if
// it arrives WHILE the job is running, and anything written inside that
// transaction becomes visible when the job finishes, which is exactly too late.
func (n *workerNode) sendBeat(ctx context.Context, b beatEnvelope) error {
	// Deliberately not ctx: a job whose deadline has just expired is precisely
	// the one whose last checkpoint is worth having, and sending on the dying
	// context would drop it.
	if _, err := n.beats.Append(context.WithoutCancel(ctx), []beatEnvelope{b}); err != nil {
		return fmt.Errorf("wings: send progress for job %s: %w", b.Job, err)
	}
	return nil
}

// run drains jobs until ctx is cancelled.
//
// Batch is the worker's concurrency, so a worker takes exactly as much work as
// it can start at once. A larger batch would commit more results per
// transaction but hold every one of them back until the slowest in the batch
// finished, which is the wrong trade when the coordinator is waiting on each.
func (n *workerNode) run(ctx context.Context) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	go n.tailControl(ctx)
	go n.serveNested(ctx)
	return n.client.Run(ctx, "wings-worker-"+n.id, dsclient.Processor[jobEnvelope, resultEnvelope]{
		In:      n.jobs,
		Out:     n.out,
		Group:   consumerGroup,
		Batch:   n.concurrency,
		Process: n.process,
	})
}

// tailControl follows the coordinator's word about jobs already here, and
// stops the attempts it names.
//
// Read from the END of the stream: whatever a previous coordinator said is
// about attempts that are long gone, and a cancellation for an attempt not
// running here is nothing to do either way.
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
// take it back, and acts on it once.
//
// See [PreemptionURLEnv]. A request that blocks until the answer changes is
// used as such; one that answers at once is asked again after a moment. An
// error is not a preemption — a metadata server that is briefly unreachable
// is not a machine that is going away — so it is retried, a little later.
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

// leave is what a worker does with the notice: tells the coordinator, so its
// jobs are moved now; refuses new ones; and ends the attempts running here,
// whose answers are no longer wanted anywhere.
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

// attemptKey names one attempt of one job, for running.
func attemptKey(job string, attempt int) string { return job + "/" + strconv.Itoa(attempt) }

// startAttempt registers an attempt as running and returns the context to run
// it under, which a cancellation from the coordinator ends.
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

// stopAttempt ends an attempt the coordinator no longer wants, if it is
// running here.
func (n *workerNode) stopAttempt(c controlEnvelope) {
	why := c.Why
	if why == "" {
		why = "nobody is waiting for it"
	}
	key := attemptKey(c.Job, c.Attempt)
	n.runMu.Lock()
	cancel, ok := n.running[key]
	if !ok {
		// Not running yet, or already finished. Either way the answer is the
		// same: if it turns up on the queue later, it is not run.
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

// process runs a batch, up to concurrency at a time.
//
// It never returns an error. A work function that fails or panics produces a
// result carrying that error, because the alternative — surfacing it here —
// aborts the transaction and redelivers the whole batch, and a deterministic
// failure would then do that forever. Transport and encoding trouble is the
// only thing that legitimately fails a batch, and by this point neither is
// still possible.
func (n *workerNode) process(ctx context.Context, batch []jobEnvelope) ([]resultEnvelope, error) {
	out := make([]resultEnvelope, len(batch))
	sem := make(chan struct{}, n.concurrency)
	var wg sync.WaitGroup
	for i, job := range batch {
		wg.Go(func() {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				out[i] = resultEnvelope{ID: job.ID, Error: ctx.Err().Error()}
				return
			}
			defer func() { <-sem }()
			out[i] = n.runOne(ctx, job)
		})
	}
	wg.Wait()
	// The one other thing that fails a batch: the machine is being taken
	// back. The coordinator was told and is moving these jobs, and a result
	// from here — an error saying the attempt was cut short — could reach it
	// BEFORE the move and be delivered as the job's answer. Failing the batch
	// aborts the transaction, so nothing from this batch is ever committed,
	// and the loop ends: this worker takes no more.
	if n.leaving.Load() {
		return nil, errLeaving
	}
	return out, nil
}

// errLeaving ends a worker's loop once its machine is being taken back.
var errLeaving = errors.New("wings: this worker's machine is being taken back")

func (n *workerNode) runOne(ctx context.Context, job jobEnvelope) (res resultEnvelope) {
	res.ID = job.ID
	res.Attempt = job.Attempt

	if n.leaving.Load() {
		res.Error = "wings: this worker's machine is being taken back; the job was not started"
		return res
	}

	// The function's own bound wins over the cluster-wide default: one function
	// is a millisecond of arithmetic and another an hour of transcoding, and
	// the number that knows which is the one declared beside the code. A
	// function this worker does not have is left to Execute to refuse, which
	// names what it does have.
	timeout := n.timeout
	if bounds, ok := flow.BoundsOf(job.Func); ok && bounds.Timeout > 0 {
		timeout = bounds.Timeout
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	// Stoppable from the coordinator's side. A job whose last caller gave up,
	// or that was moved elsewhere, is one nobody wants the answer to, and the
	// slot it holds is worth more than the answer.
	ctx, finish := n.startAttempt(ctx, job)
	defer finish()

	// Progress reporting, and whatever the last attempt got to. Installed for
	// every job rather than only for functions that declare a heartbeat
	// timeout: calling Heartbeat is always allowed, and it is the checkpoint
	// that makes a redispatch cheap whether or not anything is watching the
	// clock.
	outputs := newAttemptOutputs(n, job)
	ctx = flow.WithProgress(ctx, progressOf{n, job.ID, job.Attempt, outputs}, flow.Resume{
		Attempt: job.Attempt, Checkpoint: job.Checkpoint, Steps: job.Steps,
	})
	// And the wings half: which job this is and the worker it is on, so what
	// it writes goes on this worker's streams, and what its earlier attempts
	// wrote can be read back.
	state := &jobState{id: job.ID, attempt: job.Attempt, priors: job.Priors, node: n, outputs: outputs}
	ctx = withJob(ctx, state)
	// A function this job calls is the cluster's to run, like any other: the
	// call is read out of this job's history by the coordinator and its
	// answer comes back on the control stream. See nested.go.
	exec := nestedExecutor{n: n, job: state}
	ctx = flow.Bind(ctx, exec)
	// Now, not when the job was appended: the coordinator's clocks on this job
	// run from here, so time it spent waiting behind others on this worker is
	// not counted against the work. Best-effort like every beat — a lost one
	// is made good by the first real report, and until then the job is merely
	// not yet under its bounds.
	if err := n.sendBeat(ctx, beatEnvelope{Job: job.ID, Attempt: job.Attempt, Started: true}); err != nil {
		n.log.Warn("wings: could not report a job as started", "job", job.ID, "err", err)
	}

	// A panicking work function must cost one job, not the worker.
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 8192)
			buf = buf[:runtime.Stack(buf, false)]
			n.log.Error("wings: work function panicked", "fn", job.Func, "job", job.ID, "panic", r)
			res = resultEnvelope{ID: job.ID, Attempt: job.Attempt,
				Error: fmt.Sprintf("wings: %s panicked: %v\n\n%s", job.Func, r, buf)}
		}
	}()

	// As a run, not a bare call. The function's body is a run's main thread,
	// so it may fork, use a channel, read the clock, sleep and call other
	// functions, and a retry replays all of that from the history rather than
	// doing it again — the history being the coordinator's copy of the last
	// attempt's, put on this worker under this attempt's name before the job
	// arrived. One attempt per dispatch: whether to try again, and where, is
	// the coordinator's decision, and the error is its input.
	//
	// Named for the JOB, not the attempt: the name is what the calls this run
	// makes are recorded under, and a retry that replays one must present it
	// as the same call, or the coordinator dispatches it twice.
	payload, err := flow.RunCall(ctx, jobRunName(job.ID), job.Func, job.Payload,
		flow.WithStore(&historyStore{a: outputs, name: historyName(job.ID, job.Attempt)}),
		flow.WithExecutor(exec), flow.Once())
	// Whatever the attempt wrote is committed before its answer leaves: a
	// result whose recordings could still be lost would be a handle to
	// nothing. A commit that fails is the attempt failing.
	if cerr := outputs.finish(ctx); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		res.Error = err.Error()
		// Say what actually happened. A work function that gives up on its
		// context reports "context deadline exceeded", which names neither the
		// function nor the bound it broke — and this is the one place that
		// knows both. The parent's cancellation is excluded: a worker being
		// shut down is not a slow job.
		if timeout > 0 && errors.Is(err, context.DeadlineExceeded) && ctx.Err() != nil {
			res.Error = fmt.Sprintf("wings: %s exceeded its %s timeout", job.Func, timeout)
		}
		// Likewise a job the coordinator stopped: say so, rather than
		// "context canceled". Nobody reads this result, but the worker's log
		// does.
		if cause := context.Cause(ctx); cause != nil && errors.Is(err, context.Canceled) && cause != context.Canceled {
			res.Error = cause.Error()
		}
		return res
	}
	// Refused here rather than discovered on the far side. A result the
	// transport cannot carry does not fail cleanly there: the read that could
	// not carry it looked like a dropped connection, and the worker holding it
	// — and its machine — paid for one job's mistake. Here the job pays, with
	// an error that says what to do instead.
	if len(payload) > maxResult {
		res.Error = fmt.Sprintf("wings: the result of %s is %d bytes, more than a result may be (%d); "+
			"return a wings.Artifact for output this size rather than a value", job.Func, len(payload), maxResult)
		return res
	}
	res.Payload = payload
	return res
}

// servedBroker is a broker a WORKER PROCESS stands up for itself, together with
// the gRPC server that lets the coordinator in.
//
// Only the out-of-process targets build one. In process there is no socket and
// no second process, so there is nothing to serve and the cluster's own engine
// is used directly.
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
	s := &servedBroker{broker: b, client: dsclient.Wrap(b.Client())}

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		_ = b.Close()
		return nil, fmt.Errorf("wings: listen on %s: %w", listen, err)
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxMessage),
		grpc.MaxSendMsgSize(maxMessage),
	)
	protos.RegisterDurableStreamsServer(srv, b.Service())
	s.srv, s.lis = srv, lis

	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			log.Error("wings: broker server stopped", "err", err)
		}
	}()
	return s, nil
}

func (s *servedBroker) addr() string { return s.lis.Addr().String() }

func (s *servedBroker) close() error {
	if s == nil {
		return nil
	}
	if s.srv != nil {
		s.srv.GracefulStop()
	}
	// The client wraps the broker's own backend, which Close also releases, so
	// only one of them may do it.
	return s.broker.Close()
}

// isWorkerProcess reports whether this process was started by wings as a
// worker.
func isWorkerProcess() bool { return os.Getenv(envMode) == modeWorker }

// runWorkerProcess is what a worker binary does instead of returning from
// Start. It serves until the process is killed, which is how the coordinator
// stops it.
func runWorkerProcess(ctx context.Context, log *slog.Logger) error {
	concurrency, _ := strconv.Atoi(os.Getenv(envConcurrency))
	jobTimeout, _ := time.ParseDuration(os.Getenv(envJobTimeout))
	id := os.Getenv(envWorkerID)
	if id == "" {
		id = "worker"
	}

	dir := os.Getenv(envDir)
	if dir == "" {
		var err error
		if dir, err = os.MkdirTemp("", "wings-worker-*"); err != nil {
			return fmt.Errorf("wings: worker data dir: %w", err)
		}
	}
	listen := os.Getenv(envListen)
	if listen == "" {
		listen = "127.0.0.1:0"
	}

	b, err := startServedBroker(dir, listen, log)
	if err != nil {
		return err
	}
	defer b.close()

	n, err := newWorkerNode(ctx, b.client, id, concurrency, jobTimeout, log)
	if err != nil {
		return err
	}

	// A machine that will be told when it is being taken back, and where.
	if url := os.Getenv(PreemptionURLEnv); url != "" {
		go n.watchPreemption(ctx, url)
	}

	// The parent reads this to learn the port, so it must be the first thing on
	// stdout and must be flushed before anything blocks.
	fmt.Fprintln(os.Stdout, readyPrefix+b.addr())

	n.log.Info("wings: worker serving", "addr", b.addr(), "concurrency", n.concurrency)
	err = n.run(ctx)
	if n.leaving.Load() {
		// The loop ended because the machine is being taken back. The broker
		// stays up until it is: the coordinator is still copying what the
		// jobs here wrote, and those seconds are what the notice is for.
		n.log.Info("wings: no longer taking work; serving what was written here until the machine goes")
		<-ctx.Done()
		return nil
	}
	return err
}

package wings

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/broker/embed"
	"github.com/ligustah/durable_streams/broker/protos"
	"github.com/ligustah/durable_streams/dsclient"
	"google.golang.org/grpc"

	"github.com/ligustah/wings/internal/invoke"
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

	client *dsclient.Client
	jobs   *dsclient.Stream[jobEnvelope]
	out    *dsclient.Stream[resultEnvelope]
	beats  *dsclient.Stream[beatEnvelope]

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
	for _, name := range []string{jobStreamFor(n.id), resultStreamFor(n.id), beatStreamFor(n.id)} {
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
	return n.client.Run(ctx, "wings-worker-"+n.id, dsclient.Processor[jobEnvelope, resultEnvelope]{
		In:      n.jobs,
		Out:     n.out,
		Group:   consumerGroup,
		Batch:   n.concurrency,
		Process: n.process,
	})
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
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				out[i] = resultEnvelope{ID: job.ID, Error: ctx.Err().Error()}
				return
			}
			defer func() { <-sem }()
			out[i] = n.runOne(ctx, job)
		}()
	}
	wg.Wait()
	return out, nil
}

func (n *workerNode) runOne(ctx context.Context, job jobEnvelope) (res resultEnvelope) {
	res.ID = job.ID
	res.Attempt = job.Attempt

	h, ok := lookup(job.Func)
	if !ok {
		// Nearly always a worker built from different source than the
		// coordinator, so say what this binary does have.
		res.Error = fmt.Sprintf("wings: no work function %q registered in this worker; it defines: %s",
			job.Func, strings.Join(definedNames(), ", "))
		return res
	}

	// The function's own bound wins over the cluster-wide default: one function
	// is a millisecond of arithmetic and another an hour of transcoding, and
	// the number that knows which is the one declared beside the code.
	timeout := n.timeout
	if d := h.options().timeout; d > 0 {
		timeout = d
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Progress reporting, and whatever the last attempt got to. Installed for
	// every job rather than only for functions that declare a heartbeat
	// timeout: calling Heartbeat is always allowed, and it is the checkpoint
	// that makes a redispatch cheap whether or not anything is watching the
	// clock.
	ctx = withBeat(ctx, &beatState{
		job: job.ID, sink: n, in: job.Checkpoint, steps: job.Steps,
		attempt: job.Attempt, priors: job.Priors,
	})
	// Now, not when the job was appended: the coordinator's clocks on this job
	// run from here, so time it spent waiting behind others on this worker is
	// not counted against the work. Best-effort like every beat — a lost one
	// is made good by the first real report, and until then the job is merely
	// not yet under its bounds.
	if err := n.sendBeat(ctx, beatEnvelope{Job: job.ID, Attempt: job.Attempt, Started: true}); err != nil {
		n.log.Warn("wings: could not report a job as started", "job", job.ID, "err", err)
	}
	// The worker's own broker, bound so a job can read what its previous
	// attempts wrote. A worker is not a place work dispatches to and has no
	// host; this is only storage, which is all Replay needs.
	ctx = invoke.WithStreams(ctx, n.client)

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

	payload, err := h.invoke(ctx, job.Payload)
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

	// The parent reads this to learn the port, so it must be the first thing on
	// stdout and must be flushed before anything blocks.
	fmt.Fprintln(os.Stdout, readyPrefix+b.addr())

	n.log.Info("wings: worker serving", "addr", b.addr(), "concurrency", n.concurrency)
	return n.run(ctx)
}

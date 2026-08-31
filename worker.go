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
	"github.com/ligustah/durable_streams/dswire"
	"google.golang.org/grpc"
)

// workerNode is one worker: a single-node broker plus the loop that drains its
// job stream.
//
// The SAME type serves all three targets. An in-process worker is one with no
// listener; a remote one is the identical thing with a socket in front of it.
// That is deliberate — a bug that only reproduces on a cloud VM is a bug in
// nine lines of transport, not in the worker.
type workerNode struct {
	id          string
	concurrency int
	timeout     time.Duration
	log         *slog.Logger

	broker *embed.InProcess
	client *dsclient.Client
	jobs   *dsclient.Stream[jobEnvelope]
	out    *dsclient.Stream[resultEnvelope]

	srv *grpc.Server
	lis net.Listener
}

type workerConfig struct {
	id          string
	dir         string
	listen      string // empty means do not serve; in-process callers hold the backend directly
	concurrency int
	timeout     time.Duration
	log         *slog.Logger
}

// startWorkerNode brings the broker up, declares the two streams, and — when
// asked — starts serving gRPC. It does not begin consuming; call run for that.
func startWorkerNode(ctx context.Context, cfg workerConfig) (*workerNode, error) {
	if cfg.log == nil {
		cfg.log = slog.Default()
	}
	if cfg.concurrency <= 0 {
		cfg.concurrency = runtime.NumCPU()
	}

	b, err := embed.StartInProcess(embed.InProcessConfig{Dir: cfg.dir, Logger: streamLogger(cfg.log)})
	if err != nil {
		return nil, fmt.Errorf("wings: start broker for worker %s: %w", cfg.id, err)
	}

	n := &workerNode{
		id:          cfg.id,
		concurrency: cfg.concurrency,
		timeout:     cfg.timeout,
		log:         cfg.log.With("worker", cfg.id),
		broker:      b,
		client:      dsclient.Wrap(b.Client()),
	}

	if err := n.declareStreams(ctx); err != nil {
		_ = n.close()
		return nil, err
	}

	if cfg.listen != "" {
		if err := n.serve(cfg.listen); err != nil {
			_ = n.close()
			return nil, err
		}
	}
	return n, nil
}

// declareStreams creates the job and result streams if they are not already
// there. StreamExists is node-local, which is exactly the right question for a
// broker that is one node by construction.
func (n *workerNode) declareStreams(ctx context.Context) error {
	for _, name := range []string{jobStream, resultStream} {
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
	if n.jobs, err = n.client.OpenStream[jobEnvelope](jobStream); err != nil {
		return fmt.Errorf("wings: open %s: %w", jobStream, err)
	}
	if n.out, err = n.client.OpenStream[resultEnvelope](resultStream); err != nil {
		return fmt.Errorf("wings: open %s: %w", resultStream, err)
	}
	return nil
}

// serve exposes the broker on addr. The listener is always loopback in
// practice: a remote worker is reached through a tunnel, and the broker speaks
// no authentication of its own, so binding anything routable would publish an
// open one.
func (n *workerNode) serve(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("wings: listen on %s: %w", addr, err)
	}
	srv := grpc.NewServer()
	protos.RegisterDurableStreamsServer(srv, n.broker.Service())
	n.srv, n.lis = srv, lis
	go func() {
		if err := srv.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			n.log.Error("wings: broker server stopped", "err", err)
		}
	}()
	return nil
}

// addr is where this worker's broker is listening, or "" when it is not.
func (n *workerNode) addr() string {
	if n.lis == nil {
		return ""
	}
	return n.lis.Addr().String()
}

// backend is the handle an in-process coordinator talks to directly, with no
// socket and no marshalling in between.
func (n *workerNode) backend() dswire.Backend { return n.broker.Client() }

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

	h, ok := lookup(job.Func)
	if !ok {
		// Nearly always a worker built from different source than the
		// coordinator, so say what this binary does have.
		res.Error = fmt.Sprintf("wings: no work function %q registered in this worker; it defines: %s",
			job.Func, strings.Join(definedNames(), ", "))
		return res
	}

	if n.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, n.timeout)
		defer cancel()
	}

	// A panicking work function must cost one job, not the worker.
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 8192)
			buf = buf[:runtime.Stack(buf, false)]
			n.log.Error("wings: work function panicked", "fn", job.Func, "job", job.ID, "panic", r)
			res = resultEnvelope{ID: job.ID, Error: fmt.Sprintf("wings: %s panicked: %v\n\n%s", job.Func, r, buf)}
		}
	}()

	payload, err := h.invoke(ctx, job.Payload)
	if err != nil {
		res.Error = err.Error()
		return res
	}
	res.Payload = payload
	return res
}

func (n *workerNode) close() error {
	if n == nil {
		return nil
	}
	if n.srv != nil {
		n.srv.GracefulStop()
	}
	var errs []error
	if n.client != nil {
		errs = append(errs, n.client.Close())
	}
	if n.broker != nil {
		errs = append(errs, n.broker.Close())
	}
	return errors.Join(errs...)
}

// isWorkerProcess reports whether this process was started by wings as a
// worker.
func isWorkerProcess() bool { return os.Getenv(envMode) == modeWorker }

// runWorkerProcess is what a worker binary does instead of returning from
// Start. It serves until the process is killed, which is how the coordinator
// stops it.
func runWorkerProcess(ctx context.Context, log *slog.Logger) error {
	concurrency, _ := strconv.Atoi(os.Getenv(envConcurrency))
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

	n, err := startWorkerNode(ctx, workerConfig{
		id:          os.Getenv(envWorkerID),
		dir:         dir,
		listen:      listen,
		concurrency: concurrency,
		log:         log,
	})
	if err != nil {
		return err
	}
	defer n.close()

	// The parent reads this to learn the port, so it must be the first thing on
	// stdout and must be flushed before anything blocks.
	fmt.Fprintln(os.Stdout, readyPrefix+n.addr())

	n.log.Info("wings: worker serving", "addr", n.addr(), "concurrency", n.concurrency)
	return n.run(ctx)
}

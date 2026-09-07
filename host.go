package wings

import (
	"context"
	"errors"
	"runtime"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
)

// Run executes body as a durable run named name on this cluster. Forked threads
// ([flow.Context.Go], [flow.Context.Map]) go to workers; direct calls run here.
// Running the same name in the same Dir again replays the body to where it
// stopped and continues, so the body must be deterministic. opts pass through to
// [flow.Run]; the store and placer are the cluster's.
func (c *Cluster) Run(ctx context.Context, name string, body func(ctx flow.Context) error, opts ...flow.RunOption) error {
	all, err := c.runOptions(opts)
	if err != nil {
		return err
	}
	ctx, done := c.hosting(ctx)
	defer done()
	err = flow.Run(ctx, name, body, all...)
	if err == nil {
		c.forgetRun(name)
	}
	return err
}

// RunWorkflow runs root function f on in, under f's own name, so a coordinator
// restarted over the same Dir resumes it with the input the first run recorded.
// f must be declared a root with [flow.Main]. See [Cluster.Run].
func (c *Cluster) RunWorkflow[In, Out any](ctx context.Context, f flow.Func[In, Out], in In, opts ...flow.RunOption) error {
	all, err := c.runOptions(opts)
	if err != nil {
		return err
	}
	ctx, done := c.hosting(ctx)
	defer done()
	if err = flow.RunMain(ctx, f, in, all...); err != nil {
		return err
	}
	if name, nerr := flow.NameOf(f); nerr == nil {
		c.forgetRun(name)
	}
	return nil
}

// Signal delivers a typed event to a running workflow by name: the run named
// run receives it through [flow.Context.Signal] under name. It is held until the
// run asks for it, so the run need not be waiting yet.
func (c *Cluster) Signal[In any](ctx context.Context, run, name string, v In) error {
	return flow.Deliver(ctx, clusterChannels{c}, run, name, v)
}

func (c *Cluster) runWorkflow(ctx context.Context, name string, input []byte) error {
	all, err := c.runOptions(nil)
	if err != nil {
		return err
	}
	ctx, done := c.hosting(ctx)
	defer done()
	err = flow.RunWorkflow(ctx, name, input, all...)
	if err == nil {
		c.forgetRun(name)
	}
	return err
}

func (c *Cluster) runOptions(opts []flow.RunOption) ([]flow.RunOption, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	return append(append([]flow.RunOption{}, opts...),
		flow.WithStore(flow.NewStore(client)),
		flow.WithPlacer(clusterPlacer{c}),
		flow.WithChannelHost(clusterChannels{c}),
	), nil
}

// Bind returns a context on which defined functions are dispatched to this
// cluster's workers, outside any run and without being recorded. Use
// [Cluster.Run] for anything that must survive a restart.
func (c *Cluster) Bind(ctx context.Context) flow.Context {
	return flow.Bind(withCluster(ctx, c), clusterExecutor{c})
}

// clusterExecutor is the cluster as a [flow.Executor]: a call is a job sent to a
// worker.
type clusterExecutor struct{ c *Cluster }

func (e clusterExecutor) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	return e.c.runJob(ctx, jobEnvelope{Func: name, Payload: payload})
}

func (c *Cluster) runJob(ctx context.Context, job jobEnvelope) ([]byte, error) {
	p, err := c.submitJob(ctx, job)
	if err != nil {
		return nil, err
	}
	res, err := c.await(ctx, p)
	if err != nil {
		return nil, err
	}
	if res.Error != "" {
		// Only the message survives the worker boundary, not the error value.
		return nil, errors.New(res.Error)
	}
	return res.Payload, nil
}

// clusterPlacer is the cluster as a [flow.Placer]: a thread is a job sent to a
// worker, as a function on its input or as run-code lineage (lineage.go). It
// stays home when it has no lineage to start from, or the worker lacks the code.
type clusterPlacer struct{ c *Cluster }

func (p clusterPlacer) Place(ctx context.Context, th flow.Thread, body func(flow.Context) ([]byte, error)) ([]byte, error) {
	ctx = flow.WithOrigin(ctx, threadOrigin(th))
	if th.Fn != "" {
		return p.c.runJob(ctx, jobEnvelope{Func: th.Fn, Payload: th.Input})
	}
	if !rootKnown(th.Root) {
		return flow.InProcess().Place(ctx, th, body)
	}
	// Run code may use any channel of the run, not only one it was handed.
	if err := flow.Share(ctx); err != nil {
		return nil, err
	}
	out, err := p.c.runJob(ctx, jobEnvelope{Root: th.Root, Lineage: th.Lineage})
	if err != nil && unknownRoot(err) {
		return flow.InProcess().Place(ctx, th, body)
	}
	return out, err
}

func threadOrigin(th flow.Thread) flow.Origin {
	return flow.Origin{Run: th.Run, Thread: th.ID}
}

type clusterKey struct{}

// withCluster marks a context as the coordinator's, so what needs the cluster's
// own storage — replaying a recording, draining a shared channel — can find it.
func withCluster(ctx context.Context, c *Cluster) context.Context {
	return context.WithValue(ctx, clusterKey{}, c)
}

// hosting binds a run's context to the cluster's, so a cluster that stops
// interrupts its runs rather than failing them; the next coordinator over the
// same Dir resumes them and rejoins their threads (recover.go).
func (c *Cluster) hosting(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx = flow.WithMaxParallelism(withCluster(ctx, c), c.maxParallelism())
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

// maxParallelism estimates how many calls the cluster can run at once — largest
// fleet times per-worker concurrency — for [flow.Context.MaxParallelism]. An
// estimate, since a worker may choose its own concurrency from a CPU count the
// coordinator cannot see.
func (c *Cluster) maxParallelism() int {
	workers := c.cfg.Scaling.Max
	if workers < 1 {
		workers = 1
	}
	concurrency := c.cfg.Concurrency
	if concurrency < 1 {
		concurrency = runtime.NumCPU()
	}
	return workers * concurrency
}

func clusterFrom(ctx context.Context) *Cluster {
	c, _ := ctx.Value(clusterKey{}).(*Cluster)
	return c
}

// jobState is what a running job carries on a worker: which job and attempt, the
// worker, its earlier attempts' recordings, and the transaction its output goes in.
type jobState struct {
	id      string
	attempt int
	priors  []Recording
	node    *workerNode
	outputs *attemptOutputs
}

type jobKey struct{}

func withJob(ctx context.Context, j *jobState) context.Context {
	return context.WithValue(ctx, jobKey{}, j)
}

func jobFrom(ctx context.Context) *jobState {
	j, _ := ctx.Value(jobKey{}).(*jobState)
	return j
}

// streamsFor returns storage from a context that is either the coordinator's
// (the cluster's own client) or a running job's (its worker's broker).
func streamsFor(ctx context.Context) (*dsclient.Client, error) {
	if j := jobFrom(ctx); j != nil {
		return j.node.client, nil
	}
	c := clusterFrom(ctx)
	if c == nil {
		return nil, errors.New("wings: this context belongs to neither a cluster nor a running job; " +
			"use the context a workflow or a run was given, or Cluster.Run or Cluster.Bind")
	}
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("wings: this cluster has no durable streams")
	}
	return client, nil
}

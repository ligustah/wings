package wings

import (
	"context"
	"errors"
	"runtime"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
)

// Run executes body as a durable run on this cluster.
//
// Every thread the body forks — [flow.Context.Go], [flow.Context.Map] — goes
// to a worker; a function it calls directly runs here. Both are recorded in
// the run's history, which lives on the cluster's own storage under Dir. Run the same
// name in the same Dir again and the body is replayed to where it stopped and
// carried on from there — a coordinator that crashed mid-run resumes rather
// than restarts, rejoining the threads it had forked where they are still
// running (recover.go), and one that already finished does nothing. The body must
// therefore be deterministic; [flow] says what that costs.
//
// opts are passed through to [flow.Run]; the store and the placer are this
// cluster's and cannot be overridden.
func (c *Cluster) Run(ctx context.Context, name string, body func(ctx flow.Context) error, opts ...flow.RunOption) error {
	all, err := c.runOptions(opts)
	if err != nil {
		return err
	}
	ctx, done := c.hosting(ctx)
	defer done()
	return flow.Run(ctx, name, body, all...)
}

// RunWorkflow runs a root function on in, on this cluster, the way
// [CoordinatorMain] runs the one it was asked for: under the function's own
// name, so a coordinator started again over the same Dir resumes it — with
// the input the first start recorded, whatever is passed here.
//
// f must have been declared a root with [flow.Main]. See [Cluster.Run] for what
// a run on a cluster is and what opts may say.
func (c *Cluster) RunWorkflow[In, Out any](ctx context.Context, f flow.Func[In, Out], in In, opts ...flow.RunOption) error {
	all, err := c.runOptions(opts)
	if err != nil {
		return err
	}
	ctx, done := c.hosting(ctx)
	defer done()
	return flow.RunMain(ctx, f, in, all...)
}

// runWorkflow is RunWorkflow for a coordinator, which has the workflow as a
// name and the input as JSON off the command line, or nil to resume.
func (c *Cluster) runWorkflow(ctx context.Context, name string, input []byte) error {
	all, err := c.runOptions(nil)
	if err != nil {
		return err
	}
	ctx, done := c.hosting(ctx)
	defer done()
	return flow.RunWorkflow(ctx, name, input, all...)
}

// runOptions is opts with this cluster's store and placer appended, so that
// they win. No executor: a function the body calls directly runs where the
// body is, on the coordinator, and only the threads it forks go to workers.
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

// Bind returns a context on which defined functions are called on this
// cluster's workers, outside any run.
//
// A call made this way is dispatched and not recorded: nothing replays it.
// That is right for a script or a test that wants one answer from the fleet;
// anything that should survive a restart belongs in [Cluster.Run].
func (c *Cluster) Bind(ctx context.Context) flow.Context {
	return flow.Bind(withCluster(ctx, c), clusterExecutor{c})
}

// clusterExecutor is the cluster as a [flow.Executor]: a call is a job sent
// to a worker, and the answer is what came back.
//
// A separate type rather than a method on Cluster because Invoke is the
// vocabulary of the seam, not of a cluster, and putting it on Cluster would
// put encoded payloads on its public API.
type clusterExecutor struct{ c *Cluster }

func (e clusterExecutor) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	return e.c.runJob(ctx, jobEnvelope{Func: name, Payload: payload})
}

// runJob sends one job to a worker and returns what came back.
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
		// The value does not survive the trip, only the message: match on
		// content rather than identity across a worker boundary.
		return nil, errors.New(res.Error)
	}
	return res.Payload, nil
}

// clusterPlacer is the cluster as a [flow.Placer]: a thread is a job sent
// to a worker, and the join is what came back. One that runs a function is
// sent as the function on its input; one that runs run code is sent as its
// lineage, for the worker to replay its way to — see lineage.go — and stays
// home only when it has no lineage a worker could start from, or the worker
// turns out not to hold the code.
type clusterPlacer struct{ c *Cluster }

func (p clusterPlacer) Place(ctx context.Context, th flow.Thread, body func(flow.Context) ([]byte, error)) ([]byte, error) {
	ctx = flow.WithOrigin(ctx, threadOrigin(th))
	if th.Fn != "" {
		return p.c.runJob(ctx, jobEnvelope{Func: th.Fn, Payload: th.Input})
	}
	if !rootKnown(th.Root) {
		return flow.InProcess().Place(ctx, th, body)
	}
	// Every channel of the run, whether or not the thread was handed one:
	// it is the run's code, and may use any of them.
	if err := flow.Share(ctx); err != nil {
		return nil, err
	}
	out, err := p.c.runJob(ctx, jobEnvelope{Root: th.Root, Lineage: th.Lineage})
	if err != nil && unknownRoot(err) {
		return flow.InProcess().Place(ctx, th, body)
	}
	return out, err
}

// threadOrigin is the origin a thread's job carries: the thread itself, at
// step zero — a thread is one piece of work, and its name is what a retry
// of the parent presents again, which is how the job is rejoined.
func threadOrigin(th flow.Thread) flow.Origin {
	return flow.Origin{Run: th.Run, Thread: th.ID}
}

type clusterKey struct{}

// withCluster marks a context as the coordinator's, so what needs the
// cluster's own storage — replaying a recording, draining a shared channel —
// can find it.
func withCluster(ctx context.Context, c *Cluster) context.Context {
	return context.WithValue(ctx, clusterKey{}, c)
}

// hosting binds a run's context to the cluster's: a cluster that stops takes
// its runs with it, as an INTERRUPTION and not a failure. A run whose
// context ends is resumed by whoever runs it next — a coordinator started
// again over the same Dir — where one that merely got an error from a call
// records that the run failed. The cluster's threads on its workers carry
// on meanwhile, and the next coordinator rejoins them; see recover.go.
func (c *Cluster) hosting(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx = flow.WithMaxParallelism(withCluster(ctx, c), c.maxParallelism())
	ctx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.ctx, cancel)
	return ctx, func() { stop(); cancel() }
}

// maxParallelism estimates how many calls the cluster can run at once: the
// largest the fleet may grow to, times how many threads each worker runs. It
// is what a run body reads through [flow.Context.MaxParallelism] to size a
// fan-out. An estimate, because a worker left to choose its own concurrency
// decides from a CPU count the coordinator cannot see (worker.go), so the
// coordinator stands in with its own.
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

// jobState is what a running job carries on a worker: which job and attempt
// it is, the worker it is on, and what its earlier attempts recorded.
//
// The worker installs one for every job it starts. It is the wings half of
// what [flow.WithProgress] installs: flow knows about progress and steps,
// and this knows about the streams a job's output goes on.
type jobState struct {
	id      string
	attempt int
	priors  []Recording
	node    *workerNode
	// outputs is the transaction everything this attempt writes goes in.
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

// streamsFor is somewhere to read and write, from a context that is either
// the coordinator's or a running job's.
//
// On the coordinator it is the cluster's own instance. On a worker it is that
// worker's own broker: a worker is not a place work dispatches to, but it does
// have storage, and a job that wants to read what a previous attempt of it
// wrote needs exactly that.
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

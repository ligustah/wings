package wings

import (
	"context"
	"errors"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
)

// Run executes body as a durable run on this cluster, the way [CoordinatorMain]
// runs your Coordinate.
//
// Every function the body calls goes to a worker and is recorded in the run's
// history, which lives on the cluster's own storage under Dir. Run the same
// name in the same Dir again and the body is replayed to where it stopped and
// carried on from there — a coordinator that crashed mid-run resumes rather
// than restarts, and one that already finished does nothing. The body must
// therefore be deterministic; [flow] says what that costs.
//
// opts are passed through to [flow.Run]; the store and the executor are this
// cluster's and cannot be overridden.
func (c *Cluster) Run(ctx context.Context, name string, body func(ctx flow.Context) error, opts ...flow.RunOption) error {
	client, err := c.sharedClient()
	if err != nil {
		return err
	}
	all := append(append([]flow.RunOption{}, opts...),
		flow.WithStore(flow.NewStore(client)),
		flow.WithExecutor(clusterExecutor{c}),
	)
	return flow.Run(withCluster(ctx, c), name, body, all...)
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
	p, err := e.c.submit(ctx, name, payload)
	if err != nil {
		return nil, err
	}
	res, err := e.c.await(ctx, p)
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

type clusterKey struct{}

// withCluster marks a context as the coordinator's, so what needs the
// cluster's own storage — reading an artifact back, replaying a recording —
// can find it.
func withCluster(ctx context.Context, c *Cluster) context.Context {
	return context.WithValue(ctx, clusterKey{}, c)
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
			"use the context Coordinate was given, or Cluster.Run or Cluster.Bind")
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

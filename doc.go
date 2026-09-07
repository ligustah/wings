// Package wings runs [flow] workflows across a fleet of workers.
//
// Work is written against package flow: functions declared with [flow.Define]
// and a root marked with [flow.Main]. A [Cluster] runs it. Where each call runs
// — a goroutine in this process, a child process, or a cloud VM — is chosen at
// startup by the [Target], and changes nothing about the code:
//
//	var Render = flow.Define(func(ctx flow.Context, f Frame) (Image, error) {
//		return render(f)
//	})
//
//	var Frames = flow.Define(func(ctx flow.Context, job Job) (flow.None, error) {
//		_, err := ctx.Map(Render, job.Frames)
//		return flow.None{}, err
//	})
//	var _ = flow.Main(Frames)
//
// A definition's name is inferred from the variable it is assigned to;
// [flow.WithName] overrides it.
//
// You write a library package, not a main. The wings build command generates
// the coordinator and worker mains; [Start] and [Cluster] are also usable
// directly.
//
// # Workers
//
// A wings program is compiled twice: a coordinator, and a worker embedded in
// it. Work functions must be registered by [flow.Define] at package scope, so
// both halves have them. On a worker, [Start] serves until shutdown and never
// returns; code before it runs on every worker.
//
// # Semantics
//
// Delivery is at-least-once — a work function may physically run more than once
// — but processing is exactly-once: a call's result is recorded and replayed, so
// the workflow observes it a single time. Make side-effecting functions
// idempotent, and independent of the coordinator's machine: a worker shares no
// filesystem, globals, or handles with it.
//
// Two bounds declared with [flow.WithTimeout] and [flow.WithHeartbeatTimeout]
// govern a long job: exceeding the total fails the call; going quiet past the
// heartbeat moves it to another worker, resuming from its last
// [flow.Context.Heartbeat].
//
// Large output does not belong in a result; stream it over a [flow.Channel]
// with [flow.ByteWriter] and [flow.ByteReader]. A per-job event log a retry can
// resume from uses [Record] and [Replay].
package wings

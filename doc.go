// Package wings distributes calls to a typed Go function across workers.
//
// You define the work function once, and call it through a [Cluster]. Where it
// actually runs — a goroutine in this process, a child process on this machine,
// or a cloud VM that did not exist a minute ago — is a one-line choice at
// startup and changes nothing about the code that defines or calls the work.
//
//	package job
//
//	var Render = wings.Define("render", func(ctx context.Context, f Frame) (Image, error) {
//		return render(f)
//	})
//
//	// Coordinate is called once, with a cluster that is already up.
//	func Coordinate(ctx context.Context, c *wings.Cluster) error {
//		images, err := wings.Map(ctx, Render, frames)
//		...
//	}
//
// You write a LIBRARY package, not a main. The `wings build` command generates
// both mains — see that command's documentation — and the target is then a flag:
//
//	myapp -target inprocess
//	myapp -target local -workers 4
//	myapp -target remote -workers 8 -min-workers 2 -max-workers 16
//
// Nothing above changes between them.
//
// # Two halves, one program
//
// A wings program is compiled twice, into a coordinator and a worker. The
// worker gets your work functions and nothing else — no provisioning, no
// [Coordinate]. The coordinator gets everything, plus the worker embedded
// inside it, so it can deploy one without a Go toolchain or a source tree.
//
// Work functions must therefore be registered by [Define] at PACKAGE SCOPE: a
// worker process never runs Coordinate, and a function defined inside it would
// not exist in the process meant to run it.
//
// # Using this package directly
//
// [Start] and [Cluster] are usable without the build tool. In a process wings
// started as a worker, [Start] SERVES UNTIL SHUTDOWN AND NEVER RETURNS — the
// lines after it are coordinator-only by construction. That is the one
// surprising thing here, so it is stated rather than discovered.
//
// # How many workers
//
// Either a fixed count ([Config.Workers]) or a policy ([Config.Scaling]) that
// follows the queue. The policy is expressed only in jobs — how much backlog one
// worker should carry, how long an idle one may linger — so the same numbers add
// goroutines, child processes or cloud VMs depending on nothing but the target.
//
// # Delivery semantics
//
// Delivery is AT-LEAST-ONCE. Each worker owns the queue of work assigned to it,
// so when a worker dies the coordinator re-dispatches whatever it had not yet
// heard back about — which means a job interrupted late may run twice. Work
// functions should be idempotent. They should also be pure with respect to
// anything on the coordinator's machine: a worker may be on another continent
// and shares no filesystem, no globals, and no open handles with the caller.
//
// # Long jobs
//
// Two bounds, both declared beside the work with [WithTimeout] and
// [WithHeartbeatTimeout], because "too slow" and "stuck" deserve different
// answers. Exceeding a total bound FAILS the call: retrying would spend the
// same time again to reach the same answer. Going quiet for longer than the
// heartbeat bound MOVES it to another worker, on the suspicion that the machine
// rather than the work is at fault.
//
// Moving is affordable because a job reports where it has got to. [Heartbeat]
// records a position; [Checkpoint] reads back whatever the previous attempt
// last recorded, so a retry resumes instead of starting over. Only the latest
// survives — it is a position, not a log.
//
// [Step] is the same thing with the bookkeeping taken away: name the phases of a
// long job, and a job that moves replays the ones that finished and runs the
// rest. What can never move is the running goroutine itself — its stack, its
// locals, its open sockets — so the only thing that crosses a machine boundary
// is a value the work function made explicit. The phase that was in flight when
// the move happened is paid for twice, and that cost is irreducible: nobody can
// say whether it finished.
//
// # Recordings
//
// A long job's progress is usually a sequence of events. [Record] keeps that
// sequence: one event is one record on a durable stream of its own, copied onto
// the coordinator's storage as it is produced, and read back one at a time with
// [Replay]. What travels in the result is a small handle.
//
// A job that is moved is handed what its earlier attempts recorded, through
// [Priors], and can play those events back into itself to reach the point the
// last one stopped at. That is the reason this is a stream of events and not a
// file: a file is opaque, and a log is a position.
//
// This is durability for the job's OWN state, deliberately outside the durable
// execution wings does for the job itself. wings stores the events and gives
// them back, and never reads one. [Step] and [Heartbeat] are
// the other thing: they are about resuming a job, not about describing what it
// did.
//
// # Artifacts
//
// Separately, and sharing nothing with the above: a job can produce a file.
// [Create] gives an io.Writer whose bytes leave the worker as they are written
// and land on the coordinator's storage, and [Open] reads them back. A retry is
// not handed its predecessor's files, because a file is not a position.
//
// # Communication
//
// Workers and the coordinator talk over durable streams, and that is an
// implementation detail on purpose: no stream, client, offset, or broker
// appears anywhere in this package's API.
//
// Every worker has a pair of streams of its own, one for its jobs and one for
// its results, and the coordinator reaches them through a dsclient.Client — the
// only durable-streams API this package's coordinator uses. What sits under
// that client is the sole difference between the targets: in process it is the
// cluster's own embedded engine, which every worker is on too, so there is no
// socket and nothing to serve; out of process it is gRPC to a broker the worker
// stood up for itself.
//
// The coordinator also keeps a durable record of its own decisions — which job
// went to which node, what came back, which workers came and went — on that
// same embedded engine. Broker-less by construction: nothing else reads it, and
// its value is that it outlives the process that wrote it. A job dispatched
// from inside a workflow is recorded with the flow, run and thread it is a step
// of, so the record answers "which activity of which run went where" and not
// merely "which job".
//
// # Machines outlive the coordinator
//
// A cloud machine does not stop existing because the process that asked for it
// died, and it does not stop billing either. So for [Remote] targets wings
// writes down what it is about to create BEFORE it creates it: it mints a lease
// of its own, records the intent, and only then calls [Provisioner.Provision].
// Recording after the fact would leave a window in which a billed machine
// exists that nothing knows about, and that is the window a crash finds.
//
// On startup, before provisioning anything, a coordinator offers every
// unreleased lease back to the provisioner via [Reattacher]. A machine whose
// worker is still alive is picked up where it left off, queue and results
// intact; one that cannot be resumed is destroyed rather than left running.
// Recovered machines count towards the worker target.
//
// What is NOT recovered is a call. A caller's goroutine died with the process
// that made it, and a result that reaches the new coordinator for a job it never
// dispatched is dropped. A workflow (package flow) is the exception: its history
// is the durable thing, and a rerun dispatches again whatever had not returned.
package wings

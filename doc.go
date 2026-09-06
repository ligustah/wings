// Package wings runs flows across workers.
//
// The work is written against package flow: functions declared with
// flow.Define, and a body that calls them. wings is an executor for those
// calls — a [Cluster] is where they go to run — and where that is — a
// goroutine in this process, a child process on this machine, or a cloud VM
// that did not exist a minute ago — is a one-line choice at startup that
// changes nothing about the code that defines or calls the work.
//
//	package job
//
//	var Render = flow.Define(func(ctx flow.Context, f Frame) (Image, error) {
//		return render(f)
//	}, flow.WithName("render"))
//
//	// A root is run as a flow once the cluster is up.
//	var Frames = flow.Define(func(ctx flow.Context, job Job) (flow.None, error) {
//		images, err := ctx.Map(Render, job.Frames)
//		...
//	}, flow.WithName("frames"))
//	var _ = flow.Main(Frames)
//
// Nothing in that file names a cluster. The body reaches it through its
// context: [Cluster.Run] runs a flow whose calls are dispatched to the
// cluster's workers and whose history is kept on the cluster's own storage, so
// a coordinator started again over the same directory replays what it already
// did. [Cluster.Bind] gives a context for calls outside any run, which are
// dispatched the same way and recorded nowhere.
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
// workflow to run. The coordinator gets everything, plus the worker embedded
// inside it, so it can deploy one without a Go toolchain or a source tree.
//
// Work functions must therefore be registered by flow.Define at PACKAGE SCOPE:
// a worker process never runs a workflow, and a function defined inside one
// would not exist in the process meant to run it.
//
// What goes to a worker is a THREAD: one forked with flow.Context.Go or
// flow.Context.Map, which runs one function on one input, and is what the
// parent's history recorded at the fork; or one forked with
// flow.Context.Spawn, which runs a closure of the workflow's own code, sent
// as its LINEAGE — the path of threads from the workflow down to it — for
// the worker to replay its way to the closure, see [flow.RunLineage]. A
// function called directly runs on the calling thread, where the call is
// made. On the worker the thread runs as a flow run of its own — see
// [flow.RunThread] — so a work function may fork, use channels, sleep and
// call other functions, and a retry replays what its predecessor already
// did. Everything an attempt writes is committed
// as one transaction at its heartbeats, steps, forks and return. A thread a
// work function forks goes back to the cluster to be placed: the coordinator
// reads the fork out of its copy of the run's history and answers it on the
// worker's control stream, so a function that fans out fans out across the
// fleet, and a worker never waits on itself. A flow.Channel handed to a
// function in its input crosses machines too: a shared channel is a durable
// stream relayed through the coordinator, which gives each value to one
// receiver, wherever that receiver runs, and keeps the channel's capacity.
//
// # Using this package directly
//
// [Start] and [Cluster] are usable without the build tool. In a process wings
// started as a worker, [Start] SERVES UNTIL SHUTDOWN AND NEVER RETURNS — the
// lines after it are coordinator-only by construction. That is the one
// surprising thing here, so it is stated rather than discovered.
//
// The other half of it: everything BEFORE Start runs in every worker too. A
// worker is this same binary, so package initialisers, flag parsing, and
// whatever main does on its way to Start all run once per worker, on the
// worker's machine. Work that belongs to the coordinator alone — opening the
// output file, reading the job list — goes after Start, or in the workflow.
//
// # How many workers
//
// Either a fixed count ([Config.Workers]) or a policy ([Config.Scaling]) that
// follows the queue. The policy is expressed only in jobs — how much backlog one
// worker should carry, how long an idle one may linger — so the same numbers add
// goroutines, child processes or cloud VMs depending on nothing but the target.
//
// A worker runs as many threads at once as its concurrency says, and a
// thread that waits — for a thread it forked, for a channel, for the clock —
// gives its slot up until the wait is over, so a worker's load is what it is
// running rather than what it holds; one that waits for long enough, or
// sleeps past flow.ShortSleep, is unloaded, and the coordinator dispatches it
// afresh when what it waits for happens. A job is placed on the least loaded
// worker when it is submitted, and queued work is evened out afterwards: a
// worker that arrives later, or frees up sooner, is given jobs still waiting
// unstarted on another's queue. Without
// that, a fleet grown or repaired mid-run would have done nothing for the work
// already queued.
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
// Two bounds, both declared beside the work with flow.WithTimeout and
// flow.WithHeartbeatTimeout, because "too slow" and "stuck" deserve different
// answers. Exceeding a total bound FAILS the call: retrying would spend the
// same time again to reach the same answer. Going quiet for longer than the
// heartbeat bound MOVES it to another worker, on the suspicion that the machine
// rather than the work is at fault.
//
// Moving is affordable because a job reports where it has got to. flow.Context.Heartbeat
// records a position; flow.Context.Checkpoint reads back whatever the previous attempt
// last recorded, so a retry resumes instead of starting over. Only the latest
// survives — it is a position, not a log.
//
// A job whose phases are expensive breaks them into calls to other functions:
// each call's result is recorded, so a job that moves replays the calls that
// finished and runs the rest. What can never move is the running goroutine
// itself — its stack, its locals, its open sockets — so the only thing that
// crosses a machine boundary is a value made explicit. The phase in flight when
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
// them back, and never reads one. flow.Context.Heartbeat is the other thing:
// it is about resuming a job, not about describing what it did.
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
// its value is that it outlives the process that wrote it. A job is recorded
// with the run, thread and step it is a call of, so the record answers "which
// call of which run went where" and not merely "which job".
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
// What is NOT recovered is a bare call. A caller's goroutine died with the
// process that made it, and a result that reaches the new coordinator for a
// job it never dispatched is dropped. A run is different: its history is the
// durable thing, and a coordinator started again over the same Dir replays
// what returned and dispatches again whatever had not — which is what makes
// a workflow, itself a run, survive the process that was running it.
package wings

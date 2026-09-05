# wings

Define a typed Go function. Call it across a cluster that did not exist a minute ago.

```go
var Render = flow.Define("render", func(ctx flow.Context, f Frame) (Image, error) {
    return render(f)
})

var Main = flow.DefineWorkflow("render", func(ctx flow.Context, job Job) error {
    images, err := ctx.Map(Render, job.Frames)   // runs wherever the workers are
    ...
})
```

That is the whole API. The functions and the body are written against
[`flow`](flow), a durable-execution framework that knows nothing about
clusters; wings is where a flow's calls go to run. The context they are given
is a `flow.Context` — a `context.Context`, so it goes anywhere one is wanted,
with everything flow can do for that code as methods on it. Where that is — goroutines
here, child processes on this machine, or GCP VMs provisioned on demand — is a
flag at run time, not a change to the code.

```sh
./myapp -target inprocess          # goroutines in this process
./myapp -target local -workers 4   # child processes on this machine
./myapp -target remote -workers 8 -provider gcp \
        -gcp.project my-proj -gcp.zone europe-west1-b
```

The worker count can follow the queue instead of being chosen:

```sh
./myapp -target remote -min-workers 2 -max-workers 16 -jobs-per-worker 4 \
        -provider gcp -gcp.project my-proj -gcp.zone europe-west1-b
```

Your program names no cloud, imports no SDK and holds no credentials. It cannot
tell which of those three it is running under — `oblivious_test.go` parses the
example and fails the build if it ever gains a `Target`, a provisioner or a
cloud import.

## Build

`wings build` compiles your program twice and embeds the worker in the
coordinator, so the coordinator is self-contained: no Go toolchain, no source
tree, nothing to copy alongside it.

```sh
go run github.com/ligustah/wings/cmd/wings build \
    -pkg ./job \
    -coordinator windows/amd64 \
    -worker linux/amd64 \
    -o myapp.exe
```

```
worker       linux/amd64      15.2 MB
embedded     linux/amd64       5.9 MB (gzipped)
coordinator  windows/amd64    38.1 MB
```

The two halves are separate programs generated into a scratch directory (your
source tree is not written to):

| | contains | built for |
|---|---|---|
| **worker** | your work functions, the worker loop | the machines it will run on |
| **coordinator** | your work functions, your workflows, the providers, the embedded worker | the machine *you* run it on |

Because they are separate, **the coordinator is never cross-compiled** — only
the worker is — and the worker does not link the cloud SDK that deploys it. That
split is worth 13 MB on the example's worker.

`-providers` picks which clouds the coordinator can reach (default `gcp`, empty
links none); `-coordinator-pkg` splits your own package if it too should stay out
of the worker.

## What you write

A **library** package, not a `main`. `wings build` generates both mains and
imports your package into each.

```go
package job

import "github.com/ligustah/wings/flow"

// Work functions are package-scope vars, so a worker process — which never runs
// a workflow — still has them registered.
var Render = flow.Define("render", func(ctx flow.Context, f Frame) (Image, error) {
    return render(f)
})

// A workflow is run as a flow once the cluster is up. A coordinator restarted
// over the same -dir replays what it already did rather than doing it again.
var Main = flow.DefineWorkflow("render", func(ctx flow.Context, job Job) error {
    images, err := ctx.Map(Render, job.Frames)
    if err != nil {
        return err
    }
    return write(images, job.Out)
})
```

That is the whole file. No cloud appears in it — and no cluster either. The
cluster reaches the body through its context: every thread a flow forks —
`ctx.Map`, `ctx.Go` — goes to the placer bound there, which for the
coordinator is the cluster's workers. A function called directly runs where
the call is made, on the thread that made it.

A package that defines one workflow is a binary that runs it. Define several
and the binary takes `-workflow <name>`; leave it off and it lists them. Each
runs under its own name in `-dir`, so two workflows over one directory keep
separate histories.

The workflow's input is a typed value, given on the command line as JSON:

```sh
./myapp -target local -input '{"frames":["a.blend","b.blend"],"out":"./frames"}'
./myapp -target local -input @job.json
```

Start a fresh run without it and the binary shows the shape it wants. A
workflow that takes nothing declares `flow.None`. The input is part of the
run's history: a coordinator restarted over the same `-dir` is given what the
first start recorded, and one restarted with a different `-input` is refused
rather than quietly replaying one input's history against another.

Flags you register in that package are parsed too — `CoordinatorMain` calls
`flag.Parse()` on the default set, so your own flags sit beside `-target` and
`-workers` without wings knowing about them.

## Autoscaling

Set `-max-workers` (or `Config.Scaling`) and wings sizes the cluster from the
queue: workers wanted is outstanding jobs ÷ `-jobs-per-worker`, rounded up and
clamped between `-min-workers` and `-max-workers`. A worker idle for longer than
`-idle-timeout` is retired.

The policy names no provider — it is jobs and durations only — so the same
numbers add goroutines, child processes or VMs depending on nothing but
`-target`. Which means a policy you tuned locally means the same thing in
production.

A plain `-workers N` is the same policy with the floor and the ceiling both at
N: there is one loop that keeps the fleet at its size, however the size was
asked for. So a fixed fleet is *maintained*, not launched once — a worker that
dies or is preempted is replaced on the next tick, and a job with nowhere to go
in the meantime waits for the replacement rather than failing. The caller's own
context bounds that wait.

A worker that arrives is also *given something to do*. A job is placed on the
least loaded worker when it is submitted, so without more a replacement — or a
scale-up — would find every queued job already addressed to somebody else and
sit idle while the survivor worked through a queue built for two. So once a
tick the watchdog moves jobs still waiting unstarted on one worker's queue to
a worker with clearly less to do, until the two are within a job of each
other. Such a move does not count against the job's attempts: nothing ran.

Scaling down never drops work: a worker is retired only while idle, and idle is
decided under the same lock that assigns jobs, so nothing can be sent to a worker
already on its way out. Failing to provision is not fatal — the cluster keeps
running at its current size and tries again on the next tick, because a quota
refusal should cost throughput, not the run.

| flag | meaning |
|---|---|
| `-max-workers` | ceiling; setting it is what turns autoscaling on. A spend limit as much as a capacity one |
| `-min-workers` | floor, held even with an empty queue (at least 1) |
| `-jobs-per-worker` | backlog one worker is expected to carry (default: `-concurrency` if set, else 1) |
| `-idle-timeout` | how long a worker must have had nothing to do (default 60s) |
| `-scale-interval` | how often the policy is evaluated (default 2s) |
| `-max-scale-step` | most workers one decision may add — lower it when provisioning is rate-limited |

`-idle-timeout` is the one to think about, because the right value is dominated
by what a *replacement* costs. A goroutine is free to recreate; a VM is minutes
of boot plus an upload, so a timeout that looks thrifty locally can leave a
remote cluster permanently rebuilding itself.

## How it works

Every worker owns a pair of [durable-streams](../durable_streams) streams,
`wings.jobs.<worker>` and `wings.results.<worker>`. The coordinator writes jobs
to a chosen worker and tails that worker's results. **Workers never talk to each
other**, and the coordinator only ever dials outward — so it works from a laptop
behind NAT.

The coordinator speaks only `dsclient`. What sits under that client is the sole
difference between the three targets:

```
Coordinator ──dsclient.Client──> dswire.Backend
                                   ├─ the cluster's own embedded engine  (goroutine workers)
                                   ├─ gRPC on 127.0.0.1                  (child process)
                                   └─ gRPC through an SSH tunnel         (cloud VM)
```

In process there is nothing to serve and nothing to dial, because both halves
are the same program: coordinator and *all* its workers meet on one embedded
engine, opening the same streams from either side. Out of process each worker
stands up a single-node broker for itself and the coordinator reaches it over
gRPC.

One seam, three constructors. The worker loop, the encoding and the dispatch are
identical in all three, which is why a bug that only shows up on a cloud VM is a
bug in the transport rather than in your work.

Streams, offsets, brokers and clients appear nowhere in the public API.

### The coordinator's own record

On that same embedded engine — broker-less, no listener, no port — the
coordinator keeps a stream of what it decided: every job accepted and **which
node it was sent to**, every result that came back, every redispatch after a
worker was lost, every worker that entered or left service. Nothing reads it
during the run. Its value is that it outlives the process, so a coordinator that
died has still left an account of what it had done.

When the job was a thread of a flow run — which on a coordinator every job is,
since the workflow itself is one — the entry says so: the run and the thread.
That is what makes the record answerable at the level anyone actually asks
at: not "job 3f went to remote-2" but "thread main.1 of order-77 went to
remote-2 and never came back". Only the run knows which run a thread belongs
to, so it stamps it on the dispatch and the coordinator writes it down. A bare
call made on `Cluster.Bind` belongs to nothing larger and leaves those columns
empty.

Writes go through a buffered channel drained by one goroutine and batched, so
recording never becomes backpressure on the work. A full buffer drops entries
rather than blocking a submit — and counts them, so a gap in the record is
reported rather than silent.

Every entry names the **coordinator run** that wrote it. The record is
append-only and survives the process, so several runs share it, and a reader who
cannot tell them apart cannot tell a job that is still outstanding from one a
previous run finished. For the same reason every name a coordinator mints —
worker ids, job ids — carries that run's short identifier: names outlive the
process that chose them (a worker's mirror stream is named after the worker and
sits on a persistent `Dir`), so a counter restarting at zero would hand a new
worker a name whose stream already has a read position, and every job sent to it
would go unanswered. That identifier is deliberately *not* stored: its whole
purpose is to differ from last time.

### Machines outlive the coordinator

A cloud machine does not stop existing because the process that asked for it
died, and it does not stop billing either. So before wings asks a cloud for
anything it mints a **lease** — an identity of its own — and writes an *intent*
record to a second stream on that same embedded engine. Only then does it call
`Provision`. The machine is recorded as ready once a worker is running on it,
and released when it is destroyed.

Write-ahead is the whole point. Recording a machine once the API returned would
leave a window in which a billed VM exists that nothing on earth knows about,
and that window is exactly the one a crash finds. A failure to write the intent
refuses the launch outright, for the same reason.

On startup, before it provisions anything, a coordinator reads that record and
offers every unreleased lease to the provisioner. What comes back is still out
there: a machine whose worker is alive is picked up where it left off — tunnel
reopened, queue and results intact, no upload and no restart, because the worker
was launched detached precisely so it outlives the session that started it — and
one that cannot be resumed is destroyed rather than left running. Leases nothing
came back for are closed, so no later start hunts for a machine that is already
gone. Recovered machines count towards the worker target, so a restart provisions
only the difference.

### What a coordinator restart keeps

Be precise about which half may die. A **worker** may: its queue is on its own
streams, and the coordinator redispatches what it had not heard back about. A
**machine** may lose its coordinator: the lease record above brings it back.
The **coordinator itself** may not, for a bare call. The goroutine that was
waiting for the answer died with the process, and the new process — which reads
its own record and each worker's mirror only for the position to continue from —
drops a result for a job it never dispatched. The work a worker was still doing
finishes and is written down, and nobody collects it.

What does survive a coordinator restart is a **flow run** ([`flow`](flow)), and
a workflow is one: its history is kept under `-dir`, and a coordinator started
again over the same directory replays what returned and dispatches again what
had not. A call that was in flight when the coordinator died is therefore run
twice, once by each coordinator, and the second copy is the one whose answer
counts. Work functions are idempotent for exactly this reason. A workflow
that already finished does nothing at all on a restart.

### Remote deployment

For `-target remote`, per machine: provision → wait for SSH → upload the
embedded worker → start it bound to **loopback only** → open an SSH port-forward
→ dial the broker through it.

The broker speaks no authentication, so the tunnel *is* the access control.
Nothing wings starts is reachable from the internet, and no firewall rule is
needed. An ephemeral ed25519 keypair is minted per run and installed via
instance metadata; nothing wings creates outlives the cluster.

**Spot instances** (`-gcp.spot`) are the cheap option and a real one: a
preempted worker is a lost worker, its jobs are moved and the scaler replaces
the machine. Google announces a preemption thirty seconds ahead, and the worker
uses them: it tells the coordinator, which moves its jobs then and there, and
stops taking new ones. A preempted instance deletes itself rather than
stopping, and for a worker that simply stops answering the coordinator asks the
cloud whether it still exists, so a preemption costs seconds rather than the
whole reconnect window either way. Idempotent work is the price.

The worker goes up **straight from memory** — it is decompressed once and each
machine's upload reads from that one copy, so nothing is written to the
coordinator's disk merely to have a path to hand to something. The transfer
speaks the scp source protocol over an ordinary exec channel rather than using
SFTP, because SFTP is a *subsystem* an SSH server need not offer, while running
a command is a capability wings already depends on to start the worker at all.
The protocol carries the size, so a connection that dies mid-copy is an error
rather than a truncated executable that fails confusingly later.

## Delivery semantics

**At-least-once.** A worker owns the queue of work assigned to it, so when one
dies the coordinator re-dispatches whatever it had not yet heard back about —
which means a job interrupted late may run twice.

**Work functions must be idempotent.** They should also be pure with respect to
the coordinator's machine: a worker may be on another continent and shares no
filesystem, no globals and no open handles with the caller.

A work function that returns an error is a normal result, not a transport
failure; the error is re-raised on the coordinator as a plain error (the value
does not survive the trip, only the message). A work function that panics costs
one job, not the worker.

## Long jobs

Two different bounds, because "too slow" and "stuck" deserve different answers,
and both are declared beside the work rather than on the cluster — one function
is a millisecond of arithmetic and another an hour of transcoding, and a single
cluster-wide number is either useless to one or fatal to the other.

```go
var Transcode = flow.Define("transcode", transcode,
    flow.WithTimeout(2*time.Hour),            // total: exceeding it FAILS
    flow.WithHeartbeatTimeout(30*time.Second) // quiet: exceeding it MOVES
)
```

`WithTimeout` bounds one call end to end. A call that blows it fails and is not
retried: exceeding a bound on total duration says the work is too slow or stuck
on something no other machine would be luckier with, and retrying would spend
the same time again to reach the same answer. The worker enforces it locally, so
the error names the function and the bound.

`WithHeartbeatTimeout` says the opposite: the machine is the suspect. A job that
goes quiet for longer is moved to another worker. Moving it is affordable
because the job reports where it has got to as it goes:

```go
func transcode(ctx flow.Context, in Job) (Out, error) {
    from, _, err := ctx.Checkpoint[int]()   // 0 on the first attempt
    if err != nil {
        return Out{}, err
    }
    for i := from; i < in.Frames; i++ {
        // ... one frame ...
        ctx.Heartbeat(i+1)
    }
}
```

Only the latest heartbeat survives — this is a position, not a log — and it is
handed to the next attempt, which resumes from it instead of starting over. Beats
ride a stream of their own per worker rather than the result stream, which is
transactional and would not make them visible until the job finished, which is
exactly too late. Delivery is best-effort by design: a beat that goes missing
costs a retry a little redone work, while a beat that blocked the work function
to guarantee delivery would cost the work.

Both clocks start when the worker begins the job, not when it was sent: a job can
wait behind others on a busy worker for longer than its own bound, and none of
that is the work's fault. The heartbeat clock then runs from that start, so a
function that declares one must actually beat. A third bound, `WithStartTimeout`,
is for the wait itself — how long a job may sit unstarted on a worker's queue
before it is moved — and is off unless asked for, because on a saturated cluster
moving a queued job only puts it at the back of another queue.

### Steps

`Step` is the same mechanism with the bookkeeping taken away. Name the
phases of a long job and a move replays the ones that finished:

```go
func restore(ctx flow.Context, in Backup) (Report, error) {
    snap, err := ctx.Step("snapshot", func(ctx flow.Context) (Snapshot, error) {
        return takeSnapshot(ctx, in.Source)      // twenty minutes
    })
    if err != nil {
        return Report{}, err
    }
    return ctx.Step("restore", func(ctx flow.Context) (Report, error) {
        return restoreInto(ctx, snap, in.Target) // another forty
    })
}
```

A job moved after the snapshot finished replays it — a decode, not twenty
minutes — and starts the restore on the new worker. The phase that was actually
in flight is paid for twice, and that cost is irreducible: nobody can say whether
it finished.

Steps are identified by their position, so they must be called in the same order
every attempt, from one goroutine; a name that does not match the one recorded at
that position is an error rather than somebody else's value. Each finished step
is one message to the coordinator, which accumulates them, so a step costs the
same however many came before it — but it is a message all the same. Use steps
for coarse phases and `Heartbeat` for a position inside a loop.

### Inside a run

A work function checkpoints the same way whatever called it — `Step` and
`Heartbeat` do not know or care which run they are part of. What the run adds
is a second way to be interrupted: the run itself can fail and be retried while
the thread is still running. The retry **rejoins** the thread already in
flight instead of dispatching a second copy, so the progress that copy would
have thrown away is kept. The coordinator recognises it by the run and the
thread's name, which replay gives it again on every attempt.

### The thread is the unit

A flow run is made of **threads**: the body is the main thread, and every
`ctx.Go`, `ctx.Map` and `ctx.Spawn` forks another. Each thread has a history
of its own, on a stream of its own. The fork in the parent's history says what
the thread is to do — the function and its input — and the join says what it
produced; a replay of the parent that finds the join never runs the thread
again, and one that finds only the fork starts the thread, which replays *its*
history and carries on. That is the whole reason a thread is what the cluster
hands to a machine: its stream is all that has to travel.

So what goes to a worker is a thread that runs a function, and it runs there
as a **flow run of its own**, with its history on the worker's storage. A work
function may do everything a workflow body may — fork with `ctx.Go` and
`ctx.Spawn`, use a channel between its threads, `ctx.Map` over other
functions, read `ctx.Now`, `ctx.Sleep`, wrap an outside answer in
`ctx.Effect` — and a retry **replays** all of it from the history instead of
doing it again. A thread it forked before it was moved is answered from the
record; the one it was waiting on is rejoined. The same determinism rules
apply as to any run body, and a function that uses none of those primitives
records nothing and behaves exactly as before.

A function called **directly** — `Digest(ctx, w)` rather than
`ctx.Go(Digest, w)` — runs on the calling thread, wherever that is: on the
coordinator in a workflow body, on the worker inside a work function. It
blocks its caller either way, so sending it elsewhere would move the CPU while
the caller's slot sat idle. It is recorded and replayed like any call; it is
just not placed. Fan out with `Map` or `Go` when the point is other machines.

Everything an attempt writes on its worker — that history, its recordings, its
files — goes into **one transaction**, committed at the points that mean
something: before every heartbeat and step report, when the function returns,
and by age as a net under a function that reports nothing for a long time. What
the coordinator is told about progress is therefore never ahead of what it can
copy, and what a retry is handed is consistent across all of them: the history
that says which recordings were made and the recordings themselves were
committed together.

A worker runs as many threads at once as its concurrency says, and **a thread
that waits is not running**: waiting for a thread it forked, for a channel or
for the clock, it gives its slot up, and takes one back — ahead of any job
still queued — when the wait is over. A wait that lasts is reported, so the
coordinator neither moves the job for silence nor counts it against the
worker's load. One worker with one slot can therefore run a job and the
thread that job is waiting on.

A thread a work function forks is **the cluster's to place**, like any other.
There is no request message: the coordinator keeps a copy of every attempt's
history, reads the forks out of it, runs each where the load is lowest and
sends the result back on the worker's control stream. Forking is therefore a
commit point on the worker. The run is named for the job, not the attempt, so
a retry that replays a fork presents the same thread, and the coordinator
hands back the result it kept — or lets the retry rejoin the thread still in
flight — rather than running it twice. A job waiting on a thread it forked is
not moved for silence, however long the thread takes; its total timeout still
runs. A thread of run code — `ctx.Spawn` — has no function a worker could be
handed, and runs where its parent is.

A wait that lasts is **unloaded**. A thread parked for a minute — on a join, a
receive, a send — has its attempt ended where it stands, history committed,
and the job handed back to the coordinator with a note of what it was waiting
for; a sleep past `flow.ShortSleep` is handed back at once, with the wake-up
time. The coordinator keeps the job off every worker until the condition
holds — the deadline passes, the thread it was joining finishes, something
arrives on the channel — then dispatches it afresh to whichever worker is
least loaded, history first, and it replays to the wait and finds what it was
waiting for. A worker keeps nothing of a thread that is waiting for tomorrow.

### Channels across machines

A `flow.Channel` **travels in a call's input** like any other value. The
workflow creates one and hands it to a function; the function sends into it or
receives from it; two functions on two workers can share one the same way. On
the wire a shared channel is a durable stream relayed through the coordinator:
every run that uses it has an outbox — on a worker, written inside the
attempt's transaction and copied home like a recording — and the coordinator
merges the outboxes into one canonical stream and pushes it to every worker
using the channel. A worker subscribes by creating its outbox, which the output
mirror discovers; nothing asks.

Two things differ from a channel between threads. A shared channel is a
**queue, not a rendezvous**: `Send` completes once the value is durable,
whatever capacity the channel was created with. And **every run that receives
sees every value** — threads within one run still compete, but two runs each
get the whole sequence. Receives replay exactly as they do between threads: a
moved function is handed the same values in the same order from the channel's
record, on a worker that never saw the sender.

## Recordings

A long job's progress is usually a **sequence of events** — a simulation's ticks,
a solver's moves, a crawl's fetches. wings can keep that sequence for you, and
give it back to the job when the job has to start again somewhere else.

```go
rec, err := wings.Record[Event](ctx, "replay")
for step := range simulation(ctx) {
    if err := rec.Record(step.Event()); err != nil { return Result{}, err }
}
if err := rec.Close(); err != nil { return Result{}, err }
return Result{Replay: rec.Recording()}, nil    // a handle, not the events
```

and on the coordinator:

```go
for ev, err := range wings.Replay[Event](ctx, played.Replay) {
    if err != nil { return err }
    ...
}
```

**One event is one record.** They go onto a durable stream of their own on the
worker, become visible at the job's commit points — every heartbeat, every step,
its return — are copied onto the coordinator's own storage as they are
committed, and come back out one at a time in the order they went in. Neither end ever holds the log:
a reader takes as many as it wants and pays for no more, and breaking out of the
range stops the reading.

**A retry gets handed what its predecessor wrote.** This is the part that makes
it worth doing at all. A simulation that streams a hundred megabytes of events
and dies at minute fifty is no use if the retry starts from zero.

```go
if priors := wings.Priors(ctx); len(priors) > 0 {
    for ev, err := range wings.Replay[Event](ctx, priors[len(priors)-1]) {
        if err != nil { break }    // a log nobody closed stops early; that is fine
        sim.Apply(ev)
    }
}
```

Each prior is a **prefix** of the same work, not a continuation — attempt 0 and
attempt 1 both start from the beginning — so replay one of them, not all. None is
`Complete` and none reports its `Events`, because the attempt that would have
counted them did not survive to. A truncated log is precisely a record of how far
the work got, and `Replay` reads one to whatever end it has.

This is durability for the job's **own** state, deliberately outside the durable
execution wings does for the job itself. `Step` and `Heartbeat` are for resuming
a job; a recording is for describing what it did. wings stores the events and
gives them back, and never reads one.

**Cleaning up.** A recording lives on the coordinator's `Dir` until
`rec.Discard(ctx)` — only the caller knows when it has been read. The recordings
of attempts that were abandoned are removed without being asked once the job
settles: nobody holds a handle to those.

## Artifacts

Separately, and with nothing in common but the word "big": a job can produce a
**file**. A result is one record on one stream, held whole in memory at both
ends, which is the wrong shape for a render, an archive or a core dump.

```go
out, err := wings.Create(ctx, "render")
_ = encode(ctx, out)                  // an ordinary io.Writer
_ = out.Close()
return Result{Video: out.Artifact()}, nil
```

read back with `wings.Open(ctx, a)`, an `io.ReadCloser`, and removed with
`a.Discard(ctx)`. What a job writes is opaque bytes in whatever format it and its
caller agree on.

Artifacts are not events and are not replayed into anything. A retry is not
handed its predecessor's files, because a file is not a position.

### What cannot be moved

The running goroutine. Its stack, its locals, its open sockets and half-filled
buffers are on that machine and stay there — Go cannot serialise a running
goroutine, and much of what one holds is not serialisable at anyone's hands. So
the only thing that can cross a machine boundary is a value the work function
made explicit, which is what a checkpoint and a step are. Everything else the
coordinator has — the pending set, the mirrored results, the queue — it already
holds, and none of it is what a half-finished job knows.

## Configuration

Used directly rather than through `wings build`:

```go
c, err := wings.Start(ctx, wings.Config{
    Target:      wings.Remote(gcp.New(gcp.Config{Project: "p", Zone: "z"})),
    Workers:     8,               // or a Scaling policy instead
    Concurrency: 4,               // running threads per worker; 0 = the worker decides
    JobTimeout:  5 * time.Minute,
})
defer c.Stop(ctx)
```

`Stop` deletes provisioned machines. They are billed until it runs — call it in
a defer.

A coordinator built with plain `go build` carries no worker and cross-compiles
one at dispatch time instead, which needs a Go toolchain and the module source.
`wings build` is what removes that requirement.

## Other clouds

Two ways, depending on whether the choice should be a flag.

**A flag-selectable provider.** Implement `Provider` and register it from an
init, the way a database driver does. `wings build -providers you/cloud` links it
into the coordinator, and it becomes `-provider yourcloud` with its flags
prefixed `-yourcloud.*`.

```go
type Provider interface {
    Name() string
    Flags(fs *flag.FlagSet)
    New() (wings.Provisioner, error)
}
```

**One baked in.** Export `func Provisioner() wings.Provisioner` from your
package; it takes precedence over `-provider`. This is the escape hatch, and it
is the one thing that puts a cloud back into your source.

Either way the SSH deployment, tunnelling and worker protocol are shared:

```go
type Provisioner interface {
    Provision(ctx context.Context, leases []string) ([]Machine, error)
}

type Machine interface {
    ID() string
    Upload(ctx context.Context, src io.Reader, size int64, remotePath string) error
    Start(ctx context.Context, cmd string, env map[string]string) error
    Forward(ctx context.Context, remotePort int) (localAddr string, err error)
    Close(ctx context.Context) error
}
```

The **leases** are identities wings minted, not names the cloud chose, and that
direction is the point: the coordinator writes down what it is about to create
*before* it creates it. Your implementation must make each machine findable by
its lease afterwards — as its name, a tag, a label, whatever the cloud offers —
and `ID()` must return it. A lease is ten lowercase alphanumeric characters
starting with a letter, so it is safe to embed in any cloud's naming rules.

Implement `Reattacher` too if the cloud can look a machine up, which is nearly
all of them:

```go
type Reattacher interface {
    Provisioner
    Reattach(ctx context.Context, leases []string) ([]Machine, error)
}
```

Return only the machines that still exist; say nothing about the rest and wings
closes them out. Without it a restarted coordinator can only destroy what it
finds, which is safe and wastes everything those machines had done.

## Requirements

- Go 1.27+ (generic methods; inherited from durable_streams)
- `../durable_streams` beside this repo — it is not published, so `go.mod`
  resolves it by `replace`

## Example

[`examples/digest`](examples/digest) is a complete program — one work function
and a coordinator body, naming no cloud — that runs on all three targets.

```sh
go run ./cmd/wings build -pkg ./examples/digest -o digest
./digest -target local -workers 4 -jobs 32
```

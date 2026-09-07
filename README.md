# wings

Define a typed Go function. Call it across a cluster that did not exist a minute ago.

```go
var Render = flow.Define(func(ctx flow.Context, f Frame) (Image, error) {
    return render(f)
})

// A root, marked with flow.Main, runs as a flow once the cluster is up.
var Frames = flow.Define(func(ctx flow.Context, job Job) (flow.None, error) {
    images, err := ctx.Map(Render, job.Frames)   // runs wherever the workers are
    if err != nil {
        return flow.None{}, err
    }
    return flow.None{}, write(images, job.Out)
})
var _ = flow.Main(Frames)
```

Both are plain functions declared with `flow.Define`: one called or forked is what
another system would call an activity; one marked `flow.Main` is what it would call
a workflow. Same declaration, same replay, same durability. They are written against
[`flow`](flow), a durable-execution package that knows nothing about clusters; wings
is where a flow's calls go to run. Where that is — goroutines here, child processes,
or cloud VMs provisioned on demand — is a flag at run time, not a change to the code:

```sh
./myapp -target inprocess          # goroutines in this process
./myapp -target local -workers 4   # child processes on this machine
./myapp -target remote -workers 8 -provider gcp \
        -gcp.project my-proj -gcp.zone europe-west1-b
```

Or let the worker count follow the queue:

```sh
./myapp -target remote -min-workers 2 -max-workers 16 -jobs-per-worker 4 \
        -provider gcp -gcp.project my-proj -gcp.zone europe-west1-b
```

Your program names no cloud, imports no SDK, and holds no credentials.

## What you write

A **library** package, not a `main`. `wings build` generates both mains and imports
your package into each.

```go
package job

import "github.com/ligustah/wings/flow"

var Render = flow.Define(func(ctx flow.Context, f Frame) (Image, error) {
    return render(f)
})

var Frames = flow.Define(func(ctx flow.Context, job Job) (flow.None, error) {
    images, err := ctx.Map(Render, job.Frames)
    if err != nil {
        return flow.None{}, err
    }
    return flow.None{}, write(images, job.Out)
})
var _ = flow.Main(Frames)
```

A definition's name is inferred from the variable it is assigned to — `Render`,
`Frames` — so `flow.WithName("…")` is needed only to override it. The name identifies
the function in a run's history and on the wire.

Mark one root with `flow.Main` and the binary runs it; mark several and it takes
`-workflow <name>`. The input is a typed value given as JSON, and is part of the run's
history — a coordinator restarted over the same `-dir` replays the recorded input:

```sh
./myapp -target local -input '{"frames":["a.blend","b.blend"],"out":"./frames"}'
./myapp -target local -input @job.json
```

## Build

`wings build` compiles your program twice and embeds the worker in the coordinator,
so the coordinator is self-contained — no Go toolchain, no source tree.

```sh
go run github.com/ligustah/wings/cmd/wings build \
    -pkg ./job -coordinator windows/amd64 -worker linux/amd64 -o myapp.exe
```

| | contains | built for |
|---|---|---|
| **worker** | your work functions, the worker loop | the machines it runs on |
| **coordinator** | your functions, your workflows, the providers, the embedded worker | the machine you run it on |

Only the worker is cross-compiled, and it does not link the cloud SDK that deploys
it. `-providers` picks which clouds the coordinator can reach (default `gcp`).

## Delivery and processing

Delivery is **at-least-once**: a worker owns its queue, so when it dies the
coordinator redispatches whatever it had not heard back about, and a job interrupted
late may run twice. Processing is **exactly-once**: a call's result is recorded and
replayed, so a workflow observes it a single time however many times the job
physically ran. Make side-effecting work functions idempotent, and independent of the
coordinator's machine — a worker shares no filesystem, globals, or handles with it.

A work function that returns an error is a normal result (the message crosses the
boundary, not the error value). One that panics costs one job, not the worker.

## Long jobs

Two bounds, declared beside the work, because "too slow" and "stuck" deserve
different answers:

```go
var Transcode = flow.Define(transcode,
    flow.WithTimeout(2*time.Hour),             // total: exceeding it FAILS the call
    flow.WithHeartbeatTimeout(30*time.Second), // quiet: exceeding it MOVES it
)
```

`WithTimeout` fails a call that runs too long (retrying would spend the same time for
the same answer). `WithHeartbeatTimeout` moves a job that goes quiet to another
worker. Moving is affordable because a job reports where it got to:

```go
func transcode(ctx flow.Context, in Job) (Out, error) {
    from, _, err := ctx.Checkpoint[int]()   // 0 on the first attempt
    if err != nil {
        return Out{}, err
    }
    for i := from; i < in.Frames; i++ {
        // ... one frame ...
        ctx.Heartbeat(i + 1)
    }
}
```

Only the latest heartbeat survives — a position, not a log — and the next attempt
resumes from it. A job whose phases are expensive breaks them into calls to other
functions: each call's result is recorded, so a move replays the phases that finished
and runs the rest.

## Channels

A `flow.Channel` travels in a call's input like any other value; two functions on two
workers can share one. On the wire it is a durable stream relayed through the
coordinator: each value goes to one receiver, capacity holds across machines, and
receives replay in the recorded order.

Large output does not belong in a result. Stream it over a channel of `flow.Bytes`,
which `flow.ByteWriter` and `flow.ByteReader` turn into an `io.Writer` and
`io.Reader`:

```go
var Render = flow.Define(func(ctx flow.Context, in RenderIn) (flow.None, error) {
    w := flow.NewByteWriter(ctx, in.Out)   // an ordinary io.Writer
    defer w.Close()
    return flow.None{}, encode(w)
})

// The workflow makes the channel, forks the render, and reads the bytes back.
out := ctx.NewBufferedChannel[flow.Bytes](8)
done := ctx.Go(Render, RenderIn{Out: out})
_, err := io.Copy(dst, flow.NewByteReader(ctx, out))
```

Chunks are `flow.ByteChunk` in size, recorded and replayed like any channel value, so
a moved job gets back exactly what it streamed.

Where a channel is drained matters. A `Recv` on the run's own thread takes from a
buffer the coordinator holds; a `Recv` on a *worker* thread is a round trip to the
coordinator per item — a want, a park, and a wake. For a high-volume channel, drain
it on the run's own thread (or a thread that stays on the coordinator) rather than
forking the draining loop onto a worker.

`NewUnboundedChannel` has no capacity limit: `Send` never blocks and a sender is
never parked. A parked sender can be *unloaded* — its attempt cancelled and its
whole job replayed on the next one — so when the receiver is guaranteed to drain
the channel, an unbounded channel avoids that cost. It can grow without limit if
the receiver falls behind, so use it only where the drain keeps up.

## Recordings

A long job's progress is often a sequence of events. `Record` keeps one on a durable
stream copied to the coordinator; a moved attempt is handed what its predecessors
wrote, through `Priors`, and replays it to catch up.

```go
rec, err := wings.Record[Event](ctx, "replay")
for step := range simulation(ctx) {
    if err := rec.Record(step.Event()); err != nil { return Result{}, err }
}
if err := rec.Close(); err != nil { return Result{}, err }
return Result{Replay: rec.Recording()}, nil    // a handle, not the events
```

## Inspecting a run

`-ui 127.0.0.1:8080` serves a read-only web UI and JSON API on the coordinator: the
cluster's workers and pending queue, the runs it has recorded, and each run's threads
with their decoded event timeline.

```
./myapp -target local -workers 4 -ui 127.0.0.1:8080
```

To look at a run that has already stopped, point `-inspect` at its `-dir` — it serves
the same UI over the recorded history and runs no workflow:

```
./myapp -inspect ./wings-data -ui 127.0.0.1:8080
```

The API is the same data the UI draws: `GET /api/runs`, `/api/runs/{run}`,
`/api/workers`, `/api/pending`, `/api/status`. Histories are complete in the
coordinator's engine for the in-process and `-local-shared-broker` targets; for a
target where each worker keeps its own data, `-inspect` that directory instead.

## Autoscaling

Set `-max-workers` (or `Config.Scaling`) and wings sizes the fleet from the queue:
workers wanted is outstanding jobs ÷ `-jobs-per-worker`, clamped to
`[-min-workers, -max-workers]`. The policy names no provider, so the same numbers add
goroutines, child processes, or VMs depending only on `-target`.

| flag | meaning |
|---|---|
| `-max-workers` | ceiling; setting it turns autoscaling on |
| `-min-workers` | floor, held even with an empty queue (at least 1) |
| `-jobs-per-worker` | backlog one worker should carry (default: `-concurrency`, else 1) |
| `-idle-timeout` | how long a worker must sit idle before retirement (default 60s) |
| `-scale-interval` | how often the policy runs (default 2s) |
| `-max-scale-step` | most workers one decision may add |

A plain `-workers N` is the same policy with floor and ceiling at N — a fixed fleet is
*maintained*, so a worker that dies or is preempted is replaced.

## How it works

Every worker owns a pair of [durable-streams](https://github.com/ligustah/durable_streams)
streams for its jobs and results. The coordinator writes jobs to a chosen worker and
tails its results; **workers never talk to each other**, and the coordinator only
dials outward, so it runs from a laptop behind NAT. The coordinator speaks only
`dsclient`; what sits under it is the only difference between targets:

```
Coordinator ──dsclient──> ├─ the cluster's own embedded engine   (in process)
                          ├─ gRPC on 127.0.0.1                    (child process)
                          └─ gRPC through an SSH tunnel           (cloud VM)
```

Durability rests on a few guarantees:

- **A flow run survives a coordinator restart.** Its history lives under `-dir`; a
  coordinator started again over the same directory replays it to where it stopped
  and **rejoins** the threads it had forked rather than forking them again.
- **A cloud machine outlives the process that made it.** wings writes a lease down
  *before* it provisions, and on startup offers every unreleased lease back to the
  provisioner — a live machine is resumed, a dead one destroyed.
- **A worker's output is copied off it while it is up.** What an attempt commits comes
  home as its transactions; a shared channel's outbox is mirrored continuously.

For `-target remote`, per machine: provision → upload the embedded worker → start it
bound to loopback → open an SSH tunnel → dial the broker through it. The broker speaks
no authentication; the tunnel is the access control, and nothing wings starts is
reachable from the internet. Spot instances (`-gcp.spot`) are supported: a
preemption's 30-second notice is used to move the worker's jobs before it dies.

## Configuration

Used directly, without `wings build`:

```go
c, err := wings.Start(ctx, wings.Config{
    Target:      wings.Remote(gcp.New(gcp.Config{Project: "p", Zone: "z"})),
    Workers:     8,               // or a Scaling policy
    Concurrency: 4,               // running threads per worker; 0 = the worker decides
    JobTimeout:  5 * time.Minute,
})
defer c.Stop(ctx)   // deletes provisioned machines — they bill until it runs
```

## Other clouds

Implement `Provider` and register it from an `init`, the way a database driver does;
`wings build -providers you/cloud` links it in as `-provider yourcloud`. Or export
`func Provisioner() wings.Provisioner` from your package to bake one in.

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

Leases are identities wings mints before it creates anything; your implementation
makes each machine findable by its lease, and `ID()` returns it. Implement
`Reattacher` too so a restarted coordinator can recover running machines.

## Requirements

- Go 1.27+ (generic methods)

## Example

[`examples/digest`](examples/digest) is a complete program — one work function and a
coordinator body, naming no cloud — that runs on all three targets.

```sh
go run ./cmd/wings build -pkg ./examples/digest -o digest
./digest -target local -workers 4 -jobs 32
```

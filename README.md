# wings

Define a typed Go function. Call it across a cluster that did not exist a minute ago.

```go
var Render = wings.Define("render", func(ctx context.Context, f Frame) (Image, error) {
    return render(f)
})

func Coordinate(ctx context.Context, c *wings.Cluster) error {
    images, err := c.Map(ctx, Render, frames)   // runs wherever the workers are
    ...
}
```

That is the whole API. Where the work runs — goroutines here, child processes on
this machine, or GCP VMs provisioned on demand — is a flag at run time, not a
change to the code.

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
| **coordinator** | your work functions, `Coordinate`, the providers, the embedded worker | the machine *you* run it on |

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

import (
    "context"

    "github.com/ligustah/wings"
)

// Work functions are package-scope vars, so a worker process — which never runs
// Coordinate — still has them registered.
var Render = wings.Define("render", func(ctx context.Context, f Frame) (Image, error) {
    return render(f)
})

// Coordinate is called once, with a cluster that is already up.
func Coordinate(ctx context.Context, c *wings.Cluster) error {
    images, err := c.Map(ctx, Render, frames)
    if err != nil {
        return err
    }
    return write(images)
}
```

That is the whole file. No cloud appears in it.

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

Scaling down never drops work: a worker is retired only while idle, and idle is
decided under the same lock that assigns jobs, so nothing can be sent to a worker
already on its way out. Failing to provision is not fatal — the cluster keeps
running at its current size and tries again on the next tick, because a quota
refusal should cost throughput, not the run.

| flag | meaning |
|---|---|
| `-max-workers` | ceiling; setting it is what turns autoscaling on. A spend limit as much as a capacity one |
| `-min-workers` | floor, held even with an empty queue (at least 1) |
| `-jobs-per-worker` | backlog one worker is expected to carry (default 1) |
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

When the job was a step of a workflow (the [`flow`](flow) package), the entry says so — the
flow, the run, the thread and the position in that run's history. That is what
makes the record answerable at the level anyone actually asks at: not "job 3f
went to remote-2" but "the second activity of order-77 went to remote-2 and
never came back". Only the workflow knows which run a call belongs to, so it
stamps it on the dispatch and the coordinator writes it down. A bare call
belongs to nothing larger and leaves those columns empty.

Writes go through a buffered channel drained by one goroutine and batched, so
recording never becomes backpressure on the work. A full buffer drops entries
rather than blocking a submit — and counts them, so a gap in the record is
reported rather than silent.

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

### Remote deployment

For `-target remote`, per machine: provision → wait for SSH → upload the
embedded worker → start it bound to **loopback only** → open an SSH port-forward
→ dial the broker through it.

The broker speaks no authentication, so the tunnel *is* the access control.
Nothing wings starts is reachable from the internet, and no firewall rule is
needed. An ephemeral ed25519 keypair is minted per run and installed via
instance metadata; nothing wings creates outlives the cluster.

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

## Configuration

Used directly rather than through `wings build`:

```go
c, err := wings.Start(ctx, wings.Config{
    Target:      wings.Remote(gcp.New(gcp.Config{Project: "p", Zone: "z"})),
    Workers:     8,               // or a Scaling policy instead
    Concurrency: 4,               // jobs at once per worker; 0 = the worker decides
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

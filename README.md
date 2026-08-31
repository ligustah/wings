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
./myapp -target remote -workers 8  # cloud VMs, provisioned and torn down
```

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
worker       linux/amd64      28.2 MB
embedded     linux/amd64       8.6 MB (gzipped)
coordinator  windows/amd64    40.8 MB
```

The two halves are separate programs generated into a scratch directory (your
source tree is not written to):

| | contains | built for |
|---|---|---|
| **worker** | your work functions, the worker loop | the machines it will run on |
| **coordinator** | your work functions, `Coordinate`, provisioning, the embedded worker | the machine *you* run it on |

Because they are separate, **the coordinator is never cross-compiled** — only
the worker is — and the worker need not link the cloud SDK that deploys it.

## What you write

A **library** package, not a `main`. `wings build` generates both mains and
imports your package into each.

```go
package job

import (
    "context"

    "github.com/ligustah/wings"
    "github.com/ligustah/wings/gcp"
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

// Provisioner is optional. Exporting it is what enables -target=remote.
func Provisioner() wings.Provisioner {
    return gcp.New(gcp.Config{Project: "my-project", Zone: "europe-west1-b"})
}
```

Flags you register in that package are parsed too — `CoordinatorMain` calls
`flag.Parse()` on the default set, so your own flags sit beside `-target` and
`-workers` without wings knowing about them.

## How it works

Each worker hosts its own single-node [durable-streams](../durable_streams)
broker with two streams, `wings.jobs` and `wings.results`. The coordinator
writes jobs to a chosen worker and tails that worker's results. **Workers never
talk to each other**, and the coordinator only ever dials outward — so it works
from a laptop behind NAT.

```
Coordinator ──dsclient.Client──> dswire.Backend
                                   ├─ in-memory broker      (goroutine worker)
                                   ├─ gRPC on 127.0.0.1     (child process)
                                   └─ gRPC through an SSH tunnel (cloud VM)
```

One seam, three constructors. The worker loop, the encoding and the dispatch are
identical in all three, which is why a bug that only shows up on a cloud VM is a
bug in the transport rather than in your work.

Streams, offsets, brokers and clients appear nowhere in the public API.

### Remote deployment

For `-target remote`, per machine: provision → wait for SSH → upload the
embedded worker → start it bound to **loopback only** → open an SSH port-forward
→ dial the broker through it.

The broker speaks no authentication, so the tunnel *is* the access control.
Nothing wings starts is reachable from the internet, and no firewall rule is
needed. An ephemeral ed25519 keypair is minted per run and installed via
instance metadata; nothing wings creates outlives the cluster.

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
    Workers:     8,
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

Implement `Provisioner`; the SSH deployment, tunnelling and worker protocol are
shared.

```go
type Provisioner interface {
    Provision(ctx context.Context, n int) ([]Machine, error)
}

type Machine interface {
    ID() string
    Upload(ctx context.Context, localPath, remotePath string) error
    Start(ctx context.Context, cmd string, env map[string]string) error
    Forward(ctx context.Context, remotePort int) (localAddr string, err error)
    Close(ctx context.Context) error
}
```

## Requirements

- Go 1.27+ (generic methods; inherited from durable_streams)
- `../durable_streams` beside this repo — it is not published, so `go.mod`
  resolves it by `replace`

## Example

[`examples/digest`](examples/digest) is a complete program — work function,
coordinator body, provisioner — that runs on all three targets.

```sh
go run ./cmd/wings build -pkg ./examples/digest -o digest
./digest -target local -workers 4 -jobs 32
```

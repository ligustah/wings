package wings

import (
	"context"
	"io"
)

// Provisioner creates machines for wings to run workers on.
//
// Implement it to support a cloud wings does not ship. It deliberately knows
// nothing about wings: it hands back machines you can copy a file to, run a
// command on, and open a tunnel through. Everything above — what binary goes
// there, what port it listens on, how work reaches it — is this package's
// business, so a new cloud is one small type and not a second copy of the
// worker protocol.
type Provisioner interface {
	// Provision brings up one machine per lease and returns them once they
	// accept connections. Implementations should clean up anything they created
	// if they return an error.
	//
	// The leases are identities WINGS minted, not names the cloud chose, and
	// that direction is load-bearing: the coordinator writes down what it is
	// about to create before it creates it, so a crash mid-creation still
	// leaves a note naming the machine. An implementation must make each
	// machine findable by its lease afterwards — as its name, a tag, a label,
	// whatever the cloud offers — and [Machine.ID] must return it.
	//
	// A lease is short, lowercase and alphanumeric, so it is safe to embed in
	// whatever a cloud's naming rules allow.
	Provision(ctx context.Context, leases []string) ([]Machine, error)
}

// Reattacher is a Provisioner that can find machines it created earlier.
//
// Optional, and worth implementing: without it a coordinator that restarts
// cannot recover the machines it left running, and can only destroy them —
// which is safe, and wastes everything they had done.
type Reattacher interface {
	Provisioner

	// Reattach returns the machines among leases that still exist and can be
	// reached.
	//
	// A lease it cannot find is not an error: the machine may have been
	// preempted, deleted, or never created at all, and reporting that as a
	// failure would make an ordinary recovery look broken. Return what is
	// there; the coordinator treats the rest as gone.
	//
	// The machines come back after the process that created them, so an
	// implementation must re-establish whatever access it needs — a fresh
	// credential is the coordinator's to install, not something it kept.
	Reattach(ctx context.Context, leases []string) ([]Machine, error)
}

// Machine is one host a worker can be deployed onto.
type Machine interface {
	// ID returns the lease this machine was created for.
	//
	// The lease, not the cloud's own name for it: the lease is what the
	// coordinator wrote down before the machine existed, and matching a
	// recovered machine back to its record is the whole reason it can recover
	// at all.
	ID() string

	// Upload writes size bytes from src to remotePath, creating parent
	// directories, and makes it executable.
	//
	// A reader rather than a path because the thing being uploaded is usually
	// the worker embedded in the coordinator, which is already in memory: a
	// path would mean writing 30-odd MB to disk for no reason but to have
	// something to name. Implementations must not assume src is seekable, and
	// must read exactly size bytes from it.
	Upload(ctx context.Context, src io.Reader, size int64, remotePath string) error

	// Start launches cmd in the background with env set, and returns as soon as
	// it is running rather than waiting for it to exit.
	Start(ctx context.Context, cmd string, env map[string]string) error

	// Forward opens a tunnel to remotePort on the machine and returns a local
	// address to dial. The worker's broker binds to loopback on the machine and
	// is reachable no other way, so this is also what keeps it off the public
	// internet — it is unauthenticated, and a tunnel is the authentication.
	Forward(ctx context.Context, remotePort int) (localAddr string, err error)

	// Close releases the machine and anything allocated for it.
	Close(ctx context.Context) error
}

// PreemptionURLEnv names an environment variable a [Machine] may add to the
// worker's environment in [Machine.Start]: a URL that answers "TRUE" once the
// cloud has decided to take the machine back.
//
// Clouds that reclaim machines usually say so a little ahead of time — Google
// gives thirty seconds, on its metadata server — and a worker that hears it
// uses them: it tells the coordinator at once, so its jobs are moved now
// rather than when the connection is found dead, and stops taking new ones.
// The worker polls the URL and treats a request that blocks until the answer
// changes as the cloud's favour, not a requirement. It sends its worker id in
// an X-Wings-Worker header and a Metadata-Flavor: Google header, which the
// metadata server requires and everything else ignores.
const PreemptionURLEnv = "WINGS_PREEMPTION_URL"

// Prober is a Machine that can say whether it still exists.
//
// Optional, and worth implementing for any cloud that can answer. A worker
// that stops answering is given [Config.ReconnectTimeout] to come back,
// because a dropped connection is usually the network and the machine behind
// it is fine. A cloud can often say outright that it is not — a Spot instance
// that was preempted, one that somebody deleted — and a coordinator that can
// ask gives such a worker up at once, moves its jobs and replaces it, rather
// than at the end of the window.
type Prober interface {
	Machine

	// Alive reports whether the machine still exists and is running, or is on
	// its way to running. An error means the question could not be answered,
	// and the coordinator keeps waiting.
	Alive(ctx context.Context) (bool, error)
}

// Target says where workers run. Use [InProcess], [LocalProcess] or [Remote].
//
// The three kinds are fixed and the type is opaque, because extending wings
// means teaching it a new CLOUD, not a new execution model — that is what
// [Provisioner] is for.
type Target struct {
	kind targetKind
	prov Provisioner
}

type targetKind int

const (
	// targetInProcess runs workers as goroutines here, over an in-memory
	// broker: no network, no marshalling, no child process.
	targetInProcess targetKind = iota
	// targetLocalProcess runs workers as child copies of this binary, each
	// serving its broker on a loopback port.
	targetLocalProcess
	// targetRemote provisions machines and deploys this binary to them.
	targetRemote
)

// InProcess runs workers as goroutines in this process.
//
// The fastest mode and the one to test with: it exercises the same worker loop,
// the same encoding and the same dispatch as the other two, over a broker that
// never touches a socket.
func InProcess() Target { return Target{kind: targetInProcess} }

// LocalProcess runs each worker as a child copy of this binary on this machine.
//
// Real process isolation and real gRPC, without a cloud account — the mode that
// catches anything that only breaks once work crosses a process boundary.
func LocalProcess() Target { return Target{kind: targetLocalProcess} }

// Remote provisions machines through p and deploys this binary to each.
//
//	wings.Remote(gcp.New(gcp.Config{Project: "p", Zone: "europe-west1-b"}))
func Remote(p Provisioner) Target { return Target{kind: targetRemote, prov: p} }

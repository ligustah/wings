package wings

import (
	"context"
	"io"
)

// Provisioner creates machines for wings to run workers on. Implement it to
// support a cloud wings does not ship.
type Provisioner interface {
	// Provision brings up one machine per lease and returns them once they accept
	// connections, cleaning up on error. Each lease is a short, safe-to-embed id
	// wings minted; the machine must be findable by it afterwards and
	// [Machine.ID] must return it.
	Provision(ctx context.Context, leases []string) ([]Machine, error)
}

// Reattacher is a [Provisioner] that can recover machines it created earlier, so
// a restarted coordinator resumes them instead of destroying them. Optional but
// worth implementing.
type Reattacher interface {
	Provisioner

	// Reattach returns those of leases that still exist and are reachable,
	// re-establishing access as needed. A lease it cannot find is omitted, not an
	// error: the coordinator treats the rest as gone.
	Reattach(ctx context.Context, leases []string) ([]Machine, error)
}

// Machine is one host a worker can be deployed onto.
type Machine interface {
	// ID returns the lease this machine was created for.
	ID() string

	// Upload writes exactly size bytes from src to remotePath, creating parent
	// directories, and makes it executable. src may not be seekable.
	Upload(ctx context.Context, src io.Reader, size int64, remotePath string) error

	// Start launches cmd in the background with env set, returning once it is
	// running.
	Start(ctx context.Context, cmd string, env map[string]string) error

	// Forward opens a tunnel to remotePort and returns a local address to dial.
	// The worker's broker binds to loopback and authenticates nobody, so the
	// tunnel is what keeps it off the public internet.
	Forward(ctx context.Context, remotePort int) (localAddr string, err error)

	// Close releases the machine and anything allocated for it.
	Close(ctx context.Context) error
}

// PreemptionURLEnv names an environment variable a [Machine] may set in
// [Machine.Start]: a URL that answers "TRUE" once the cloud is reclaiming the
// machine. A worker polls it and, on notice, moves its jobs and stops taking new
// ones before the connection dies.
const PreemptionURLEnv = "WINGS_PREEMPTION_URL"

// Prober is a [Machine] that can report whether it still exists, so a
// coordinator gives a confirmed-dead worker up at once rather than waiting out
// [Config.ReconnectTimeout]. Optional.
type Prober interface {
	Machine

	// Alive reports whether the machine still exists and is running or starting.
	// An error means the question could not be answered, and the coordinator
	// keeps waiting.
	Alive(ctx context.Context) (bool, error)
}

// Target says where workers run. Use [InProcess], [LocalProcess] or [Remote].
type Target struct {
	kind targetKind
	prov Provisioner
}

type targetKind int

const (
	targetInProcess targetKind = iota
	targetLocalProcess
	targetRemote
)

// InProcess runs workers as goroutines over an in-memory broker: no network, no
// child process. The fastest mode, and the one to test with.
func InProcess() Target { return Target{kind: targetInProcess} }

// LocalProcess runs each worker as a child copy of this binary over loopback
// gRPC: real process isolation without a cloud account.
func LocalProcess() Target { return Target{kind: targetLocalProcess} }

// Remote provisions machines through p and deploys this binary to each.
//
//	wings.Remote(gcp.New(gcp.Config{Project: "p", Zone: "europe-west1-b"}))
func Remote(p Provisioner) Target { return Target{kind: targetRemote, prov: p} }

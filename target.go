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
	// Provision brings up n machines and returns them once they accept
	// connections. Implementations should clean up anything they created if
	// they return an error.
	Provision(ctx context.Context, n int) ([]Machine, error)
}

// Machine is one host a worker can be deployed onto.
type Machine interface {
	// ID names the machine in logs and errors. Human-readable; uniqueness
	// within one run is enough.
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
//	wings.Remote(wings.GCP(wings.GCPConfig{Project: "p", Zone: "europe-west1-b"}))
func Remote(p Provisioner) Target { return Target{kind: targetRemote, prov: p} }

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
//		images, err := c.Map(ctx, Render, frames)
//		...
//	}
//
// You write a LIBRARY package, not a main. The `wings build` command generates
// both mains — see that command's documentation — and the target is then a flag:
//
//	myapp -target inprocess
//	myapp -target local -workers 4
//	myapp -target remote -workers 8
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
// # Delivery semantics
//
// Delivery is AT-LEAST-ONCE. Each worker owns the queue of work assigned to it,
// so when a worker dies the coordinator re-dispatches whatever it had not yet
// heard back about — which means a job interrupted late may run twice. Work
// functions should be idempotent. They should also be pure with respect to
// anything on the coordinator's machine: a worker may be on another continent
// and shares no filesystem, no globals, and no open handles with the caller.
//
// # Communication
//
// Workers and the coordinator talk over durable streams, and that is an
// implementation detail on purpose: no stream, client, offset, or broker
// appears anywhere in this package's API. See transport.go for the seam.
package wings

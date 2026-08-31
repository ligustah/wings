package wings

import "github.com/ligustah/wings/internal/payload"

// SetEmbeddedWorker registers the worker binary compiled into this program.
//
// YOU DO NOT CALL THIS. `wings build` generates a coordinator main that carries
// the worker it just compiled, and hands the bytes to [CoordinatorMain]:
//
//	//go:embed wings_worker.bin.gz
//	var wingsWorker []byte
//
//	func main() {
//		wings.CoordinatorMain(wings.CoordinatorOptions{Worker: wingsWorker, …})
//	}
//
// This function is the equivalent for a program that builds its own main and
// still wants an embedded worker. gz must be gzip-compressed.
//
// Calling it by hand is legitimate if you have your own build pipeline and want
// to supply the worker yourself — the contract is just "these bytes are an
// executable for goos/goarch that this same program can run as a worker".
func SetEmbeddedWorker(gz []byte, goos, goarch string) {
	payload.Set(gz, goos, goarch)
}

// EmbeddedWorker reports the platform of the worker built into this binary.
//
// ok is false for a coordinator built with plain `go build`, which can still
// run remote workers — it cross-compiles one at dispatch time, needing a Go
// toolchain and the module source that a `wings build` binary does not.
func EmbeddedWorker() (goos, goarch string, ok bool) {
	m, ok := payload.Info()
	return m.GOOS, m.GOARCH, ok
}

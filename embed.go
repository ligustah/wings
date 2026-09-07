package wings

import "github.com/ligustah/wings/internal/payload"

// SetEmbeddedWorker registers a gzip-compressed worker binary compiled for
// goos/goarch. `wings build` generates a coordinator that calls this; supply the
// bytes yourself only if you build your own main.
func SetEmbeddedWorker(gz []byte, goos, goarch string) {
	payload.Set(gz, goos, goarch)
}

// EmbeddedWorker reports the platform of the worker built into this binary. ok
// is false for a coordinator built with plain `go build`, which cross-compiles a
// worker at dispatch time instead.
func EmbeddedWorker() (goos, goarch string, ok bool) {
	m, ok := payload.Info()
	return m.GOOS, m.GOARCH, ok
}

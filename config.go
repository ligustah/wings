package wings

import (
	"log/slog"
	"time"
)

// Environment variables wings sets on a worker. They are the entire contract
// between a coordinator and a worker process, which is why they are few: a
// worker that can read these can be started by hand for debugging.
const (
	envMode        = "WINGS_MODE"
	envListen      = "WINGS_LISTEN"
	envDir         = "WINGS_DIR"
	envConcurrency = "WINGS_CONCURRENCY"
	envWorkerID    = "WINGS_WORKER_ID"

	modeWorker = "worker"

	// readyPrefix is what a worker prints on stdout once its broker is
	// serving. The parent reads the address from it rather than guessing a
	// port, so there is no window where the port it chose was taken by
	// something else in between.
	readyPrefix = "WINGS_READY "

	// defaultRemotePort is the loopback port a remote worker's broker binds.
	// Fixed rather than negotiated because a freshly provisioned machine has
	// nothing else on it, and the coordinator reaches it through a tunnel it
	// opened by number.
	defaultRemotePort = 9440
)

// Config configures a [Cluster].
type Config struct {
	// Target says where workers run. The zero value is [InProcess].
	Target Target

	// Workers is how many workers to run. Defaults to 1 for [InProcess] — one
	// in-process worker with Concurrency goroutines is the same machine either
	// way — and to 2 otherwise.
	Workers int

	// Concurrency is how many jobs one worker runs at once. Zero lets each
	// worker decide from its own CPU count, which is the only correct default
	// when the worker is on hardware the coordinator has never seen.
	Concurrency int

	// Dir is where broker data lives. Empty uses a temporary directory that is
	// removed on [Cluster.Stop] — right for transient fan-out, wrong if you
	// want a worker's queue to survive a restart.
	Dir string

	// JobTimeout bounds a single work function call. Zero means no limit.
	JobTimeout time.Duration

	// Build controls cross-compilation of the worker binary. Used only by
	// [Remote].
	Build BuildConfig

	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// BuildConfig describes how to produce the worker binary for a remote machine.
//
// The running process cannot simply copy itself to a cloud VM — a Windows .exe
// is not a Linux worker — so wings compiles one.
type BuildConfig struct {
	// Package is the package to build, resolved against the working directory.
	// Defaults to ".", which is right when your main package is also the one
	// that calls wings.
	Package string
	// GOOS and GOARCH default to linux/amd64, matching the default machine
	// image.
	GOOS, GOARCH string
	// Tags is passed to -tags.
	Tags string
	// Env is appended to the build environment, after GOOS/GOARCH. Use it for
	// CGO_ENABLED or a module proxy setting.
	Env []string
}

func (c *Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func (c *Config) workers() int {
	if c.Workers > 0 {
		return c.Workers
	}
	if c.Target.kind == targetInProcess {
		return 1
	}
	return 2
}

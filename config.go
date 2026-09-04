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
	envJobTimeout  = "WINGS_JOB_TIMEOUT"
	envWorkerID    = "WINGS_WORKER_ID"

	modeWorker = "worker"

	// readyPrefix is what a worker prints on stdout once its broker is
	// serving. The parent reads the address from it rather than guessing a
	// port, so there is no window where the port it chose was taken by
	// something else in between.
	readyPrefix = "WINGS_READY "

	// pollInterval bounds a single blocking read of a worker's results.
	//
	// It is not a timeout in the usual sense — nothing is wrong when it expires,
	// it just means the worker produced nothing in that time. It exists because
	// a read on a broken connection can hang rather than fail, and a read that
	// can hang forever is a worker that can be lost silently.
	pollInterval = 30 * time.Second

	// maxMessage is the largest gRPC message the coordinator and a worker will
	// exchange.
	//
	// gRPC defaults to four megabytes, which is nothing here: a work function's
	// result travels as one record, and a result bigger than the limit did not
	// merely fail — the read that could not carry it looked exactly like a
	// dropped connection, so the worker was retried for the whole reconnect
	// window, declared dead, and its job redispatched to another worker that
	// produced the same oversized result. An unbounded loop, at two minutes a
	// turn.
	//
	// Sixty-four megabytes is generous for a result and still an amount of
	// memory a process can hold several of. Anything genuinely large belongs in
	// an [Artifact], which is streamed in chunks and never held whole.
	maxMessage = 64 << 20

	// watchdogInterval is how often outstanding jobs are checked against their
	// deadlines.
	//
	// A sweep rather than a timer per job: the question is only ever "has this
	// deadline already passed", and walking the outstanding set once a second
	// costs nothing beside the work it represents. It also bounds how late a
	// verdict can be, which is why it is well under any timeout worth setting.
	watchdogInterval = time.Second

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
	//
	// Ignored when Scaling is enabled, which decides the count instead.
	Workers int

	// Scaling makes the worker count follow the queue. The zero value is off.
	//
	// It is provider-independent: the same policy adds goroutines, processes or
	// cloud VMs depending only on Target.
	Scaling Scaling

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

	// ReconnectTimeout is how long a worker may be unreachable before the
	// coordinator gives up on it, redispatches its work and releases it.
	// Defaults to 2 minutes; a negative value gives up immediately.
	//
	// The right value is a judgement about which mistake is cheaper. Too short
	// and a network blip costs a machine that was fine and still held its queue.
	// Too long and a genuinely dead worker's jobs sit unredispatched for that
	// whole period. Two minutes is on the patient side because a worker's queue
	// survives a dropped connection — the work is not lost while we wait, it is
	// only paused.
	ReconnectTimeout time.Duration

	// MaxAttempts bounds how many workers one job may be tried on before the
	// coordinator gives up on it. Defaults to 5; a value below 1 means one
	// attempt and no retries.
	//
	// It exists because at-least-once has no natural end. A job that kills the
	// worker it lands on — a result too large to read, a panic in a native
	// library, a machine-agnostic wedge — is moved to the next worker, which
	// meets the same fate, forever. The failure is then invisible: the caller
	// waits, nothing errors, and the cluster looks merely busy. A bound turns
	// that into an error naming the last reason.
	MaxAttempts int

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

func (c *Config) reconnect() time.Duration {
	if c.ReconnectTimeout != 0 {
		return c.ReconnectTimeout
	}
	return 2 * time.Minute
}

// attempts is the number of workers one job may be tried on.
func (c Config) attempts() int {
	if c.MaxAttempts != 0 {
		return max(c.MaxAttempts, 1)
	}
	return 5
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

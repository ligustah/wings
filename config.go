package wings

import (
	"log/slog"
	"time"
)

// Environment variables wings sets on a worker — the whole contract between a
// coordinator and a worker, so a worker can be started by hand for debugging.
const (
	envMode        = "WINGS_MODE"
	envListen      = "WINGS_LISTEN"
	envDir         = "WINGS_DIR"
	envBroker      = "WINGS_BROKER"
	envConcurrency = "WINGS_CONCURRENCY"
	envJobTimeout  = "WINGS_JOB_TIMEOUT"
	envWorkerID    = "WINGS_WORKER_ID"

	modeWorker = "worker"

	// readyPrefix precedes the address a worker prints once its broker serves,
	// so the parent reads the port rather than guessing one.
	readyPrefix = "WINGS_READY "

	// maxMessage is the largest gRPC message coordinator and worker exchange.
	// Well above gRPC's 4MiB default: an oversized read is indistinguishable
	// from a dropped connection, which would redispatch the job into a loop.
	maxMessage = 64 << 20

	// maxResult is the largest encoded result a worker will send, below
	// maxMessage by room for a record's framing. Enforced worker-side, where the
	// result is in hand before it is committed.
	maxResult = maxMessage - maxMessage/8

	// resultBatch is how many results the coordinator reads at once; halved when
	// a batch is too large to carry, grown back on success.
	resultBatch = 256

	// watchdogInterval is how often outstanding jobs are checked against their
	// deadlines.
	watchdogInterval = time.Second

	// rebalanceAfter is how long a job waits unstarted on one worker's queue,
	// while another has less to do, before it is moved.
	rebalanceAfter = watchdogInterval

	// rebalanceStep bounds the moves one rebalance sweep makes.
	rebalanceStep = 64

	// defaultRemotePort is the loopback port a remote worker's broker binds.
	defaultRemotePort = 9440

	// defaultDataDir is a coordinator's data directory when Config.Dir is unset:
	// a directory in the working directory, kept — a durable run's state belongs
	// somewhere it survives, which a temp dir is not.
	defaultDataDir = "wings-data"

	// defaultWorkerDataDir is a worker's data directory when it is started by hand
	// without WINGS_DIR; a coordinator always sets one. Named apart from
	// defaultDataDir so a worker and a coordinator in one directory do not clash.
	defaultWorkerDataDir = "wings-worker-data"
)

// pollInterval bounds one blocking read of a worker's results, beats or
// cancellations; expiry means the worker produced nothing, not that anything is
// wrong. A var so tests can shorten it.
var pollInterval = 30 * time.Second

// Config configures a [Cluster].
type Config struct {
	// Target says where workers run. The zero value is [InProcess].
	Target Target

	// Workers is the worker count to keep — a dead or preempted worker is
	// replaced. Defaults to 1 for [InProcess], 2 otherwise. Ignored when Scaling
	// is set.
	Workers int

	// Scaling makes the worker count follow the queue. The zero value is off,
	// and is provider-independent.
	Scaling Scaling

	// Concurrency is how many threads one worker runs at once; a waiting thread
	// does not count. Zero lets each worker decide from its own CPU count.
	Concurrency int

	// Dir is where the coordinator keeps its durable data. Empty uses
	// ./wings-data in the working directory, kept across restarts so a run
	// resumes; set it to put that state somewhere else.
	Dir string

	// LocalSharedBroker, for the [LocalProcess] target, has the worker child
	// processes write into the coordinator's own broker instead of each running
	// its own — no per-worker data and no output copied between brokers, at the
	// cost of no longer rehearsing the remote target's mirror. Ignored by other
	// targets.
	LocalSharedBroker bool

	// JobTimeout bounds a single work function call. Zero means no limit.
	JobTimeout time.Duration

	// ReconnectTimeout is how long a worker may be unreachable before the
	// coordinator gives up on it and redispatches its work. Defaults to 2
	// minutes; negative gives up immediately.
	ReconnectTimeout time.Duration

	// MaxAttempts bounds how many workers one job may be tried on before the
	// coordinator gives up — the bound that turns an unkillable poison-pill job
	// into an error. Defaults to 5; below 1 means one attempt.
	MaxAttempts int

	// Build controls cross-compilation of the worker binary. Used only by [Remote].
	Build BuildConfig

	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// BuildConfig describes how to cross-compile the worker binary for a remote
// machine, since the running process cannot copy itself to a different OS/arch.
type BuildConfig struct {
	// Package to build, resolved against the working directory. Defaults to ".".
	Package string
	// GOOS and GOARCH default to linux/amd64, matching the default machine image.
	GOOS, GOARCH string
	// Tags is passed to -tags.
	Tags string
	// Env is appended to the build environment; use it for CGO_ENABLED or a proxy.
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

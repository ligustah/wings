package wings

import (
	"log/slog"
	"time"
)

// Environment variables wings sets on a worker — the whole contract between a
// coordinator and a worker, so a worker can be started by hand for debugging.
const (
	envMode           = "WINGS_MODE"
	envListen         = "WINGS_LISTEN"
	envDir            = "WINGS_DIR"
	envBroker         = "WINGS_BROKER"
	envConcurrency    = "WINGS_CONCURRENCY"
	envJobTimeout     = "WINGS_JOB_TIMEOUT"
	envWorkerID       = "WINGS_WORKER_ID"
	envCompression    = "WINGS_COMPRESSION"
	envCommitInterval = "WINGS_COMMIT_INTERVAL"
	envLogLevel       = "WINGS_LOG_LEVEL"
	envLogBytes       = "WINGS_LOG_BYTES"
	// envP2PJoin is the coordinator's peer address a p2p worker child joins; its
	// presence is what tells the child to bring up its own cluster node rather
	// than a broker of its own. envP2PRF carries the replication factor.
	envP2PJoin = "WINGS_P2P_JOIN"
	envP2PRF   = "WINGS_P2P_RF"

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

// The coordinator's pull of a worker can wedge inside the broker on a
// transaction it can neither finish nor abandon, without returning to be
// retried. These bound that: while the coordinator is behind the worker's own
// committed offsets and applying nothing new, pullInterruptGrace is how long
// before the pull is reconnected (which clears a transient stall), and
// pullFaultGrace how long before the worker is treated as lost and its jobs
// redispatched, so a run recovers or fails rather than hanging forever. A
// pull that is merely slow keeps advancing and never trips either. Vars so
// tests can shorten them.
var (
	pullWatchInterval  = 5 * time.Second
	pullInterruptGrace = 45 * time.Second
	pullFaultGrace     = 2 * time.Minute
)

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

	// UI, when set to a listen address (e.g. "127.0.0.1:8080"), serves a
	// read-only inspection API and web UI for the cluster. Empty leaves it off.
	// A bare port or ":port" or "0.0.0.0:port" binds every interface.
	UI string

	// RetainHistory keeps a forked thread's history after it is joined, instead of
	// dropping it, so a finished run's whole thread tree stays inspectable in the UI.
	// It covers a thread that runs in the coordinator's own process and one placed on
	// a worker alike; by default either is dropped once the thread has returned, its
	// result being recorded in the caller. Off by default.
	RetainHistory bool

	// RetainChannelData keeps a settled activity's shared-channel streams, instead
	// of dropping them when it returns. A receiver records what it took by identity
	// alone and reads the bytes back from the channel's canonical stream, so that
	// stream is the one copy of the channel's values; it is the bulk of the data
	// and is dead once the activity returns — its result is recorded and a replay
	// never re-enters it — so it is dropped by default. Set this to keep it, for
	// replaying a returned activity step by step while debugging.
	RetainChannelData bool

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

	// Compression is the storage codec for the durable streams this cluster
	// creates — history, channels, and the rest. The zero value uses the default
	// (Zstd); set [CompressionNone] to store uncompressed. See [Compression].
	Compression Compression

	// CommitInterval coalesces the transactional commits of the coordinator and its
	// workers alike: a thread's transaction is held open and its writes — history
	// and channel sends — commit at most once per interval, trading fewer fsyncs
	// for a tail that a crash replays rather than loses. A thread still flushes when
	// it blocks (so a value reaches whoever waits on it), on each heartbeat or
	// checkpoint, and when it ends, so delivery and reported progress stay correct;
	// only work in flight coalesces. Passed to workers as WINGS_COMMIT_INTERVAL. The
	// zero value uses a 1s default; set a negative value to commit every event, so a
	// restart loses nothing at the cost of an fsync per event.
	CommitInterval time.Duration

	// LogLevel is the lowest level [flow.Context.Logger] records; a line below it
	// writes neither the log stream nor the history marker that dedupes it. Fixed for
	// the life of a run and passed to workers as WINGS_LOG_LEVEL, so a thread filters
	// the same wherever it runs and a replay filters as its first attempt did. The
	// zero value logs Info and above.
	LogLevel slog.Level

	// LogBytes bounds one thread's durable log: past it the oldest whole segments are
	// dropped, so a run's logs stay bounded even though they outlive the history a
	// purge removes. Passed to workers as WINGS_LOG_BYTES. The zero value uses a
	// 32 MiB budget; a negative value keeps every line.
	LogBytes int64

	// Build controls cross-compilation of the worker binary. Used only by [Remote].
	Build BuildConfig

	// P2P, when non-nil, turns on peer-to-peer replication: the cluster's nodes
	// form a durable-streams cluster and hold replicas of one another's streams,
	// so a peer already has the data when a node is lost. It layers on top of
	// Target, which still says where nodes run. Nil is off, leaving the
	// coordinator relay in place.
	P2P *P2P

	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// P2P configures the peer-to-peer replication mode; set [Config.P2P] to enable
// it. The zero value is valid and uses the defaults.
type P2P struct {
	// ReplicationFactor is how many nodes hold a copy of each stream. Zero uses
	// the default (3); a cluster smaller than this replicates to every node.
	ReplicationFactor int
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

// defaultCommitInterval is what an unset CommitInterval resolves to.
const defaultCommitInterval = time.Second

// commitInterval resolves CommitInterval: unset (zero) takes the 1s default, a
// negative value means commit every event (eager), and a positive value is used
// as given. Resolved once on the coordinator, then passed to workers verbatim, so
// an unset env on a worker still means eager rather than re-defaulting.
func (c Config) commitInterval() time.Duration {
	if c.CommitInterval < 0 {
		return 0
	}
	if c.CommitInterval == 0 {
		return defaultCommitInterval
	}
	return c.CommitInterval
}

// defaultLogBytes is the per-thread log budget an unset LogBytes resolves to.
const defaultLogBytes = 32 << 20

// logBytes resolves LogBytes: unset (zero) takes the 32 MiB default, a negative
// value keeps every line (no budget), a positive value is used as given. Resolved on
// the coordinator and passed to workers verbatim, so 0 in the worker env means
// unbounded rather than re-defaulting.
func (c Config) logBytes() int64 {
	if c.LogBytes < 0 {
		return 0
	}
	if c.LogBytes == 0 {
		return defaultLogBytes
	}
	return c.LogBytes
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

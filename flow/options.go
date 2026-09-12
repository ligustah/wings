package flow

import (
	"reflect"
	"time"
)

// runOptions configure one [Run].
type runOptions struct {
	store    Store
	executor Executor
	placer   Placer
	parker   Parker
	host     ChannelHost
	clock    Clock

	version       int
	maxAttempts   int
	initialDelay  time.Duration
	maxDelay      time.Duration
	permanent     []error
	once          bool
	retainHistory bool

	// blockingBeat is how often [Context.Blocking] commits the calling thread's
	// transaction and reports liveness while its work runs off-thread. The executor
	// sets it to suit its transaction timeout and heartbeat bound; zero takes the default.
	blockingBeat time.Duration

	// input is the run body's input, recorded; inputType what it expects. Set
	// by a [Workflow], not by callers.
	input     []byte
	inputType reflect.Type

	// root and rootID are the lineage a thread of run code forked here carries,
	// so another process can start it. See lineage.go.
	root   Root
	rootID string
}

func rootOption(r Root) RunOption { return func(o *runOptions) { o.root = r } }

func inputOption(payload []byte, t reflect.Type) RunOption {
	return func(o *runOptions) { o.input, o.inputType = payload, t }
}

func newRunOptions(fns []RunOption) runOptions {
	o := runOptions{
		executor:     Local(),
		placer:       InProcess(),
		clock:        systemClock{},
		version:      1,
		maxAttempts:  10,
		initialDelay: time.Second,
		maxDelay:     time.Minute,
		blockingBeat: defaultBlockingBeat,
	}
	for _, fn := range fns {
		fn(&o)
	}
	return o
}

// RunOption configures one call to [Run].
type RunOption func(*runOptions)

// WithStore says where this run's history is kept. Required.
func WithStore(s Store) RunOption { return func(o *runOptions) { o.store = s } }

// defaultBlockingBeat is the fallback [Context.Blocking] keep-alive period when an
// executor sets none; well under the backend's default transaction timeout.
const defaultBlockingBeat = 15 * time.Second

// WithBlockingBeat sets how often [Context.Blocking] commits the calling thread's
// transaction and reports liveness while its work runs. Pick it below both the
// transaction timeout and any heartbeat bound. Zero keeps the default.
func WithBlockingBeat(d time.Duration) RunOption {
	return func(o *runOptions) {
		if d > 0 {
			o.blockingBeat = d
		}
	}
}

// WithExecutor says where the run's calls go. Defaults to [Local].
func WithExecutor(e Executor) RunOption { return func(o *runOptions) { o.executor = e } }

// Version records a revision label with each attempt. It does not by itself
// prevent the continuity errors that editing a body in flight causes; rename the
// run for that.
func Version(v int) RunOption { return func(o *runOptions) { o.version = v } }

// MaxAttempts caps how many times a failing run is retried. Default 10.
func MaxAttempts(n int) RunOption { return func(o *runOptions) { o.maxAttempts = n } }

// RetainHistory keeps a forked thread's recorded history after it is joined,
// instead of dropping it once its result is in the parent. Off by default (a
// joined thread's history is not needed to replay the run); turn it on so a run's
// full thread tree stays inspectable after it finishes.
func RetainHistory() RunOption { return func(o *runOptions) { o.retainHistory = true } }

// Once gives the run's main thread a single attempt and returns the body's error
// unwrapped, without backoff — for a process running a thread on another's
// behalf, where retry is decided elsewhere. Continuity, permanent, and
// suspension errors are still reported as such; in-process forked threads keep
// their retries.
func Once() RunOption { return func(o *runOptions) { o.once = true } }

// Backoff sets the delay before the first retry and the ceiling it doubles
// towards. Defaults are 1s and 1m.
func Backoff(initial, max time.Duration) RunOption {
	return func(o *runOptions) { o.initialDelay, o.maxDelay = initial, max }
}

// PermanentErrors names errors that must never be retried, matched with
// errors.Is — for errors from code you cannot wrap in [Permanent].
func PermanentErrors(errs ...error) RunOption {
	return func(o *runOptions) { o.permanent = append(o.permanent, errs...) }
}

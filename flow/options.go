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

	version      int
	maxAttempts  int
	initialDelay time.Duration
	maxDelay     time.Duration
	permanent    []error
	once         bool

	// input is what the run's body is given, in recorded form, or nil when
	// none was given; inputType is what the body expects, or nil when it
	// expects nothing. Set by a [Workflow], not by callers.
	input     []byte
	inputType reflect.Type
}

func inputOption(payload []byte, t reflect.Type) RunOption {
	return func(o *runOptions) { o.input, o.inputType = payload, t }
}

func newRunOptions(fns []RunOption) runOptions {
	o := runOptions{
		executor:     Local(),
		placer:       InProcess(),
		version:      1,
		maxAttempts:  10,
		initialDelay: time.Second,
		maxDelay:     time.Minute,
	}
	for _, fn := range fns {
		fn(&o)
	}
	return o
}

// RunOption configures one call to [Run].
type RunOption func(*runOptions)

// WithStore says where this run's history is kept. Required.
//
// Required rather than defaulted because the choice is consequential and silent
// either way: an in-memory default would give a run that looks durable and is
// not, and a durable default would put files somewhere the caller did not
// choose.
func WithStore(s Store) RunOption { return func(o *runOptions) { o.store = s } }

// WithExecutor says where the run's calls go. Defaults to [Local].
func WithExecutor(e Executor) RunOption { return func(o *runOptions) { o.executor = e } }

// Version marks a revision of a run's shape.
//
// Editing a run's body while runs of it are in flight is what produces
// continuity errors: the old runs replay their history against the new code
// and find it disagrees. Bumping the version does not fix that by itself — it
// is a label recorded with each attempt — but it is what makes the mismatch
// legible afterwards, and the honest fix is a new name.
func Version(v int) RunOption { return func(o *runOptions) { o.version = v } }

// MaxAttempts caps how many times a failing run is retried. Default 10.
func MaxAttempts(n int) RunOption { return func(o *runOptions) { o.maxAttempts = n } }

// Once gives the run's main thread a single attempt and returns the body's
// error as it was, unwrapped and without backoff.
//
// For a process that runs a thread on somebody else's behalf — an executor's
// worker, say — where whether and where to try again is decided elsewhere,
// and the error is that decision's input rather than this run's verdict.
// A continuity or permanent failure is still reported as such, and so is a
// suspension: a thread that sleeps past [ShortSleep] returns an error
// [IsSuspended] recognises, saying when it is to be run again. The threads
// the body forks in-process are not that process's to hand back, and keep
// their retries.
func Once() RunOption { return func(o *runOptions) { o.once = true } }

// Backoff sets the delay before the first retry and the ceiling it doubles
// towards. Defaults are 1s and 1m.
func Backoff(initial, max time.Duration) RunOption {
	return func(o *runOptions) { o.initialDelay, o.maxDelay = initial, max }
}

// PermanentErrors names errors that must never be retried, matched with
// errors.Is.
//
// The alternative to declaring them is wrapping each return in [Permanent],
// which is fine until the error comes from a library you do not control.
func PermanentErrors(errs ...error) RunOption {
	return func(o *runOptions) { o.permanent = append(o.permanent, errs...) }
}

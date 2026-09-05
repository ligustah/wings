package flow

import (
	"time"
)

// runOptions configure one [Run].
type runOptions struct {
	store    Store
	executor Executor

	version      int
	maxAttempts  int
	initialDelay time.Duration
	maxDelay     time.Duration
	permanent    []error
}

func newRunOptions(fns []RunOption) runOptions {
	o := runOptions{
		executor:     Local(),
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

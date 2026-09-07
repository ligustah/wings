package flow

import "time"

// Clock is where a run reads time: [Context.Now], [Context.Sleep], and the waits
// between a run's attempts. The default is the system clock; a test supplies its
// own with [WithClock] to run long sleeps instantly. See package
// [github.com/ligustah/wings/flow/flowtest].
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

// systemClock is the default Clock: real time.
type systemClock struct{}

func (systemClock) Now() time.Time                         { return time.Now() }
func (systemClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// WithClock sets the clock a run reads time from. Defaults to the system clock.
func WithClock(c Clock) RunOption { return func(o *runOptions) { o.clock = c } }

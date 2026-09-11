package flow

import "context"

// Wait says what a thread is about to wait for.
type Wait struct {
	Run, Thread string
	// On is what the thread waits on: one of the Wait constants.
	On string
	// Channel names the channel for a wait on one, and Seq the thread's receive
	// or send number, so a holder can tell whether the wait has been answered.
	Channel string
	Seq     uint64
}

// What a thread can wait on.
const (
	WaitJoin   = "join"   // another thread, in [Future.Await]
	WaitRecv   = "recv"   // a value on a channel
	WaitSend   = "send"   // room on a channel, or a receiver
	WaitSleep  = "sleep"  // the clock, in [Context.Sleep]
	WaitSelect = "select" // the first of several cases, in [Selector.Do]
)

// Parker is told when a thread waits and when its wait is over, so an executor
// can stop counting a waiting thread against its concurrency. Park is called as
// the thread waits and returns at once; the resume it returns is called when the
// wait ends and blocks until the thread may run again. Both run on the thread's
// own goroutine and must honour their context. A run given no Parker parks
// nothing.
type Parker interface {
	Park(ctx context.Context, w Wait) (resume func(ctx context.Context) error)
}

// WithParker installs p to be told when this run's threads wait.
func WithParker(p Parker) RunOption { return func(o *runOptions) { o.parker = p } }

func (t *threadState) park(ctx context.Context, on string) func(ctx context.Context) error {
	return t.parkOn(ctx, on, "", 0)
}

func (t *threadState) parkOn(ctx context.Context, on, channel string, seq uint64) func(ctx context.Context) error {
	if t.readonly {
		return noResume
	}
	// Flush before waiting, so a coalescing sink makes this thread's writes durable
	// and visible to whoever it is about to wait on (see [BoundaryCommitter]). This
	// is independent of the parker, which only accounts a waiting thread's
	// concurrency: a run with no parker (the coordinator's own) still flushes here.
	_ = t.commitBoundary(ctx)
	if t.run.parker == nil {
		return noResume
	}
	resume := t.run.parker.Park(ctx, Wait{Run: t.run.name, Thread: t.id, On: on, Channel: channel, Seq: seq})
	if resume == nil {
		return noResume
	}
	return resume
}

func noResume(context.Context) error { return nil }

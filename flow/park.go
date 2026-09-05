package flow

import "context"

// Wait says what a thread is about to wait for.
type Wait struct {
	Run, Thread string
	// On is what the thread waits on: one of the Wait constants.
	On string
}

// What a thread can wait on.
const (
	WaitJoin  = "join"  // another thread, in [Future.Await]
	WaitRecv  = "recv"  // a value on a channel
	WaitSend  = "send"  // room on a channel, or a receiver
	WaitSleep = "sleep" // the clock, in [Context.Sleep]
)

// Parker is told when a thread stops running and when it wants to run
// again.
//
// A thread that waits — for a thread it forked, for a channel, for the clock
// — holds nothing the run needs while it waits, and a process that bounds
// how many threads it runs at once has no reason to count it. Park is called
// as the thread is about to wait, and returns at once; the resume it gives
// back is called when the wait is over, and returns when the thread may run
// again, which is where such a process makes the thread take its turn. A
// run given no Parker parks nothing.
//
// Both are called on the thread's own goroutine, so a resume that blocks
// blocks the thread, which is the point; it must honour its context, since
// that is how a thread that is no longer wanted is let go.
type Parker interface {
	Park(ctx context.Context, w Wait) (resume func(ctx context.Context) error)
}

// WithParker installs p to be told when this run's threads wait.
func WithParker(p Parker) RunOption { return func(o *runOptions) { o.parker = p } }

// park tells the run's parker this thread is about to wait, and returns what
// to call when it is done. Never nil.
func (t *threadState) park(ctx context.Context, on string) func(ctx context.Context) error {
	if t.run.parker == nil {
		return noResume
	}
	resume := t.run.parker.Park(ctx, Wait{Run: t.run.name, Thread: t.id, On: on})
	if resume == nil {
		return noResume
	}
	return resume
}

func noResume(context.Context) error { return nil }

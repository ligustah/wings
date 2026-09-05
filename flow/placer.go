package flow

import (
	"context"
	"errors"
)

// Thread is a forked thread as a [Placer] sees it: which run it belongs to,
// what it is called, and what it is to do.
//
// A thread forked with [Context.Go] or [Context.Map] runs one defined
// function on one input, and Fn and Input say which: a placer can send that
// anywhere the function is defined. A thread forked with [Context.Spawn] runs
// a body of the run's own code, which only the process holding that code can
// run; Fn is empty for one of those, and the body is handed to the placer
// alongside.
type Thread struct {
	// Run is the run the thread belongs to.
	Run string
	// ID is the thread's name within the run, "<parent>.<n>": the parent's
	// name and how many threads the parent had forked before it. The same
	// code forks the same names in the same order on every attempt, which
	// is what lets a later attempt find this thread's history.
	ID string
	// Parent is the thread that forked this one.
	Parent string
	// Fn is the function the thread runs, and Input its encoded input.
	// Empty for a thread that runs a body of run code.
	Fn    string
	Input []byte
}

// Key identifies the thread across attempts of its run, for a placer that
// keeps track of the threads it has running.
func (th Thread) Key() string { return th.Run + "/" + th.ID }

// Placer is where forked threads go.
//
// The thread is the unit of work that moves between machines: it has a
// history of its own, on a stream of its own, and it is over when its parent
// joins it. A placer runs one somewhere and returns what it produced. The one
// in this package, [InProcess], runs it on a goroutine of the calling
// process, sharing the run's channels; a cluster's sends a thread that runs a
// function to a machine that has it, and keeps a thread that runs run code at
// home.
//
// ctx is the parent thread's, carrying the run; a placer that runs the
// thread in this process hands it on, and one that runs it elsewhere uses it
// to know when the parent has stopped wanting the answer. body is what the
// thread does, for a placer that runs it here — for a thread with a Fn it is
// that function on that input, already bound.
//
// Place is called once per thread per attempt of the parent, and a parent
// retried while a thread is still running calls it again with the same
// Thread. A placer that can recognise the thread still in flight — by
// [Thread.Key] — returns that one's result rather than starting another.
type Placer interface {
	Place(ctx context.Context, th Thread, body func(ctx Context) ([]byte, error)) ([]byte, error)
}

// InProcess runs every forked thread on a goroutine of the calling process.
// It is the placer a [Run] uses when given no other, and the one a placer
// that cannot ship a thread — because it runs run code, not a function —
// falls back to.
//
// A thread placed here is retried on its own, under the run's retry options,
// before its failure is reported: the thread is the unit of durability, and
// a thread that failed for a transient reason replays its own history and
// carries on, without its parent knowing.
func InProcess() Placer { return inProcess{} }

type inProcess struct{}

func (inProcess) Place(ctx context.Context, th Thread, body func(ctx Context) ([]byte, error)) ([]byte, error) {
	parent := threadFrom(ctx)
	if parent == nil {
		return nil, errors.New("flow: a thread can only be placed in-process from inside its run")
	}
	r := &threadRunner{
		run:   parent.run,
		name:  th.Run,
		id:    th.ID,
		fn:    th.Fn,
		input: th.Input,
		body:  body,
		opts:  parent.run.opts,
	}
	return r.execute(ctx)
}

// WithPlacer says where the threads this run forks go. Defaults to
// [InProcess].
func WithPlacer(p Placer) RunOption { return func(o *runOptions) { o.placer = p } }

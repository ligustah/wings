package flow

import (
	"context"
	"errors"
)

// Thread is a forked thread as a [Placer] sees it. One from [Context.Go] or
// [Context.Map] runs the function Fn on Input; one from [Context.Spawn] runs run
// code (Fn empty), reachable elsewhere by replay through Root and Lineage.
type Thread struct {
	// Run is the run the thread belongs to.
	Run string
	// ID is the thread's name within the run, "<parent>.<n>", assigned the same
	// on every attempt so a later attempt finds this thread's history.
	ID string
	// Parent is the thread that forked this one.
	Parent string
	// Fn is the function the thread runs, and Input its encoded input. Empty for
	// a thread that runs run code.
	Fn    string
	Input []byte
	// Root and Lineage let another process reach a thread of run code: Root a
	// thread it can start by name, Lineage the thread ids from that root to this
	// one. See [RunLineage].
	Root    Root
	Lineage []string
}

// Key identifies the thread across attempts of its run, for a placer that
// keeps track of the threads it has running.
func (th Thread) Key() string { return th.Run + "/" + th.ID }

// Placer is where forked threads go. It runs a thread somewhere and returns what
// it produced; body is the thread's work, already bound, for a placer that runs
// it in this process. Place is called once per thread per parent attempt, with
// the same [Thread] if the parent is retried mid-flight — recognise it by
// [Thread.Key] to return the running one's result rather than starting another.
type Placer interface {
	Place(ctx context.Context, th Thread, body func(ctx Context) ([]byte, error)) ([]byte, error)
}

// InProcess runs every forked thread on a goroutine of the calling process. It
// is the default placer, and the fallback for a placer that cannot ship a thread.
// A thread placed here is retried on its own under the run's retry options before
// its failure is reported.
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

// WithPlacer says where the threads this run forks go. Defaults to [InProcess].
func WithPlacer(p Placer) RunOption { return func(o *runOptions) { o.placer = p } }

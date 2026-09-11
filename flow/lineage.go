package flow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Placing a thread of run code elsewhere. A thread is a closure, which cannot be
// shipped between processes; what can is its LINEAGE — the path of thread ids
// from a root the other process can start on its own (a workflow by name, or a
// function on its recorded input) down to the thread, plus each thread on that
// path's history. The other process replays each ancestor from its history,
// read-only, to the fork that made the next, and runs the last for real. A run
// started with a bare body under [Run] has no startable root, so its threads of
// run code stay home.

// Root is the thread a lineage starts from, as another process can start it: a
// workflow by name, or a function on its input. Exactly one is set.
type Root struct {
	Workflow string
	Function string
	Input    []byte
}

// known reports whether the root names something a process can start from.
func (r Root) known() bool { return r.Workflow != "" || r.Function != "" }

// lineageOf is the path of thread ids from root down to thread, root first, or
// nil if thread does not descend from root.
func lineageOf(root, thread string) []string {
	if thread == root {
		return []string{root}
	}
	if !strings.HasPrefix(thread, root+".") {
		return nil
	}
	rest := strings.Split(strings.TrimPrefix(thread, root+"."), ".")
	path := make([]string, 0, len(rest)+1)
	path = append(path, root)
	for _, seg := range rest {
		path = append(path, path[len(path)-1]+"."+seg)
	}
	return path
}

// RunLineage runs the last thread of lineage in this process by replaying its
// ancestors from their histories to the forks that made them — see the top of
// this file — then running it as [RunThread] would, with [Once] applying to it.
//
// Every thread on the path must have its history in the store, and root says how
// to start the first; the workflow or function it names must be defined here.
// What the thread forks goes to the run's [Placer], and the run needs a
// [ChannelHost], since the rest of it is elsewhere.
func RunLineage(ctx context.Context, run string, root Root, lineage []string, opts ...RunOption) ([]byte, error) {
	ro := newRunOptions(opts)
	if run == "" {
		return nil, errors.New("flow: RunLineage requires a run name")
	}
	if ro.store == nil {
		return nil, errors.New("flow: RunLineage requires a Store")
	}
	if len(lineage) == 0 {
		return nil, errors.New("flow: RunLineage requires a lineage")
	}
	for i := 1; i < len(lineage); i++ {
		if !strings.HasPrefix(lineage[i], lineage[i-1]+".") {
			return nil, fmt.Errorf("flow: %q does not descend from %q; a lineage is a path of threads", lineage[i], lineage[i-1])
		}
	}
	if len(lineage) == 1 {
		if root.Function == "" {
			return nil, errors.New("flow: a lineage of one thread must be a function")
		}
		return RunThread(ctx, run, lineage[0], root.Function, root.Input, opts...)
	}

	body, err := rootBody(root)
	if err != nil {
		return nil, err
	}
	ro.root, ro.rootID = root, lineage[0]
	if ro.host == nil {
		return nil, errors.New("flow: RunLineage requires a ChannelHost: the run's channels are shared with the rest of it")
	}
	// One runState for every thread here, ancestor and target alike: they are
	// threads of one run, and the channels between them live on it.
	rs := newRunState(run, ro)
	rs.fragment = true
	defer rs.finish()

	p := &lineagePlacer{ctx: ctx, rs: rs, path: lineage, inner: ro.placer,
		done: make(chan lineageResult, 1), changed: make(chan struct{}, 1)}
	rs.placer = p
	rs.forked = p.forked

	// Stop the root's replay as soon as the target is over, reached the end of
	// its history or not.
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rootDone := make(chan error, 1)
	go func() {
		r := &threadRunner{run: rs, name: run, id: lineage[0], fn: root.Function, input: root.Input,
			body: body, opts: ro, readonly: true}
		_, err := r.execute(rctx)
		rootDone <- err
	}()

	var rootErr error
	select {
	case res := <-p.done:
		return res.out, res.err
	case rootErr = <-rootDone:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// The root stopped before the target reported. A path thread whose fork is
	// on record may still be on its way to the placer, or replaying there: the
	// root's replay only outran it. Wait until nothing on the path is in flight;
	// if the target has not come by then, the histories do not reach it.
	for p.inflight() > 0 {
		select {
		case res := <-p.done:
			return res.out, res.err
		case <-p.changed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	select {
	case res := <-p.done:
		return res.out, res.err
	default:
	}
	if failed := p.failure(); failed != nil {
		return nil, failed
	}
	if rootErr != nil && !errors.Is(rootErr, errExhausted) {
		return nil, fmt.Errorf("flow: replay thread %s to the fork of %s: %w", lineage[0], lineage[1], rootErr)
	}
	return nil, fmt.Errorf("flow: the histories on the lineage of %s do not reach its fork", p.target())
}

// rootBody is the body of a lineage's root thread.
func rootBody(root Root) (func(ctx Context) ([]byte, error), error) {
	switch {
	case root.Workflow != "":
		workflowsMu.RLock()
		w, ok := workflows[root.Workflow]
		workflowsMu.RUnlock()
		if !ok {
			return nil, fmt.Errorf("flow: %w: no workflow %q is defined in this process; it defines: %s",
				ErrUnknownRoot, root.Workflow, definedWorkflows())
		}
		return w.threadBody(), nil
	case root.Function != "":
		if _, ok := lookup(root.Function); !ok {
			return nil, fmt.Errorf("flow: %w: no function %q is defined in this process", ErrUnknownRoot, root.Function)
		}
		return functionBody(root.Function, root.Input), nil
	}
	return nil, fmt.Errorf("flow: %w: the lineage names no workflow and no function to start from", ErrUnknownRoot)
}

// ErrUnknownRoot is returned by [RunLineage] when the root's workflow or
// function is not defined in this process, so it cannot start the lineage.
var ErrUnknownRoot = errors.New("flow: unknown lineage root")

type lineageResult struct {
	out []byte
	err error
}

// lineagePlacer is the placer of the threads a lineage's replay forks: those on
// the path are replayed, the last for real, and the rest are not run.
type lineagePlacer struct {
	ctx   context.Context // RunLineage's: the target's, not an ancestor's
	rs    *runState
	path  []string
	inner Placer // where the target's own forks go
	done  chan lineageResult

	// changed is nudged whenever a path thread stops being in flight.
	changed chan struct{}

	mu       sync.Mutex
	flying   int   // path threads forked and not yet done replaying
	failed   error // what an ancestor's replay failed with, if one did
	taken    bool
	reported bool
}

func (p *lineagePlacer) target() string { return p.path[len(p.path)-1] }

// onPath reports whether id is the target or one of its ancestors: one this
// process replays or runs.
func (p *lineagePlacer) onPath(id string) bool {
	for _, want := range p.path[1:] {
		if id == want {
			return true
		}
	}
	return false
}

// forked is told of every fork before the placer sees it. A fork of a path
// thread means it is coming, so a root whose replay ends first has not outrun
// the history, only the goroutine.
func (p *lineagePlacer) forked(th Thread, joined bool) {
	if !p.onPath(th.ID) {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if joined {
		// Its parent's history already holds its join: it, and everything under
		// it including the target, is over. The histories to run them from are
		// gone.
		if th.ID == p.target() && !p.reported {
			p.reported = true
			p.done <- lineageResult{nil, fmt.Errorf("flow: thread %s has already finished: its parent's history holds its join", th.ID)}
		}
		return
	}
	p.flying++
}

// landed is forked's other end: a path thread is done replaying.
func (p *lineagePlacer) landed(err error) {
	p.mu.Lock()
	p.flying--
	if err != nil && !errors.Is(err, errExhausted) && p.failed == nil {
		p.failed = err
	}
	p.mu.Unlock()
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

// inflight is how many path threads are forked and still replaying.
func (p *lineagePlacer) inflight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.flying
}

// failure is what an ancestor's replay failed with, wrapped to say so.
func (p *lineagePlacer) failure() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failed == nil {
		return nil
	}
	return fmt.Errorf("flow: replay the lineage of %s: %w", p.target(), p.failed)
}

func (p *lineagePlacer) Place(ctx context.Context, th Thread, body func(ctx Context) ([]byte, error)) ([]byte, error) {
	if th.Parent == p.target() {
		// The target's own fork: real work, sent where the run sends its threads.
		if p.inner == nil {
			return InProcess().Place(ctx, th, body)
		}
		return p.inner.Place(ctx, th, body)
	}
	depth := -1
	for i, id := range p.path {
		if id == th.Parent {
			depth = i
		}
	}
	if depth < 0 || depth+1 >= len(p.path) || th.ID != p.path[depth+1] {
		// Forked by an ancestor, off the path: not this process's to run. Its
		// join, if the ancestor got that far, is in the history.
		return nil, errOffLineage
	}
	if th.ID != p.target() {
		// An ancestor: replayed to the next fork, no further.
		r := &threadRunner{run: p.rs, name: th.Run, id: th.ID, fn: th.Fn, input: th.Input,
			body: body, opts: p.rs.opts, readonly: true}
		out, err := r.execute(ctx)
		p.landed(err)
		return out, err
	}

	p.mu.Lock()
	if p.taken {
		p.mu.Unlock()
		p.landed(errors.New("flow: the target of a lineage was forked twice"))
		return nil, errors.New("flow: the target of a lineage was forked twice")
	}
	p.taken = true
	p.mu.Unlock()
	defer p.landed(nil)

	// The target, for real, on RunLineage's own context: the ancestor that
	// forked it stops once it is over. Its panic is caught here as its result,
	// not by the ancestor's fork, which would take it for the ancestor's own and
	// leave RunLineage with nothing to report.
	r := &threadRunner{run: p.rs, name: th.Run, id: th.ID, fn: th.Fn, input: th.Input,
		body: body, opts: p.rs.opts, top: true}
	out, err := func() (out []byte, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				err = fmt.Errorf("flow: panic in forked work: %v", rec)
			}
		}()
		return r.execute(p.ctx)
	}()
	p.mu.Lock()
	if !p.reported {
		p.reported = true
		p.done <- lineageResult{out, err}
	}
	p.mu.Unlock()
	return out, err
}

// errOffLineage is returned for an ancestor's fork of a thread off the path:
// nothing runs here, and the join in the history says what it made.
var errOffLineage = errors.New("flow: a thread off the lineage is not run here")

// errExhausted is what a read-only replay meets at the end of its history: the
// next thing its body would do is new, and a replay does nothing new.
var errExhausted = errors.New("flow: the history ends here; a read-only replay goes no further")

// Share makes every channel of the run reachable from other processes, for a
// placer about to send a thread of run code elsewhere: the thread may use any of
// them. Channels made after this leave the run the ordinary way, encoded in a
// value. No-op outside a run.
func Share(ctx context.Context) error {
	t := threadFrom(ctx)
	if t == nil {
		return nil
	}
	r := t.run
	r.mu.Lock()
	names := make([]string, 0, len(r.channels))
	for name := range r.channels {
		names = append(names, name)
	}
	r.mu.Unlock()
	for _, name := range names {
		if _, err := r.export(ctx, name, t.peek() != nil, t.qualified(), modeBoth); err != nil {
			return err
		}
	}
	return nil
}

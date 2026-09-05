package flow

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

// A thread of run code — one forked with [Context.Spawn] — is a closure, and
// a closure cannot be sent to another process. What CAN be sent is the way
// to reach it: the run's code is in every process that holds it, and the
// closure is what that code arrives at after replaying the thread's
// ancestors up to the fork that made it. So a thread of run code is placed
// elsewhere by shipping its LINEAGE — the path of thread ids from a thread
// the other process can start on its own down to the thread itself — and
// the histories of the threads on that path. The other process replays each
// ancestor from its history, read-only, to the fork of the next; the fork
// hands it the closure; and the last one it runs for real, as [RunThread]
// runs a thread that runs a function.
//
// The root of a lineage is a thread the other process can start with nothing
// but a name: a workflow, by the name it is defined under, or a function on
// its recorded input. A run started with a bare body under [Run] has no such
// root, and its threads of run code stay home.
//
// An ancestor replayed this way is REPLAYED ONLY. It records nothing, it
// parks nowhere, it does not report progress, and when its history runs out
// it stops rather than doing anything new; what it computes between the
// events of its history it computes again, which is the same determinism a
// retry already asks of it. A thread it forks that is not on the path is not
// run: its join, if the ancestor reached it, is in the history.

// Root is the thread a lineage starts from, as another process can start
// it: a workflow by name, or a function on its input. Exactly one is set.
type Root struct {
	Workflow string
	Function string
	Input    []byte
}

// known reports whether the root is one a process can start from.
func (r Root) known() bool { return r.Workflow != "" || r.Function != "" }

// lineageOf is the path of thread ids from root to thread: root's ancestry
// is dropped, and every id between is one segment longer than the last.
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

// RunLineage runs the last thread of lineage in this process, by replaying
// its ancestors from their histories to the forks that made them — see the
// top of this file — and then running it as [RunThread] would: on its own
// history, under the run's options, [Once] applying to it.
//
// The histories of every thread on the path must be in the store. root says
// how to start the first; the workflow or function it names must be defined
// in this process. What the thread forks goes to the run's [Placer], and
// every channel of the run is shared through its [ChannelHost], since the
// rest of the run is elsewhere.
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
	// The one runState for every thread here, ancestor and target alike:
	// they are threads of one run, and the channels between them live on it.
	rs := newRunState(run, ro)
	rs.fragment = true
	defer rs.finish()

	p := &lineagePlacer{ctx: ctx, rs: rs, path: lineage, inner: ro.placer, done: make(chan lineageResult, 1)}
	rs.placer = p
	rs.forked = p.forked

	// The root's replay is stopped as soon as the target is over, whether
	// or not it reached the end of its history.
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rootDone := make(chan error, 1)
	go func() {
		r := &threadRunner{run: rs, name: run, id: lineage[0], fn: root.Function, input: root.Input,
			body: body, opts: ro, readonly: true}
		_, err := r.execute(rctx)
		rootDone <- err
	}()

	select {
	case res := <-p.done:
		return res.out, res.err
	case err := <-rootDone:
		// The root stopped. If the target's fork is on record it is on its
		// way to the placer, or running there, and the root's replay only
		// outran it; otherwise the history does not reach the fork, or the
		// replay failed first.
		if p.expected() {
			select {
			case res := <-p.done:
				return res.out, res.err
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		if err == nil || errors.Is(err, errExhausted) {
			return nil, fmt.Errorf("flow: the history of thread %s does not reach the fork of %s", lineage[0], lineage[1])
		}
		return nil, fmt.Errorf("flow: replay thread %s to the fork of %s: %w", lineage[0], lineage[1], err)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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

// ErrUnknownRoot is returned by [RunLineage] when the root of the lineage is
// not something this process can start: the workflow or function it names
// is not defined here. A placer that gets it can only run the thread where
// the code is.
var ErrUnknownRoot = errors.New("flow: unknown lineage root")

type lineageResult struct {
	out []byte
	err error
}

// lineagePlacer is the placer of the threads a lineage's replay forks: those
// on the path are replayed, the last for real, and the rest are not run.
type lineagePlacer struct {
	ctx   context.Context // RunLineage's: the target's, not an ancestor's
	rs    *runState
	path  []string
	inner Placer // where the target's own forks go
	done  chan lineageResult

	mu       sync.Mutex
	pending  bool // the target's fork is recorded and on its way here
	taken    bool
	reported bool
}

func (p *lineagePlacer) target() string { return p.path[len(p.path)-1] }

// forked is told of every fork as it is recorded, before the placer sees it:
// a fork of the target means the target is coming, and a root whose replay
// ends first has not outrun the history, only the goroutine.
func (p *lineagePlacer) forked(th Thread, joined bool) {
	if th.ID != p.target() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending = true
	if joined && !p.reported {
		// Its parent's history already holds its join: it is over, and its
		// own history — what this process would have run it from — is gone.
		p.reported = true
		p.done <- lineageResult{nil, fmt.Errorf("flow: thread %s has already finished: its parent's history holds its join", th.ID)}
	}
}

// expected reports whether the target's fork has been recorded.
func (p *lineagePlacer) expected() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pending
}

func (p *lineagePlacer) Place(ctx context.Context, th Thread, body func(ctx Context) ([]byte, error)) ([]byte, error) {
	if th.Parent == p.target() {
		// The target's own fork, which is real work: it goes where the run
		// sends its threads.
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
		// Forked by an ancestor, off the path: not this process's to run.
		// Its join, if the ancestor got that far, is in the history.
		return nil, errOffLineage
	}
	if th.ID != p.target() {
		// An ancestor: replayed to the next fork, and no further.
		r := &threadRunner{run: p.rs, name: th.Run, id: th.ID, fn: th.Fn, input: th.Input,
			body: body, opts: p.rs.opts, readonly: true}
		return r.execute(ctx)
	}

	p.mu.Lock()
	if p.taken {
		p.mu.Unlock()
		return nil, errors.New("flow: the target of a lineage was forked twice")
	}
	p.taken = true
	p.mu.Unlock()

	// The target, for real: on RunLineage's own context, since the ancestor
	// that forked it is stopped once it is over.
	r := &threadRunner{run: p.rs, name: th.Run, id: th.ID, fn: th.Fn, input: th.Input,
		body: body, opts: p.rs.opts, top: true}
	out, err := r.execute(p.ctx)
	p.mu.Lock()
	if !p.reported {
		p.reported = true
		p.done <- lineageResult{out, err}
	}
	p.mu.Unlock()
	return out, err
}

// errOffLineage is what a replayed ancestor's fork of a thread not on the
// path returns: nothing ran, and the join in the history says what it made.
var errOffLineage = errors.New("flow: a thread off the lineage is not run here")

// errExhausted is what a read-only replay meets at the end of its history:
// the next thing its body does would be new, and a replay does nothing new.
var errExhausted = errors.New("flow: the history ends here; a read-only replay goes no further")

// Share makes every channel of the run reachable from other processes, for
// a placer about to send a thread of run code elsewhere: the thread may use
// any of them. Channels made after this leave the run the ordinary way, in a
// value that is encoded. No-op outside a run.
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
		if _, err := r.export(ctx, name, t.peek() != nil); err != nil {
			return err
		}
	}
	return nil
}

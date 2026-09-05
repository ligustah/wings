package flow

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// Workflow is a named run body: the outer piece of code a program is about,
// declared once with [DefineWorkflow] and started with [Workflow.Run].
//
// A workflow is to a run what a [Func] is to a call — the definition, of which
// an execution is an instance. Declaring it gives it a name, and the name is
// what a program that hosts workflows (a `wings build` coordinator, say) uses
// to say which one it was asked to run. A package may define several.
type Workflow struct {
	name string
	body func(ctx Context) error
	opts []RunOption
}

// DefineWorkflow registers a run body under name and returns the handle.
//
// Call it in a package-scope var, for the same reason [Define] asks that: the
// registry is what a hosting program consults, and a workflow defined inside
// a function body is not there when it looks. opts are the [RunOption]s every
// run of this workflow starts with; options given to [Workflow.Run] are
// applied after them, and so win.
//
// Panics if name is empty or already defined — programming errors, reported
// the one way a package-init mistake cannot be ignored.
func DefineWorkflow(name string, body func(ctx Context) error, opts ...RunOption) Workflow {
	if name == "" {
		panic("flow: DefineWorkflow requires a non-empty name")
	}
	if body == nil {
		panic("flow: DefineWorkflow requires a non-nil body")
	}
	w := Workflow{name: name, body: body, opts: slices.Clone(opts)}

	workflowsMu.Lock()
	defer workflowsMu.Unlock()
	if _, dup := workflows[name]; dup {
		panic(fmt.Sprintf("flow: workflow %q is already defined", name))
	}
	workflows[name] = w
	return w
}

// Name is the name the workflow was defined under.
func (w Workflow) Name() string { return w.name }

// Run executes the workflow as a durable run named after it, to completion.
//
// This is [Run] with the workflow's own name and body, and one run per name
// per store follows from that: run it again on the same store and it resumes
// where it stopped, or returns at once if it already finished. That is the
// right shape for a program that is one workflow over one directory; a
// workflow that must run many times over, each under its own name, is a run
// body handed to [Run] directly.
func (w Workflow) Run(ctx context.Context, opts ...RunOption) error {
	if w.body == nil {
		return errors.New("flow: Run on a zero Workflow; declare one with flow.DefineWorkflow")
	}
	all := make([]RunOption, 0, len(w.opts)+len(opts))
	all = append(all, w.opts...)
	all = append(all, opts...)
	return Run(ctx, w.name, w.body, all...)
}

var (
	workflowsMu sync.RWMutex
	workflows   = map[string]Workflow{}
)

// LookupWorkflow returns the workflow defined under name in this process.
func LookupWorkflow(name string) (Workflow, bool) {
	workflowsMu.RLock()
	defer workflowsMu.RUnlock()
	w, ok := workflows[name]
	return w, ok
}

// Workflows lists every workflow defined in this process, sorted by name.
//
// For a program that hosts workflows and must say which it can run — or,
// when exactly one is defined, run that without being told.
func Workflows() []Workflow {
	workflowsMu.RLock()
	defer workflowsMu.RUnlock()
	all := make([]Workflow, 0, len(workflows))
	for _, w := range workflows {
		all = append(all, w)
	}
	slices.SortFunc(all, func(a, b Workflow) int { return cmp.Compare(a.name, b.name) })
	return all
}

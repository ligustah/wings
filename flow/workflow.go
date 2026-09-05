package flow

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"

	"github.com/ligustah/durable_streams/dswire"
)

// Workflow is a named run body with a typed input: the outer piece of code a
// program is about, declared once with [DefineWorkflow] and started with
// [Workflow.Run].
//
// A workflow is to a run what a [Func] is to a call — the definition, of which
// an execution is an instance. Declaring it gives it a name, and the name is
// what a program that hosts workflows (a `wings build` coordinator, say) uses
// to say which one it was asked to run. A package may define several.
type Workflow[In any] struct {
	name  string
	body  func(ctx Context, in In) error
	opts  []RunOption
	codec dswire.Codec[In]
}

// None is the input type of a workflow that takes no input.
//
//	var Main = flow.DefineWorkflow("main", func(ctx flow.Context, _ flow.None) error { … })
//
// A program hosting such a workflow does not ask for input, and refuses any.
type None = struct{}

// DefineWorkflow registers a run body under name and returns the handle.
//
// Call it in a package-scope var, for the same reason [Define] asks that: the
// registry is what a hosting program consults, and a workflow defined inside
// a function body is not there when it looks. opts are the [RunOption]s every
// run of this workflow starts with; options given to [Workflow.Run] are
// applied after them, and so win.
//
// In is what a run of the workflow is given, encoded the way a function's
// input is, and it is part of the run's history: the first attempt records
// it, and every later attempt is given the recorded value — a resumed run
// cannot be handed different input, which would be a different run. Use
// [None] for a workflow that takes nothing.
//
// Panics if name is empty or already defined — programming errors, reported
// the one way a package-init mistake cannot be ignored.
func DefineWorkflow[In any](name string, body func(ctx Context, in In) error, opts ...RunOption) Workflow[In] {
	if name == "" {
		panic("flow: DefineWorkflow requires a non-empty name")
	}
	if body == nil {
		panic("flow: DefineWorkflow requires a non-nil body")
	}
	w := Workflow[In]{
		name:  name,
		body:  body,
		opts:  slices.Clone(opts),
		codec: dswire.ReflectCodec[In]{New: allocator[In]()},
	}

	workflowsMu.Lock()
	defer workflowsMu.Unlock()
	if _, dup := workflows[name]; dup {
		panic(fmt.Sprintf("flow: workflow %q is already defined", name))
	}
	workflows[name] = w
	return w
}

// Name is the name the workflow was defined under.
func (w Workflow[In]) Name() string { return w.name }

// Run executes the workflow on in as a durable run named after it, to
// completion.
//
// This is [Run] with the workflow's own name and body, and one run per name
// per store follows from that: run it again on the same store and it resumes
// where it stopped, or returns at once if it already finished. On a resume
// the input recorded by the first attempt is what the body sees; passing a
// different one is an error, not a quiet restart. That is the right shape for
// a program that is one workflow over one directory; a workflow that must run
// many times over, each under its own name, is a run body handed to [Run]
// directly.
func (w Workflow[In]) Run(ctx context.Context, in In, opts ...RunOption) error {
	if w.body == nil {
		return errors.New("flow: Run on a zero Workflow; declare one with flow.DefineWorkflow")
	}
	payload, err := dswire.EncodeRecord(w.codec, in)
	if err != nil {
		return fmt.Errorf("flow: encode input for workflow %q: %w", w.name, err)
	}
	return w.run(ctx, payload, opts)
}

// run is Run past the encoding: the input is already in its recorded form,
// or nil when the caller gave none and expects a recorded one.
func (w Workflow[In]) run(ctx context.Context, payload []byte, opts []RunOption) error {
	all := make([]RunOption, 0, len(w.opts)+len(opts)+1)
	all = append(all, w.opts...)
	all = append(all, opts...)
	all = append(all, inputOption(payload, w.inputType()), rootOption(Root{Workflow: w.name}))

	return Run(ctx, w.name, w.call, all...)
}

// call runs the body on the input the run recorded.
func (w Workflow[In]) call(ctx Context) error {
	var in In
	recorded := inputFrom(ctx)
	if recorded == nil {
		// Nothing was given and Run let that pass, so In is None.
		return w.body(ctx, in)
	}
	in, err := dswire.DecodeRecord(w.codec, recorded)
	if err != nil {
		// The recorded input does not decode as In any more: the workflow
		// changed shape under a run in flight. No retry can fix that.
		return Permanent(fmt.Errorf("flow: decode the recorded input of workflow %q: %w", w.name, err))
	}
	return w.body(ctx, in)
}

// threadBody is the workflow as the body of its main thread, for a process
// replaying it from history. See lineage.go.
func (w Workflow[In]) threadBody() func(ctx Context) ([]byte, error) {
	return func(ctx Context) ([]byte, error) { return nil, w.call(ctx) }
}

// runJSON is run for a hosting program, which has the input as text.
func (w Workflow[In]) runJSON(ctx context.Context, input []byte, opts []RunOption) error {
	if input == nil {
		return w.run(ctx, nil, opts)
	}
	var in In
	if err := json.Unmarshal(input, &in); err != nil {
		return fmt.Errorf("flow: input for workflow %q does not decode as %s: %w; it looks like %s",
			w.name, w.inputType(), err, exampleInput(w.inputType()))
	}
	return w.Run(ctx, in, opts...)
}

// inputType is In, or nil for [None].
func (w Workflow[In]) inputType() reflect.Type {
	var zero In
	t := reflect.TypeOf(&zero).Elem()
	if t == reflect.TypeOf(None{}) {
		return nil
	}
	return t
}

// workflowHandler is the non-generic boundary that lets differently-typed
// workflows live in one registry, the way handler does for functions.
type workflowHandler interface {
	Name() string
	inputType() reflect.Type
	runJSON(ctx context.Context, input []byte, opts []RunOption) error
	threadBody() func(ctx Context) ([]byte, error)
}

var (
	workflowsMu sync.RWMutex
	workflows   = map[string]workflowHandler{}
)

// WorkflowInfo describes a defined workflow to a program that hosts them.
type WorkflowInfo struct {
	// Name is what the workflow was defined under.
	Name string
	// Input is the type a run of it is given, or nil for a workflow that takes
	// [None].
	Input reflect.Type
}

// Workflows lists every workflow defined in this process, sorted by name.
//
// For a program that hosts workflows and must say which it can run — or,
// when exactly one is defined, run that without being told.
func Workflows() []WorkflowInfo {
	workflowsMu.RLock()
	defer workflowsMu.RUnlock()
	all := make([]WorkflowInfo, 0, len(workflows))
	for _, w := range workflows {
		all = append(all, WorkflowInfo{Name: w.Name(), Input: w.inputType()})
	}
	slices.SortFunc(all, func(a, b WorkflowInfo) int { return cmp.Compare(a.Name, b.Name) })
	return all
}

// RunWorkflow runs the workflow defined under name on input given as JSON —
// the other end of [Workflows], for a program that has the name and the input
// as text off a command line rather than as values in its own code.
//
// input is decoded as the workflow's input type, so its shape is the type's
// JSON shape. Nil input means none was given: right for a workflow that takes
// [None], and for resuming a run whose input is already recorded; a fresh run
// of a workflow that takes input is refused with an example of what it wants.
// The error for an unknown name lists the names there are.
func RunWorkflow(ctx context.Context, name string, input []byte, opts ...RunOption) error {
	workflowsMu.RLock()
	w, ok := workflows[name]
	workflowsMu.RUnlock()
	if !ok {
		return fmt.Errorf("flow: no workflow %q is defined in this process; it defines: %s",
			name, definedWorkflows())
	}
	return w.runJSON(ctx, input, opts)
}

func definedWorkflows() string {
	names := ""
	for i, w := range Workflows() {
		if i > 0 {
			names += ", "
		}
		names += w.Name
	}
	return names
}

// exampleInput renders the zero value of t as JSON, which is the shape an
// input has to take. For an error message, so a caller who left the input off
// is told what it looks like rather than where to read about it.
func exampleInput(t reflect.Type) string {
	if t == nil {
		return "nothing"
	}
	v := reflect.Zero(t)
	if t.Kind() == reflect.Pointer {
		v = reflect.New(t.Elem())
	}
	b, err := json.Marshal(v.Interface())
	if err != nil {
		return t.String()
	}
	return string(b)
}

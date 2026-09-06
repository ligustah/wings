package flow

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"sync"

	"github.com/ligustah/durable_streams/dswire"
)

// None is the input type of a function that takes no input.
//
//	var Main = flow.Define(func(ctx flow.Context, _ flow.None) (flow.None, error) { … })
//	var _ = flow.Main(Main)
//
// A program hosting such a root does not ask for input, and refuses any.
type None = struct{}

// Main marks a defined function as a root the program can be asked to run — the
// outer piece of code a run is about, what used to be a "workflow".
//
//	var Ingest = flow.Define(func(ctx flow.Context, in Job) (flow.None, error) { … })
//	var _ = flow.Main(Ingest)
//
// A root is nothing but a function run as the main thread of a run, on the
// process that starts it (a coordinator), rather than dispatched: everything it
// calls goes to the run's executor, everything it forks to the placer. There is
// no separate workflow type — a root is a [Func] like any other, and Main only
// records that this one may be started by name.
//
// A program may Main several functions; a host that runs one is told which by
// name (nothing to say when there is exactly one). Call it at package scope,
// next to the definition:
//
//	var _ = flow.Main(Ingest)
//
// The name is recovered from the function, so it is whatever [WithName] or the
// build step gave the definition. Panics if f was not made by [Define], or if a
// function of that name is already a root. Returns a zero value only so it can
// sit in a package-scope `var _ =`.
func Main[In, Out any](f Func[In, Out]) struct{} {
	cap, err := describe(Context{context.Background()}, f, *new(In))
	if err != nil {
		panic("flow: Main requires a function made by flow.Define: " + err.Error())
	}
	codec := dswire.ReflectCodec[In]{New: allocator[In]()}
	// The encoded zero input, so a root taking [None] has a valid recorded input
	// rather than a nil one — nil does not decode, and a None root is given no
	// input to record otherwise.
	empty, err := dswire.EncodeRecord(codec, *new(In))
	if err != nil {
		panic("flow: Main: encode the zero input of " + cap.name + ": " + err.Error())
	}
	m := mainHandler{
		name:       cap.name,
		inType:     inputTypeFor[In](),
		emptyInput: empty,
		decode: func(input []byte) ([]byte, error) {
			var in In
			if err := json.Unmarshal(input, &in); err != nil {
				return nil, err
			}
			return dswire.EncodeRecord(codec, in)
		},
	}
	workflowsMu.Lock()
	defer workflowsMu.Unlock()
	if _, dup := workflows[m.name]; dup {
		panic(fmt.Sprintf("flow: %q is already a root; flow.Main was called on it twice", m.name))
	}
	workflows[m.name] = m
	return struct{}{}
}

// mainHandler is a function registered as a root: enough to start it by name
// with input given as JSON, and to replay it as the body of a lineage's root
// thread.
type mainHandler struct {
	name   string
	inType reflect.Type
	// emptyInput is the encoded zero input, used when the root takes [None] so
	// its recorded input is a valid encoded None{} rather than nil.
	emptyInput []byte
	// decode turns input given as JSON into the function's recorded input form.
	decode func(input []byte) ([]byte, error)
}

func (m mainHandler) Name() string            { return m.name }
func (m mainHandler) inputType() reflect.Type { return m.inType }

// threadBody runs the registered function as the body of its main thread,
// reading the input the run recorded — for the live root and for a process
// replaying it from history (see lineage.go).
//
// Execute directly, not functionBody: a root has no caller to hand a failure
// to, so its error must reach the run's own classify/retry as itself — a
// transient failure retries, as a workflow body always has — rather than being
// wrapped as the call failure a called function's is.
func (m mainHandler) threadBody() func(ctx Context) ([]byte, error) {
	name := m.name
	return func(ctx Context) ([]byte, error) {
		return Execute(ctx, name, inputFrom(ctx))
	}
}

// runJSON is run for a hosting program, which has the input as text.
func (m mainHandler) runJSON(ctx context.Context, input []byte, opts []RunOption) error {
	var payload []byte
	if input != nil {
		var err error
		if payload, err = m.decode(input); err != nil {
			return fmt.Errorf("flow: input for %q does not decode as %s: %w; it looks like %s",
				m.name, m.inType, err, exampleInput(m.inType))
		}
	}
	return m.run(ctx, payload, opts)
}

// run executes the root as a durable run named after it, to completion. The
// input is already in its recorded form, or nil when the caller gave none and
// expects a recorded one (a resume).
func (m mainHandler) run(ctx context.Context, payload []byte, opts []RunOption) error {
	if m.inType == nil {
		// A root taking None is always given None{}: never a nil payload, which
		// does not decode, and never a resume that reads a different input.
		payload = m.emptyInput
	}
	all := make([]RunOption, 0, len(opts)+2)
	all = append(all, opts...)
	all = append(all, inputOption(payload, m.inType), rootOption(Root{Workflow: m.name}))
	body := m.threadBody()
	return Run(ctx, m.name, func(ctx Context) error {
		_, err := body(ctx)
		return err
	}, all...)
}

// RunMain runs the root function f on in as a durable run named after it, to
// completion — the typed counterpart of [RunWorkflow], for a caller that has
// the function and a value rather than a name and JSON.
//
// f must have been registered with [Main]. As with any run, the name is the
// identity in the store: run it again on the same store and it resumes, with
// the input the first attempt recorded — passing a different one is an error,
// not a quiet restart.
func RunMain[In, Out any](ctx context.Context, f Func[In, Out], in In, opts ...RunOption) error {
	cap, err := describe(Context{context.Background()}, f, in)
	if err != nil {
		return fmt.Errorf("flow: RunMain requires a function made by flow.Define: %w", err)
	}
	workflowsMu.RLock()
	h, ok := workflows[cap.name]
	workflowsMu.RUnlock()
	if !ok {
		return fmt.Errorf("flow: %q is not registered as a root; declare it with flow.Main", cap.name)
	}
	return h.run(ctx, cap.payload, opts)
}

// inputTypeFor is In, or nil for [None].
func inputTypeFor[In any]() reflect.Type {
	var zero In
	t := reflect.TypeOf(&zero).Elem()
	if t == reflect.TypeOf(None{}) {
		return nil
	}
	return t
}

// workflowHandler is the non-generic boundary that lets differently-typed roots
// live in one registry, the way handler does for functions.
type workflowHandler interface {
	Name() string
	inputType() reflect.Type
	runJSON(ctx context.Context, input []byte, opts []RunOption) error
	run(ctx context.Context, payload []byte, opts []RunOption) error
	threadBody() func(ctx Context) ([]byte, error)
}

var (
	workflowsMu sync.RWMutex
	workflows   = map[string]workflowHandler{}
)

// WorkflowInfo describes a root to a program that hosts them.
type WorkflowInfo struct {
	// Name is what the root was defined under.
	Name string
	// Input is the type a run of it is given, or nil for one that takes [None].
	Input reflect.Type
}

// Workflows lists every root declared with [Main] in this process, sorted by
// name.
//
// For a program that hosts roots and must say which it can run — or, when
// exactly one is declared, run that without being told.
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

// RunWorkflow runs the root declared under name on input given as JSON — the
// other end of [Workflows], for a program that has the name and the input as
// text off a command line rather than as values in its own code.
//
// input is decoded as the root's input type, so its shape is the type's JSON
// shape. Nil input means none was given: right for a root that takes [None],
// and for resuming a run whose input is already recorded; a fresh run of a root
// that takes input is refused with an example of what it wants. The error for
// an unknown name lists the names there are.
func RunWorkflow(ctx context.Context, name string, input []byte, opts ...RunOption) error {
	workflowsMu.RLock()
	w, ok := workflows[name]
	workflowsMu.RUnlock()
	if !ok {
		return fmt.Errorf("flow: no root %q is declared in this process; it declares: %s",
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

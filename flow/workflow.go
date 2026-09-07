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

// None is the input type of a function that takes no input. A program hosting
// such a root does not ask for input, and refuses any.
//
//	var Main = flow.Define(func(ctx flow.Context, _ flow.None) (flow.None, error) { … })
//	var _ = flow.Main(Main)
type None = struct{}

// Main marks a defined function as a root the program can be asked to run: a
// [Func] run as the main thread of a run on the process that starts it, rather
// than dispatched. Call it at package scope next to the definition. The name is
// the definition's. A program may Main several functions and a host picks one by
// name.
//
//	var Ingest = flow.Define(func(ctx flow.Context, in Job) (flow.None, error) { … })
//	var _ = flow.Main(Ingest)
//
// Panics if f was not made by [Define], or if a function of that name is already
// a root. Returns a zero value only so it can sit in a package-scope var _ =.
func Main[In, Out any](f Func[In, Out]) struct{} {
	cap, err := describe(Context{context.Background()}, f, *new(In))
	if err != nil {
		panic("flow: Main requires a function made by flow.Define: " + err.Error())
	}
	codec := dswire.ReflectCodec[In]{New: allocator[In]()}
	// The encoded zero input, so a [None] root records a valid input rather than a
	// nil one, which does not decode.
	empty, err := dswire.EncodeRecord(codec, *new(In))
	if err != nil {
		panic("flow: Main: encode the zero input of a root: " + err.Error())
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
	// The name may not be known until the build step's table resolves the
	// function; registerMain holds the root until then. See names.go.
	registerMain(cap.handler, m)
	return struct{}{}
}

// registerMain records a root now if its name is known, or against its function's
// later resolution when it is not.
func registerMain(h handler, m mainHandler) {
	workflowsMu.Lock()
	defer workflowsMu.Unlock()
	if m.name != "" {
		addWorkflowLocked(m)
		return
	}
	pendingMains = append(pendingMains, pendingMain{h: h, m: m})
}

// addWorkflowLocked inserts a named root, panicking on a duplicate. Call with
// workflowsMu held.
func addWorkflowLocked(m mainHandler) {
	if _, dup := workflows[m.name]; dup {
		panic(fmt.Sprintf("flow: %q is already a root; flow.Main was called on it twice", m.name))
	}
	workflows[m.name] = m
}

// pendingMain is a root whose function had no name when [Main] ran.
type pendingMain struct {
	h handler
	m mainHandler
}

var pendingMains []pendingMain

// resolvePendingMains registers every held root whose function has since been
// named. Called by [RegisterCallSiteNames].
func resolvePendingMains() {
	workflowsMu.Lock()
	defer workflowsMu.Unlock()
	kept := pendingMains[:0]
	for _, pm := range pendingMains {
		name := pm.h.Name()
		if name == "" {
			kept = append(kept, pm)
			continue
		}
		pm.m.name = name
		addWorkflowLocked(pm.m)
	}
	pendingMains = kept
}

// mainHandler is a function registered as a root: enough to start it by name with
// JSON input, and to replay it as a lineage's root thread.
type mainHandler struct {
	name   string
	inType reflect.Type
	// emptyInput is the encoded zero input, used for a [None] root.
	emptyInput []byte
	// decode turns JSON input into the function's recorded input form.
	decode func(input []byte) ([]byte, error)
}

func (m mainHandler) Name() string            { return m.name }
func (m mainHandler) inputType() reflect.Type { return m.inType }

// threadBody runs the registered function as its main thread's body. It calls
// [Execute] directly, not functionBody: a root has no caller to hand a failure
// to, so its error reaches the run's own retry as itself rather than wrapped as a
// call failure.
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

// run executes the root as a durable run named after it. payload is already in
// recorded form, or nil for a resume.
func (m mainHandler) run(ctx context.Context, payload []byte, opts []RunOption) error {
	if m.inType == nil {
		payload = m.emptyInput // a None root is always given None{}
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

// RunMain runs the root function f on in as a durable run named after it — the
// typed counterpart of [RunWorkflow]. f must be registered with [Main]. The name
// is the identity in the store: run it again and it resumes with the recorded
// input, and a different input is an error.
func RunMain[In, Out any](ctx context.Context, f Func[In, Out], in In, opts ...RunOption) error {
	ensureNamesResolved()
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
// share one registry.
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
// name, for a program that hosts roots.
func Workflows() []WorkflowInfo {
	ensureNamesResolved()
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
// other end of [Workflows]. Nil input means none was given, right for a [None]
// root or a resume; a fresh run of a root that takes input is refused with an
// example. The error for an unknown name lists the names there are.
func RunWorkflow(ctx context.Context, name string, input []byte, opts ...RunOption) error {
	ensureNamesResolved()
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

// exampleInput renders the zero value of t as JSON, for an error that tells a
// caller what an input looks like.
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

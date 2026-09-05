package flow_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ligustah/wings/flow"
)

type batch struct {
	Items []int `json:"items"`
}

var (
	runsOfCounted atomic.Int32
	lastBatch     atomic.Pointer[batch]

	// Defined at package scope, as the docs ask, so the registry sees them.
	counted = flow.DefineWorkflow("wf.counted", func(ctx flow.Context, in batch) error {
		runsOfCounted.Add(1)
		lastBatch.Store(&in)
		_, err := ctx.Map(double, in.Items)
		return err
	})
	_ = flow.DefineWorkflow("wf.another", func(ctx flow.Context, _ flow.None) error { return nil })
)

// THE POINT: a workflow is a run body with a name, and running it IS running
// it under that name: the second Run on the same store finds the finished
// history and executes nothing.
func TestAWorkflowRunsOnceUnderItsOwnName(t *testing.T) {
	store := flow.NewMemStore()
	runsOfCounted.Store(0)
	in := batch{Items: []int{1, 2, 3}}

	if err := counted.Run(context.Background(), in, flow.WithStore(store)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := counted.Run(context.Background(), in, flow.WithStore(store)); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if n := runsOfCounted.Load(); n != 1 {
		t.Fatalf("the body ran %d times; a finished workflow must not run again on the same store", n)
	}

	events, err := store.Events(context.Background(), counted.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 {
		t.Fatalf("no history under %q; the workflow's name must be the run's name", counted.Name())
	}
}

// THE POINT: the input is part of the run. A resumed attempt is given what the
// first one recorded — even when the caller left it off — and a caller who
// brings something else is told, since replaying one input's history against
// another is a different run wearing this one's name.
func TestAResumedWorkflowIsGivenItsRecordedInput(t *testing.T) {
	store := flow.NewMemStore()
	attempts := 0
	var seen []int
	w := flow.DefineWorkflow("wf.resumed", func(ctx flow.Context, in batch) error {
		attempts++
		seen = append(seen, in.Items...)
		if attempts == 1 {
			return errors.New("first attempt dies")
		}
		return nil
	}, flow.MaxAttempts(1), flow.Backoff(0, 0))

	first := batch{Items: []int{7}}
	if err := w.Run(context.Background(), first, flow.WithStore(store)); err == nil {
		t.Fatal("the first attempt should have failed")
	}

	// Resumed by name with no input, the way a restarted coordinator does it.
	if err := flow.RunWorkflow(context.Background(), "wf.resumed", nil, flow.WithStore(store)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if want := []int{7, 7}; !slices.Equal(seen, want) {
		t.Fatalf("the attempts saw %v, want %v: the resume must be given the recorded input", seen, want)
	}

	// A different input is refused, not silently replaced — on a run in
	// flight, and on one that already finished, which is the same mistake.
	store2 := flow.NewMemStore()
	attempts = 0
	_ = w.Run(context.Background(), first, flow.WithStore(store2))
	err := w.Run(context.Background(), batch{Items: []int{8}}, flow.WithStore(store2))
	if err == nil || !strings.Contains(err.Error(), "different input") {
		t.Fatalf("want a refusal naming the different input, got %v", err)
	}
	err = w.Run(context.Background(), batch{Items: []int{8}}, flow.WithStore(store))
	if err == nil || !strings.Contains(err.Error(), "different input") {
		t.Fatalf("a finished run given different input must be refused too, got %v", err)
	}
}

// A fresh run of a workflow that takes input, given none, is refused with the
// shape it wants — the one thing the caller needs to fix the command line.
func TestAFreshRunWithoutItsInputIsRefusedWithAnExample(t *testing.T) {
	err := flow.RunWorkflow(context.Background(), "wf.counted", nil, flow.WithStore(flow.NewMemStore()))
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	for _, want := range []string{"flow_test.batch", `{"items":null}`} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}

	// A workflow that takes None wants nothing and gets none.
	if err := flow.RunWorkflow(context.Background(), "wf.another", nil, flow.WithStore(flow.NewMemStore())); err != nil {
		t.Fatalf("a None workflow must run without input: %v", err)
	}
}

// RunWorkflow decodes JSON into the workflow's input type: the path a hosting
// program takes from a command line.
func TestRunWorkflowByNameDecodesJSONInput(t *testing.T) {
	runsOfCounted.Store(0)
	err := flow.RunWorkflow(context.Background(), "wf.counted", []byte(`{"items":[4,5]}`),
		flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("RunWorkflow: %v", err)
	}
	if got := lastBatch.Load(); got == nil || !slices.Equal(got.Items, []int{4, 5}) {
		t.Fatalf("the body was given %v, want items [4 5]", got)
	}

	err = flow.RunWorkflow(context.Background(), "wf.counted", []byte(`{"items":"no"}`),
		flow.WithStore(flow.NewMemStore()))
	if err == nil || !strings.Contains(err.Error(), "does not decode as flow_test.batch") {
		t.Fatalf("want a decode error naming the type, got %v", err)
	}

	err = flow.RunWorkflow(context.Background(), "wf.nope", nil, flow.WithStore(flow.NewMemStore()))
	if err == nil || !strings.Contains(err.Error(), "wf.another, wf.counted") {
		t.Fatalf("want an unknown-name error listing the names, got %v", err)
	}
}

// The registry is what a hosting program consults, so it has to list what was
// defined, by name, in an order that does not depend on init order, and say
// what each takes.
func TestDefinedWorkflowsAreListedByName(t *testing.T) {
	var names []string
	byName := map[string]flow.WorkflowInfo{}
	for _, w := range flow.Workflows() {
		names = append(names, w.Name)
		byName[w.Name] = w
	}
	if !slices.IsSorted(names) {
		t.Errorf("Workflows are not sorted: %v", names)
	}
	if got := byName["wf.counted"].Input; got != reflect.TypeFor[batch]() {
		t.Errorf("wf.counted's input is %v, want batch", got)
	}
	if got := byName["wf.another"].Input; got != nil {
		t.Errorf("wf.another's input is %v, want nil for None", got)
	}
}

// Options declared with the workflow are its defaults; the caller's win.
func TestRunOptionsGivenAtDefinitionApplyAndCanBeOverridden(t *testing.T) {
	attempts := 0
	w := flow.DefineWorkflow("wf.flaky", func(ctx flow.Context, _ flow.None) error {
		attempts++
		return errors.New("not yet")
	}, flow.MaxAttempts(2), flow.Backoff(0, 0))

	err := w.Run(context.Background(), flow.None{}, flow.WithStore(flow.NewMemStore()))
	if err == nil || !strings.Contains(err.Error(), "after 2 attempts") {
		t.Fatalf("want failure after the 2 attempts the definition allows, got %v", err)
	}

	attempts = 0
	err = w.Run(context.Background(), flow.None{}, flow.WithStore(flow.NewMemStore()), flow.MaxAttempts(3))
	if err == nil || !strings.Contains(err.Error(), "after 3 attempts") {
		t.Fatalf("want the caller's 3 attempts to override the definition's 2, got %v", err)
	}
}

func TestDefiningTheSameWorkflowTwicePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("a duplicate DefineWorkflow did not panic")
		}
	}()
	flow.DefineWorkflow("wf.counted", func(ctx flow.Context, _ flow.None) error { return nil })
}

func TestTheZeroWorkflowSaysSo(t *testing.T) {
	var w flow.Workflow[flow.None]
	if err := w.Run(context.Background(), flow.None{}, flow.WithStore(flow.NewMemStore())); err == nil ||
		!strings.Contains(err.Error(), "DefineWorkflow") {
		t.Fatalf("want an error naming DefineWorkflow, got %v", err)
	}
}

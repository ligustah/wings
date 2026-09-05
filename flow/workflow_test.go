package flow_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ligustah/wings/flow"
)

var (
	runsOfCounted atomic.Int32

	// Defined at package scope, as the docs ask, so the registry sees them.
	counted = flow.DefineWorkflow("wf.counted", func(ctx flow.Context) error {
		runsOfCounted.Add(1)
		_, err := ctx.Map(double, []int{1, 2, 3})
		return err
	})
	_ = flow.DefineWorkflow("wf.another", func(ctx flow.Context) error { return nil })
)

// THE POINT: a workflow is a run body with a name, and running it IS running
// it under that name: the second Run on the same store finds the finished
// history and executes nothing.
func TestAWorkflowRunsOnceUnderItsOwnName(t *testing.T) {
	store := flow.NewMemStore()
	runsOfCounted.Store(0)

	if err := counted.Run(context.Background(), flow.WithStore(store)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := counted.Run(context.Background(), flow.WithStore(store)); err != nil {
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

// The registry is what a hosting program consults, so it has to list what was
// defined, by name, in an order that does not depend on init order.
func TestDefinedWorkflowsAreListedByName(t *testing.T) {
	var names []string
	for _, w := range flow.Workflows() {
		names = append(names, w.Name())
	}
	if !slices.IsSorted(names) {
		t.Errorf("Workflows are not sorted: %v", names)
	}
	for _, want := range []string{"wf.another", "wf.counted"} {
		if !slices.Contains(names, want) {
			t.Errorf("%q is defined but not listed: %v", want, names)
		}
		if w, ok := flow.LookupWorkflow(want); !ok || w.Name() != want {
			t.Errorf("LookupWorkflow(%q) = %v, %v", want, w.Name(), ok)
		}
	}
	if _, ok := flow.LookupWorkflow("wf.nope"); ok {
		t.Error("LookupWorkflow found a workflow that was never defined")
	}
}

// Options declared with the workflow are its defaults; the caller's win.
func TestRunOptionsGivenAtDefinitionApplyAndCanBeOverridden(t *testing.T) {
	attempts := 0
	w := flow.DefineWorkflow("wf.flaky", func(ctx flow.Context) error {
		attempts++
		return errors.New("not yet")
	}, flow.MaxAttempts(2), flow.Backoff(0, 0))

	err := w.Run(context.Background(), flow.WithStore(flow.NewMemStore()))
	if err == nil || !strings.Contains(err.Error(), "after 2 attempts") {
		t.Fatalf("want failure after the 2 attempts the definition allows, got %v", err)
	}

	attempts = 0
	err = w.Run(context.Background(), flow.WithStore(flow.NewMemStore()), flow.MaxAttempts(3))
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
	flow.DefineWorkflow("wf.counted", func(ctx flow.Context) error { return nil })
}

func TestTheZeroWorkflowSaysSo(t *testing.T) {
	var w flow.Workflow
	if err := w.Run(context.Background(), flow.WithStore(flow.NewMemStore())); err == nil ||
		!strings.Contains(err.Error(), "DefineWorkflow") {
		t.Fatalf("want an error naming DefineWorkflow, got %v", err)
	}
}

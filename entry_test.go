package wings

import (
	"strings"
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: a program that defines one workflow runs it unasked; one that
// defines several must be told, and is told what it can be told.
func TestChoosingTheWorkflowToRun(t *testing.T) {
	one := flow.DefineWorkflow("entry.one", func(ctx flow.Context) error { return nil })
	two := flow.DefineWorkflow("entry.two", func(ctx flow.Context) error { return nil })

	for _, tc := range []struct {
		name    string
		flag    string
		defined []flow.Workflow
		want    string // workflow name, or a fragment of the error
		fails   bool
	}{
		{"none defined", "", nil, "flow.DefineWorkflow", true},
		{"one, unnamed", "", []flow.Workflow{one}, "entry.one", false},
		{"one, named", "entry.one", []flow.Workflow{one}, "entry.one", false},
		{"several, unnamed", "", []flow.Workflow{one, two}, "entry.one, entry.two", true},
		{"several, named", "entry.two", []flow.Workflow{one, two}, "entry.two", false},
		{"unknown name", "entry.three", []flow.Workflow{one, two}, "entry.one, entry.two", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, err := chooseWorkflow(tc.flag, tc.defined)
			if tc.fails {
				if err == nil {
					t.Fatalf("chose %q; want an error", w.Name())
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error %q does not mention %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatalf("chooseWorkflow: %v", err)
			}
			if w.Name() != tc.want {
				t.Fatalf("chose %q, want %q", w.Name(), tc.want)
			}
		})
	}
}

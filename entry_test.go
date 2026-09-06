package wings

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: a program that defines one workflow runs it unasked; one that
// defines several must be told, and is told what it can be told.
func TestChoosingTheWorkflowToRun(t *testing.T) {
	one := flow.WorkflowInfo{Name: "entry.one"}
	two := flow.WorkflowInfo{Name: "entry.two"}

	for _, tc := range []struct {
		name    string
		flag    string
		defined []flow.WorkflowInfo
		want    string // workflow name, or a fragment of the error
		fails   bool
	}{
		{"none defined", "", nil, "flow.Main", true},
		{"one, unnamed", "", []flow.WorkflowInfo{one}, "entry.one", false},
		{"one, named", "entry.one", []flow.WorkflowInfo{one}, "entry.one", false},
		{"several, unnamed", "", []flow.WorkflowInfo{one, two}, "entry.one, entry.two", true},
		{"several, named", "entry.two", []flow.WorkflowInfo{one, two}, "entry.two", false},
		{"unknown name", "entry.three", []flow.WorkflowInfo{one, two}, "entry.one, entry.two", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, err := chooseWorkflow(tc.flag, tc.defined)
			if tc.fails {
				if err == nil {
					t.Fatalf("chose %q; want an error", w.Name)
				}
				if !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("error %q does not mention %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatalf("chooseWorkflow: %v", err)
			}
			if w.Name != tc.want {
				t.Fatalf("chose %q, want %q", w.Name, tc.want)
			}
		})
	}
}

// -input is the JSON itself or @file; nothing means nothing, and a workflow
// that takes nothing refuses to be given something.
func TestReadingTheWorkflowInput(t *testing.T) {
	typed := flow.WorkflowInfo{Name: "entry.typed", Input: reflect.TypeFor[struct{ N int }]()}
	none := flow.WorkflowInfo{Name: "entry.none"}

	file := filepath.Join(t.TempDir(), "in.json")
	if err := os.WriteFile(file, []byte(`{"N":2}`), 0o644); err != nil {
		t.Fatal(err)
	}

	if got, err := readInput("", typed); err != nil || got != nil {
		t.Errorf("no -input: got %q, %v; want nil, nil (flow decides whether that is allowed)", got, err)
	}
	if got, err := readInput(`{"N":1}`, typed); err != nil || string(got) != `{"N":1}` {
		t.Errorf("literal: got %q, %v", got, err)
	}
	if got, err := readInput("@"+file, typed); err != nil || string(got) != `{"N":2}` {
		t.Errorf("@file: got %q, %v", got, err)
	}
	if _, err := readInput("@"+filepath.Join(t.TempDir(), "missing.json"), typed); err == nil {
		t.Error("a missing @file was not reported")
	}
	if _, err := readInput(`{}`, none); err == nil || !strings.Contains(err.Error(), "takes no input") {
		t.Errorf("input for a None workflow: got %v; want a refusal", err)
	}
}

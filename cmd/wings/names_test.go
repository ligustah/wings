package main

import (
	"testing"

	"github.com/ligustah/wings/flow"

	// Imported for effect: its nameless Define calls run at init and wait for
	// the table this test builds from the same source.
	"github.com/ligustah/wings/cmd/wings/internal/namesfixture"
)

// THE POINT: the line inspectAPI reads for a Define is the line Define records
// off the call stack at run time, so a name inferred from the source resolves
// the real definition. If they disagreed, the definition would stay nameless and
// default to its own call site instead of taking the variable's name — which is
// exactly what this checks does NOT happen, across the single-line, multi-line
// and multi-line-call forms real code uses.
func TestInferredNamesMatchRuntimeCallSites(t *testing.T) {
	_ = namesfixture.Alpha // keep the import if the fixture is ever trimmed

	api, err := inspectAPI("internal/namesfixture")
	if err != nil {
		t.Fatalf("inspectAPI: %v", err)
	}

	// A named definition contributes nothing to infer.
	for site, name := range api.names {
		if name == "Delta" {
			t.Errorf("the WithName'd definition should not be in the table, but %s => Delta", site)
		}
	}

	flow.RegisterCallSiteNames(api.names)

	// Each nameless definition now answers to its variable's name.
	for _, name := range []string{"Alpha", "Beta", "Gamma", "Root"} {
		if _, ok := flow.BoundsOf(name); !ok {
			t.Errorf("no function registered as %q — the inferred line did not match the runtime call site", name)
		}
	}

	// The named one kept its explicit name, not the variable's.
	if _, ok := flow.BoundsOf("fixture.delta"); !ok {
		t.Error("the WithName'd definition is not registered under its explicit name")
	}
	if _, ok := flow.BoundsOf("Delta"); ok {
		t.Error("the WithName'd definition was also registered under its variable name")
	}

	// The Main-marked root is a root under its inferred name.
	var haveRoot bool
	for _, w := range flow.Workflows() {
		if w.Name == "Root" {
			haveRoot = true
		}
	}
	if !haveRoot {
		t.Error("the Main-marked root is not registered under its inferred name")
	}
}

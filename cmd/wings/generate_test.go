package main

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func mustGenerate(t *testing.T, b []byte, err error) string {
	t.Helper()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	src := string(b)
	// Everything generated must at least parse; jennifer renders through
	// go/format, so a failure here means the shape is wrong, not the spacing.
	if _, perr := parser.ParseFile(token.NewFileSet(), "main.go", src, parser.AllErrors); perr != nil {
		t.Fatalf("generated source does not parse: %v\n\n%s", perr, src)
	}
	return src
}

// THE POINT: //go:embed applies to the declaration IMMEDIATELY below it. A
// blank line in between demotes it to an ordinary comment, and the coordinator
// then builds cleanly and ships with an empty worker — a failure that only
// appears when someone tries to provision a machine.
//
// jennifer separates top-level statements with a blank line, so the directive
// and the var have to be one statement. This is the test that they still are.
func TestEmbedDirectiveStaysAttachedToItsVar(t *testing.T) {
	b, err := coordinatorMain("example.com/app", "example.com/app", platform{"linux", "amd64"}, true, nil, nil)
	src := mustGenerate(t, b, err)

	lines := strings.Split(src, "\n")
	idx := -1
	for i, l := range lines {
		if strings.HasPrefix(strings.TrimSpace(l), "//go:embed") {
			idx = i
			break
		}
	}
	if idx < 0 {
		t.Fatalf("no //go:embed directive was generated:\n\n%s", src)
	}
	if idx+1 >= len(lines) {
		t.Fatal("//go:embed is the last line")
	}
	next := strings.TrimSpace(lines[idx+1])
	if !strings.HasPrefix(next, "var wingsWorker") {
		t.Fatalf("the line after //go:embed is %q, not the var declaration.\n"+
			"The directive is inert and the coordinator would carry no worker.\n\n%s", next, src)
	}
	if !strings.Contains(src, blobName) {
		t.Errorf("the directive should name %s", blobName)
	}
}

func TestCoordinatorMainWiresTheOptions(t *testing.T) {
	b, err := coordinatorMain("example.com/app", "example.com/app", platform{"linux", "arm64"}, true, nil, nil)
	src := mustGenerate(t, b, err)

	for _, want := range []string{
		`_ "embed"`,
		`app "example.com/app"`,
		"wings.CoordinatorMain",
		"wings.CoordinatorOptions",
		"Worker:",
		`WorkerOS:    "linux"`,
		`WorkerArch:  "arm64"`,
		"app.Provisioner()",
	} {
		if !strings.Contains(src, want) {
			t.Errorf("generated coordinator is missing %q:\n\n%s", want, src)
		}
	}
}

// Without an exported Provisioner the field must be nil rather than absent or a
// call to a function that does not exist.
func TestCoordinatorMainWithoutProvisioner(t *testing.T) {
	b, err := coordinatorMain("example.com/app", "example.com/app", platform{"linux", "amd64"}, false, nil, nil)
	src := mustGenerate(t, b, err)

	// With nothing in main to name it, the package must still be linked in,
	// or the coordinator has no workflow to run.
	if !strings.Contains(src, `_ "example.com/app"`) {
		t.Errorf("the package must be imported for its flow.Main calls:\n\n%s", src)
	}
	if strings.Contains(src, "app.Provisioner") {
		t.Errorf("generated a call to Provisioner the package does not export:\n\n%s", src)
	}
	if !strings.Contains(src, "Provisioner: nil") {
		t.Errorf("Provisioner should be explicitly nil:\n\n%s", src)
	}
}

// A split build still has to link the work package, or the coordinator has
// nothing registered to dispatch against.
func TestSplitBuildStillLinksTheWorkPackage(t *testing.T) {
	b, err := coordinatorMain("example.com/app/job", "example.com/app/coord", platform{"linux", "amd64"}, false, nil, nil)
	src := mustGenerate(t, b, err)

	if !strings.Contains(src, `_ "example.com/app/job"`) {
		t.Errorf("the work package must be linked in for its Define calls:\n\n%s", src)
	}
	if !strings.Contains(src, `_ "example.com/app/coord"`) {
		t.Errorf("the coordinator package must be linked in for its flow.Main calls:\n\n%s", src)
	}

	// When main has to call its Provisioner, the same package is imported by
	// name instead — once, not both ways.
	b, err = coordinatorMain("example.com/app/job", "example.com/app/coord", platform{"linux", "amd64"}, true, nil, nil)
	src = mustGenerate(t, b, err)
	if !strings.Contains(src, `app "example.com/app/coord"`) {
		t.Errorf("the coordinator package must be imported as app when its Provisioner is called:\n\n%s", src)
	}
	if n := strings.Count(src, `"example.com/app/coord"`); n != 1 {
		t.Errorf("imported the coordinator package %d times, want 1:\n\n%s", n, src)
	}
}

// The unsplit case must not import the same package twice.
func TestUnsplitBuildImportsThePackageOnce(t *testing.T) {
	b, err := coordinatorMain("example.com/app", "example.com/app", platform{"linux", "amd64"}, false, nil, nil)
	src := mustGenerate(t, b, err)

	if n := strings.Count(src, `"example.com/app"`); n != 1 {
		t.Errorf("imported the package %d times, want 1:\n\n%s", n, src)
	}
}

// THE POINT: the inferred names table is compiled into BOTH mains and seeded
// before any work runs, so a Define with no WithName is named from its variable
// on the coordinator and the worker alike.
func TestBothMainsSeedTheNamesTable(t *testing.T) {
	names := map[string]string{"job.go:12": "Render", "job.go:18": "Frames"}

	wb, werr := workerMain("example.com/app", names)
	worker := mustGenerate(t, wb, werr)
	cb, cerr := coordinatorMain("example.com/app", "example.com/app",
		platform{"linux", "amd64"}, false, nil, names)
	coord := mustGenerate(t, cb, cerr)

	for _, src := range []string{worker, coord} {
		for _, want := range []string{
			"flow.RegisterCallSiteNames",
			`"job.go:12": "Render"`,
			`"job.go:18": "Frames"`,
		} {
			if !strings.Contains(src, want) {
				t.Errorf("generated main is missing %q:\n\n%s", want, src)
			}
		}
	}
}

// With every definition named by WithName there is nothing to infer, and no
// table call is emitted.
func TestNoNamesTableWhenEmpty(t *testing.T) {
	wb, werr := workerMain("example.com/app", nil)
	worker := mustGenerate(t, wb, werr)
	if strings.Contains(worker, "RegisterCallSiteNames") {
		t.Errorf("emitted a names table for a package that needs none:\n\n%s", worker)
	}
}

func TestWorkerMainIsJustTheEntrypoint(t *testing.T) {
	b, err := workerMain("example.com/app", nil)
	src := mustGenerate(t, b, err)

	if !strings.Contains(src, `_ "example.com/app"`) {
		t.Errorf("the work package must be imported for effect:\n\n%s", src)
	}
	if !strings.Contains(src, "wings.WorkerMain()") {
		t.Errorf("missing the worker entrypoint:\n\n%s", src)
	}
	// A worker deploys nothing, so it must not carry a worker of its own.
	if strings.Contains(src, "go:embed") {
		t.Errorf("the worker must not embed a worker:\n\n%s", src)
	}
	if strings.Contains(src, "CoordinatorMain") {
		t.Errorf("the worker must not be a coordinator:\n\n%s", src)
	}
}

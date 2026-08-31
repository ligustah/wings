package wings

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

// THE POINT: a program that uses wings must not be able to tell where its work
// is running, and must not name a cloud anywhere. Where it runs is a command
// line flag.
//
// This is checked against real source rather than asserted in prose, because it
// is the kind of property that decays one convenience import at a time — the
// first `if target == remote` in a work function is how a library like this
// stops being worth using.
func TestExampleProgramNamesNoTargetAndNoCloud(t *testing.T) {
	dir := filepath.Join("examples", "digest")
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("example not present: %v", err)
	}

	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse %s: %v", dir, err)
	}

	// Imports that would mean the program knows where it runs.
	forbiddenImports := []string{
		"github.com/ligustah/wings/gcp",
		"cloud.google.com/go/compute",
		"golang.org/x/crypto/ssh",
		"github.com/ligustah/durable_streams",
	}
	// Identifiers that choose or describe a target.
	forbiddenIdents := []string{
		"InProcess", "LocalProcess", "Remote", "Target", "Provisioner", "Machine",
	}

	for _, p := range pkgs {
		for name, f := range p.Files {
			for _, imp := range f.Imports {
				path := strings.Trim(imp.Path.Value, `"`)
				for _, bad := range forbiddenImports {
					if strings.HasPrefix(path, bad) {
						t.Errorf("%s imports %q.\n"+
							"A wings program must not know where it runs; that is a -target flag.",
							name, path)
					}
				}
			}

			ast.Inspect(f, func(n ast.Node) bool {
				id, ok := n.(*ast.Ident)
				if !ok {
					return true
				}
				if slices.Contains(forbiddenIdents, id.Name) {
					t.Errorf("%s references %s at %s.\n"+
						"Choosing where work runs belongs on the command line, not in the program.",
						name, id.Name, fset.Position(id.Pos()))
				}
				return true
			})
		}
	}
}

// THE POINT: the same work function, over the same inputs, must produce exactly
// the same results whichever target ran it.
//
// The other target tests assert each target works. This one asserts they AGREE,
// which is the actual promise — a difference between them is the library
// failing at its one job, and comparing outputs is the only way to see it.
func TestTargetsProduceIdenticalResults(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	in := make([]int, 40)
	for i := range in {
		in[i] = i * 7
	}

	run := func(target Target, workers int) []int {
		t.Helper()
		c := start(t, Config{Target: target, Workers: workers, Concurrency: 3})
		got, err := c.Map(t.Context(), double, in)
		if err != nil {
			t.Fatalf("Map: %v", err)
		}
		return got
	}

	inproc := run(InProcess(), 2)
	local := run(LocalProcess(), 3)

	if !slices.Equal(inproc, local) {
		t.Fatalf("targets disagree.\n  inprocess: %v\n      local: %v", inproc, local)
	}
	// And both must be right, not merely equal to each other.
	for i, got := range inproc {
		if want := in[i] * 2; got != want {
			t.Fatalf("result %d is %d, want %d", i, got, want)
		}
	}
}

// A work function is handed nothing but a context and its input, so there is
// nothing in scope for it to branch on. This pins that: the same call, on two
// targets, cannot observe a difference.
func TestWorkFunctionSeesTheSameContextShapeEverywhere(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	for _, tc := range []struct {
		name   string
		target Target
	}{
		{"inprocess", InProcess()},
		{"local", LocalProcess()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := start(t, Config{Target: tc.target, Workers: 1, Concurrency: 1})

			// A deadline set by the coordinator must reach the work function
			// identically on both, since JobTimeout is the only deadline wings
			// imposes and it is configured, not inferred from the target.
			got, err := c.Call(t.Context(), reportsDeadline, 0)
			if err != nil {
				t.Fatalf("Call: %v", err)
			}
			if got != "no deadline" {
				t.Errorf("work function saw %q; with no JobTimeout set it must see no deadline "+
					"on every target", got)
			}
		})
	}
}

var reportsDeadline = Define("test.deadline", func(ctx context.Context, _ int) (string, error) {
	if _, ok := ctx.Deadline(); ok {
		return "has deadline", nil
	}
	return "no deadline", nil
})

// JobTimeout, when set, must apply on every target — the guarantee is the
// config's, not the transport's.
func TestJobTimeoutAppliesOnEveryTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	for _, tc := range []struct {
		name   string
		target Target
	}{
		{"inprocess", InProcess()},
		{"local", LocalProcess()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := start(t, Config{Target: tc.target, Workers: 1, JobTimeout: 100 * time.Millisecond})

			if _, err := c.Call(t.Context(), slow, 10*time.Second); err == nil {
				t.Fatal("want a timeout, got nil")
			} else if !strings.Contains(err.Error(), "deadline exceeded") {
				t.Fatalf("want a deadline error, got: %v", err)
			}
		})
	}
}

package flow

import (
	"context"
	"runtime"
	"testing"
)

// THE POINT: a nameless Define takes the name RegisterCallSiteNames supplies for
// its call site, and is then registered and callable under it.
func TestACallSiteNameResolvesAName(t *testing.T) {
	_, file, line, _ := runtime.Caller(0)
	fn := Define(func(ctx Context, in int) (int, error) { return in * 2, nil })
	site := siteKey(file, line+1) // the Define call is the next line

	RegisterCallSiteNames(map[string]string{site: "test.callsite.double"})

	out, err := fn(Bind(context.Background(), Local()), 2)
	if err != nil {
		t.Fatalf("after resolution: %v", err)
	}
	if out != 4 {
		t.Fatalf("got %d, want 4 — the resolved function did not run", out)
	}
	if _, ok := lookup("test.callsite.double"); !ok {
		t.Fatal("the resolved function was not registered under its name")
	}
}

// THE POINT: with no WithName and no table entry, a definition defaults to its
// own call site as its name — so the flow package used without the build step
// still runs, each function identified by where it was written.
func TestANamelessDefinitionDefaultsToItsCallSite(t *testing.T) {
	_, file, line, _ := runtime.Caller(0)
	fn := Define(func(ctx Context, in int) (int, error) { return in + 1, nil })
	site := siteKey(file, line+1) // the Define call is the next line

	// No RegisterCallSiteNames for this site: calling it defaults the name.
	out, err := fn(Bind(context.Background(), Local()), 41)
	if err != nil {
		t.Fatalf("calling a nameless, unmapped function: %v", err)
	}
	if out != 42 {
		t.Fatalf("got %d, want 42 — the defaulted function did not run", out)
	}
	if _, ok := lookup(site); !ok {
		t.Fatalf("the function was not registered under its call site %q", site)
	}
}

// THE POINT: a root marked with Main on a nameless function is held until the
// function is named, then registered under the resolved name — so flow.Main
// works with an inferred name exactly as with an explicit one, even though Main
// runs before the name is in. (No use-point like Workflows is touched before
// the table is supplied, since a use finalizes names by defaulting them.)
func TestMainHoldsANamelessRootUntilResolved(t *testing.T) {
	_, file, line, _ := runtime.Caller(0)
	root := Define(func(ctx Context, _ None) (None, error) { return None{}, nil })
	_ = Main(root)
	site := siteKey(file, line+1) // the Define call is the next line

	RegisterCallSiteNames(map[string]string{site: "test.callsite.root"})

	var found bool
	for _, w := range Workflows() {
		if w.Name == "test.callsite.root" {
			found = true
		}
	}
	if !found {
		t.Fatal("the root was not registered under its resolved name")
	}
}

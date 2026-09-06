// Package namesfixture is a fixture for the build step's name inference: a
// handful of nameless flow.Define calls in the forms real code uses, so a test
// can check the line the parser reads for each matches the line Define records
// off the call stack at run time.
package namesfixture

import "github.com/ligustah/wings/flow"

// Single-line form, the common one.
var Alpha = flow.Define(func(ctx flow.Context, in int) (int, error) { return in, nil })

// Body spanning several lines, the call still opening on the var's line.
var Beta = flow.Define(func(ctx flow.Context, in int) (int, error) {
	return in * 2, nil
})

// The call itself spanning lines: flow.Define( opens here, arguments below.
var Gamma = flow.Define(
	func(ctx flow.Context, in int) (int, error) { return in + 1, nil },
)

// Already named: nothing to infer, so it must not appear in the table, and it
// registers under the given name rather than the variable's.
var Delta = flow.Define(func(ctx flow.Context, in int) (int, error) {
	return in, nil
}, flow.WithName("fixture.delta"))

// A root marked with Main, its name inferred like any other.
var Root = flow.Define(func(ctx flow.Context, _ flow.None) (flow.None, error) { return flow.None{}, nil })

var _ = flow.Main(Root)

// Package namesfixture holds nameless flow.Define calls in the forms real code
// uses, so a test can check the build step infers each one's name at the line
// Define records at run time.
package namesfixture

import "github.com/ligustah/wings/flow"

// Single-line form.
var Alpha = flow.Define(func(ctx flow.Context, in int) (int, error) { return in, nil })

// Multi-line body.
var Beta = flow.Define(func(ctx flow.Context, in int) (int, error) {
	return in * 2, nil
})

// Multi-line call.
var Gamma = flow.Define(
	func(ctx flow.Context, in int) (int, error) { return in + 1, nil },
)

// Already named: must not appear in the inferred table.
var Delta = flow.Define(func(ctx flow.Context, in int) (int, error) {
	return in, nil
}, flow.WithName("fixture.delta"))

// A root, its name inferred like any other.
var Root = flow.Define(func(ctx flow.Context, _ flow.None) (flow.None, error) { return flow.None{}, nil })

var _ = flow.Main(Root)

package wings

import (
	"context"
	"errors"
	"fmt"

	"github.com/ligustah/wings/internal/invoke"
)

// Map runs f on every input and returns the results in the order the inputs
// were given.
//
// Every input is dispatched before any result is waited on, so the work is
// actually in flight in parallel rather than merely submitted in a loop.
//
// The parallelism comes from the context's host rather than from goroutines
// started here, which is what makes this work unchanged inside a workflow: a
// cluster runs the calls concurrently, while a workflow forks a thread per index
// so the event log comes out in the same order every attempt. Written with `go`
// instead, this function would be correct on a cluster and would quietly destroy
// replay in a workflow.
//
// If some inputs fail, the successful outputs are still returned in their places
// and the error joins every failure, each naming its index. Check the error
// before trusting a position you did not verify.
func Map[In, Out any](ctx context.Context, f Func[In, Out], ins []In) ([]Out, error) {
	outs := make([]Out, len(ins))
	if len(ins) == 0 {
		return outs, nil
	}

	host := invoke.From(ctx)
	if host == nil {
		return outs, errors.New("wings: Map was called on a context that is not bound to a cluster " +
			"or a workflow; use the context Coordinate was given, or Cluster.Bind")
	}

	errs := host.Parallel(ctx, len(ins), func(ctx context.Context, i int) error {
		out, err := f(ctx, ins[i])
		if err != nil {
			return err
		}
		outs[i] = out
		return nil
	})

	var joined []error
	for i, err := range errs {
		if err != nil {
			joined = append(joined, fmt.Errorf("wings: input %d: %w", i, err))
		}
	}
	return outs, errors.Join(joined...)
}

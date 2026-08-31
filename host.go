package wings

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ligustah/wings/internal/invoke"
)

// Bind returns a context in which defined work functions dispatch to this
// cluster.
//
// A work function is called, not passed to a method, so the context is how it
// learns where to run. [CoordinatorMain] binds the context it hands to your
// Coordinate, so a program built with `wings build` never calls this; it is here
// for a cluster you started yourself.
func (c *Cluster) Bind(ctx context.Context) context.Context {
	return invoke.With(ctx, clusterHost{c})
}

// clusterHost dispatches straight to workers, with no record kept.
//
// A separate type rather than methods on Cluster because Invoke and Parallel are
// the vocabulary of the seam, not of a cluster, and putting them on Cluster
// would put encoded payloads on its public API.
type clusterHost struct{ c *Cluster }

func (h clusterHost) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	p, err := h.c.submit(ctx, name, payload)
	if err != nil {
		return nil, err
	}
	res, err := h.c.await(ctx, p)
	if err != nil {
		return nil, err
	}
	if res.Error != "" {
		// The value does not survive the trip, only the message: match on
		// content rather than identity across a worker boundary.
		return nil, errors.New(res.Error)
	}
	return res.Payload, nil
}

// Parallel runs every index at once. A cluster has no ordering to protect —
// each call is an independent job — so goroutines are the whole implementation.
func (h clusterHost) Parallel(ctx context.Context, n int, body func(context.Context, int) error) []error {
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					errs[i] = fmt.Errorf("wings: panic in parallel body %d: %v", i, r)
				}
			}()
			errs[i] = body(ctx, i)
		}()
	}
	wg.Wait()
	return errs
}

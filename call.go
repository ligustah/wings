package wings

import (
	"context"
	"errors"
	"fmt"

	"github.com/ligustah/durable_streams/dswire"
)

// Call runs f on a worker and returns its result.
//
// The error is whatever the work function returned, reconstructed on this side
// as a plain error: the value does not survive the trip, only its message, so
// match on content rather than identity across a worker boundary.
func (c *Cluster) Call[In, Out any](ctx context.Context, f *Func[In, Out], in In) (Out, error) {
	var zero Out

	payload, err := dswire.EncodeRecord(f.inCodec, in)
	if err != nil {
		return zero, fmt.Errorf("wings: encode input for %q: %w", f.name, err)
	}

	p, err := c.submit(ctx, f.name, payload)
	if err != nil {
		return zero, err
	}
	res, err := c.await(ctx, p)
	if err != nil {
		return zero, err
	}
	return decodeResult(f, res)
}

// Map runs f on every input, spread across the workers, and returns the results
// in the order the inputs were given.
//
// Every input is dispatched before any result is waited on, so the work is
// actually in flight in parallel rather than merely submitted in a loop.
//
// If some inputs fail, the successful outputs are still returned in their
// places and the error joins every failure, each naming its index. Check the
// error before trusting a position you did not verify.
func (c *Cluster) Map[In, Out any](ctx context.Context, f *Func[In, Out], ins []In) ([]Out, error) {
	outs := make([]Out, len(ins))
	if len(ins) == 0 {
		return outs, nil
	}

	pending := make([]*pendingJob, len(ins))
	var errs []error

	for i, in := range ins {
		payload, err := dswire.EncodeRecord(f.inCodec, in)
		if err != nil {
			errs = append(errs, fmt.Errorf("wings: input %d: encode: %w", i, err))
			continue
		}
		p, err := c.submit(ctx, f.name, payload)
		if err != nil {
			errs = append(errs, fmt.Errorf("wings: input %d: %w", i, err))
			continue
		}
		pending[i] = p
	}

	for i, p := range pending {
		if p == nil {
			continue
		}
		res, err := c.await(ctx, p)
		if err != nil {
			errs = append(errs, fmt.Errorf("wings: input %d: %w", i, err))
			continue
		}
		out, err := decodeResult(f, res)
		if err != nil {
			errs = append(errs, fmt.Errorf("wings: input %d: %w", i, err))
			continue
		}
		outs[i] = out
	}
	return outs, errors.Join(errs...)
}

func decodeResult[In, Out any](f *Func[In, Out], res resultEnvelope) (Out, error) {
	var zero Out
	if res.Error != "" {
		return zero, errors.New(res.Error)
	}
	out, err := dswire.DecodeRecord(f.outCodec, res.Payload)
	if err != nil {
		return zero, fmt.Errorf("wings: decode output for %q: %w", f.name, err)
	}
	return out, nil
}

package wings

import (
	"context"
	"errors"
	"fmt"
)

// ProvisionerSpec is one provisioner in a priority chain, optionally capped at
// Cap machines (0 means no limit).
type ProvisionerSpec struct {
	Provisioner Provisioner
	Cap         int
}

// Provisioners chains provisioners in priority order: each takes up to its cap of
// the leases, the next takes what remains, and a provisioner that fails or comes
// up short spills its leases to the one after it. The result is itself a
// [Reattacher], asking every chained provisioner to recover its own machines.
//
//	wings.Remote(wings.Provisioners(
//		wings.ProvisionerSpec{Provisioner: gcp.New(cfg), Cap: 8},
//		wings.ProvisionerSpec{Provisioner: aws.New(cfg)},
//	))
func Provisioners(specs ...ProvisionerSpec) Provisioner {
	return &chain{specs: specs}
}

type chain struct{ specs []ProvisionerSpec }

var _ Reattacher = (*chain)(nil)

func (c *chain) Provision(ctx context.Context, leases []string) ([]Machine, error) {
	remaining := leases
	var got []Machine
	var spills []error

	for _, spec := range c.specs {
		if len(remaining) == 0 {
			break
		}
		offer := remaining
		if spec.Cap > 0 && spec.Cap < len(offer) {
			offer = offer[:spec.Cap]
		}

		machines, err := spec.Provisioner.Provision(ctx, offer)
		if err != nil {
			// The offer spills to the next provisioner rather than failing the run;
			// the cause is kept so a run that spills to the end can report why.
			spills = append(spills, fmt.Errorf("%T: %w", spec.Provisioner, err))
			continue
		}
		got = append(got, machines...)

		done := make(map[string]bool, len(machines))
		for _, m := range machines {
			done[m.ID()] = true
		}
		remaining = filterOut(remaining, done)
	}

	if len(remaining) > 0 {
		release := context.WithoutCancel(ctx)
		for _, m := range got {
			_ = m.Close(release)
		}
		err := fmt.Errorf("wings: no provisioner could supply %d of %d machines", len(remaining), len(leases))
		if len(spills) > 0 {
			return nil, errors.Join(append([]error{err}, spills...)...)
		}
		return nil, err
	}
	return got, nil
}

func (c *chain) Reattach(ctx context.Context, leases []string) ([]Machine, error) {
	var found []Machine
	seen := map[string]bool{}
	var errs []error
	for _, spec := range c.specs {
		re, ok := spec.Provisioner.(Reattacher)
		if !ok {
			continue
		}
		machines, err := re.Reattach(ctx, leases)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, m := range machines {
			if !seen[m.ID()] {
				seen[m.ID()] = true
				found = append(found, m)
			}
		}
	}
	if len(found) == 0 && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return found, nil
}

func filterOut(leases []string, done map[string]bool) []string {
	var rest []string
	for _, l := range leases {
		if !done[l] {
			rest = append(rest, l)
		}
	}
	return rest
}

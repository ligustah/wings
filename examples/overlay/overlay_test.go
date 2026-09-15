package overlay

import (
	"context"
	"testing"

	"github.com/ligustah/wings"
)

type fakeProvisioner struct {
	got []string
}

func (f *fakeProvisioner) Provision(ctx context.Context, leases []string) ([]wings.Machine, error) {
	f.got = append(f.got, leases...)
	return make([]wings.Machine, len(leases)), nil
}

func TestCompositeSplitsLeasesRoundRobin(t *testing.T) {
	a, b := &fakeProvisioner{}, &fakeProvisioner{}
	c := &composite{provs: []wings.Provisioner{a, b}}

	machines, err := c.Provision(context.Background(), []string{"l0", "l1", "l2", "l3", "l4"})
	if err != nil {
		t.Fatal(err)
	}
	if len(machines) != 5 {
		t.Fatalf("got %d machines, want 5", len(machines))
	}
	if want := []string{"l0", "l2", "l4"}; !equal(a.got, want) {
		t.Errorf("prov a got %v, want %v", a.got, want)
	}
	if want := []string{"l1", "l3"}; !equal(b.got, want) {
		t.Errorf("prov b got %v, want %v", b.got, want)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

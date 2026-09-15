package wings

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

type stubMachine struct {
	id     string
	closed *bool
}

func (m stubMachine) ID() string { return m.id }
func (m stubMachine) Upload(context.Context, io.Reader, int64, string) error {
	return nil
}
func (m stubMachine) Start(context.Context, string, map[string]string) error { return nil }
func (m stubMachine) Forward(context.Context, int) (string, error)           { return "", nil }
func (m stubMachine) Close(context.Context) error {
	if m.closed != nil {
		*m.closed = true
	}
	return nil
}

type stubProvisioner struct {
	name       string
	got        []string
	fail       bool
	reattaches []string
	noReattach bool
}

func (p *stubProvisioner) Provision(_ context.Context, leases []string) ([]Machine, error) {
	p.got = append(p.got, leases...)
	if p.fail {
		return nil, errors.New(p.name + " down")
	}
	machines := make([]Machine, len(leases))
	for i, l := range leases {
		machines[i] = stubMachine{id: l}
	}
	return machines, nil
}

func (p *stubProvisioner) Reattach(_ context.Context, leases []string) ([]Machine, error) {
	var machines []Machine
	for _, l := range leases {
		if slices.Contains(p.reattaches, l) {
			machines = append(machines, stubMachine{id: l})
		}
	}
	return machines, nil
}

func ids(machines []Machine) []string {
	out := make([]string, len(machines))
	for i, m := range machines {
		out[i] = m.ID()
	}
	slices.Sort(out)
	return out
}

func TestChainFillsInPriorityOrderUpToCap(t *testing.T) {
	a := &stubProvisioner{name: "a"}
	b := &stubProvisioner{name: "b"}
	c := Provisioners(
		ProvisionerSpec{Provisioner: a, Cap: 2},
		ProvisionerSpec{Provisioner: b},
	)

	machines, err := c.Provision(context.Background(), []string{"l0", "l1", "l2", "l3"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"l0", "l1"}; !slices.Equal(a.got, want) {
		t.Errorf("a got %v, want %v", a.got, want)
	}
	if want := []string{"l2", "l3"}; !slices.Equal(b.got, want) {
		t.Errorf("b got %v, want %v", b.got, want)
	}
	if want := []string{"l0", "l1", "l2", "l3"}; !slices.Equal(ids(machines), want) {
		t.Errorf("machines %v, want %v", ids(machines), want)
	}
}

func TestChainSpillsAFailedProvisionerToTheNext(t *testing.T) {
	a := &stubProvisioner{name: "a", fail: true}
	b := &stubProvisioner{name: "b"}
	c := Provisioners(
		ProvisionerSpec{Provisioner: a, Cap: 2},
		ProvisionerSpec{Provisioner: b},
	)

	machines, err := c.Provision(context.Background(), []string{"l0", "l1", "l2"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"l0", "l1", "l2"}; !slices.Equal(b.got, want) {
		t.Errorf("b got %v, want %v (the whole run should spill)", b.got, want)
	}
	if want := []string{"l0", "l1", "l2"}; !slices.Equal(ids(machines), want) {
		t.Errorf("machines %v, want %v", ids(machines), want)
	}
}

func TestChainErrorsAndClosesWhenLeasesRemain(t *testing.T) {
	var closed1, closed2 bool
	supplyOne := &stubProvisioner{name: "one"}
	c := &chain{specs: []ProvisionerSpec{
		{Provisioner: provFunc(func(leases []string) ([]Machine, error) {
			_ = supplyOne
			return []Machine{
				stubMachine{id: leases[0], closed: &closed1},
				stubMachine{id: leases[1], closed: &closed2},
			}, nil
		}), Cap: 2},
	}}

	_, err := c.Provision(context.Background(), []string{"l0", "l1", "l2"})
	if err == nil {
		t.Fatal("want error when the chain cannot place every lease")
	}
	if !closed1 || !closed2 {
		t.Error("machines provisioned before the shortfall must be closed")
	}
}

func TestChainErrorNamesWhyEachProvisionerFailed(t *testing.T) {
	a := &stubProvisioner{name: "a", fail: true}
	b := &stubProvisioner{name: "b", fail: true}
	c := Provisioners(
		ProvisionerSpec{Provisioner: a},
		ProvisionerSpec{Provisioner: b},
	)

	_, err := c.Provision(context.Background(), []string{"l0"})
	if err == nil {
		t.Fatal("want error when every provisioner fails")
	}
	if msg := err.Error(); !strings.Contains(msg, "a down") || !strings.Contains(msg, "b down") {
		t.Errorf("error should carry each provisioner's cause, got: %s", msg)
	}
}

func TestChainReattachMergesEachProvisionersOwn(t *testing.T) {
	a := &stubProvisioner{name: "a", reattaches: []string{"l0"}}
	b := &stubProvisioner{name: "b", reattaches: []string{"l2"}}
	c := Provisioners(
		ProvisionerSpec{Provisioner: a},
		ProvisionerSpec{Provisioner: b},
	).(Reattacher)

	found, err := c.Reattach(context.Background(), []string{"l0", "l1", "l2"})
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"l0", "l2"}; !slices.Equal(ids(found), want) {
		t.Errorf("reattached %v, want %v", ids(found), want)
	}
}

type provFunc func(leases []string) ([]Machine, error)

func (f provFunc) Provision(_ context.Context, leases []string) ([]Machine, error) {
	return f(leases)
}

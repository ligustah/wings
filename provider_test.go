package wings

import (
	"flag"
	"testing"
)

type fakeProvider struct {
	name string
	err  error
}

func (p fakeProvider) Name() string        { return p.name }
func (p fakeProvider) Flags(*flag.FlagSet) {}
func (p fakeProvider) New() (Provisioner, error) {
	return &stubProvisioner{name: p.name}, p.err
}

func registerFake(t *testing.T, name string) {
	t.Helper()
	RegisterProvider(fakeProvider{name: name})
	t.Cleanup(func() {
		providerMu.Lock()
		delete(providers, name)
		providerMu.Unlock()
	})
}

func TestResolveProviderSingle(t *testing.T) {
	registerFake(t, "fakeone")
	prov, err := resolveProvider("fakeone")
	if err != nil {
		t.Fatal(err)
	}
	if _, chained := prov.(*chain); chained {
		t.Error("a single provider must not be wrapped in a chain")
	}
}

func TestResolveProviderListBuildsChainInOrder(t *testing.T) {
	registerFake(t, "fakea")
	registerFake(t, "fakeb")

	prov, err := resolveProvider("fakea:3,fakeb")
	if err != nil {
		t.Fatal(err)
	}
	c, ok := prov.(*chain)
	if !ok {
		t.Fatalf("got %T, want *chain", prov)
	}
	if len(c.specs) != 2 {
		t.Fatalf("got %d specs, want 2", len(c.specs))
	}
	if c.specs[0].Cap != 3 {
		t.Errorf("first cap = %d, want 3", c.specs[0].Cap)
	}
	if got := c.specs[0].Provisioner.(*stubProvisioner).name; got != "fakea" {
		t.Errorf("first provider = %q, want fakea", got)
	}
	if c.specs[1].Cap != 0 {
		t.Errorf("second cap = %d, want 0 (uncapped)", c.specs[1].Cap)
	}
}

func TestResolveProviderRejectsBadCap(t *testing.T) {
	registerFake(t, "fakec")
	if _, err := resolveProvider("fakec:nope"); err == nil {
		t.Error("want an error for a non-integer cap")
	}
}

func TestResolveProviderUnknownName(t *testing.T) {
	registerFake(t, "faked")
	if _, err := resolveProvider("faked,ghost"); err == nil {
		t.Error("want an error naming the unknown provider")
	}
}

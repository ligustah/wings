package wings

import (
	"flag"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
)

// Provider builds a [Provisioner] from command-line flags.
//
// It exists so that WHERE work runs is chosen when a program is run, not when
// it is written. A program's own code names no cloud, imports no SDK and holds
// no credentials; it defines work and a [Cluster.Map] over it, and `-target
// remote -provider gcp` is what decides the rest.
//
// Register one from a package's init, the way a database driver does, and the
// generated coordinator imports that package for effect:
//
//	func init() { wings.RegisterProvider(&provider{}) }
//
// A provider's flags are exposed with its name as a prefix — a Flags method
// registering "project" becomes -gcp.project — so two providers can be linked
// in at once without colliding.
type Provider interface {
	// Name is how -provider selects this one. Lowercase, no dots.
	Name() string

	// Flags registers the provider's options on fs. The set is its own, and
	// wings prefixes every flag with Name before parsing.
	Flags(fs *flag.FlagSet)

	// New builds the provisioner once the flags are parsed. Return an error
	// naming the missing flag when the configuration is incomplete; it is
	// reported to the user as-is.
	New() (Provisioner, error)
}

var (
	providerMu sync.RWMutex
	providers  = map[string]Provider{}
)

// RegisterProvider makes p selectable with -provider.
//
// Panics on a duplicate or an empty name, both of which are programming errors
// visible at init.
func RegisterProvider(p Provider) {
	name := p.Name()
	if name == "" {
		panic("wings: a provider must have a name")
	}
	providerMu.Lock()
	defer providerMu.Unlock()
	if _, dup := providers[name]; dup {
		panic(fmt.Sprintf("wings: provider %q is already registered", name))
	}
	providers[name] = p
}

// ProviderNames lists the registered providers, sorted.
func ProviderNames() []string {
	providerMu.RLock()
	defer providerMu.RUnlock()
	names := make([]string, 0, len(providers))
	for n := range providers {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// providerUsage describes -provider using what is actually linked in, so the
// help text of a binary built without any cloud does not advertise one.
func providerUsage() string {
	names := ProviderNames()
	if len(names) == 0 {
		return "which cloud to provision from (none linked into this binary)"
	}
	return "which cloud to provision from for -target remote: " + strings.Join(names, " | ")
}

func lookupProvider(name string) (Provider, bool) {
	providerMu.RLock()
	defer providerMu.RUnlock()
	p, ok := providers[name]
	return p, ok
}

// registerProviderFlags exposes every registered provider's flags on fs,
// prefixed with the provider's name.
//
// All of them, not just the selected one: flags have to be registered before
// parsing, and which provider is selected is itself a parsed flag. Sharing the
// flag.Value means parsing -gcp.project writes straight into the provider's own
// field, so nothing has to be copied back afterwards.
func registerProviderFlags(fs *flag.FlagSet) {
	providerMu.RLock()
	defer providerMu.RUnlock()

	for _, name := range sortedKeys(providers) {
		p := providers[name]
		sub := flag.NewFlagSet(name, flag.ContinueOnError)
		p.Flags(sub)
		sub.VisitAll(func(f *flag.Flag) {
			fs.Var(f.Value, name+"."+f.Name, f.Usage)
		})
	}
}

// resolveProvider picks the provisioner for -target remote.
func resolveProvider(name string) (Provisioner, error) {
	names := ProviderNames()

	switch {
	case len(names) == 0:
		return nil, fmt.Errorf("-target=remote needs a provider and none is linked in.\n" +
			"Build with `wings build -providers gcp`, or export " +
			"`func Provisioner() wings.Provisioner` from your package for a cloud wings does not ship")
	case name == "" && len(names) == 1:
		// Unambiguous: one provider linked in, so naming it would be ceremony.
		name = names[0]
	case name == "":
		return nil, fmt.Errorf("-provider is required: %s are linked in", strings.Join(names, ", "))
	}

	p, ok := lookupProvider(name)
	if !ok {
		return nil, fmt.Errorf("unknown -provider %q; linked in: %s", name, strings.Join(names, ", "))
	}
	prov, err := p.New()
	if err != nil {
		return nil, fmt.Errorf("provider %s: %w", name, err)
	}
	return prov, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

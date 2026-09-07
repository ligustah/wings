package wings

import (
	"flag"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
)

// Provider builds a [Provisioner] from command-line flags, so a program names no
// cloud and `-target remote -provider gcp` decides at runtime. Register one from
// init like a database driver:
//
//	func init() { wings.RegisterProvider(&provider{}) }
//
// Each provider's flags are prefixed with its name (a "project" flag becomes
// -gcp.project), so several can be linked in at once.
type Provider interface {
	// Name is how -provider selects this one. Lowercase, no dots.
	Name() string

	// Flags registers the provider's options on fs; wings prefixes each with Name.
	Flags(fs *flag.FlagSet)

	// New builds the provisioner once flags are parsed, returning an error naming
	// any missing flag.
	New() (Provisioner, error)
}

var (
	providerMu sync.RWMutex
	providers  = map[string]Provider{}
)

// RegisterProvider makes p selectable with -provider. Call it from init. Panics
// on a duplicate or empty name.
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

// registerProviderFlags exposes every provider's flags on fs, prefixed with its
// name — all of them, since which provider is selected is itself a parsed flag.
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

func resolveProvider(name string) (Provisioner, error) {
	names := ProviderNames()

	switch {
	case len(names) == 0:
		return nil, fmt.Errorf("-target=remote needs a provider and none is linked in.\n" +
			"Build with `wings build -providers gcp`, or export " +
			"`func Provisioner() wings.Provisioner` from your package for a cloud wings does not ship")
	case name == "" && len(names) == 1:
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

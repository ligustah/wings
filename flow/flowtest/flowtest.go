// Package flowtest is a testing kit for flow workflows: a virtual clock that
// runs long waits instantly, and a harness that runs a workflow and checks it
// replays deterministically.
//
//	h := flowtest.New(t)
//	h.Run("nightly", nightly)  // sleeps and suspensions return at once
//	h.Replay("nightly", nightly)
package flowtest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// Clock is a virtual clock that advances only as a run waits — a sleep, a
// suspension, a retry backoff — so a workflow with long waits runs at once and
// Now reflects the elapsed virtual time. It satisfies [flow.Clock].
type Clock struct {
	mu  sync.Mutex
	now time.Time
}

// NewClock returns a virtual clock started at start.
func NewClock(start time.Time) *Clock { return &Clock{now: start} }

// Now returns the current virtual time.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After advances the clock by d and fires at once, so a wait for d returns
// immediately with virtual time moved forward.
func (c *Clock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	if d > 0 {
		c.now = c.now.Add(d)
	}
	now := c.now
	c.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- now
	return ch
}

// Advance moves virtual time forward by d, without waiting on anything.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// Harness runs workflows for a test with an in-memory [flow.Store] and a virtual
// [Clock].
type Harness struct {
	Store *flow.MemStore
	Clock *Clock
	tb    testing.TB
}

// New returns a harness bound to tb, with a fresh store and a clock started at
// the Unix epoch.
func New(tb testing.TB) *Harness {
	tb.Helper()
	return &Harness{Store: flow.NewMemStore(), Clock: NewClock(time.Unix(0, 0).UTC()), tb: tb}
}

// Options are the run options the harness supplies — its store and clock —
// followed by extra. Pass them to [flow.Run] for what the harness has no helper
// for.
func (h *Harness) Options(extra ...flow.RunOption) []flow.RunOption {
	return append([]flow.RunOption{flow.WithStore(h.Store), flow.WithClock(h.Clock)}, extra...)
}

// Run runs body to completion, failing the test if it returns an error.
func (h *Harness) Run(name string, body func(flow.Context) error, extra ...flow.RunOption) {
	h.tb.Helper()
	if err := flow.Run(context.Background(), name, body, h.Options(extra...)...); err != nil {
		h.tb.Fatalf("run %q: %v", name, err)
	}
}

// Replay re-runs a finished run's body against its recorded history and fails
// the test if it diverges — the determinism check for an author. Run the same
// name first.
func (h *Harness) Replay(name string, body func(flow.Context) error, extra ...flow.RunOption) {
	h.tb.Helper()
	if err := flow.Replay(context.Background(), name, body, h.Options(extra...)...); err != nil {
		h.tb.Fatalf("run %q did not replay deterministically: %v", name, err)
	}
}

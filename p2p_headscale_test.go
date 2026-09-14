package wings

import "testing"

// TestP2POverlayClusterRunsWork drives the whole hosted-overlay path: a p2p
// coordinator with Config.P2P.Overlay set runs the control plane in a child,
// enrols its own node onto the tailnet, and hands its worker child the same
// credentials, which brings its node up and joins over the overlay. It forces
// all peer traffic over the embedded DERP relay (TS_DEBUG_ALWAYS_USE_DERP), so
// work that completes proves the cluster formed and replicated across processes
// entirely through the coordinator's own TLS control plane and DERP relay — the
// path two peers behind separate NATs would take, without direct UDP.
func TestP2POverlayClusterRunsWork(t *testing.T) {
	if testing.Short() {
		t.Skip("hosts a control plane child and enrols the coordinator and a worker over the overlay")
	}
	// Both the coordinator's in-process node and the worker child (which inherits
	// this env) drop direct UDP and relay through the embedded DERP region.
	t.Setenv("TS_DEBUG_ALWAYS_USE_DERP", "1")
	addr, err := freeLoopbackAddr()
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	c := start(t, Config{
		Target:      LocalProcess(),
		Workers:     1,
		Concurrency: 1,
		P2P:         &P2P{ReplicationFactor: 2, Overlay: addr},
		Logger:      quietP2PLogger(),
	})

	got, err := p2pSquare(c.Bind(t.Context()), 12)
	if err != nil {
		t.Fatalf("call work over overlay: %v", err)
	}
	if got != 144 {
		t.Fatalf("p2pSquare(12) = %d, want 144", got)
	}
}

package wings

import "testing"

// TestP2POverlayClusterRunsWork drives the whole hosted-overlay path: a p2p
// coordinator with Config.P2P.Overlay set runs the control plane in a child,
// enrols its own node onto the tailnet, and hands its worker child the same
// credentials, which brings its node up and joins over the overlay. Work that
// completes proves the cluster formed and replicated across processes entirely
// over the tailnet the coordinator hosts, TLS control plane and embedded DERP
// included.
func TestP2POverlayClusterRunsWork(t *testing.T) {
	if testing.Short() {
		t.Skip("hosts a control plane child and enrols the coordinator and a worker over the overlay")
	}
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

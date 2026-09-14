package wings

import (
	"context"
	"testing"
	"time"
)

// TestP2PHostedHeadscaleEnrollsNode brings the embedded control plane up the way
// a p2p coordinator does — as a child process running the real Serve, not the
// test-only in-process hooks — and enrols a userspace node against the credentials
// it prints back. It proves the self-exec hosting path end to end: the child
// serves, mints a key, and a tsnet node reaches it and gets a tailnet address.
func TestP2PHostedHeadscaleEnrollsNode(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a headscale child process and a userspace tailnet node")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	addr, err := freeLoopbackAddr()
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	hs, err := startHostedHeadscale(ctx, t.TempDir(), addr)
	if err != nil {
		t.Fatalf("host headscale: %v", err)
	}
	defer hs.stop()
	if hs.control == "" || hs.authKey == "" {
		t.Fatalf("child returned empty credentials: %+v", hs)
	}

	node := tsNode(t, ctx, hs.control, hs.authKey, "probe")
	deadline := time.Now().Add(60 * time.Second)
	for {
		if ip4, _ := node.TailscaleIPs(); ip4.IsValid() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("node never got a tailnet address from the hosted control plane")
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// TestP2POverlayClusterRunsWork drives the whole hosted-overlay path: a p2p
// coordinator with Config.P2P.Overlay set runs the control plane in a child,
// enrols its own node onto the tailnet, and hands its worker child the same
// credentials, which brings its node up and joins over the overlay. Work that
// completes proves the cluster formed and replicated across processes entirely
// over the tailnet the coordinator hosts.
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

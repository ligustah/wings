package wings

import (
	"os"
	"testing"
)

// TestP2POverlayClusterRunsWork drives the whole hosted-overlay path against a
// real control plane: a p2p coordinator enrols its own node onto the tailnet and
// hands its worker child the same credentials, which brings its node up and joins
// over the overlay. Opt-in — set HEADSCALE_URL and HEADSCALE_PRE_AUTH_KEY (the
// same names the CLI reads from .env) — since it needs a reachable control plane.
func TestP2POverlayClusterRunsWork(t *testing.T) {
	control := os.Getenv(envHeadscaleURL)
	authKey := os.Getenv(envHeadscaleAuthKey)
	if control == "" || authKey == "" {
		t.Skipf("set %s and %s to run the overlay end-to-end", envHeadscaleURL, envHeadscaleAuthKey)
	}
	c := start(t, Config{
		Target:      LocalProcess(),
		Workers:     1,
		Concurrency: 1,
		P2P:         &P2P{ReplicationFactor: 2, OverlayControl: control, OverlayAuthKey: authKey},
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

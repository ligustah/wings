package wings

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"

	hscontrol "github.com/juanfont/headscale/hscontrol"
	"github.com/juanfont/headscale/hscontrol/types"
	"tailscale.com/tailcfg"
	"tailscale.com/tsnet"
)

// embedHeadscale stands up an in-process Headscale control server on a real
// loopback listener, so tsnet nodes reach it at a normal http:// URL. It mirrors
// Headscale's own servertest bring-up: a minimal SQLite config, a placeholder
// DERP map so MapResponse generation works, and the batcher and ephemeral GC
// started through the test-only hooks (the only way in without the full Serve).
func embedHeadscale(t *testing.T) (app *hscontrol.Headscale, url string) {
	t.Helper()
	dir := t.TempDir()
	v4 := netip.MustParsePrefix("100.64.0.0/10")
	v6 := netip.MustParsePrefix("fd7a:115c:a1e0::/48")
	cfg := types.Config{
		ServerURL:           "http://localhost:0",
		NoisePrivateKeyPath: dir + "/noise_private.key",
		Node:                types.NodeConfig{Ephemeral: types.EphemeralConfig{InactivityTimeout: 30 * time.Second}},
		PrefixV4:            &v4,
		PrefixV6:            &v6,
		IPAllocation:        types.IPAllocationStrategySequential,
		Database:            types.DatabaseConfig{Type: "sqlite3", Sqlite: types.SqliteConfig{Path: dir + "/headscale.db"}},
		Policy:              types.PolicyConfig{Mode: types.PolicyModeDB},
		Taildrop:            types.TaildropConfig{Enabled: true},
		Tuning: types.Tuning{
			BatchChangeDelay:               50 * time.Millisecond,
			BatcherWorkers:                 1,
			NodeMapSessionBufferedChanSize: 30,
		},
	}
	var err error
	if app, err = hscontrol.NewHeadscale(&cfg); err != nil {
		t.Fatalf("NewHeadscale: %v", err)
	}
	app.GetState().SetDERPMap(&tailcfg.DERPMap{Regions: map[int]*tailcfg.DERPRegion{
		900: {RegionID: 900, RegionCode: "test", RegionName: "Test", Nodes: []*tailcfg.DERPNode{
			{Name: "test0", RegionID: 900, HostName: "127.0.0.1", IPv4: "127.0.0.1", DERPPort: -1},
		}},
	}})
	app.StartBatcherForTest(t)
	app.StartEphemeralGCForTest(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("control listen: %v", err)
	}
	srv := &http.Server{Handler: app.HTTPHandler(), ReadHeaderTimeout: 10 * time.Second}
	go srv.Serve(ln) //nolint:errcheck // returns on Close
	// Close the DB before the temp dir is removed: Windows will not unlink the
	// SQLite file while it is open.
	t.Cleanup(func() { srv.Close(); ln.Close(); app.GetState().Close() })

	url = "http://" + ln.Addr().String()
	app.SetServerURLForTest(t, url)
	return app, url
}

// authKey mints a reusable pre-auth key under a fresh user, for zero-touch tsnet
// enrollment.
func authKey(t *testing.T, app *hscontrol.Headscale, user string) string {
	t.Helper()
	st := app.GetState()
	u, _, err := st.CreateUser(types.User{Name: user})
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	uid := types.UserID(u.ID)
	pak, err := st.CreatePreAuthKey(&uid, true, false, nil, nil)
	if err != nil {
		t.Fatalf("CreatePreAuthKey: %v", err)
	}
	return pak.Key
}

// tsNode brings up a userspace tsnet node enrolled against the embedded control
// server, and returns it up and addressable.
func tsNode(t *testing.T, ctx context.Context, url, key, host string) *tsnet.Server {
	t.Helper()
	s := &tsnet.Server{
		Dir:        t.TempDir(),
		Hostname:   host,
		ControlURL: url,
		AuthKey:    key,
		Ephemeral:  true,
		Logf:       func(string, ...any) {},
		UserLogf:   func(string, ...any) {},
	}
	t.Cleanup(func() { s.Close() })
	if _, err := s.Up(ctx); err != nil {
		t.Fatalf("tsnet %s up: %v", host, err)
	}
	return s
}

// TestP2POverlayCarriesTraffic proves the Phase 4 connectivity premise in
// process: an embedded Headscale control server enrolls two userspace tsnet
// nodes, and a connection dialed from one to the other over the overlay carries
// bytes — the network a p2p node's listener seam plugs into.
func TestP2POverlayCarriesTraffic(t *testing.T) {
	if testing.Short() {
		t.Skip("brings up an embedded control server and two userspace tailnet nodes")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	app, url := embedHeadscale(t)
	key := authKey(t, app, "wings")

	server := tsNode(t, ctx, url, key, "server")
	client := tsNode(t, ctx, url, key, "client")

	ln, err := server.Listen("tcp", ":9000")
	if err != nil {
		t.Fatalf("overlay listen: %v", err)
	}
	defer ln.Close()
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			return
		}
		defer conn.Close()
		io.Copy(conn, conn) //nolint:errcheck // echo until closed
	}()

	ip4, _ := server.TailscaleIPs()
	if !ip4.IsValid() {
		t.Fatal("server node has no tailnet address")
	}

	var conn net.Conn
	deadline := time.Now().Add(60 * time.Second)
	for {
		conn, err = client.Dial(ctx, "tcp", fmt.Sprintf("%s:9000", ip4))
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial over overlay: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, 4)
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf) != "ping" {
		t.Fatalf("echo = %q, want %q", buf, "ping")
	}
}

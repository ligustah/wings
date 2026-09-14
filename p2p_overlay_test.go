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
	"github.com/ligustah/durable_streams/dsclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// TestP2PBrokerPlacesOverOverlay runs a real wings p2p broker whose endpoint is
// served on the overlay through the listener seam, with the peer dialer routed
// over the same tailnet. Placing a stream requires the controller to reach the
// broker, so a placement that completes proves the peer/replication plane rides
// tsnet — the path a clustered p2p deployment depends on.
//
// The client leader-routing plane is a separate matter: the routing client dials
// re-resolved leaders through the broker's clusterPlacement.clientAt, which drops
// the caller's dial options, so a client cannot yet reach a leader over the
// overlay. That needs an upstream dial-options seam, filed with the DS agent.
func TestP2PBrokerPlacesOverOverlay(t *testing.T) {
	if testing.Short() {
		t.Skip("brings up an embedded control server, a tailnet node and a broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	app, url := embedHeadscale(t)
	key := authKey(t, app, "wings")
	brokerTS := tsNode(t, ctx, url, key, "broker")

	// Advertise the broker's concrete tailnet address: a wildcard bind would
	// advertise a host-less address the controller cannot dial back.
	bip, _ := brokerTS.TailscaleIPs()
	if !bip.IsValid() {
		t.Fatal("broker node has no tailnet address")
	}

	n, err := startP2PNode(ctx, p2pNodeConfig{
		id:                "coordinator",
		dir:               t.TempDir(),
		peerAddr:          fmt.Sprintf("%s:9100", bip),
		bootstrap:         true,
		replicationFactor: 1,
		reconcile:         150 * time.Millisecond,
		log:               quietP2PLogger(),
		listen: func(lctx context.Context, addr string) (net.Listener, error) {
			return brokerTS.Listen("tcp", addr)
		},
		peerDialOptions: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(dctx context.Context, addr string) (net.Conn, error) {
				return brokerTS.Dial(dctx, "tcp", addr)
			}),
		},
	})
	if err != nil {
		t.Fatalf("start broker over overlay: %v", err)
	}
	defer n.close()
	if err := n.awaitReady(ctx); err != nil {
		t.Fatalf("await ready: %v", err)
	}

	const stream = "p2p.overlay.broker"
	if err := n.client.CreateStream(ctx, stream, &dsclient.StreamConfig{Partitions: 1}); err != nil {
		t.Fatalf("place stream over overlay: %v", err)
	}
	s, err := n.client.OpenStream[string](stream)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := s.Append(ctx, []string{"placed"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	recs, err := s.Read(ctx, 0, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 1 || recs[0].Record != "placed" {
		t.Fatalf("read %+v, want one record %q", recs, "placed")
	}
}

// TestP2PWorkerJoinsOverOverlay drives the real worker bring-up: a coordinator
// on the overlay, then startWorkerP2PNode reading the overlay env vars, so the
// worker enrolls its own tsnet node and joins the coordinator across the tailnet
// through the peer dialer. A replicated stream then places on both, proving the
// worker joined and the controller reached both nodes over the overlay.
func TestP2PWorkerJoinsOverOverlay(t *testing.T) {
	if testing.Short() {
		t.Skip("brings up an embedded control server, two tailnet nodes and a cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	app, url := embedHeadscale(t)
	key := authKey(t, app, "wings")

	coordTS := tsNode(t, ctx, url, key, "coordinator")
	w, err := overlayHooks(coordTS)
	if err != nil {
		t.Fatalf("coordinator overlay hooks: %v", err)
	}
	coord, err := startP2PNode(ctx, p2pNodeConfig{
		id:                "coordinator",
		dir:               t.TempDir(),
		peerAddr:          w.peerAddr,
		bootstrap:         true,
		replicationFactor: 2,
		reconcile:         150 * time.Millisecond,
		log:               quietP2PLogger(),
		listen:            w.listen,
		peerDialOptions:   w.peerDial,
		clientDialOptions: w.clientDial,
		raftListen:        w.raftListen,
		raftDial:          w.raftDial,
		raftAddr:          w.raftAddr,
	})
	if err != nil {
		t.Fatalf("start coordinator over overlay: %v", err)
	}
	defer coord.close()
	if err := coord.awaitReady(ctx); err != nil {
		t.Fatalf("coordinator await ready: %v", err)
	}

	// The worker child reads these to bring its own overlay node up and join.
	t.Setenv(envDir, t.TempDir())
	t.Setenv(envP2PJoin, coord.peerAddr)
	t.Setenv(envP2PRF, "2")
	t.Setenv(envP2POverlayControl, url)
	t.Setenv(envP2POverlayAuthKey, key)

	worker, err := startWorkerP2PNode(ctx, "worker", quietP2PLogger())
	if err != nil {
		t.Fatalf("worker join over overlay: %v", err)
	}
	defer worker.close()

	const stream = "p2p.overlay.replicated"
	if err := coord.client.CreateStream(ctx, stream, &dsclient.StreamConfig{Partitions: 1}); err != nil {
		t.Fatalf("create replicated stream: %v", err)
	}

	// Both nodes must come to see the stream placed: that the controller reached
	// each over the overlay to place a second replica.
	deadline := time.Now().Add(30 * time.Second)
	for {
		_, coordOK := coord.leaderOf(stream)
		_, workerOK := worker.leaderOf(stream)
		if coordOK && workerOK {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream not placed on both nodes over the overlay (coordinator=%v worker=%v)", coordOK, workerOK)
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Written on the coordinator, read back through the worker's routing client:
	// data crosses the cluster over the overlay.
	cs, err := coord.client.OpenStream[string](stream)
	if err != nil {
		t.Fatalf("coordinator open: %v", err)
	}
	if _, err := cs.Append(ctx, []string{"replicated"}); err != nil {
		t.Fatalf("append on coordinator: %v", err)
	}
	ws, err := worker.client.OpenStream[string](stream)
	if err != nil {
		t.Fatalf("worker open: %v", err)
	}
	rdeadline := time.Now().Add(20 * time.Second)
	for {
		recs, rerr := ws.Read(ctx, 0, 1)
		if rerr == nil && len(recs) == 1 && recs[0].Record == "replicated" {
			break
		}
		if time.Now().After(rdeadline) {
			t.Fatalf("worker did not read the coordinator's write over the overlay: recs=%v err=%v", recs, rerr)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

package wings

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"time"

	"github.com/ligustah/durable_streams/dsclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"tailscale.com/tsnet"
)

// liveOverlay returns the hosted control plane's URL and pre-auth key from the
// environment (the same names the CLI reads from .env), skipping the test when
// they are unset: these tests join a real tailnet, so they are opt-in.
func liveOverlay(t *testing.T) (url, key string) {
	t.Helper()
	url, key = os.Getenv(envHeadscaleURL), os.Getenv(envHeadscaleAuthKey)
	if url == "" || key == "" {
		t.Skipf("set %s and %s to run the overlay tests against a hosted control plane", envHeadscaleURL, envHeadscaleAuthKey)
	}
	return url, key
}

// tsNode brings up a userspace tsnet node enrolled against the control server at
// url, and returns it up and addressable.
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

// TestP2POverlayCarriesTraffic proves the connectivity premise: the control
// plane enrolls two userspace tsnet nodes, and a connection dialed from one to
// the other over the overlay carries bytes — the network a p2p node's listener
// seam plugs into.
func TestP2POverlayCarriesTraffic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	url, key := liveOverlay(t)

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
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	url, key := liveOverlay(t)
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
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()

	url, key := liveOverlay(t)

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

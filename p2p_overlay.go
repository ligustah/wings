package wings

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ligustah/durable_streams/broker/cluster"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"tailscale.com/tsnet"
)

// startOverlayNode brings a userspace Tailscale node up and enrolled in the
// control server at control using the pre-auth key, its state under dir. The
// node is reachable only through the overlay, so a broker served on it and the
// peers that dial it cross NATs over the tailnet rather than the host network.
func startOverlayNode(ctx context.Context, dir, control, authKey, host string) (*tsnet.Server, error) {
	quiet := func(string, ...any) {}
	s := &tsnet.Server{
		Dir:        dir,
		Hostname:   host,
		ControlURL: control,
		AuthKey:    authKey,
		Ephemeral:  true,
		Logf:       quiet,
		UserLogf:   quiet,
	}
	if _, err := s.Up(ctx); err != nil {
		_ = s.Close()
		return nil, fmt.Errorf("wings: join overlay as %s: %w", host, err)
	}
	return s, nil
}

// overlayWiring is everything startP2PNode needs to serve and dial over an
// overlay node: the broker and raft listeners bind the tailnet, the peer and
// raft dialers reach it, and the addresses advertise the node's own tailnet
// address (concrete, since a wildcard would advertise a host peers cannot dial
// back). Peer and raft each get their own listener on the node, at their own
// port.
type overlayWiring struct {
	listen     p2pListen
	peerDial   []grpc.DialOption
	peerAddr   string
	raftListen p2pListen
	raftDial   cluster.DialFunc
	raftAddr   string
}

// overlayHooks derives the wiring from an up overlay node.
func overlayHooks(s *tsnet.Server) (overlayWiring, error) {
	ip4, _ := s.TailscaleIPs()
	if !ip4.IsValid() {
		return overlayWiring{}, fmt.Errorf("wings: overlay node has no tailnet address")
	}
	listen := func(_ context.Context, addr string) (net.Listener, error) {
		return s.Listen("tcp", addr)
	}
	addr := fmt.Sprintf("%s:0", ip4)
	return overlayWiring{
		listen: listen,
		peerDial: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(dctx context.Context, a string) (net.Conn, error) {
				return s.Dial(dctx, "tcp", a)
			}),
		},
		peerAddr:   addr,
		raftListen: listen,
		raftDial: func(a string, timeout time.Duration) (net.Conn, error) {
			dctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			return s.Dial(dctx, "tcp", a)
		},
		raftAddr: addr,
	}, nil
}

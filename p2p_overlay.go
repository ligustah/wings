package wings

import (
	"context"
	"fmt"
	"net"

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

// overlayHooks turns an up overlay node into the pieces startP2PNode needs to
// serve and dial over it: a listener that binds the tailnet, dial options that
// dial it, and the node's own tailnet address to advertise (a concrete address,
// since a wildcard would advertise a host peers cannot dial back).
func overlayHooks(s *tsnet.Server) (listen p2pListen, dial []grpc.DialOption, peerAddr string, err error) {
	ip4, _ := s.TailscaleIPs()
	if !ip4.IsValid() {
		return nil, nil, "", fmt.Errorf("wings: overlay node has no tailnet address")
	}
	listen = func(_ context.Context, addr string) (net.Listener, error) {
		return s.Listen("tcp", addr)
	}
	dial = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(dctx context.Context, addr string) (net.Conn, error) {
			return s.Dial(dctx, "tcp", addr)
		}),
	}
	return listen, dial, fmt.Sprintf("%s:0", ip4), nil
}

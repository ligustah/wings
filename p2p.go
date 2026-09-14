package wings

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/ligustah/durable_streams/broker/affinity"
	"github.com/ligustah/durable_streams/broker/cluster"
	"github.com/ligustah/durable_streams/broker/daemon"
	"github.com/ligustah/durable_streams/broker/embed"
	"github.com/ligustah/durable_streams/broker/peer"
	"github.com/ligustah/durable_streams/broker/protos"
	"github.com/ligustah/durable_streams/broker/replica"
	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"
	"google.golang.org/grpc"
	"tailscale.com/tsnet"
)

// defaultReplicationFactor is how many nodes hold a copy of each stream when the
// p2p mode is on and no factor is set.
const defaultReplicationFactor = 3

// defaultP2PReconcile is the control-plane reconcile-and-heartbeat interval when
// a p2p node sets none; the broker session timeout derives from it.
const defaultP2PReconcile = time.Second

// p2pNode is one broker of a p2p cluster: a durable-streams engine plus the
// control plane, replica manager and client that make it hold and serve replicas
// of the cluster's streams. The coordinator runs the bootstrap voter; each data
// node runs an observer that joins it.
type p2pNode struct {
	id      string
	engine  *embed.Engine
	svc     *daemon.Service
	node    *cluster.Node
	manager *replica.Manager
	backend dswire.Backend
	client  *dsclient.Client
	srv     *grpc.Server
	cancel  context.CancelFunc

	// overlay is the node's userspace Tailscale node when it serves over one;
	// nil on the host network. Closed with the node.
	overlay *tsnet.Server

	peerAddr string
	raftAddr string
}

// p2pNodeConfig describes one broker to bring up. Empty addresses pick free
// loopback ports, for an in-process or local cluster.
type p2pNodeConfig struct {
	id  string
	dir string

	peerAddr string // where peers and clients reach this broker
	raftAddr string // where consensus listens

	bootstrap bool   // form a new single-node cluster; exactly one node, exactly once
	observer  bool   // join without a vote — a data node, which the coordinator is not
	join      string // peer address of a member to join through; empty for the bootstrap node

	replicationFactor int
	reconcile         time.Duration
	log               *slog.Logger

	// listen creates the peer+client endpoint's listener; nil binds plain TCP.
	// A node wiring a userspace overlay supplies one that listens on it, so peers
	// behind different NATs reach one another over the same network.
	listen p2pListen

	// peerDialOptions is how the node dials its peers and how the controller
	// reaches it — replication, join, forwarding and placement. A node on an
	// overlay supplies a context dialer that rides it, matching listen.
	peerDialOptions []grpc.DialOption

	// raftListen and raftDial carry consensus over the overlay too; nil binds and
	// dials raft over plain TCP. With raftListen set, raftAddr is advertise-only,
	// resolved from the listener the node actually binds.
	raftListen p2pListen
	raftDial   cluster.DialFunc
}

// p2pListen creates the listener a p2p node serves its peer and client endpoint
// on, given the address to advertise.
type p2pListen func(ctx context.Context, addr string) (net.Listener, error)

// tcpListen is the default p2pListen: a plain TCP bind, for an in-process or
// local cluster with no overlay.
func tcpListen(ctx context.Context, addr string) (net.Listener, error) {
	return (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
}

// startP2PNode assembles one clustered broker following broker/embed's wiring:
// an engine, a served peer+client endpoint, the control plane, the replica
// manager, and a placement-aware client. The bootstrap node forms the cluster;
// others set join to its peer address.
func startP2PNode(ctx context.Context, cfg p2pNodeConfig) (_ *p2pNode, err error) {
	log := cfg.log
	if log == nil {
		log = slog.Default()
	}
	raftAddr := cfg.raftAddr
	if raftAddr == "" {
		if raftAddr, err = freeLoopbackAddr(); err != nil {
			return nil, fmt.Errorf("wings: pick raft address: %w", err)
		}
	}

	engine, err := embed.StartEngine(embed.CoordinatorConfig{Dir: cfg.dir, Logger: streamLogger(log)})
	if err != nil {
		return nil, fmt.Errorf("wings: start p2p engine in %s: %w", cfg.dir, err)
	}
	nodeCtx, cancel := context.WithCancel(ctx)
	n := &p2pNode{id: cfg.id, engine: engine, raftAddr: raftAddr, cancel: cancel}
	defer func() {
		if err != nil {
			n.close()
		}
	}()

	// Report co-write statistics so the controller's rebalancer co-locates
	// streams written together — a run's history and the channel outboxes its
	// threads commit alongside it — onto one node, keeping those writes
	// node-local and a peer holding the data. A non-correctness hint: in-memory,
	// decayed and bounded, and every placement constraint outranks it.
	if n.svc, err = daemon.New(engine.Coordinator, daemon.WithColocationStatistics(affinity.Config{})); err != nil {
		return nil, fmt.Errorf("wings: p2p daemon: %w", err)
	}

	peerAddr := cfg.peerAddr
	if peerAddr == "" {
		peerAddr = "127.0.0.1:0"
	}
	listen := cfg.listen
	if listen == nil {
		listen = tcpListen
	}
	lis, err := listen(nodeCtx, peerAddr)
	if err != nil {
		return nil, fmt.Errorf("wings: p2p listen on %s: %w", peerAddr, err)
	}
	n.peerAddr = lis.Addr().String()
	n.srv = grpc.NewServer(grpc.MaxRecvMsgSize(maxMessage), grpc.MaxSendMsgSize(maxMessage))
	// Both services on one listener: a client handle re-resolved to a remote
	// leader dials the client address, and a peer-only listener answers it
	// "unknown service".
	protos.RegisterDurableStreamsPeerServer(n.srv, daemon.NewPeerService(n.svc))
	protos.RegisterDurableStreamsServer(n.srv, n.svc)
	go func() {
		if serveErr := n.srv.Serve(lis); serveErr != nil && !errors.Is(serveErr, grpc.ErrServerStopped) {
			log.Error("wings: p2p broker server stopped", "err", serveErr)
		}
	}()

	// On an overlay consensus binds and dials the tailnet too; the listener's own
	// address is what peers dial, so raftAddr becomes advertise-only. The control
	// plane closes the listener with the node.
	cp := embed.ControlPlaneConfig{
		Identity:    peer.Identity{ID: peer.ID(cfg.id), PeerAddr: n.peerAddr, ClientAddr: n.peerAddr},
		RaftAddr:    raftAddr,
		DataDir:     cfg.dir,
		Bootstrap:   cfg.bootstrap,
		Coordinator: engine.Coordinator,
		Logger:      streamLogger(log),
	}
	if cfg.raftListen != nil {
		raftLis, lerr := cfg.raftListen(nodeCtx, raftAddr)
		if lerr != nil {
			return nil, fmt.Errorf("wings: p2p raft listen on %s: %w", raftAddr, lerr)
		}
		cp.RaftAddr = raftLis.Addr().String()
		cp.RaftListener = raftLis
		cp.RaftDial = cfg.raftDial
	}
	ident := cp.Identity
	if n.node, err = embed.StartControlPlane(cp); err != nil {
		return nil, err
	}
	if n.node == nil {
		return nil, fmt.Errorf("wings: p2p node %s: no control plane came up", cfg.id)
	}

	reconcile := cfg.reconcile
	if reconcile <= 0 {
		reconcile = defaultP2PReconcile
	}
	if n.manager, err = embed.StartCluster(nodeCtx, embed.ClusterConfig{
		Identity:          ident,
		Dir:               cfg.dir,
		Join:              cfg.join,
		Observer:          cfg.observer,
		ReconcileInterval: reconcile,
		PeerDialOptions:   cfg.peerDialOptions,
		Logger:            streamLogger(log),
	}, n.node, engine, n.svc); err != nil {
		return nil, err
	}
	if n.manager == nil {
		return nil, fmt.Errorf("wings: p2p node %s: replica manager did not start", cfg.id)
	}

	rf := cfg.replicationFactor
	if rf <= 0 {
		rf = defaultReplicationFactor
	}
	if n.backend, err = embed.ClusterClient(engine, n.manager, n.node, embed.ClusterClientConfig{
		Self:              cfg.id,
		ReplicationFactor: int32(rf),
		Reclaimed:         n.svc.HasReclaimed,
	}); err != nil {
		return nil, err
	}
	n.client = dsclient.Wrap(n.backend)
	return n, nil
}

// awaitReady blocks until this node has replayed the consensus log, the cluster
// has a controller, and the node has dropped whatever the cluster says it no
// longer holds — so on a restart its existing streams are served rather than
// refused as unreclaimed. It returns ctx's error if ctx ends first.
func (n *p2pNode) awaitReady(ctx context.Context) error {
	for {
		if n.node.CaughtUp() && n.node.LeaderID() != "" && n.svc.HasReclaimed() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// leaderOf reports the cluster node currently leading a stream's single
// partition, and whether the cluster has placed it at all. A placed but
// leaderless partition (mid-election) reports ok false, since nothing serves it
// yet. Wings streams are single-partition, so partition 0 is the whole stream.
func (n *p2pNode) leaderOf(stream string) (id string, ok bool) {
	leader, _, placed := n.manager.PartitionLeader(stream, 0)
	return leader, placed && leader != ""
}

// close releases the node, mirroring a real shutdown: stop the background loops,
// stop answering, leave the control plane, then close the engine. Idempotent.
func (n *p2pNode) close() {
	if n == nil {
		return
	}
	if n.cancel != nil {
		n.cancel()
	}
	if n.srv != nil {
		n.srv.Stop()
	}
	// The client wraps the engine's backend, which Close also releases; only one may.
	if n.backend != nil {
		_ = n.backend.Close()
	}
	if n.node != nil {
		_ = n.node.Close()
	}
	if n.engine != nil {
		_ = n.engine.Close()
	}
	if n.overlay != nil {
		_ = n.overlay.Close()
	}
}

// startWorkerP2PNode brings up a worker process's own node of the p2p cluster:
// an observer that joins the coordinator at WINGS_P2P_JOIN and holds replicas,
// so the worker's streams live on more than the coordinator. It waits until the
// node has joined and caught up before returning.
func startWorkerP2PNode(ctx context.Context, id string, log *slog.Logger) (*p2pNode, error) {
	dir := os.Getenv(envDir)
	if dir == "" {
		dir = defaultWorkerDataDir
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wings: worker data dir: %w", err)
	}
	rf, _ := strconv.Atoi(os.Getenv(envP2PRF))
	cfg := p2pNodeConfig{
		id:                id,
		dir:               dir,
		observer:          true,
		join:              os.Getenv(envP2PJoin),
		replicationFactor: rf,
		log:               log,
	}

	// On an overlay the worker serves and dials over its own tsnet node rather
	// than the host network, so it reaches a coordinator behind a different NAT.
	var overlay *tsnet.Server
	if control := os.Getenv(envP2POverlayControl); control != "" {
		var err error
		if overlay, err = startOverlayNode(ctx, filepath.Join(dir, "overlay"), control, os.Getenv(envP2POverlayAuthKey), id); err != nil {
			return nil, err
		}
		w, err := overlayHooks(overlay)
		if err != nil {
			overlay.Close()
			return nil, err
		}
		cfg.listen, cfg.peerDialOptions, cfg.peerAddr = w.listen, w.peerDial, w.peerAddr
		cfg.raftListen, cfg.raftDial, cfg.raftAddr = w.raftListen, w.raftDial, w.raftAddr
	}

	n, err := startP2PNode(ctx, cfg)
	if err != nil {
		if overlay != nil {
			overlay.Close()
		}
		return nil, err
	}
	if overlay != nil {
		n.overlay = overlay
	}
	if err := n.awaitReady(ctx); err != nil {
		n.close()
		return nil, err
	}
	return n, nil
}

type nodeAddrs struct {
	Peer string `json:"peer"`
	Raft string `json:"raft"`
}

// coordinatorAddrs returns the coordinator node's stable peer and raft
// addresses, allocating and persisting them under dir on first use so a restart
// binds the same ones: raft recognises a resuming node by the address in its
// persisted configuration, so a node that came back on a fresh port would be a
// stranger to the cluster it was part of.
func coordinatorAddrs(dir string) (peerAddr, raftAddr string, err error) {
	path := filepath.Join(dir, "p2p-coordinator.json")
	if b, err := os.ReadFile(path); err == nil {
		var a nodeAddrs
		if err := json.Unmarshal(b, &a); err == nil && a.Peer != "" && a.Raft != "" {
			return a.Peer, a.Raft, nil
		}
	}
	if peerAddr, err = freeLoopbackAddr(); err != nil {
		return "", "", err
	}
	if raftAddr, err = freeLoopbackAddr(); err != nil {
		return "", "", err
	}
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	b, err := json.Marshal(nodeAddrs{Peer: peerAddr, Raft: raftAddr})
	if err != nil {
		return "", "", err
	}
	if err = os.WriteFile(path, b, 0o644); err != nil {
		return "", "", err
	}
	return peerAddr, raftAddr, nil
}

// clusterControlDir is where a node keeps its control-plane raft state, relative
// to its data directory — matching embed.ControlPlaneConfig's default siting.
const clusterControlDir = ".cluster"

// clusterStateExists reports whether a node's data directory already holds
// control-plane state, so a coordinator resumes its cluster on restart rather
// than bootstrapping a second one.
func clusterStateExists(dir string) bool {
	info, err := os.Stat(filepath.Join(dir, clusterControlDir))
	return err == nil && info.IsDir()
}

// freeLoopbackAddr picks a loopback address the OS reports free. It races an
// immediate rebind — the port is free between probe and bind — so it is only for
// the raft listener, which binds the address itself a moment later.
func freeLoopbackAddr() (string, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := l.Addr().String()
	return addr, l.Close()
}

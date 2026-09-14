package wings

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/wings/flow"
)

var p2pSquare = flow.Define(func(ctx flow.Context, n int) (int, error) { return n * n, nil })

// TestP2PInProcessRunsWork starts a p2p cluster on the in-process target — a
// coordinator bootstrap node and two worker observer nodes — and dispatches work
// to it, so the whole wiring runs end to end over a replicated cluster.
func TestP2PInProcessRunsWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := Start(ctx, Config{
		Target:  InProcess(),
		Workers: 2,
		Dir:     t.TempDir(),
		P2P:     &P2P{ReplicationFactor: 2},
		Logger:  quietP2PLogger(),
	})
	if err != nil {
		t.Fatalf("start p2p cluster: %v", err)
	}
	defer c.Stop(context.Background())

	bctx := c.Bind(ctx)
	got, err := p2pSquare(bctx, 7)
	if err != nil {
		t.Fatalf("call work: %v", err)
	}
	if got != 49 {
		t.Fatalf("p2pSquare(7) = %d, want 49", got)
	}
}

func quietP2PLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestP2PInProcessResumesAcrossRestart shows a p2p cluster brought up a second
// time over the same Dir resumes its control plane rather than forming a new one,
// so it goes on dispatching work.
func TestP2PInProcessResumesAcrossRestart(t *testing.T) {
	ctx := context.Background()
	cfg := Config{
		Target:  InProcess(),
		Workers: 2,
		Dir:     t.TempDir(),
		P2P:     &P2P{ReplicationFactor: 2},
		Logger:  quietP2PLogger(),
	}

	c1, err := Start(ctx, cfg)
	if err != nil {
		t.Fatalf("start 1: %v", err)
	}
	got, err := p2pSquare(c1.Bind(ctx), 6)
	if err != nil {
		t.Fatalf("call 1: %v", err)
	}
	if got != 36 {
		t.Fatalf("p2pSquare(6) = %d, want 36", got)
	}
	if err := c1.Stop(ctx); err != nil {
		t.Fatalf("stop 1: %v", err)
	}

	c2, err := Start(ctx, cfg)
	if err != nil {
		t.Fatalf("start 2 (resume): %v", err)
	}
	defer c2.Stop(ctx)
	got, err = p2pSquare(c2.Bind(ctx), 8)
	if err != nil {
		t.Fatalf("call 2: %v", err)
	}
	if got != 64 {
		t.Fatalf("p2pSquare(8) = %d, want 64", got)
	}
}

// TestP2PSingleNodeRoundTrips brings up one bootstrap node and shows a stream
// created through the cluster catalog takes writes and reads them back.
func TestP2PSingleNodeRoundTrips(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n, err := startP2PNode(ctx, p2pNodeConfig{
		id:                "coordinator",
		dir:               t.TempDir(),
		bootstrap:         true,
		replicationFactor: 1,
		reconcile:         150 * time.Millisecond,
		log:               quietP2PLogger(),
	})
	if err != nil {
		t.Fatalf("start bootstrap node: %v", err)
	}
	defer n.close()

	if err := n.awaitReady(ctx); err != nil {
		t.Fatalf("await ready: %v", err)
	}

	const stream = "p2p.roundtrip"
	if err := n.client.CreateStream(ctx, stream, &dsclient.StreamConfig{Partitions: 1}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	s, err := n.client.OpenStream[string](stream)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	want := []string{"a", "b", "c"}
	if _, err := s.Append(ctx, want); err != nil {
		t.Fatalf("append: %v", err)
	}

	recs, err := s.Read(ctx, 0, len(want))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != len(want) {
		t.Fatalf("read %d records, want %d", len(recs), len(want))
	}
	for i, r := range recs {
		if r.Record != want[i] {
			t.Fatalf("record %d = %q, want %q", i, r.Record, want[i])
		}
	}
}

// TestP2PReplicatesAcrossNodes forms a two-node cluster — a bootstrap voter and
// a data observer — and shows a stream created at replication factor 2 is placed
// on both brokers, so a peer already holds the data.
func TestP2PReplicatesAcrossNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const reconcile = 150 * time.Millisecond
	coord, err := startP2PNode(ctx, p2pNodeConfig{
		id:                "coordinator",
		dir:               t.TempDir(),
		bootstrap:         true,
		replicationFactor: 2,
		reconcile:         reconcile,
		log:               quietP2PLogger(),
	})
	if err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	defer coord.close()

	data, err := startP2PNode(ctx, p2pNodeConfig{
		id:                "data-1",
		dir:               t.TempDir(),
		observer:          true,
		join:              coord.peerAddr,
		replicationFactor: 2,
		reconcile:         reconcile,
		log:               quietP2PLogger(),
	})
	if err != nil {
		t.Fatalf("start data node: %v", err)
	}
	defer data.close()

	if err := coord.awaitReady(ctx); err != nil {
		t.Fatalf("await ready: %v", err)
	}
	// Placement can only put two replicas once both brokers are registered.
	waitFor(t, "both brokers registered", func() bool {
		return len(coord.node.Registry().Unfenced()) == 2
	})

	const stream = "p2p.replicated"
	if err := coord.client.CreateStream(ctx, stream, &dsclient.StreamConfig{Partitions: 1}); err != nil {
		t.Fatalf("create stream: %v", err)
	}

	replicas, _, placed := coord.manager.AssignmentOf(stream, 0)
	if !placed {
		t.Fatalf("stream %q not placed after CreateStream returned", stream)
	}
	if got := distinct(replicas); len(got) != 2 {
		t.Fatalf("stream placed on %v, want 2 distinct replicas", replicas)
	}

	s, err := coord.client.OpenStream[string](stream)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	want := []string{"x", "y"}
	if _, err := s.Append(ctx, want); err != nil {
		t.Fatalf("append: %v", err)
	}
	recs, err := s.Read(ctx, 0, len(want))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != len(want) {
		t.Fatalf("read %d records, want %d", len(recs), len(want))
	}
}

func distinct(ss []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range ss {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

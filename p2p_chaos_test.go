package wings

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ligustah/durable_streams/broker/cluster"
	"github.com/ligustah/durable_streams/dsclient"
)

// bringUpDataNode starts an observer data node joined to a bootstrap node.
func bringUpDataNode(ctx context.Context, t *testing.T, id, join string, rf int, reconcile time.Duration) *p2pNode {
	t.Helper()
	n, err := startP2PNode(ctx, p2pNodeConfig{
		id: id, dir: t.TempDir(), observer: true, join: join,
		replicationFactor: rf, reconcile: reconcile, log: quietP2PLogger(),
	})
	if err != nil {
		t.Fatalf("start %s: %v", id, err)
	}
	return n
}

// replicasOf returns the distinct brokers the controller currently places a
// stream's single partition on.
func replicasOf(observer *p2pNode, stream string) []string {
	reps, _, placed := observer.manager.AssignmentOf(stream, 0)
	if !placed {
		return nil
	}
	return distinct(reps)
}

// liveReplicasOf returns how many of a stream's replicas are on brokers still
// live — the real redundancy, as opposed to the count the replica list claims.
func liveReplicasOf(observer *p2pNode, stream string, live map[string]bool) int {
	n := 0
	for _, r := range replicasOf(observer, stream) {
		if live[r] {
			n++
		}
	}
	return n
}

// TestP2PChaosKillAndRecover records what a local multi-node cluster does when a
// data node is killed and then recovered, on today's default placement.
//
// It surfaces the durability gap the chaos scenario is meant to catch: a HARD
// kill does not self-heal. The killed broker keeps its slot in every replica
// list, so Repair — which tops up only a set shorter than its replication
// factor — never fires, and the affected streams sit at a single live copy with
// no automatic re-replication. Draining the dead broker is what restores the
// replication factor: it removes the dead slot, and the controller tops the
// partition back up onto a live broker.
func TestP2PChaosKillAndRecover(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node chaos experiment")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		rf        = 2
		reconcile = 150 * time.Millisecond
		streams   = 6
	)

	coord, err := startP2PNode(ctx, p2pNodeConfig{
		id: "coordinator", dir: t.TempDir(), bootstrap: true,
		replicationFactor: rf, reconcile: reconcile, log: quietP2PLogger(),
	})
	if err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	defer coord.close()

	data1 := bringUpDataNode(ctx, t, "data-1", coord.peerAddr, rf, reconcile)
	defer data1.close()
	data2 := bringUpDataNode(ctx, t, "data-2", coord.peerAddr, rf, reconcile)
	defer data2.close()

	if err := coord.awaitReady(ctx); err != nil {
		t.Fatalf("await ready: %v", err)
	}
	waitFor(t, "three brokers registered", func() bool {
		return len(coord.node.Registry().Unfenced()) == 3
	})

	names := make([]string, streams)
	for i := range names {
		s := fmt.Sprintf("wings.chaos.%d", i)
		names[i] = s
		if err := coord.client.CreateStream(ctx, s, &dsclient.StreamConfig{Partitions: 1}); err != nil {
			t.Fatalf("create %s: %v", s, err)
		}
		st, err := coord.client.OpenStream[string](s)
		if err != nil {
			t.Fatalf("open %s: %v", s, err)
		}
		if _, err := st.Append(ctx, []string{fmt.Sprintf("v%d", i)}); err != nil {
			t.Fatalf("append %s: %v", s, err)
		}
	}
	for _, s := range names {
		if got := replicasOf(coord, s); len(got) != rf {
			t.Fatalf("stream %s placed on %v, want %d distinct replicas", s, got, rf)
		}
	}

	var hadVictim []string
	for _, s := range names {
		if slices.Contains(replicasOf(coord, s), "data-2") {
			hadVictim = append(hadVictim, s)
		}
	}
	t.Logf("before kill: data-2 holds %d/%d streams", len(hadVictim), streams)
	if len(hadVictim) == 0 {
		t.Skip("no stream landed on data-2; nothing to lose by killing it")
	}

	// Kill data-2 and let the cluster fence it.
	data2.close()
	waitFor(t, "the cluster fences the killed node", func() bool {
		return len(coord.node.Registry().Unfenced()) == 2
	})

	// THE GAP: give the controller ample time, then show the streams that lived on
	// data-2 are NOT re-replicated. The dead broker keeps its slot, so each such
	// stream still names it and has only one live copy.
	live := map[string]bool{"coordinator": true, "data-1": true}
	time.Sleep(30 * reconcile)
	stranded := 0
	for _, s := range hadVictim {
		if slices.Contains(replicasOf(coord, s), "data-2") && liveReplicasOf(coord, s, live) == 1 {
			stranded++
		}
	}
	t.Logf("after kill (no drain): %d/%d ex-data-2 streams stranded at one live copy", stranded, len(hadVictim))
	if stranded != len(hadVictim) {
		t.Fatalf("expected every ex-data-2 stream stranded at one live copy without a drain; got %d/%d", stranded, len(hadVictim))
	}

	// No data lost yet: the surviving copy still serves every record.
	assertNoDataLoss(ctx, t, coord, names)

	// A fresh node joins, as the autoscaler would after a kill.
	data3 := bringUpDataNode(ctx, t, "data-3", coord.peerAddr, rf, reconcile)
	defer data3.close()
	waitFor(t, "the fresh node registers", func() bool {
		return len(coord.node.Registry().Unfenced()) == 3
	})
	time.Sleep(20 * reconcile)
	t.Logf("fresh node holds %d/%d existing streams just by joining (a bare join re-replicates nothing)",
		len(replicaStreams(coord, "data-3", names)), streams)

	// THE REMEDY: drain the dead broker. This removes its slot, and the controller
	// tops each partition back up onto a live broker — restoring the replication
	// factor a bare join did not.
	cat := cluster.NewCatalog(coord.node)
	if _, err := cat.Cordon("data-2", true); err != nil {
		t.Fatalf("cordon data-2: %v", err)
	}
	if _, _, err := cat.Drain("data-2"); err != nil {
		t.Fatalf("drain data-2: %v", err)
	}
	live["data-3"] = true
	recoverStart := time.Now()
	waitFor(t, "every stream back to R=2 on live brokers with the dead node gone", func() bool {
		for _, s := range names {
			reps := replicasOf(coord, s)
			if len(reps) != rf || slices.Contains(reps, "data-2") {
				return false
			}
			if liveReplicasOf(coord, s, live) != rf {
				return false
			}
		}
		return true
	})
	t.Logf("recovery to R=2 after draining the dead node: %s", time.Since(recoverStart).Round(time.Millisecond))
	t.Logf("drained replicas landed on data-3 for %d/%d ex-data-2 streams (drain does not target the fresh node)",
		len(replicaStreams(coord, "data-3", hadVictim)), len(hadVictim))

	assertNoDataLoss(ctx, t, coord, names)

	// New work does flow to the fresh node: headroom placement favours the idlest
	// broker, so streams created after it joined can land on it.
	var fresh []string
	for i := 0; i < 4; i++ {
		s := fmt.Sprintf("wings.chaos.new.%d", i)
		fresh = append(fresh, s)
		if err := coord.client.CreateStream(ctx, s, &dsclient.StreamConfig{Partitions: 1}); err != nil {
			t.Fatalf("create %s: %v", s, err)
		}
	}
	waitFor(t, "the fresh node picks up newly created streams", func() bool {
		return len(replicaStreams(coord, "data-3", fresh)) > 0
	})
	t.Logf("fresh node holds %d/%d newly created streams", len(replicaStreams(coord, "data-3", fresh)), len(fresh))
}

// replicaStreams returns which of the named streams the controller currently
// places a replica of on brokerID.
func replicaStreams(observer *p2pNode, brokerID string, names []string) []string {
	var out []string
	for _, s := range names {
		if slices.Contains(replicasOf(observer, s), brokerID) {
			out = append(out, s)
		}
	}
	return out
}

// TestP2PPlacementSplit runs a real cluster and checks the placement split holds:
// the expendable per-worker streams never land on the tainted coordinator, and
// the durable tier (coordinator state and run history) leads on the coordinator.
func TestP2PPlacementSplit(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node placement experiment")
	}
	c := start(t, Config{
		Target:      InProcess(),
		Workers:     2,
		Concurrency: 1,
		Dir:         t.TempDir(),
		P2P:         &P2P{ReplicationFactor: 2},
		Logger:      quietP2PLogger(),
	})

	if _, err := p2pSquare(c.Bind(t.Context()), 7); err != nil {
		t.Fatalf("run work: %v", err)
	}

	reg := c.p2p.node.Registry()
	waitFor(t, "the durable tier leads on the coordinator and the expendable tier stays off it", func() bool {
		for _, a := range reg.Assignments() {
			if coordinatorLeads(a.GetStream()) {
				if a.GetLeader() != coordinatorID {
					return false
				}
			} else if slices.Contains(a.GetReplicas(), coordinatorID) {
				return false
			}
		}
		return true
	})

	var durable, expendable int
	for _, a := range reg.Assignments() {
		if coordinatorLeads(a.GetStream()) {
			durable++
		} else {
			expendable++
		}
	}
	t.Logf("durable streams led by coordinator: %d; expendable streams off coordinator: %d", durable, expendable)
	if expendable == 0 {
		t.Fatal("no expendable streams observed; the split was not exercised")
	}
}

func assertNoDataLoss(ctx context.Context, t *testing.T, observer *p2pNode, names []string) {
	t.Helper()
	for i, s := range names {
		st, err := observer.client.OpenStream[string](s)
		if err != nil {
			t.Fatalf("reopen %s: %v", s, err)
		}
		recs, err := st.Read(ctx, 0, 1)
		if err != nil {
			t.Fatalf("read %s: %v", s, err)
		}
		if len(recs) != 1 || recs[0].Record != fmt.Sprintf("v%d", i) {
			t.Fatalf("stream %s lost data: got %v", s, recs)
		}
	}
}

package wings

import (
	"context"
	"io"
	"log/slog"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/durable_streams/broker/cluster"
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

// TestP2PLocalProcessRunsWork starts a p2p cluster on the local-process target —
// a coordinator bootstrap node and worker child processes that each bring up
// their own observer node and join it — and dispatches work, so the replication
// wiring runs across real process boundaries, not one address space.
func TestP2PLocalProcessRunsWork(t *testing.T) {
	c := start(t, Config{
		Target:      LocalProcess(),
		Workers:     2,
		Concurrency: 1,
		P2P:         &P2P{ReplicationFactor: 2},
		Logger:      quietP2PLogger(),
	})

	got, err := p2pSquare(c.Bind(t.Context()), 12)
	if err != nil {
		t.Fatalf("call work: %v", err)
	}
	if got != 144 {
		t.Fatalf("p2pSquare(12) = %d, want 144", got)
	}
}

// TestP2PLocalProcessChannelCrossesProcesses shows a channel carries values
// between worker child processes over the replicated cluster, so a channel's
// data reaches its reader through replication rather than the coordinator relay
// even when writer and reader are separate processes.
func TestP2PLocalProcessChannelCrossesProcesses(t *testing.T) {
	c := start(t, Config{
		Target:      LocalProcess(),
		Workers:     2,
		Concurrency: 1,
		P2P:         &P2P{ReplicationFactor: 2},
		Logger:      quietP2PLogger(),
	})

	var got int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int]()
		fut := ctx.Go(sums, feed{Values: r})
		for _, v := range []int{1, 2, 3, 4} {
			if err := w.Send(ctx, v); err != nil {
				return err
			}
		}
		if err := w.Close(ctx); err != nil {
			return err
		}
		var err error
		got, err = fut.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 10 {
		t.Fatalf("summed %d, want 10", got)
	}
}

// TestP2PAChannelWaitStaysLoadedAndWakes proves a channel receiver in p2p stays
// loaded across a wait that would unload it elsewhere, and is woken in place by
// the worker's own stream subscription when the value arrives — the coordinator
// never follows the worker-led channel stream. It runs once: no unload, no
// redispatch. It still frees its slot while parked (yield_test.go covers that).
func TestP2PAChannelWaitStaysLoadedAndWakes(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node channel wait/wake")
	}
	oldUnload, oldReport := unloadAfter, parkReport
	unloadAfter, parkReport = 150*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { unloadAfter, parkReport = oldUnload, oldReport })
	receiving.attempts.Store(0)

	c := start(t, Config{
		Target:      InProcess(),
		Workers:     2,
		Concurrency: 1,
		Dir:         t.TempDir(),
		P2P:         &P2P{ReplicationFactor: 2},
		Logger:      quietP2PLogger(),
	})

	var got int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int]()
		receiver := ctx.Go(receivesOnce, feed{Values: r})
		// Long past unloadAfter: elsewhere the receiver would be unloaded, but in
		// p2p it stays loaded and its subscription wakes it when the value lands.
		if _, err := ctx.Go(p2pSquare, 3).Await(ctx); err != nil {
			return err
		}
		if err := ctx.Sleep(600 * time.Millisecond); err != nil {
			return err
		}
		if err := w.Send(ctx, 7); err != nil {
			return err
		}
		var err error
		got, err = receiver.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 7 {
		t.Fatalf("got %d, want 7", got)
	}
	if n := receiving.attempts.Load(); n != 1 {
		t.Fatalf("the receiver ran %d attempts, want 1: it stayed loaded and was woken in place", n)
	}
}

// TestP2PAChannelWaitEvictsAndReloads proves the worker-owned eviction path: once
// p2pEvictAfter is armed the worker drops a parked receiver's footprint, holds a
// node-local watch, and reloads the job in place when the value arrives — two
// attempts, and neither the evict nor the reload goes through the coordinator.
func TestP2PAChannelWaitEvictsAndReloads(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node channel evict/reload")
	}
	oldUnload, oldReport, oldEvict := unloadAfter, parkReport, p2pEvictAfter
	unloadAfter, parkReport, p2pEvictAfter = time.Minute, 20*time.Millisecond, 80*time.Millisecond
	t.Cleanup(func() { unloadAfter, parkReport, p2pEvictAfter = oldUnload, oldReport, oldEvict })
	receiving.attempts.Store(0)

	// Concurrency 2 so the receiver gets a slot and parks at once rather than
	// queueing behind p2pSquare; the wide margin below then evicts it well before
	// the value, even on a loaded machine.
	c := start(t, Config{
		Target:      InProcess(),
		Workers:     2,
		Concurrency: 2,
		Dir:         t.TempDir(),
		P2P:         &P2P{ReplicationFactor: 2},
		Logger:      quietP2PLogger(),
	})

	var got int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int]()
		receiver := ctx.Go(receivesOnce, feed{Values: r})
		// Long past p2pEvictAfter: the receiver is evicted while it waits, then
		// reloaded in place when the value arrives.
		if _, err := ctx.Go(p2pSquare, 3).Await(ctx); err != nil {
			return err
		}
		if err := ctx.Sleep(800 * time.Millisecond); err != nil {
			return err
		}
		if err := w.Send(ctx, 7); err != nil {
			return err
		}
		var err error
		got, err = receiver.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 7 {
		t.Fatalf("got %d, want 7", got)
	}
	if n := receiving.attempts.Load(); n != 2 {
		t.Fatalf("the receiver ran %d attempts, want 2: one evicted, one reloaded in place", n)
	}
	// Worker-owned: the receiver's evict and reload never reached the coordinator,
	// so its job was neither yielded to it nor redispatched by it.
	entries := awaitJournal(t, c, func(es []journalEntry) bool {
		for _, e := range es {
			if e.Func == "test.receivesOnce" && e.Kind == journalCompleted {
				return true
			}
		}
		return false
	})
	for _, e := range entries {
		if e.Func == "test.receivesOnce" && (e.Kind == journalYielded || e.Kind == journalRedispatch) {
			t.Fatalf("the receiver's %s reached the coordinator; eviction must be worker-owned", e.Kind)
		}
	}
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

func TestP2PChannelCrossesNodes(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 1, P2P: &P2P{ReplicationFactor: 2}, Logger: quietP2PLogger()})
	var got int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int]()
		fut := ctx.Go(sums, feed{Values: r})
		for _, v := range []int{1, 2, 3, 4} {
			if err := w.Send(ctx, v); err != nil {
				return err
			}
		}
		if err := w.Close(ctx); err != nil {
			return err
		}
		var err error
		got, err = fut.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 10 {
		t.Fatalf("summed %d, want 10", got)
	}
}

// TestP2PColocationRunsFanOutChannels drives many channel-writing threads at
// once on a p2p cluster, whose brokers report co-write statistics: each thread
// commits its history and its channel outbox in one transaction, so the affinity
// tracker sees an edge per thread. The point is that turning colocation on
// leaves the transactional channel path correct — the affinity feed is a hint
// alongside the commit, not a change to it — so every consumer still sums right.
func TestP2PColocationRunsFanOutChannels(t *testing.T) {
	c := start(t, Config{
		Target:      InProcess(),
		Workers:     3,
		Concurrency: 2,
		P2P:         &P2P{ReplicationFactor: 2},
		Logger:      quietP2PLogger(),
	})

	const producers = 6
	var got []int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		futs := make([]*flow.Future[int], producers)
		for i := range producers {
			r, w := ctx.NewChannel[int]()
			futs[i] = ctx.Go(sums, feed{Values: r})
			n := i + 1
			ctx.Go(counts, writeFeed{Values: w, Count: n})
		}
		for _, f := range futs {
			v, err := f.Await(ctx)
			if err != nil {
				return err
			}
			got = append(got, v)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for i, v := range got {
		n := i + 1
		want := n * (n + 1) / 2
		if v != want {
			t.Fatalf("producer %d summed %d, want %d", n, v, want)
		}
	}
}

// TestP2PRecoveryReadsResultStreams shows that in p2p mode, where no results
// mirror is kept, settled results are recovered from the workers' own replicated
// result streams — so a restarted coordinator still sees finished jobs as done.
func TestP2PRecoveryReadsResultStreams(t *testing.T) {
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
		t.Fatalf("start: %v", err)
	}
	defer c.Stop(context.Background())

	if _, err := p2pSquare(c.Bind(ctx), 9); err != nil {
		t.Fatalf("call work: %v", err)
	}

	settled, err := c.mirroredResults(ctx)
	if err != nil {
		t.Fatalf("mirroredResults: %v", err)
	}
	if len(settled) == 0 {
		t.Fatal("no settled results found in p2p mode; a restart would re-run finished jobs")
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

// TestP2PListenSeamCarriesTraffic shows startP2PNode serves its peer and client
// endpoint on the listener the config supplies rather than a hardwired TCP bind:
// a node brought up through an injected listener round-trips a stream, so the
// seam a userspace overlay plugs into (Phase 4) carries real broker traffic.
func TestP2PListenSeamCarriesTraffic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int32
	n, err := startP2PNode(ctx, p2pNodeConfig{
		id:                "coordinator",
		dir:               t.TempDir(),
		bootstrap:         true,
		replicationFactor: 1,
		reconcile:         150 * time.Millisecond,
		log:               quietP2PLogger(),
		listen: func(lctx context.Context, addr string) (net.Listener, error) {
			calls.Add(1)
			return (&net.ListenConfig{}).Listen(lctx, "tcp", addr)
		},
	})
	if err != nil {
		t.Fatalf("start bootstrap node: %v", err)
	}
	defer n.close()
	if err := n.awaitReady(ctx); err != nil {
		t.Fatalf("await ready: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("the injected listener was used %d times, want 1", got)
	}

	const stream = "p2p.seam"
	if err := n.client.CreateStream(ctx, stream, &dsclient.StreamConfig{Partitions: 1}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	s, err := n.client.OpenStream[string](stream)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if _, err := s.Append(ctx, []string{"x"}); err != nil {
		t.Fatalf("append: %v", err)
	}
	recs, err := s.Read(ctx, 0, 1)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(recs) != 1 || recs[0].Record != "x" {
		t.Fatalf("read %+v, want one record %q", recs, "x")
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

// TestP2PRetireDrainsReplicas exercises Phase 3's cordon-then-drain: a node
// holding a replica is cordoned so nothing new lands on it and drained, and its
// copy moves onto the free peer -- the stream stays on two brokers and no longer
// on the drained one, so retiring a p2p worker does not drop a copy.
func TestP2PRetireDrainsReplicas(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const reconcile = 150 * time.Millisecond
	coord, err := startP2PNode(ctx, p2pNodeConfig{
		id: "coordinator", dir: t.TempDir(), bootstrap: true,
		replicationFactor: 2, reconcile: reconcile, log: quietP2PLogger(),
	})
	if err != nil {
		t.Fatalf("start coordinator: %v", err)
	}
	defer coord.close()

	for _, id := range []string{"data-1", "data-2"} {
		n, err := startP2PNode(ctx, p2pNodeConfig{
			id: id, dir: t.TempDir(), observer: true, join: coord.peerAddr,
			replicationFactor: 2, reconcile: reconcile, log: quietP2PLogger(),
		})
		if err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		defer n.close()
	}
	if err := coord.awaitReady(ctx); err != nil {
		t.Fatalf("await ready: %v", err)
	}
	waitFor(t, "three brokers registered", func() bool {
		return len(coord.node.Registry().Unfenced()) == 3
	})

	const stream = "p2p.drain"
	if err := coord.client.CreateStream(ctx, stream, &dsclient.StreamConfig{Partitions: 1}); err != nil {
		t.Fatalf("create stream: %v", err)
	}
	replicas, _, placed := coord.manager.AssignmentOf(stream, 0)
	if !placed || len(distinct(replicas)) != 2 {
		t.Fatalf("stream placed on %v, want 2 distinct replicas", replicas)
	}

	// Drain a data node that holds a replica; with a third broker free, its copy
	// has somewhere to go.
	var victim string
	for _, id := range []string{"data-1", "data-2"} {
		if slices.Contains(replicas, id) {
			victim = id
			break
		}
	}
	if victim == "" {
		t.Skipf("stream landed on %v; neither data node holds a replica to drain", replicas)
	}

	cat := cluster.NewCatalog(coord.node)
	if _, err := cat.Cordon(victim, true); err != nil {
		t.Fatalf("cordon %s: %v", victim, err)
	}
	if _, _, err := cat.Drain(victim); err != nil {
		t.Fatalf("drain %s: %v", victim, err)
	}

	waitFor(t, "the stream's replica moved off the drained node", func() bool {
		reps, _, ok := coord.manager.AssignmentOf(stream, 0)
		return ok && len(distinct(reps)) == 2 && !slices.Contains(reps, victim)
	})
}

// TestPickPreferring checks the locality tiebreaker in isolation: a preferred
// worker wins only when it is available, not the one being moved away from, and
// no more loaded than the balance choice — otherwise balance stands.
func TestPickPreferring(t *testing.T) {
	newWorker := func(id string, inflight int) *workerConn {
		return &workerConn{id: id, inflight: inflight}
	}
	light := newWorker("light", 1)
	heavy := newWorker("heavy", 5)
	c := &Cluster{workers: []*workerConn{light, heavy}}

	if got := c.pickPreferring(nil, nil); got != light {
		t.Fatalf("no preference: got %v, want the least loaded (light)", workerID(got))
	}
	// Prefer heavy over the balance choice: it is more loaded, so balance wins.
	if got := c.pickPreferring(nil, heavy); got != light {
		t.Fatalf("prefer heavy: got %v, want light (balance outranks a costlier preference)", workerID(got))
	}
	// Prefer light, which is also the balance choice: it wins.
	if got := c.pickPreferring(nil, light); got != light {
		t.Fatalf("prefer light: got %v, want light", workerID(got))
	}
	// Equal load: the preference decides.
	a, b := newWorker("a", 2), newWorker("b", 2)
	c.workers = []*workerConn{a, b}
	if got := c.pickPreferring(nil, b); got != b {
		t.Fatalf("equal load: got %v, want the preferred b", workerID(got))
	}
	// A draining preferred worker is ignored.
	b.draining = true
	if got := c.pickPreferring(nil, b); got != a {
		t.Fatalf("draining preference: got %v, want a", workerID(got))
	}
	// The preference is never the worker being moved away from.
	b.draining = false
	if got := c.pickPreferring(b, b); got != a {
		t.Fatalf("prefer == avoid: got %v, want a", workerID(got))
	}
}

// TestLeaderWorkerNotP2P shows the leader lookup is inert outside p2p: there is
// no cluster to ask, so it never steers placement.
func TestLeaderWorkerNotP2P(t *testing.T) {
	c := &Cluster{workers: []*workerConn{{id: "w"}}}
	if got := c.leaderWorker("anything"); got != nil {
		t.Fatalf("leaderWorker off p2p: got %v, want nil", workerID(got))
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

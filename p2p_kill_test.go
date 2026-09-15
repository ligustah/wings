package wings

import (
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// killWork runs `steps` checkpointed steps, so a worker killed mid-run leaves an
// open (prepared) transaction for its attempt — the precondition for the post-kill
// tx-wedge (see docs/ds-tx-deleted-participant-2026-09-16.md).
var killWork = flow.Define(func(ctx flow.Context, steps int) (int, error) {
	start, _, err := ctx.Checkpoint[int]()
	if err != nil {
		return 0, err
	}
	for i := start; i < steps; i++ {
		time.Sleep(200 * time.Millisecond)
		if err := ctx.Heartbeat(i + 1); err != nil {
			return 0, err
		}
	}
	return steps, nil
}, flow.WithName("test.killWork"))

// TestP2PWorkSurvivesAWorkerKilledMidCheckpoint is the p2p counterpart of
// [TestWorkSurvivesAWorkerBeingKilled]: a checkpointing job is in flight on a
// worker that is hard-killed, so its attempt's transaction is left prepared. The
// run must still complete — the coordinator redispatches the lost work.
//
// It also stands as the local reproduction attempt for finding #2 (a prepared
// attempt's tx stranded when its participant history stream is dropped on settle,
// see docs/ds-tx-deleted-participant-2026-09-16.md). Even with a wide commit
// interval holding the attempt's transaction open across the kill, the wedge does
// not reproduce over fast loopback: the coordinating broker is alive and resolves
// the abandoned transaction long before settle drops its participant. The bug was
// in durable_streams and is fixed in v0.173.0 / broker v0.278.0 (a deleted
// participant now settles the transaction rather than retrying forever).
func TestP2PWorkSurvivesAWorkerKilledMidCheckpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	c := start(t, Config{
		Target: LocalProcess(),
		P2P:    &P2P{ReplicationFactor: 2},
		// A wide commit interval holds each attempt's transaction open across the
		// kill, so the killed worker leaves it prepared — the widest window this
		// harness can open toward finding #2 without controlling broker fencing.
		CommitInterval: 5 * time.Second,
		Workers:        3,
		Concurrency:    2,
		Logger:         quietP2PLogger(),
	})

	const jobs = 9
	ins := make([]int, jobs)
	for i := range ins {
		ins[i] = 10 // 10 * 200ms = ~2s each, so work is in flight at kill time
	}

	type outcome struct {
		got []int
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		got, err := mapOn(t.Context(), c, killWork, ins)
		done <- outcome{got, err}
	}()

	time.Sleep(700 * time.Millisecond)
	victim := firstWorker(t, c)
	if victim.proc == nil {
		t.Fatal("expected a local worker with a process to kill")
	}
	t.Logf("killing worker %s (pid %d)", victim.id, victim.proc.Pid)
	if err := victim.proc.Kill(); err != nil {
		t.Fatalf("kill worker: %v", err)
	}

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("Map failed after a worker was killed: %v", res.err)
		}
		if len(res.got) != jobs {
			t.Fatalf("got %d results, want %d", len(res.got), jobs)
		}
	case <-time.After(90 * time.Second):
		t.Fatal("Map never returned after a worker was killed mid-checkpoint " +
			"— finding #2: the killed attempt's prepared tx wedges once its history stream is dropped")
	}
}

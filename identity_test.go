package wings

import (
	"context"
	"strings"
	"testing"
)

// workerIDs returns the names the cluster currently knows its workers by.
func workerIDs(c *Cluster) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.workers))
	for _, w := range c.workers {
		out = append(out, w.id)
	}
	return out
}

// THE POINT: a worker's name is not a label, it is an address. Its mirror
// stream on the coordinator is named after it and survives the process, so a
// new worker handed a name a previous run used inherits that stream's read
// position — and its results are then skipped as already seen. It never
// delivers anything, and every job sent to it hangs until it is given up on.
//
// Reattachment is what makes this reachable in one process: recovered workers
// keep the names they were started with, and a fresh one alongside them must
// not be handed the same one.
func TestAFreshWorkerNeverReusesARecoveredWorkersName(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	cloud := newFakeCloud(t)
	dir := t.TempDir()

	first := startRemote(t, dir, cloud, 1)
	if _, err := mapOn(t.Context(), first, double, []int{1, 2}); err != nil {
		t.Fatalf("Map: %v", err)
	}
	was := workerIDs(first)
	abandon(t, first)

	// One recovered, one brand new.
	second := startRemote(t, dir, cloud, 2)
	defer func() {
		if err := second.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	now := workerIDs(second)
	if len(now) != 2 {
		t.Fatalf("the restarted coordinator has workers %v, want 2", now)
	}
	if now[0] == now[1] {
		t.Fatalf("both workers are called %q; they share a mirror stream and one of them will never deliver a result", now[0])
	}
	recovered := map[string]bool{}
	for _, id := range was {
		recovered[id] = true
	}
	var fresh int
	for _, id := range now {
		if !recovered[id] {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("of workers %v, %d are new names; exactly one machine was provisioned, so exactly one name should be", now, fresh)
	}

	// And the cluster has to actually work, which is the symptom a name clash
	// produces: jobs to the colliding worker are never answered.
	got, err := mapOn(t.Context(), second, double, []int{1, 2, 3, 4, 5, 6, 7, 8})
	if err != nil {
		t.Fatalf("Map after restart: %v", err)
	}
	if len(got) != 8 || got[7] != 16 {
		t.Fatalf("got %v", got)
	}
}

// startIn brings up an in-process cluster over a directory the caller owns, so
// a second one can be opened over the same record afterwards. The shared start
// helper insists on a fresh temp dir, which is exactly what this must not have.
func startIn(t *testing.T, dir string) *Cluster {
	t.Helper()
	c, err := Start(t.Context(), Config{Target: InProcess(), Dir: dir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return c
}

// The journal is append-only and outlives the process, so several coordinator
// runs share one record. A reader who cannot tell them apart cannot tell a job
// that is still outstanding from one a previous run finished — and with a
// counter that restarts at zero, both are called "1".
func TestTheRecordCanTellTwoCoordinatorRunsApart(t *testing.T) {
	dir := t.TempDir()

	first := startIn(t, dir)
	if _, err := mapOn(t.Context(), first, double, []int{1, 2, 3}); err != nil {
		t.Fatalf("Map: %v", err)
	}
	awaitJournal(t, first, func(es []journalEntry) bool {
		return countKind(es, journalCompleted) >= 3
	})
	firstEpoch := first.epoch
	firstJobs := map[string]bool{}
	for _, e := range readJournal(t, first) {
		if e.Kind == journalSubmitted {
			firstJobs[e.Job] = true
		}
	}
	if err := first.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	second := startIn(t, dir)
	t.Cleanup(func() {
		if err := second.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	if second.epoch == firstEpoch {
		t.Fatalf("both runs call themselves %q", firstEpoch)
	}
	if _, err := mapOn(t.Context(), second, double, []int{4, 5, 6}); err != nil {
		t.Fatalf("Map: %v", err)
	}

	entries := awaitJournal(t, second, func(es []journalEntry) bool {
		return countKind(es, journalCompleted) >= 6
	})

	epochs := map[string]int{}
	for _, e := range entries {
		if e.Epoch == "" {
			t.Fatalf("a %s entry names no coordinator run", e.Kind)
		}
		epochs[e.Epoch]++
		if e.Kind == journalSubmitted {
			if firstJobs[e.Job] && e.Epoch == second.epoch {
				t.Errorf("the second run reused job id %q; the record now has two different jobs by that name", e.Job)
			}
			if !strings.HasPrefix(e.Job, e.Epoch+"-") {
				t.Errorf("job %q was written by run %q but does not say so; a result still on a worker queue "+
					"cannot then be told from a new job of the same name", e.Job, e.Epoch)
			}
		}
	}
	if len(epochs) != 2 {
		t.Fatalf("the record is written by %d coordinator runs, want 2", len(epochs))
	}
	if countKind(entries, journalClusterStart) != 2 {
		t.Errorf("%d starts were recorded; each run must say when it came up",
			countKind(entries, journalClusterStart))
	}
}

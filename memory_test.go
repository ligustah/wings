package wings

import (
	"strconv"
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: forgetRun drops exactly the finished run's lineage index, and no
// other run's — so ranAs cannot grow without bound over a coordinator's life.
func TestForgetRunEvictsOnlyItsRun(t *testing.T) {
	c := &Cluster{ranAs: map[string]string{}}
	c.ranAs[flow.Origin{Run: "alpha", Thread: "main"}.Key()] = "j1"
	c.ranAs[flow.Origin{Run: "alpha", Thread: "main.0"}.Key()] = "j2"
	c.ranAs[flow.Origin{Run: "beta", Thread: "main"}.Key()] = "j3"

	c.forgetRun("alpha")

	if len(c.ranAs) != 1 {
		t.Fatalf("ranAs has %d entries after forgetRun(alpha), want 1: %v", len(c.ranAs), c.ranAs)
	}
	if _, ok := c.ranAs[flow.Origin{Run: "beta", Thread: "main"}.Key()]; !ok {
		t.Fatalf("forgetRun(alpha) dropped beta's entry too: %v", c.ranAs)
	}
}

// THE POINT: a settling job records a ranAs entry only when its history survives
// the settle — i.e. under RetainHistory. Without it the history is dropped, so the
// entry would lead nowhere and only grow the map for the coordinator's life; a
// long-running run therefore accumulates none.
func TestForgetRecordsRanAsOnlyWhenHistoryIsKept(t *testing.T) {
	origin := flow.Origin{Run: "r", Thread: "main.0", Step: 0}
	key := origin.Key()

	newCluster := func(retain bool) *Cluster {
		return &Cluster{
			ranAs:    map[string]string{},
			pending:  map[string]*pendingJob{},
			byOrigin: map[string]*pendingJob{},
			// RetainChannelData keeps forget from spawning a channel reclaim that would
			// reach a nil client on this bare cluster; sharedUp stays false so the
			// history drop is likewise skipped. Neither is under test here.
			cfg: Config{RetainChannelData: true, RetainHistory: retain},
		}
	}
	settle := func(c *Cluster) {
		p := &pendingJob{job: jobEnvelope{ID: "j1"}, origin: origin}
		c.mu.Lock()
		c.pending[p.job.ID] = p
		c.byOrigin[key] = p
		c.forget(p)
		c.mu.Unlock()
	}

	c := newCluster(false)
	settle(c)
	if len(c.ranAs) != 0 {
		t.Fatalf("without RetainHistory a settled job recorded %d ranAs entries, want 0: %v", len(c.ranAs), c.ranAs)
	}

	rc := newCluster(true)
	settle(rc)
	if rc.ranAs[key] != "j1" {
		t.Fatalf("with RetainHistory a settled job should record ranAs[%s]=j1; got %v", key, rc.ranAs)
	}
}

// THE POINT: a run that forks and completes leaves no lineage index behind, so
// running many runs does not accumulate ranAs. (That forget populates ranAs in
// the first place is covered by TestThreadHistoryFindsAForgottenAncestorViaRanAs.)
func TestCompletedRunLeavesNoRanAs(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 2})
	name := "test-ranAs-" + strconv.FormatUint(runSeq.Add(1), 36)

	if err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		a := ctx.Go(double, 1)
		b := ctx.Go(double, 2)
		if _, err := a.Await(ctx); err != nil {
			return err
		}
		_, err := b.Await(ctx)
		return err
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	c.mu.Lock()
	left := len(c.ranAs)
	c.mu.Unlock()
	if left != 0 {
		t.Fatalf("a completed run left %d ranAs entries, want 0", left)
	}
}

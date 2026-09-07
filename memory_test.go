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

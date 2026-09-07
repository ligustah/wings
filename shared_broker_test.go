package wings

import (
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/ligustah/wings/flow"
)

func sharedLocal(t *testing.T, cfg Config) *Cluster {
	t.Helper()
	cfg.Target = LocalProcess()
	cfg.LocalSharedBroker = true
	return start(t, cfg)
}

// THE POINT: shared-broker local mode runs jobs on worker child processes the
// same as the mirrored mode — results come back correctly.
func TestLocalSharedBrokerRunsJobs(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := sharedLocal(t, Config{Workers: 2, Concurrency: 2})

	got, err := mapOn(t.Context(), c, double, []int{1, 2, 3, 4, 5})
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	if want := []int{2, 4, 6, 8, 10}; !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

// THE POINT: shared channels still cross worker processes here — the relay finds
// the workers' outboxes in the coordinator's own engine rather than copying them
// off a per-worker broker.
func TestLocalSharedBrokerChannelCrossesWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := sharedLocal(t, Config{Workers: 2, Concurrency: 1})

	var got int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		fut := ctx.Go(sums, feed{Values: ch})
		for _, v := range []int{1, 2, 3, 4} {
			if err := ch.Send(ctx, v); err != nil {
				return err
			}
		}
		if err := ch.Close(ctx); err != nil {
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
		t.Fatalf("the function summed %d, want 10", got)
	}
}

// THE POINT: the reason for the mode — no per-worker data. The coordinator keeps
// its own engine and nothing per worker, unlike the mirrored mode's <dir>/<id>.
func TestLocalSharedBrokerKeepsNoPerWorkerData(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := sharedLocal(t, Config{Workers: 2})
	if _, err := mapOn(t.Context(), c, double, []int{1, 2, 3}); err != nil {
		t.Fatalf("Map: %v", err)
	}

	entries, err := os.ReadDir(c.dir)
	if err != nil {
		t.Fatalf("read %s: %v", c.dir, err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "local-") {
			t.Fatalf("found per-worker directory %q; shared-broker mode should keep none", e.Name())
		}
	}
}

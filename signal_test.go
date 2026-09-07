package wings

import (
	"context"
	"strconv"
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: an event delivered to a run by name reaches it through the cluster,
// held until the run asks — end to end over the coordinator's channel relay.
func TestClusterSignalReachesTheRun(t *testing.T) {
	c := start(t, Config{Target: InProcess()})
	name := "test-signal-" + strconv.FormatUint(runSeq.Add(1), 36)

	go func() {
		if err := c.Signal(context.Background(), name, "approval", 7); err != nil {
			t.Errorf("Signal: %v", err)
		}
	}()

	var got int
	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		v, err := ctx.Signal[int]("approval")
		got = v
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 7 {
		t.Fatalf("the run received %d, want the delivered 7", got)
	}
}

package wings

import (
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: the relay's retire tombstones are dropped once a run's or job's
// channel streams are gone from storage, so retiredRun/retiredJob do not grow for
// the coordinator's life — but a tombstone whose streams are still present stays,
// so a late one is still recognized and reclaimed.
func TestEvictQuiescentTombstones(t *testing.T) {
	c := &Cluster{relay: &channelRelay{
		retiredRun: map[string]bool{"run1": true, "run2": true},
		retiredJob: map[string]flow.Origin{
			"job1": {Run: "run1", Thread: "t1"},
			"job2": {Run: "run2", Thread: "t2"},
		},
	}}

	// Only run1/t1's channel stream is still on storage; run2 and job2 are gone.
	present := []string{chanPrefix + streamPart("run1") + "_" + streamPart("t1") + "_ch0"}
	c.evictQuiescentTombstones(present)

	if !c.relay.retiredRun["run1"] {
		t.Error("run1's tombstone was evicted though its stream is still present")
	}
	if c.relay.retiredRun["run2"] {
		t.Error("run2's tombstone was kept though no stream of it remains")
	}
	if _, ok := c.relay.retiredJob["job1"]; !ok {
		t.Error("job1's tombstone was evicted though its stream is still present")
	}
	if _, ok := c.relay.retiredJob["job2"]; ok {
		t.Error("job2's tombstone was kept though no stream of it remains")
	}

	// Once run1's stream is gone too, its tombstones go on the next sweep.
	c.evictQuiescentTombstones(nil)
	if len(c.relay.retiredRun) != 0 || len(c.relay.retiredJob) != 0 {
		t.Errorf("tombstones remain after all streams gone: runs=%v jobs=%v", c.relay.retiredRun, c.relay.retiredJob)
	}
}

package wings

import (
	"context"
	"fmt"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// logPrefix names a thread's durable log. It is deliberately not a parseOutput
// prefix: a settled job's history drop and the per-job pulls skip it, so a run's
// log lines outlive the history a purge removes.
const logPrefix = "wings.log."

// logRetentionBytes bounds one thread's log: past it, the oldest whole segments
// are dropped, so a long run's logs stay bounded rather than growing without end
// (the log outlives the history, so nothing else reclaims it). logSegmentBytes is
// the eviction granularity — a few of them fit under the budget, and the true
// ceiling is the budget plus the active segment that is never dropped.
const (
	logRetentionBytes = 32 << 20
	logSegmentBytes   = 4 << 20
)

func logStreamName(run, thread string) string {
	return logPrefix + streamPart(run) + "." + streamPart(thread)
}

// ensureLogStream creates a thread's log stream if absent, with a byte budget so
// it self-bounds. A later opener that supplies no config gets what was declared here.
func ensureLogStream(ctx context.Context, client *dsclient.Client, name string) error {
	ok, err := client.StreamExists(ctx, name)
	if err != nil {
		return fmt.Errorf("wings: check %s: %w", name, err)
	}
	if ok {
		return nil
	}
	cfg := &dsclient.StreamConfig{
		Compression:    streamCompression,
		BlockFormat:    streamBlockFormat,
		RetentionBytes: logRetentionBytes,
		SegmentBytes:   logSegmentBytes,
	}
	if err := client.CreateStream(ctx, name, cfg); err != nil {
		return fmt.Errorf("wings: create %s: %w", name, err)
	}
	return nil
}

// clusterLogs is the [flow.LogHost] for runs on the coordinator: it writes each
// line into the logging thread's own transaction, so the line and the history
// marker that dedupes it on replay commit together.
type clusterLogs struct{ c *Cluster }

func (h clusterLogs) Log(ctx context.Context, run, thread string, rec *protos.LogRecord) error {
	out, err := h.c.coordOutputsFor(run + "/" + thread)
	if err != nil {
		return err
	}
	return out.appendLog(ctx, logStreamName(run, thread), rec)
}

var _ flow.LogHost = clusterLogs{}

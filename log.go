package wings

import (
	"context"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// logPrefix names a thread's durable log. It is deliberately not a parseOutput
// prefix: a settled job's history drop and the per-job pulls skip it, so a run's
// log lines outlive the history a purge removes.
const logPrefix = "wings.log."

func logStreamName(run, thread string) string {
	return logPrefix + streamPart(run) + "." + streamPart(thread)
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

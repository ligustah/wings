package wings

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

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

// nodeLogs is the [flow.LogHost] for a job's run on a worker: it writes each line
// into the logging thread's attempt transaction, so the line rides home to the
// coordinator on the same pull as the history marker that dedupes it (pulledStream).
type nodeLogs struct{ txns *attemptTxns }

func (h nodeLogs) Log(ctx context.Context, run, thread string, rec *protos.LogRecord) error {
	return h.txns.For(thread).appendLog(ctx, logStreamName(run, thread), rec)
}

var _ flow.LogHost = nodeLogs{}

// LogLine is one durable log line read back from a run's log.
type LogLine struct {
	Thread  string
	Time    time.Time
	Level   slog.Level
	Message string
	Attrs   map[string]string
}

// ThreadLog returns one thread's durable log lines, oldest first. A thread that
// logged nothing yields none.
func (c *Cluster) ThreadLog(ctx context.Context, run, thread string) ([]LogLine, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	return readLog(ctx, client, logStreamName(run, thread), thread)
}

// RunLogs returns a run's durable log lines across every thread that logged,
// oldest first within each thread. The lines outlive the history, so a finished
// run's logs read back even after its history is dropped.
func (c *Cluster) RunLogs(ctx context.Context, run string) ([]LogLine, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	names, err := client.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("wings: list streams: %w", err)
	}
	prefix := logPrefix + streamPart(run) + "."
	sort.Strings(names)
	var out []LogLine
	for _, name := range names {
		thread, ok := strings.CutPrefix(name, prefix)
		if !ok {
			continue
		}
		lines, err := readLog(ctx, client, name, thread)
		if err != nil {
			return nil, err
		}
		out = append(out, lines...)
	}
	return out, nil
}

func readLog(ctx context.Context, client *dsclient.Client, name, thread string) ([]LogLine, error) {
	ok, err := client.StreamExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("wings: check %s: %w", name, err)
	}
	if !ok {
		return nil, nil
	}
	st, err := eventStream[*protos.LogRecord](client, name)
	if err != nil {
		return nil, err
	}
	var out []LogLine
	for from := int64(0); ; {
		recs, err := st.Read(ctx, from, 512)
		if err != nil {
			return nil, fmt.Errorf("wings: read %s: %w", name, err)
		}
		if len(recs) == 0 {
			return out, nil
		}
		for _, r := range recs {
			lr := r.Record
			var attrs map[string]string
			if len(lr.GetAttrs()) > 0 {
				attrs = make(map[string]string, len(lr.GetAttrs()))
				for _, a := range lr.GetAttrs() {
					attrs[a.GetKey()] = a.GetValue()
				}
			}
			out = append(out, LogLine{
				Thread:  thread,
				Time:    time.Unix(0, lr.GetTimeUnixNano()),
				Level:   slog.Level(lr.GetLevel()),
				Message: lr.GetMessage(),
				Attrs:   attrs,
			})
			from = r.Offset + 1
		}
	}
}

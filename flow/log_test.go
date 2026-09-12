package flow_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

var errRetry = errors.New("fail once to force a replay")

// memLogHost keeps every durable log line, so a test can see what was written
// and — the point — that a replay did not write it again.
type memLogHost struct {
	mu    sync.Mutex
	lines []*protos.LogRecord
}

func (h *memLogHost) Log(_ context.Context, _, _ string, rec *protos.LogRecord) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, rec)
	return nil
}

func (h *memLogHost) all() []*protos.LogRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*protos.LogRecord(nil), h.lines...)
}

// THE POINT: a line logged through ctx.Logger() is written to the host once, and
// a retry that replays the same log call does NOT write it a second time — the
// history marker at the cursor tells the replay the line is already down.
func TestALoggedLineIsWrittenOnceAcrossAReplay(t *testing.T) {
	host := &memLogHost{}
	var runs atomic.Int32

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ctx.Logger().Info("hello", "n", 1)
		if runs.Add(1) == 1 {
			return errRetry
		}
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.WithLogHost(host), flow.Backoff(0, 0))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if runs.Load() != 2 {
		t.Fatalf("the body ran %d times, want 2 (a fail then a replay)", runs.Load())
	}
	lines := host.all()
	if len(lines) != 1 {
		t.Fatalf("the host holds %d lines, want 1 — the replay wrote the line again", len(lines))
	}
	if lines[0].GetMessage() != "hello" || slog.Level(lines[0].GetLevel()) != slog.LevelInfo {
		t.Fatalf("recorded %+v, want an Info \"hello\"", lines[0])
	}
}

// THE POINT: the log level is fixed run config, so a line below it is neither
// written nor recorded — the same on every attempt, since the level cannot change
// under a replay.
func TestLogLevelFiltersBelowTheConfiguredLevel(t *testing.T) {
	host := &memLogHost{}

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		log := ctx.Logger()
		log.Info("dropped")
		log.Warn("kept")
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.WithLogHost(host), flow.WithLogLevel(slog.LevelWarn), flow.Once())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	lines := host.all()
	if len(lines) != 1 || lines[0].GetMessage() != "kept" {
		t.Fatalf("host holds %+v, want only the Warn line", lines)
	}
}

// THE POINT: attributes and groups reach the durable line, groups flattened into
// dotted keys and values rendered to strings.
func TestLoggedAttributesAreFlattened(t *testing.T) {
	host := &memLogHost{}

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ctx.Logger().With("job", "j1").WithGroup("net").Info("dial", "host", "example.com", "port", 443)
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.WithLogHost(host), flow.Once())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	lines := host.all()
	if len(lines) != 1 {
		t.Fatalf("host holds %d lines, want 1", len(lines))
	}
	got := map[string]string{}
	for _, a := range lines[0].GetAttrs() {
		got[a.GetKey()] = a.GetValue()
	}
	if got["job"] != "j1" || got["net.host"] != "example.com" || got["net.port"] != "443" {
		t.Fatalf("attrs %v; want job=j1, net.host=example.com, net.port=443", got)
	}
}

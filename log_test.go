package wings

import (
	"log/slog"
	"testing"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// THE POINT: ctx.Logger() on the coordinator writes durable lines to the thread's
// own log stream — apart from history, through the thread's transaction — so the
// lines are there to read back after the run.
func TestCoordinatorLoggerWritesADurableLog(t *testing.T) {
	c := start(t, Config{Target: InProcess()})
	name := flow.NewName()

	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		log := ctx.Logger()
		log.Info("first", "n", 1)
		log.Warn("second")
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	client, err := c.sharedClient()
	if err != nil {
		t.Fatalf("shared client: %v", err)
	}
	st, err := eventStream[*protos.LogRecord](client, logStreamName(name, "main"))
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	recs, err := st.Read(t.Context(), 0, 16)
	if err != nil {
		t.Fatalf("read log: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("the log holds %d lines, want 2", len(recs))
	}
	if recs[0].Record.GetMessage() != "first" || slog.Level(recs[0].Record.GetLevel()) != slog.LevelInfo {
		t.Fatalf("line 0 = %+v, want an Info \"first\"", recs[0].Record)
	}
	if recs[1].Record.GetMessage() != "second" || slog.Level(recs[1].Record.GetLevel()) != slog.LevelWarn {
		t.Fatalf("line 1 = %+v, want a Warn \"second\"", recs[1].Record)
	}
}

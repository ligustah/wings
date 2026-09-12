package wings

import (
	"log/slog"
	"testing"
	"time"

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

// THE POINT: a forked thread's history is dropped once it is joined, but its log
// outlives it — the log stream is not a parseOutput stream, so the drop leaves it.
// Run under a CommitInterval too, so the line and its marker are shown to coalesce
// and still land.
func TestAForkedThreadsLogSurvivesItsHistoryDrop(t *testing.T) {
	c := start(t, Config{Target: InProcess(), CommitInterval: 500 * time.Millisecond})
	name := flow.NewName()

	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		_, err := ctx.Spawn(func(ctx flow.Context) (int, error) {
			ctx.Logger().Info("from the child")
			return 7, nil
		}).Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	client, err := c.sharedClient()
	if err != nil {
		t.Fatalf("shared client: %v", err)
	}

	// The join drops the child's history; give the drop a moment to land.
	hist := flow.ThreadStream(name, "main.0")
	dropped := false
	for range 50 {
		ok, err := client.StreamExists(t.Context(), hist)
		if err != nil {
			t.Fatalf("check history: %v", err)
		}
		if !ok {
			dropped = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !dropped {
		t.Fatalf("the joined child's history %s was never dropped", hist)
	}

	// Its log is still here, with the line.
	st, err := eventStream[*protos.LogRecord](client, logStreamName(name, "main.0"))
	if err != nil {
		t.Fatalf("open child log: %v", err)
	}
	recs, err := st.Read(t.Context(), 0, 8)
	if err != nil {
		t.Fatalf("read child log: %v", err)
	}
	if len(recs) != 1 || recs[0].Record.GetMessage() != "from the child" {
		t.Fatalf("child log = %+v, want one \"from the child\" line", recs)
	}
}

// logsOnWorker logs a line, so a test can fork it onto a worker and check the line
// is pulled home to the coordinator.
var logsOnWorker = flow.Define(func(ctx flow.Context, _ int) (int, error) {
	ctx.Logger().Info("hello from a worker")
	return 1, nil
}, flow.WithName("test.logsOnWorker"))

var _ = flow.Main(logsOnWorker)

// THE POINT: a thread that logs on a worker has its log pulled home, so a run's
// logs are complete on the coordinator wherever its threads ran.
func TestAWorkersLogIsPulledToTheCoordinator(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 1})
	name := flow.NewName()

	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		_, err := ctx.Go(logsOnWorker, 0).Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// The child ran on a worker; its log is pulled home asynchronously.
	found := func() bool {
		lines, err := c.RunLogs(t.Context(), name)
		if err != nil {
			t.Fatalf("RunLogs: %v", err)
		}
		for _, l := range lines {
			if l.Message == "hello from a worker" {
				return true
			}
		}
		return false
	}
	ok := false
	for range 100 {
		if found() {
			ok = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatal("the worker's log line was never pulled to the coordinator")
	}
}

// THE POINT: RunLogs reads a run's durable log back across its threads, so a
// finished run's logs can be inspected.
func TestRunLogsReadsBackARunsLog(t *testing.T) {
	c := start(t, Config{Target: InProcess()})
	name := flow.NewName()

	err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		ctx.Logger().Info("from main")
		_, err := ctx.Spawn(func(ctx flow.Context) (int, error) {
			ctx.Logger().Warn("from child")
			return 1, nil
		}).Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	lines, err := c.RunLogs(t.Context(), name)
	if err != nil {
		t.Fatalf("RunLogs: %v", err)
	}
	got := map[string]slog.Level{}
	for _, l := range lines {
		got[l.Message] = l.Level
	}
	if got["from main"] != slog.LevelInfo || got["from child"] != slog.LevelWarn {
		t.Fatalf("RunLogs returned %+v, want an Info \"from main\" and a Warn \"from child\"", lines)
	}
}

package wings

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// phases records which phases of the job actually executed, across every
// attempt. Whether an expensive one ran twice is the entire question.
var phases struct {
	mu  sync.Mutex
	ran []string
}

func ranPhase(name string) {
	phases.mu.Lock()
	defer phases.mu.Unlock()
	phases.ran = append(phases.ran, name)
}

func phasesRun() []string {
	phases.mu.Lock()
	defer phases.mu.Unlock()
	return slices.Clone(phases.ran)
}

var stall struct {
	attempts atomic.Int64
	release  chan struct{}
	once     sync.Once
}

// restore is a long job in three phases, the middle of which goes quiet on its
// first attempt — a worker that has not died but has stopped saying anything,
// which is the case a job gets moved for.
var restore = Define("test.restore", func(ctx context.Context, _ int) (string, error) {
	first := stall.attempts.Add(1) == 1

	a, err := Step(ctx, "snapshot", func(ctx context.Context) (string, error) {
		ranPhase("snapshot")
		return "s", nil
	})
	if err != nil {
		return "", err
	}

	b, err := Step(ctx, "copy", func(ctx context.Context) (string, error) {
		ranPhase("copy")
		if first {
			select {
			case <-stall.release:
			case <-ctx.Done():
			}
			return "", errors.New("test.restore: the first attempt was abandoned")
		}
		return "c", nil
	})
	if err != nil {
		return "", err
	}

	c, err := Step(ctx, "verify", func(ctx context.Context) (string, error) {
		ranPhase("verify")
		return "v", nil
	})
	if err != nil {
		return "", err
	}
	return a + b + c, nil
}, WithHeartbeatTimeout(300*time.Millisecond))

// THE POINT: relocating a job must not mean redoing it. What cannot cross a
// machine boundary is the running goroutine; what can is the result of each
// phase that finished, and shipping those is the difference between a move that
// costs the phase in progress and one that costs the whole job.
func TestAMovedJobReplaysThePhasesItAlreadyFinished(t *testing.T) {
	stall.release = make(chan struct{})
	stall.attempts.Store(0)
	phases.mu.Lock()
	phases.ran = nil
	phases.mu.Unlock()
	t.Cleanup(func() { stall.once.Do(func() { close(stall.release) }) })

	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 2})

	got, err := restore(c.Bind(t.Context()), 0)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if got != "scv" {
		t.Fatalf("got %q, want \"scv\"", got)
	}

	ran := phasesRun()
	if n := slices.Index(ran, "snapshot"); n < 0 {
		t.Fatalf("the first phase never ran: %v", ran)
	}
	snapshots := 0
	for _, p := range ran {
		if p == "snapshot" {
			snapshots++
		}
	}
	if snapshots != 1 {
		t.Errorf("the snapshot phase ran %d times across the move (%v); a finished phase must be replayed, not repeated", snapshots, ran)
	}
	// The phase that was in flight is the one that is paid for twice, and that
	// is the irreducible cost: nobody can say whether it finished.
	copies := 0
	for _, p := range ran {
		if p == "copy" {
			copies++
		}
	}
	if copies != 2 {
		t.Errorf("the interrupted phase ran %d times (%v), want 2", copies, ran)
	}
}

// A step is identified by its position, so a work function that reorders them
// between attempts would hand back somebody else's value. It is told instead.
func TestAStepThatMovesIsReported(t *testing.T) {
	st := &beatState{
		job:   "j",
		steps: []stepRecord{{Index: 0, Name: "first"}},
	}
	ctx := withBeat(context.Background(), st)

	if _, err := Step(ctx, "second", func(ctx context.Context) (int, error) {
		t.Error("the body ran despite the position holding a different step")
		return 0, nil
	}); err == nil {
		t.Fatal("want an error when a step's name does not match its position")
	} else if !strings.Contains(err.Error(), "same order") {
		t.Fatalf("got %v", err)
	}
}

// Outside a work function there is no job to record a phase of, and running the
// body anyway would be a silent lie about what Step does.
func TestStepOutsideAJobIsAnError(t *testing.T) {
	ran := false
	_, err := Step(context.Background(), "x", func(ctx context.Context) (int, error) {
		ran = true
		return 1, nil
	})
	if err == nil {
		t.Fatal("want an error from a step with no job")
	}
	if ran {
		t.Error("the body ran outside a job")
	}
}

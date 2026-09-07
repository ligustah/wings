package flowtest_test

import (
	"context"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/flowtest"
)

// THE POINT: a workflow that runs also replays clean, and the harness checks
// both.
func TestHarnessRunsAndReplays(t *testing.T) {
	body := func(ctx flow.Context) error {
		if _, err := ctx.Now(); err != nil {
			return err
		}
		_, err := ctx.Effect(func() (int, error) { return 7, nil })
		return err
	}
	h := flowtest.New(t)
	h.Run("job", body)
	h.Replay("job", body)
}

// THE POINT: a long sleep costs no real time; virtual time moves by the slept
// duration instead.
func TestVirtualClockSkipsLongSleeps(t *testing.T) {
	var before, after time.Time
	h := flowtest.New(t)
	start := time.Now()
	h.Run("nightly", func(ctx flow.Context) error {
		var err error
		if before, err = ctx.Now(); err != nil {
			return err
		}
		if err := ctx.Sleep(24 * time.Hour); err != nil {
			return err
		}
		after, err = ctx.Now()
		return err
	})
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("a 24h sleep took %s of real time; the virtual clock should skip it", elapsed)
	}
	if skipped := after.Sub(before); skipped < 24*time.Hour {
		t.Fatalf("virtual time advanced by %s, want at least 24h", skipped)
	}
}

// THE POINT: Replay catches a body whose flow operations no longer match what
// was recorded — the nondeterminism an author most wants caught.
func TestReplayDetectsDivergence(t *testing.T) {
	h := flowtest.New(t)
	takeNow := true
	body := func(ctx flow.Context) error {
		if takeNow {
			_, err := ctx.Now()
			return err
		}
		_, err := ctx.Effect(func() (int, error) { return 0, nil })
		return err
	}
	if err := flow.Run(context.Background(), "drift", body, h.Options()...); err != nil {
		t.Fatalf("run: %v", err)
	}

	takeNow = false // the code "changed" under the finished run
	err := flow.Replay(context.Background(), "drift", body, h.Options()...)
	if !flow.IsContinuity(err) {
		t.Fatalf("got %v, want a continuity error from the divergent replay", err)
	}
}

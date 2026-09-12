package flow_test

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// THE POINT: Blocking records its result like Effect, so its work runs once and a
// retry is handed the recorded value rather than running the work again.
func TestBlockingIsRecordedAndReplayed(t *testing.T) {
	var calls atomic.Int32
	var seen []int

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		v, err := ctx.Blocking(func() (int, error) {
			return int(calls.Add(1)), nil
		})
		if err != nil {
			return err
		}
		seen = append(seen, v)
		if len(seen) == 1 {
			return errors.New("fail once")
		}
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.Backoff(0, 0))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("the blocking work ran %d times, want once", calls.Load())
	}
	if len(seen) != 2 || seen[0] != 1 || seen[1] != 1 {
		t.Fatalf("saw %v; the retry must be handed the recorded value", seen)
	}
}

// THE POINT: the work runs on its own goroutine while the calling thread stays to
// keep the transaction alive, yet its result and error come back as from a plain
// call.
func TestBlockingReturnsResultAndError(t *testing.T) {
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		v, err := ctx.Blocking(func() (string, error) {
			time.Sleep(10 * time.Millisecond)
			return "done", nil
		})
		if err != nil || v != "done" {
			t.Fatalf("got (%q, %v), want (\"done\", nil)", v, err)
		}
		_, err = ctx.Blocking(func() (int, error) {
			return 0, errors.New("boom")
		})
		if err == nil || err.Error() != "boom" {
			t.Fatalf("got %v, want the work's error", err)
		}
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.Once())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestBlockingOutsideARunIsAnError(t *testing.T) {
	var ctx flow.Context
	if _, err := ctx.Blocking(func() (int, error) { return 1, nil }); err == nil {
		t.Fatal("Blocking outside a run must fail rather than run the function unrecorded")
	}
}

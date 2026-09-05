package flow_test

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: a non-deterministic call wrapped in Effect runs once. The retry
// gets the recorded answer, and an answer that was an error is an error again
// rather than a second try.
func TestEffectIsRecordedAndReplayed(t *testing.T) {
	type host struct {
		Name string `json:"name"`
	}
	var calls, failures atomic.Int32
	var seen []host
	var errs []error

	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		h, err := ctx.Effect(func() (host, error) {
			return host{Name: "worker-" + string(rune('a'+calls.Add(1)-1))}, nil
		})
		if err != nil {
			return err
		}
		seen = append(seen, h)

		_, err = ctx.Effect(func() (int, error) {
			failures.Add(1)
			return 0, errors.New("the disk was full")
		})
		errs = append(errs, err)

		if len(seen) == 1 {
			return errors.New("fail once")
		}
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.Backoff(0, 0))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if calls.Load() != 1 || failures.Load() != 1 {
		t.Fatalf("the effects ran %d and %d times, want once each", calls.Load(), failures.Load())
	}
	if len(seen) != 2 || seen[0] != seen[1] || seen[0].Name != "worker-a" {
		t.Fatalf("saw %+v; the retry must be handed the recorded value", seen)
	}
	if len(errs) != 2 || errs[0] == nil || errs[1] == nil || errs[1].Error() != "the disk was full" {
		t.Fatalf("errors %v; a recorded failure is replayed as the same failure", errs)
	}
}

// THE POINT: an effect that shows up where the history has something else is
// a continuity error, like any other change of shape between attempts.
func TestEffectOutOfPlaceIsAContinuityError(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()
	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		if _, err := ctx.Now(); err != nil {
			return err
		}
		return errors.New("fail once")
	}, flow.WithStore(store), flow.Once())
	if err == nil {
		t.Fatal("the first attempt was meant to fail")
	}

	err = flow.Run(t.Context(), name, func(ctx flow.Context) error {
		_, err := ctx.Effect(func() (string, error) { return "x", nil })
		return err
	}, flow.WithStore(store), flow.Once())
	if !flow.IsContinuity(err) || !strings.Contains(err.Error(), "EffectEvent") {
		t.Fatalf("got %v, want a continuity error naming the effect", err)
	}
}

func TestEffectOutsideARunIsAnError(t *testing.T) {
	var ctx flow.Context
	if _, err := ctx.Effect(func() (int, error) { return 1, nil }); err == nil {
		t.Fatal("Effect outside a run must fail rather than run the function unrecorded")
	}
}

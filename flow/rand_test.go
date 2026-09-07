package flow_test

import (
	"errors"
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: the seed is drawn once and recorded, so a retry replays the same
// stream of numbers rather than drawing a fresh one.
func TestRandReplaysTheSameSequence(t *testing.T) {
	var runs [][]int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, err := ctx.Rand()
		if err != nil {
			return err
		}
		var draws []int
		for range 5 {
			draws = append(draws, r.Intn(1_000_000))
		}
		runs = append(runs, draws)
		if len(runs) == 1 {
			return errors.New("fail once")
		}
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.Backoff(0, 0))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("body ran %d times, want 2 (one failure, one replay)", len(runs))
	}
	if !slicesEqual(runs[0], runs[1]) {
		t.Fatalf("the replay drew %v, want the recorded %v", runs[1], runs[0])
	}
}

// THE POINT: each Rand call has its own recorded seed, so two sources drawn in
// one run are independent rather than identical.
func TestRandCallsAreIndependent(t *testing.T) {
	var a, b int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r1, err := ctx.Rand()
		if err != nil {
			return err
		}
		r2, err := ctx.Rand()
		if err != nil {
			return err
		}
		a, b = r1.Int(), r2.Int()
		return nil
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if a == b {
		t.Fatalf("two sources drew the same first value %d; they must be independent", a)
	}
}

// THE POINT: a UUID is recorded, so a retry sees the same one.
func TestUUIDReplaysTheSameValue(t *testing.T) {
	var ids []string
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		id, err := ctx.UUID()
		if err != nil {
			return err
		}
		ids = append(ids, id)
		if len(ids) == 1 {
			return errors.New("fail once")
		}
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.Backoff(0, 0))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("saw %v; the retry must be handed the recorded UUID", ids)
	}
}

func TestRandOutsideARunIsAnError(t *testing.T) {
	var ctx flow.Context
	if _, err := ctx.Rand(); err == nil {
		t.Fatal("Rand outside a run must fail")
	}
	if _, err := ctx.UUID(); err == nil {
		t.Fatal("UUID outside a run must fail")
	}
}

func slicesEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

package flow_test

import (
	"context"
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: an event delivered from outside reaches a running workflow by name,
// and is what Signal returns.
func TestSignalIsDelivered(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()

	go func() {
		// Delivered before the run asks; the value stream holds it.
		if err := flow.Deliver(context.Background(), store, name, "approval", "ok"); err != nil {
			t.Errorf("Deliver: %v", err)
		}
	}()

	var got string
	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		v, err := ctx.Signal[string]("approval")
		got = v
		return err
	}, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != "ok" {
		t.Fatalf("Signal returned %q, want the delivered %q", got, "ok")
	}
}

// THE POINT: successive signals of one name arrive in order, and each is
// recorded, so a replay returns the same sequence.
func TestSignalsArriveInOrderAndReplay(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()

	for i := 1; i <= 3; i++ {
		if err := flow.Deliver(context.Background(), store, name, "n", i); err != nil {
			t.Fatalf("Deliver %d: %v", i, err)
		}
	}

	var got []int
	body := func(ctx flow.Context) error {
		for range 3 {
			v, err := ctx.Signal[int]("n")
			if err != nil {
				return err
			}
			got = append(got, v)
		}
		return nil
	}
	if err := flow.Run(t.Context(), name, body, flow.WithStore(store)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []int{1, 2, 3}
	if !slicesEqual(got, want) {
		t.Fatalf("signals arrived as %v, want %v", got, want)
	}

	// Replay observes the recorded signals rather than waiting for them again.
	if err := flow.Replay(t.Context(), name, body, flow.WithStore(store)); err != nil {
		t.Fatalf("replay: %v", err)
	}
}

func TestSignalOutsideARunIsAnError(t *testing.T) {
	var ctx flow.Context
	if _, err := ctx.Signal[int]("x"); err == nil {
		t.Fatal("Signal outside a run must fail")
	}
}

func TestDeliverNeedsAStore(t *testing.T) {
	if err := flow.Deliver[int](context.Background(), nil, "r", "n", 1); err == nil {
		t.Fatal("Deliver with no store must fail")
	}
	if err := flow.Deliver(context.Background(), flow.NewMemStore(), "", "n", 1); err == nil {
		t.Fatal("Deliver with no run must fail")
	}
}

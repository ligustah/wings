package wings

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ligustah/durable_streams/dswire"
)

// TestAwaitPlacementRetriesThenSucceeds shows the coordinator's stream open waits
// out the "not placed yet" window during p2p bring-up rather than failing — the
// race a GCP chaos run hit when a joining worker's streams were opened before
// their placement had propagated over the overlay.
func TestAwaitPlacementRetriesThenSucceeds(t *testing.T) {
	calls := 0
	err := awaitPlacement(context.Background(), func() error {
		calls++
		if calls < 3 {
			return fmt.Errorf("open: %w", dswire.ErrStreamNotPlacedYet)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("awaitPlacement: %v", err)
	}
	if calls != 3 {
		t.Fatalf("open called %d times, want 3 (it must retry the not-placed transient)", calls)
	}
}

// TestAwaitPlacementDoesNotRetryOtherErrors shows a real error fails at once.
func TestAwaitPlacementDoesNotRetryOtherErrors(t *testing.T) {
	boom := errors.New("boom")
	calls := 0
	err := awaitPlacement(context.Background(), func() error {
		calls++
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if calls != 1 {
		t.Fatalf("open called %d times, want 1 (a non-transient error is not retried)", calls)
	}
}

// TestAwaitPlacementIsBounded shows a stream that never places fails with the
// last not-placed error once ctx is done, rather than blocking bring-up forever.
func TestAwaitPlacementIsBounded(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := awaitPlacement(ctx, func() error {
		return fmt.Errorf("open: %w", dswire.ErrStreamNotPlacedYet)
	})
	if !errors.Is(err, dswire.ErrStreamNotPlacedYet) {
		t.Fatalf("err = %v, want ErrStreamNotPlacedYet", err)
	}
}

package wings

import (
	"context"
	"testing"
	"time"
)

// THE POINT: a slot given up goes to a thread resuming a wait before it goes
// to a thread that has not started. Finishing what is under way is worth
// more than beginning something new, and a resumed thread is usually one
// another thread is waiting on.
func TestAResumedThreadIsServedBeforeNewWork(t *testing.T) {
	s := newSlots(1)
	if err := s.acquire(context.Background(), false); err != nil {
		t.Fatal(err)
	}

	order := make(chan string, 2)
	wait := func(who string, urgent bool) {
		if err := s.acquire(context.Background(), urgent); err != nil {
			t.Error(err)
			return
		}
		order <- who
		s.release()
	}
	go wait("new", false)
	time.Sleep(20 * time.Millisecond) // queued first
	go wait("resumed", true)
	time.Sleep(20 * time.Millisecond)

	s.release()
	if got := <-order; got != "resumed" {
		t.Fatalf("the slot went to %q first, want the resumed thread", got)
	}
	if got := <-order; got != "new" {
		t.Fatalf("then to %q, want the new work", got)
	}
}

// A waiter that gives up leaves the queue, and a slot handed to it in that
// instant is not lost.
func TestAWaiterThatGivesUpDoesNotKeepTheSlot(t *testing.T) {
	s := newSlots(1)
	if err := s.acquire(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.acquire(ctx, false) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; err == nil {
		t.Fatal("want the cancelled acquire to fail")
	}
	s.release()

	// Both slots' worth of capacity is back: one acquire succeeds at once.
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.acquire(ctx, false); err != nil {
		t.Fatalf("the slot was lost: %v", err)
	}
	if s.free != 0 {
		t.Fatalf("free = %d after taking the only slot, want 0", s.free)
	}
}

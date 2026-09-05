package flow_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// received records, per attempt, the order in which the run took values off a
// channel. Determinism across a retry is the whole claim, and the order is the
// only place it is visible.
var received struct {
	mu   sync.Mutex
	byGo [][]int
}

func recordOrder(order []int) {
	received.mu.Lock()
	defer received.mu.Unlock()
	received.byGo = append(received.byGo, slices.Clone(order))
}

func resetOrders() {
	received.mu.Lock()
	defer received.mu.Unlock()
	received.byGo = nil
}

func orders() [][]int {
	received.mu.Lock()
	defer received.mu.Unlock()
	return slices.Clone(received.byGo)
}

// countEvents tallies a run's history by payload kind.
func countEvents(t *testing.T, store flow.Store, run string) map[string]int {
	t.Helper()
	evs, err := store.Events(context.Background(), run)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	out := map[string]int{}
	for _, ev := range evs {
		out[protos.EventType(ev)]++
	}
	return out
}

// A channel between threads has to work like a Go channel first, or none of
// the replay machinery underneath it matters.
func TestAChannelCarriesValuesBetweenThreads(t *testing.T) {
	var got int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx context.Context) error {
		ch := flow.NewChannel[int](ctx)

		producer := flow.Spawn(ctx, func(ctx context.Context) (int, error) {
			for i := 1; i <= 3; i++ {
				v, err := double(ctx, i)
				if err != nil {
					return 0, err
				}
				if err := ch.Send(ctx, v); err != nil {
					return 0, err
				}
			}
			return 0, ch.Close(ctx)
		})

		total := 0
		for {
			v, ok, err := ch.Recv(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			total += v
		}
		if _, err := producer.Await(ctx); err != nil {
			return err
		}
		got = total
		return nil
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 12 {
		t.Fatalf("got %d, want 12 (2+4+6)", got)
	}
}

// THE POINT: which of two concurrent senders arrives first is the operating
// system's decision, not the run's — so a replay that took "whatever is
// there" would take a different value than the run it is replaying, and every
// decision the run made from that value would be wrong. The receive is
// recorded, and a replay waits for exactly the item it took last time.
func TestAReplayedReceiveTakesTheSameValueItTookBefore(t *testing.T) {
	resetOrders()
	const n = 4

	var attempts atomic.Int64
	var got int
	name := flow.NewName()
	store := flow.NewMemStore()
	err := flow.Run(t.Context(), name, func(ctx context.Context) error {
		ch := flow.NewChannel[int](ctx)

		// Two producers racing. Each sends its own numbers as fast as it can,
		// so the interleaving is genuinely up to the scheduler.
		var producers []*flow.Future[int]
		for p := range 2 {
			producers = append(producers, flow.Spawn(ctx, func(ctx context.Context) (int, error) {
				for i := range n {
					if err := ch.Send(ctx, p*100+i); err != nil {
						return 0, err
					}
				}
				return 0, nil
			}))
		}

		var order []int
		for range 2 * n {
			v, ok, err := ch.Recv(ctx)
			if err != nil {
				return err
			}
			if !ok {
				return errors.New("channel closed early")
			}
			order = append(order, v)
		}
		for _, p := range producers {
			if _, err := p.Await(ctx); err != nil {
				return err
			}
		}
		recordOrder(order)

		// Fail the first two attempts, so the third replays both of them.
		if attempts.Add(1) <= 2 {
			return errors.New("not yet")
		}
		got = len(order)
		return nil
	}, flow.WithStore(store), quick)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 8 {
		t.Fatalf("got %d, want 8", got)
	}

	seen := orders()
	if len(seen) != 3 {
		t.Fatalf("the body ran %d times, want 3", len(seen))
	}
	for i, o := range seen[1:] {
		if !slices.Equal(o, seen[0]) {
			t.Fatalf("attempt %d received %v but the first attempt received %v; "+
				"a replayed receive must take the value it took before", i+2, o, seen[0])
		}
	}

	// And the history must not have grown a set of channel events per attempt.
	// Eight values were sent and eight received, once, however many times the
	// body ran.
	counts := countEvents(t, store, name)
	if counts["ChannelSendEvent"] != 8 {
		t.Errorf("the history holds %d sends, want 8 — a replayed send must be consumed, not appended",
			counts["ChannelSendEvent"])
	}
	if counts["ChannelRecvEvent"] != 8 {
		t.Errorf("the history holds %d receives, want 8", counts["ChannelRecvEvent"])
	}
}

// A buffered channel accepts values with nobody waiting for them, which is the
// only thing that distinguishes it from an unbuffered one.
func TestABufferedSendDoesNotWaitForAReceiver(t *testing.T) {
	const n = 4
	var got int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx context.Context) error {
		ch := flow.NewBufferedChannel[int](ctx, n)

		// Fills the buffer and returns without anybody having received. On an
		// unbuffered channel this thread would still be blocked on its first
		// send when Await was called, and the run would deadlock.
		filler := flow.Spawn(ctx, func(ctx context.Context) (int, error) {
			for i := range n {
				if err := ch.Send(ctx, i); err != nil {
					return 0, err
				}
			}
			return n, ch.Close(ctx)
		})
		if _, err := filler.Await(ctx); err != nil {
			return err
		}

		for {
			_, ok, err := ch.Recv(ctx)
			if err != nil {
				return err
			}
			if !ok {
				return nil
			}
			got++
		}
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 4 {
		t.Fatalf("got %d values back, want 4", got)
	}
}

// Closing must not throw away what was already sent, exactly as with a Go
// channel: receives drain first and only then report the channel closed.
func TestAClosedChannelDrainsBeforeItReportsClosed(t *testing.T) {
	var got string
	err := flow.Run(t.Context(), flow.NewName(), func(ctx context.Context) error {
		ch := flow.NewBufferedChannel[string](ctx, 2)

		sender := flow.Spawn(ctx, func(ctx context.Context) (int, error) {
			if err := ch.Send(ctx, "a"); err != nil {
				return 0, err
			}
			if err := ch.Send(ctx, "b"); err != nil {
				return 0, err
			}
			return 0, ch.Close(ctx)
		})
		if _, err := sender.Await(ctx); err != nil {
			return err
		}

		var parts []string
		for {
			v, ok, err := ch.Recv(ctx)
			if err != nil {
				return err
			}
			if !ok {
				got = strings.Join(parts, "")
				return nil
			}
			parts = append(parts, v)
		}
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != "ab" {
		t.Fatalf("got %q, want \"ab\"", got)
	}
}

// A channel's identity has to come from the run, not from the caller: a name
// somebody chose can be reused or built from something that varies between
// attempts, and this one cannot.
func TestChannelsAreNamedForTheThreadThatMadeThem(t *testing.T) {
	var names []string
	err := flow.Run(t.Context(), flow.NewName(), func(ctx context.Context) error {
		names = nil
		names = append(names, flow.NewChannel[int](ctx).Name())
		names = append(names, flow.NewChannel[int](ctx).Name())

		child := flow.Spawn(ctx, func(ctx context.Context) (int, error) {
			names = append(names, flow.NewChannel[int](ctx).Name())
			return 0, nil
		})
		_, err := child.Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []string{"main.ch0", "main.ch1", "main.0.ch0"}
	if !slices.Equal(names, want) {
		t.Fatalf("channels were named %v, want %v", names, want)
	}
}

// Outside a run there is no history to record a receive in, so a channel
// there is exactly the non-determinism the type exists to remove. It says so
// rather than working by accident.
func TestAChannelOutsideARunRefusesToBeUsed(t *testing.T) {
	ch := flow.NewChannel[int](context.Background())

	if err := ch.Send(context.Background(), 1); err == nil {
		t.Fatal("want an error from a send outside a run")
	} else if !strings.Contains(err.Error(), "outside a Run") {
		t.Fatalf("got %v", err)
	}
	if _, _, err := ch.Recv(context.Background()); err == nil {
		t.Fatal("want an error from a receive outside a run")
	}
	if err := ch.Close(context.Background()); err == nil {
		t.Fatal("want an error from a close outside a run")
	}
}

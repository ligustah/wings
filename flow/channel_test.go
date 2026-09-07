package flow_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

// allEvents is every event of every thread of a run that still has a
// history — main's, and any thread not yet joined.
func allEvents(store *flow.MemStore, run string) ([]*protos.Event, error) {
	var all []*protos.Event
	for _, thread := range store.Threads(run) {
		evs, err := store.Events(context.Background(), run, thread)
		if err != nil {
			return nil, err
		}
		all = append(all, evs...)
	}
	return all, nil
}

// A send records no value: only the receiver's copy is read back on replay, so
// storing the sender's too kept every value on disk twice.
func TestSendRecordsNoValue(t *testing.T) {
	store := flow.NewMemStore()
	err := flow.Run(context.Background(), "sendval", func(c flow.Context) error {
		ch := c.NewBufferedChannel[int](4)
		if err := ch.Send(c, 7); err != nil {
			return err
		}
		v, ok, err := ch.Recv(c)
		if err != nil || !ok || v != 7 {
			return fmt.Errorf("recv %d %v %v", v, ok, err)
		}
		return nil
	}, flow.WithStore(store))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	evs, err := allEvents(store, "sendval")
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var sends, recvs int
	for _, ev := range evs {
		if s := ev.GetChannelSend(); s != nil && !s.GetClosed() && !s.GetRefused() {
			sends++
			if s.GetValue() != nil {
				t.Errorf("send %s#%d recorded a value; sends should carry none", s.GetChannel(), s.GetSeq())
			}
		}
		if r := ev.GetChannelRecv(); r != nil && !r.GetClosed() {
			recvs++
			if r.GetValue().GetSerialized() == nil {
				t.Errorf("recv from %s#%d recorded no value; the receiver's copy is read back on replay",
					r.GetFromThreadId(), r.GetFromSeq())
			}
		}
	}
	if sends != 1 || recvs != 1 {
		t.Fatalf("got %d sends, %d recvs; want 1 each", sends, recvs)
	}
}

// THE POINT: resuming a channel-heavy run does not load every received value
// into memory. Values are dropped from the replay slice at load and read back
// one at a time from their offsets on the durable store, so a replay holds the
// run's metadata, not its whole traffic — the resume side of the coordinator
// memory issue. Uses the durable store (not MemStore, whose Read hands back the
// very events it keeps, so a replay would alias them and hide the cost).
func TestReplayDoesNotHoldReceivedValues(t *testing.T) {
	const n, size = 2000, 8192 // ~16 MB of received payload

	body := func(sum *int) func(flow.Context) error {
		return func(c flow.Context) error {
			ch := c.NewUnboundedChannel[[]byte]()
			for i := 0; i < n; i++ {
				b := make([]byte, size)
				for j := range b {
					b[j] = byte(i + j) // varied, so the codec cannot compress it away
				}
				if err := ch.Send(c, b); err != nil {
					return err
				}
			}
			if err := ch.Close(c); err != nil {
				return err
			}
			for {
				v, ok, err := ch.Recv(c)
				if err != nil {
					return err
				}
				if !ok {
					break
				}
				*sum += len(v) // touch it, do not retain it
			}
			return nil
		}
	}

	client, _ := streams(t, filepath.Join(t.TempDir(), "engine"))
	store := flow.NewStore(client)
	name := flow.NewName()

	var recSum int
	if err := flow.Run(t.Context(), name, body(&recSum), flow.WithStore(store)); err != nil {
		t.Fatalf("record: %v", err)
	}
	if recSum != n*size {
		t.Fatalf("recorded sum %d, want %d", recSum, n*size)
	}

	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	before := m.HeapAlloc

	var grew int64
	var repSum int
	replay := func(c flow.Context) error {
		if err := body(&repSum)(c); err != nil {
			return err
		}
		// Measured while the thread is still live and holding its replay slice.
		runtime.GC()
		runtime.ReadMemStats(&m)
		grew = int64(m.HeapAlloc) - int64(before)
		return nil
	}
	if err := flow.Replay(t.Context(), name, replay, flow.WithStore(store)); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if repSum != n*size {
		t.Fatalf("replayed sum %d, want %d", repSum, n*size)
	}
	if grew > int64(n*size/4) {
		t.Fatalf("replay held %d bytes after replaying %d received values of %d bytes (%.0f%% of the payload); "+
			"received values are being held in memory on replay", grew, n, size, 100*float64(grew)/float64(n*size))
	}
}

// countEvents tallies a run's history by payload kind.
func countEvents(t *testing.T, store *flow.MemStore, run string) map[string]int {
	t.Helper()
	evs, err := allEvents(store, run)
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
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()

		producer := ctx.Spawn(func(ctx flow.Context) (int, error) {
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
	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()

		// Two producers racing. Each sends its own numbers as fast as it can,
		// so the interleaving is genuinely up to the scheduler.
		var producers []*flow.Future[int]
		for p := range 2 {
			producers = append(producers, ctx.Spawn(func(ctx flow.Context) (int, error) {
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
	// Eight values were received, once, however many times the body ran. (The
	// sends are in the producers' own histories, which went when the
	// producers were joined.)
	counts := countEvents(t, store, name)
	if counts["ChannelRecvEvent"] != 8 {
		t.Errorf("the history holds %d receives, want 8 — a replayed receive must be consumed, not appended",
			counts["ChannelRecvEvent"])
	}
	if counts["JoinEvent"] != 2 {
		t.Errorf("the history holds %d joins, want 2", counts["JoinEvent"])
	}
}

// A buffered channel accepts values with nobody waiting for them, which is the
// only thing that distinguishes it from an unbuffered one.
func TestABufferedSendDoesNotWaitForAReceiver(t *testing.T) {
	const n = 4
	var got int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewBufferedChannel[int](n)

		// Fills the buffer and returns without anybody having received. On an
		// unbuffered channel this thread would still be blocked on its first
		// send when Await was called, and the run would deadlock.
		filler := ctx.Spawn(func(ctx flow.Context) (int, error) {
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
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewBufferedChannel[string](2)

		sender := ctx.Spawn(func(ctx flow.Context) (int, error) {
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
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		names = nil
		names = append(names, ctx.NewChannel[int]().Name())
		names = append(names, ctx.NewChannel[int]().Name())

		child := ctx.Spawn(func(ctx flow.Context) (int, error) {
			names = append(names, ctx.NewChannel[int]().Name())
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
	ctx := flow.From(context.Background())
	ch := ctx.NewChannel[int]()

	if err := ch.Send(ctx, 1); err == nil {
		t.Fatal("want an error from a send outside a run")
	} else if !strings.Contains(err.Error(), "outside a Run") {
		t.Fatalf("got %v", err)
	}
	if _, _, err := ch.Recv(ctx); err == nil {
		t.Fatal("want an error from a receive outside a run")
	}
	if err := ch.Close(ctx); err == nil {
		t.Fatal("want an error from a close outside a run")
	}
}

// A buffered channel holds as many values as its capacity says and no more:
// the send after that waits for a receive, like a send on an unbuffered one.
func TestABufferedChannelHoldsOnlyItsCapacity(t *testing.T) {
	var secondSent, firstTaken time.Time
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewBufferedChannel[int](1)
		filler := ctx.Spawn(func(ctx flow.Context) (int, error) {
			if err := ch.Send(ctx, 1); err != nil { // the one place
				return 0, err
			}
			if err := ch.Send(ctx, 2); err != nil { // waits for the place to free
				return 0, err
			}
			secondSent = time.Now()
			return 2, nil
		})
		time.Sleep(50 * time.Millisecond)
		if _, _, err := ch.Recv(ctx); err != nil {
			return err
		}
		firstTaken = time.Now()
		if _, _, err := ch.Recv(ctx); err != nil {
			return err
		}
		_, err := filler.Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if secondSent.Before(firstTaken) {
		t.Fatalf("the second send completed at %s, before the first receive at %s",
			secondSent.Format(time.StampMicro), firstTaken.Format(time.StampMicro))
	}
}

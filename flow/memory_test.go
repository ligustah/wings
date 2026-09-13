package flow

import (
	"context"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings/flow/protos"
)

// feedStore is a [Store] whose value-stream Follow hands the reader's pump a fixed
// run of value items and then idles, counting how many the pump actually took
// (yield returned true). The pump's prefetch gate blocks yield when the buffer is
// full, so fed stops climbing there until the reader drains. Every other method is
// a no-op: the test drives only the value pump.
type feedStore struct {
	items []*protos.ChannelItem
	fed   int32
}

func (s *feedStore) Begin(context.Context, string, string) (Tx, error) { return nopTx{}, nil }
func (s *feedStore) Read(context.Context, string, string, int64, int) ([]EventAt, error) {
	return nil, nil
}
func (s *feedStore) Events(context.Context, string, string) ([]*protos.Event, error) {
	return nil, nil
}
func (s *feedStore) Drop(context.Context, string, string) error { return nil }
func (s *feedStore) Follow(ctx context.Context, name string, from int64, yield func(EventAt) bool) error {
	if !strings.HasPrefix(name, ChannelValuePrefix) {
		<-ctx.Done() // the consume stream never carries anything in this test
		return ctx.Err()
	}
	for i := int(from); i < len(s.items); i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		ev := &protos.Event{Payload: protos.PackEventPayload(s.items[i])}
		if !yield(EventAt{Event: ev, Offset: int64(i)}) {
			return nil
		}
		atomic.AddInt32(&s.fed, 1)
	}
	<-ctx.Done()
	return ctx.Err()
}

// nopTx is a transaction that persists nothing, for tests that drive a pump and
// never read history back.
type nopTx struct{}

func (nopTx) Append(context.Context, *protos.Event) error         { return nil }
func (nopTx) AppendTo(context.Context, string, *protos.Event) error { return nil }
func (nopTx) Commit(context.Context) error                        { return nil }
func (nopTx) Flush(context.Context) error                         { return nil }

func heldData(cs *chanState) int {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	n := 0
	for _, it := range cs.items {
		if it.data != nil {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// discardTx persists nothing it is handed — like a real store that has serialized
// the event to disk and no longer holds the Go object. It leaves only what
// threadState itself keeps in memory, so a memory test sees that alone.
type discardTx struct{ st *discardStore }

func (tx discardTx) Append(context.Context, *protos.Event) error { tx.st.n++; return nil }
func (tx discardTx) AppendTo(context.Context, string, *protos.Event) error { return nil }
func (tx discardTx) Commit(context.Context) error { return nil }
func (tx discardTx) Flush(context.Context) error  { return nil }

type discardStore struct{ n int }

func (s *discardStore) Begin(context.Context, string, string) (Tx, error) { return discardTx{s}, nil }
func (s *discardStore) Read(context.Context, string, string, int64, int) ([]EventAt, error) {
	return nil, nil
}
func (s *discardStore) Events(context.Context, string, string) ([]*protos.Event, error) {
	return nil, nil
}
func (s *discardStore) Drop(context.Context, string, string) error { return nil }
func (s *discardStore) Follow(ctx context.Context, _ string, _ int64, _ func(EventAt) bool) error {
	<-ctx.Done()
	return ctx.Err()
}

// THE POINT: an unbounded channel's Send never blocks, so a sender is never
// parked (and so never unloaded, which would replay its whole job). One thread
// sends many values before receiving any — on a bounded or unbuffered channel
// the first send would block forever.
func TestUnboundedChannelSendNeverBlocks(t *testing.T) {
	const n = 5000
	err := Run(context.Background(), "unbounded", func(c Context) error {
		r, w := c.NewChannel[int]()
		for i := 0; i < n; i++ {
			if err := w.Send(c, i); err != nil {
				return err
			}
		}
		if err := w.Close(c); err != nil {
			return err
		}
		sum := 0
		for {
			v, ok, err := r.Recv(c)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			sum += v
		}
		if want := n * (n - 1) / 2; sum != want {
			t.Errorf("drained sum %d, want %d", sum, want)
		}
		return nil
	}, WithStore(NewMemStore()))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
}

// THE POINT: a consumed channel item drops its value, so a drained channel does
// not hold its whole traffic in memory (the second in-memory copy). The pump
// delivers the value (viaPump), which is the only put that keeps the bytes.
func TestChanStateConsumeDropsItemData(t *testing.T) {
	cs := newChanState(4)
	item := cs.put(context.Background(), "main", 0, []byte("payload"), true)
	if item.data == nil {
		t.Fatalf("a pump-delivered value should keep its bytes for the reader")
	}
	cs.consume(item)
	if item.data != nil {
		t.Fatalf("consume left %d bytes on the item; a received value should be dropped", len(item.data))
	}
}

// THE POINT: a drained channel does not keep its received items. The queue is
// pruned of consumed items, so a long drain neither walks an ever-growing list
// (quadratic) nor holds a wave's worth of item structs.
func TestChanStatePrunesReceivedItems(t *testing.T) {
	cs := newChanState(unbounded)
	const n = 5000
	for i := 0; i < n; i++ {
		it := cs.put(context.Background(), "main", uint64(i), []byte("x"), true)
		it.taken = true
		cs.consume(it)
	}
	cs.mu.Lock()
	held := len(cs.items)
	cs.mu.Unlock()
	if held > 256 {
		t.Fatalf("queue held %d items after receiving %d; received items are not pruned", held, n)
	}
}

// THE POINT: find is answered from the byKey index, and the index holds exactly
// the queued items — prune drops an entry with its item, so a drain does not leak
// a map entry per item, and a dedup lookup stays O(1) over a wave's worth of sends.
func TestChanStateFindIndexTracksItems(t *testing.T) {
	cs := newChanState(unbounded)
	const n = 5000
	for i := 0; i < n; i++ {
		cs.put(context.Background(), "main", uint64(i), []byte("x"), true)
	}
	cs.mu.Lock()
	for i := 0; i < n; i++ {
		if cs.find("main", uint64(i)) == nil {
			cs.mu.Unlock()
			t.Fatalf("find missed queued item %d", i)
		}
	}
	if len(cs.byKey) != len(cs.items) {
		idx, items := len(cs.byKey), len(cs.items)
		cs.mu.Unlock()
		t.Fatalf("index holds %d entries, queue holds %d; they must track", idx, items)
	}
	cs.mu.Unlock()

	for i := 0; i < n; i++ {
		it := cs.find("main", uint64(i))
		it.taken = true
		cs.consume(it)
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if len(cs.byKey) != len(cs.items) {
		t.Fatalf("after draining: index holds %d entries, queue holds %d; prune leaks index entries", len(cs.byKey), len(cs.items))
	}
}

// THE POINT: a send keeps only the value's identity — the bytes are on the value
// stream — so a channel's traffic is not held twice in memory. The pump fills the
// bytes back when it reads them off the stream, and a replayed send dedupes to the
// placeholder the first one queued.
func TestChanStateSendKeepsIdentityNotBytes(t *testing.T) {
	cs := newChanState(unbounded)

	it := cs.put(context.Background(), "w/main", 0, []byte("payload"), false)
	if it.data != nil {
		t.Fatalf("a send kept %d bytes; its value is on the stream, not in memory", len(it.data))
	}
	if again := cs.put(context.Background(), "w/main", 0, []byte("payload"), false); again != it {
		t.Fatalf("a replayed send was not deduped to the queued item")
	}

	// The pump delivers the bytes off the stream, filling the placeholder.
	filled := cs.put(context.Background(), "w/main", 0, []byte("payload"), true)
	if filled != it {
		t.Fatalf("the pump queued a fresh item instead of filling the placeholder")
	}
	if it.data == nil {
		t.Fatalf("the pump did not fill the placeholder's bytes for the reader")
	}
}

// THE POINT: the reader's pump holds only a bounded prefetch in memory. A sender
// races far ahead on an unbounded channel, but the pump stops pulling from the
// stream at the window and leaves the backlog there — yet still delivers every
// value once the reader drains, in order.
func TestChanStatePumpBoundsPrefetch(t *testing.T) {
	cs := newChanState(unbounded)

	const n = channelPrefetch * 4
	items := make([]*protos.ChannelItem, n)
	for i := range items {
		items[i] = &protos.ChannelItem{From: "w/main", Seq: uint64(i), Data: []byte("x")}
	}
	store := &feedStore{items: items}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cs.activate(ctx, store, "run/ch", modeRead)

	// It fills to the window and stops — the whole stream is not mirrored in.
	waitFor(t, "the prefetch window to fill", func() bool { return heldData(cs) >= channelPrefetch })
	time.Sleep(50 * time.Millisecond) // a broken gate would overrun in this window
	if h := heldData(cs); h > channelPrefetch {
		t.Fatalf("the pump buffered %d value items, want at most %d", h, channelPrefetch)
	}
	if fed := atomic.LoadInt32(&store.fed); fed > channelPrefetch {
		t.Fatalf("the pump pulled %d values from the stream, want at most %d before the reader drains", fed, channelPrefetch)
	}

	// Draining opens places; the pump advances and eventually delivers them all, in
	// order, never holding more than the window at once.
	for got := 0; got < n; {
		cs.mu.Lock()
		var next *chanItem
		for _, it := range cs.items {
			if it.data != nil && !it.consumed {
				next = it
				break
			}
		}
		cs.mu.Unlock()
		if next == nil {
			waitFor(t, "the pump to deliver the next value", func() bool { return heldData(cs) > 0 })
			continue
		}
		if next.seq != uint64(got) {
			t.Fatalf("received value %d out of order: seq %d", got, next.seq)
		}
		next.taken = true
		cs.consume(next)
		got++
		if h := heldData(cs); h > channelPrefetch {
			t.Fatalf("mid-drain the pump held %d value items, want at most %d", h, channelPrefetch)
		}
	}
	waitFor(t, "the pump to finish the stream", func() bool { return atomic.LoadInt32(&store.fed) == n })
}

// THE POINT: a live thread does not keep the values it records. Its history is
// persisted and replayed from the store, so holding it in memory only grew the
// heap for the run's whole life (the coordinator memory issue).
func TestRunDoesNotRetainRecordedValues(t *testing.T) {
	const n, size = 3000, 8192 // ~24 MB of recorded payload

	store := &discardStore{}

	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	before := m.HeapAlloc

	// Measured inside the body, while the thread is still live and holding its
	// history — the leak is during the run, and the threadState is gone once Run
	// returns.
	var grew int64
	err := Run(context.Background(), "effects", func(c Context) error {
		for i := 0; i < n; i++ {
			if _, err := c.Effect(func() ([]byte, error) {
				b := make([]byte, size)
				for j := range b {
					b[j] = byte(i + j) // varied, so the codec cannot compress it away
				}
				return b, nil
			}); err != nil {
				return err
			}
		}
		runtime.GC()
		runtime.ReadMemStats(&m)
		grew = int64(m.HeapAlloc) - int64(before)
		return nil
	}, WithStore(store))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if store.n < n {
		t.Fatalf("recorded %d events, want at least %d", store.n, n)
	}

	if grew > int64(n*size/4) {
		t.Fatalf("the run held %d bytes mid-run after recording %d values of %d bytes (%.0f%% of the payload); "+
			"recorded values are being kept in memory", grew, n, size, 100*float64(grew)/float64(n*size))
	}
}

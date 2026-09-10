package flow

import (
	"context"
	"runtime"
	"testing"

	"github.com/ligustah/wings/flow/protos"
)

// discardSink persists nothing it is handed — like a real store that has
// serialized the event to disk and no longer holds the Go object. It leaves
// only what threadState itself keeps in memory, so a memory test sees that alone
// and not the sink's own copies (which MemStore would keep).
type discardSink struct{ n int }

func (s *discardSink) Append(_ context.Context, _ *protos.Event) error { s.n++; return nil }

type discardStore struct{ sink discardSink }

func (s *discardStore) Sink(context.Context, string, string) (Sink, error) { return &s.sink, nil }
func (s *discardStore) Read(context.Context, string, string, int64, int) ([]EventAt, error) {
	return nil, nil
}
func (s *discardStore) Events(context.Context, string, string) ([]*protos.Event, error) {
	return nil, nil
}
func (s *discardStore) Drop(context.Context, string, string) error { return nil }

// THE POINT: an unbounded channel's Send never blocks, so a sender is never
// parked (and so never unloaded, which would replay its whole job). One thread
// sends many values before receiving any — on a bounded or unbuffered channel
// the first send would block forever.
func TestUnboundedChannelSendNeverBlocks(t *testing.T) {
	const n = 5000
	err := Run(context.Background(), "unbounded", func(c Context) error {
		ch := c.NewUnboundedChannel[int]()
		for i := 0; i < n; i++ {
			if err := ch.Send(c, i); err != nil {
				return err
			}
		}
		if err := ch.Close(c); err != nil {
			return err
		}
		sum := 0
		for {
			v, ok, err := ch.Recv(c)
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
// not hold its whole traffic in memory (the second in-memory copy).
func TestChanStateConsumeDropsItemData(t *testing.T) {
	cs := newChanState(4)
	item, err := cs.put(context.Background(), "main", 0, []byte("payload"), false)
	if err != nil {
		t.Fatalf("put: %v", err)
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
		it, err := cs.put(context.Background(), "main", uint64(i), []byte("x"), false)
		if err != nil {
			t.Fatalf("put: %v", err)
		}
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
		if _, err := cs.put(context.Background(), "main", uint64(i), []byte("x"), false); err != nil {
			t.Fatalf("put: %v", err)
		}
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

// THE POINT: a run that only holds a write handle for a channel reached from
// another run drops each sent value's bytes — nothing local ever reads them back
// — while keeping the identity, so a replayed send is still deduped.
func TestChanStateWriterOnlyDropsSentBytes(t *testing.T) {
	cs := newChanState(unbounded)
	cs.attached = true
	cs.noteRole(modeWrite) // a writer handle leaves reads unset

	it, err := cs.put(context.Background(), "w/main", 0, []byte("payload"), false)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if it.data != nil {
		t.Fatalf("a writer-only run kept %d bytes of a sent value", len(it.data))
	}
	if again, _ := cs.put(context.Background(), "w/main", 0, []byte("payload"), false); again != it {
		t.Fatalf("a replayed send was not deduped to the queued item")
	}

	// A read-capable handle keeps the bytes: the run may receive them.
	rs := newChanState(unbounded)
	rs.attached = true
	rs.noteRole(modeBoth)
	kept, err := rs.put(context.Background(), "w/main", 0, []byte("payload"), false)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if kept.data == nil {
		t.Fatalf("a read-capable run dropped a value it may receive")
	}
}

// grantOnWant is a channel host stub: when a receiver announces a want it queues
// a value and grants it at once, the way a real host answers a receive (the value
// precedes its grant on the record).
type grantOnWant struct {
	cs  *chanState
	seq uint64
}

func (g *grantOnWant) Send(ctx context.Context, it ChannelItem) error {
	if !it.Want {
		return nil
	}
	seq := g.seq
	g.seq++
	if _, err := g.cs.put(ctx, "s", seq, []byte("v"), false); err != nil {
		return err
	}
	g.cs.grant(ChannelItem{From: "s", Seq: seq, To: it.From, ToSeq: it.Seq})
	return nil
}

func (g *grantOnWant) Items(context.Context, func(ChannelItem) bool) error { return nil }
func (g *grantOnWant) Close() error                                        { return nil }

// THE POINT: a shared channel does not keep an entry per granted receive. awaitAny
// records a want so it is not re-sent, then drops it once the grant lands, so a
// long drain does not hold a map entry per receive for the attempt's life.
func TestChanStateAwaitAnyPrunesGrantedWants(t *testing.T) {
	cs := newChanState(unbounded)
	cs.link = &grantOnWant{cs: cs}
	th := &threadState{run: &runState{name: "r"}, id: "main"}
	const n = 5000
	for i := 0; i < n; i++ {
		it, err := cs.awaitAny(context.Background(), th, "ch", uint64(i))
		if err != nil {
			t.Fatalf("awaitAny: %v", err)
		}
		if it == nil {
			t.Fatalf("receive %d got no item", i)
		}
		cs.consume(it)
	}
	cs.mu.Lock()
	asked, granted := len(cs.asked), len(cs.granted)
	cs.mu.Unlock()
	if asked > 0 || granted > 0 {
		t.Fatalf("after %d granted receives: asked=%d, granted=%d; both should prune to 0", n, asked, granted)
	}
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
	if store.sink.n < n {
		t.Fatalf("recorded %d events, want at least %d", store.sink.n, n)
	}

	if grew > int64(n*size/4) {
		t.Fatalf("the run held %d bytes mid-run after recording %d values of %d bytes (%.0f%% of the payload); "+
			"recorded values are being kept in memory", grew, n, size, 100*float64(grew)/float64(n*size))
	}
}

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

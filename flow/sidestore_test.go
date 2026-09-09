package flow_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// splitStore is an in-memory [flow.Store] that moves a receive's value onto a
// side stream, keeping only metadata in the event — the shape a reclaiming store
// takes. It implements [flow.ValueReader] so a replay can read the values back.
type splitStore struct {
	mu     sync.Mutex
	events map[string]map[string][]*protos.Event
	values map[string]map[string][][]byte
}

func newSplitStore() *splitStore {
	return &splitStore{
		events: map[string]map[string][]*protos.Event{},
		values: map[string]map[string][][]byte{},
	}
}

func (s *splitStore) Sink(ctx context.Context, run, thread string) (flow.Sink, error) {
	return sinkFn(func(ctx context.Context, ev *protos.Event) error {
		s.mu.Lock()
		defer s.mu.Unlock()
		if rv := ev.GetChannelRecv(); rv != nil && !rv.GetClosed() && rv.GetValue() != nil {
			if s.values[run] == nil {
				s.values[run] = map[string][][]byte{}
			}
			s.values[run][thread] = append(s.values[run][thread], rv.GetValue().GetSerialized())
			stripped := proto.Clone(ev).(*protos.Event)
			stripped.GetChannelRecv().Value = nil
			ev = stripped
		}
		if s.events[run] == nil {
			s.events[run] = map[string][]*protos.Event{}
		}
		s.events[run][thread] = append(s.events[run][thread], ev)
		return nil
	}), nil
}

func (s *splitStore) Read(ctx context.Context, run, thread string, offset int64, n int) ([]flow.EventAt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	evs := s.events[run][thread]
	if offset < 0 {
		offset = 0
	}
	var out []flow.EventAt
	for i := offset; i < int64(len(evs)) && len(out) < n; i++ {
		out = append(out, flow.EventAt{Event: evs[i], Offset: i})
	}
	return out, nil
}

func (s *splitStore) Events(ctx context.Context, run, thread string) ([]*protos.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events[run][thread], nil
}

func (s *splitStore) Drop(ctx context.Context, run, thread string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.events[run], thread)
	delete(s.values[run], thread)
	return nil
}

func (s *splitStore) ReadValues(ctx context.Context, run, thread string, index int64, n int) ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	vals := s.values[run][thread]
	var out [][]byte
	for i := index; i < int64(len(vals)) && len(out) < n; i++ {
		out = append(out, vals[i])
	}
	return out, nil
}

type sinkFn func(ctx context.Context, ev *protos.Event) error

func (f sinkFn) Append(ctx context.Context, ev *protos.Event) error { return f(ctx, ev) }

// THE POINT: when a store moves received values onto a side stream, a replay
// reads each one back from there by its index, not from the event — so a
// receiver resumes with exactly the values it took, though the events carry only
// metadata. The body fails twice so the third attempt replays every receive.
func TestAReplayReadsValuesFromASideStream(t *testing.T) {
	store := newSplitStore()
	const n = 6
	var attempts atomic.Int64
	var got int
	name := flow.NewName()

	err := flow.Run(t.Context(), name, func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		sender := ctx.Spawn(func(ctx flow.Context) (int, error) {
			for i := 1; i <= n; i++ {
				if err := ch.Send(ctx, i); err != nil {
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
		if _, err := sender.Await(ctx); err != nil {
			return err
		}
		if attempts.Add(1) <= 2 {
			return errors.New("not yet")
		}
		got = total
		return nil
	}, flow.WithStore(store), quick)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != n*(n+1)/2 {
		t.Fatalf("replayed sum %d, want %d; a replay read the wrong values back from the side stream", got, n*(n+1)/2)
	}
	if attempts.Load() != 3 {
		t.Fatalf("the body ran %d times, want 3 — no replay was forced", attempts.Load())
	}

	// The values went to the side stream, and the events kept none: proof the
	// replay above had to read them back from the side stream.
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.values[name]["main"]) != n {
		t.Fatalf("the side stream holds %d values, want %d", len(store.values[name]["main"]), n)
	}
	for _, ev := range store.events[name]["main"] {
		if rv := ev.GetChannelRecv(); rv != nil && rv.GetValue() != nil {
			t.Fatal("a recorded receive kept its value inline; it should have been moved to the side stream")
		}
	}
}

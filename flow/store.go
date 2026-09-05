package flow

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow/protos"
)

// Sink is where one run's events are written down.
//
// One method, because a run's history is append-only and nothing in the
// engine ever rewrites it. That is the shape this design gained by moving off
// a relational store: the engine it comes from rewrote the whole run on every
// event — cloning the run, all its threads and all their events, per call —
// which is quadratic in the length of a run. Appending one record is not.
type Sink interface {
	Append(ctx context.Context, ev *protos.Event) error
}

// Store is where histories live: it hands out a Sink to write one run's
// events, and reads them back so a run can resume where it stopped.
//
// A run's history is the business of the process running it — nothing else
// reads it and nothing connects to it — so a Store is expected to be local
// and broker-less.
type Store interface {
	// Sink returns the destination for one run's events. Resolved once per
	// Run rather than per event.
	Sink(ctx context.Context, run string) (Sink, error)

	// Events returns everything recorded for a run, oldest first. A run that
	// has never started yields no events and no error.
	Events(ctx context.Context, run string) ([]*protos.Event, error)
}

// NewName mints a run name nothing else will have. Use it when a run has no
// natural identity of its own; prefer a name derived from what the work is
// about, since that is what makes a resumed run find its history.
func NewName() string { return uuid.NewString() }

func streamName(run string) string {
	return "flow.run." + run
}

// streamStore keeps each run's history on its own durable stream.
//
// Per run rather than one stream for everything because a stream is the unit
// of reading here: resuming a run means replaying exactly its own events, and
// a shared stream would mean reading everyone else's to find them.
type streamStore struct {
	client *dsclient.Client
}

// NewStore returns a Store backed by a durable-streams client.
func NewStore(client *dsclient.Client) Store { return &streamStore{client: client} }

func (s *streamStore) open(ctx context.Context, run string) (*dsclient.Stream[*protos.Event], error) {
	name := streamName(run)

	ok, err := s.client.StreamExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("flow: check %s: %w", name, err)
	}
	if !ok {
		if err := s.client.CreateStream(ctx, name, nil); err != nil {
			return nil, fmt.Errorf("flow: create %s: %w", name, err)
		}
	}
	// The codec resolves *protos.Event through proto.Message, so events go down
	// as protobuf rather than as JSON of a protobuf. New is not optional: Event
	// is a pointer type, and a codec with no way to allocate one can encode but
	// cannot decode — which shows up only when the history is read back, which
	// is to say on the resume that the whole design exists for.
	st, err := s.client.OpenStream[*protos.Event](name, dsclient.WithCodec[*protos.Event](
		dswire.ReflectCodec[*protos.Event]{New: func() *protos.Event { return &protos.Event{} }},
	))
	if err != nil {
		return nil, fmt.Errorf("flow: open %s: %w", name, err)
	}
	return st, nil
}

func (s *streamStore) Sink(ctx context.Context, run string) (Sink, error) {
	st, err := s.open(ctx, run)
	if err != nil {
		return nil, err
	}
	return &streamSink{stream: st}, nil
}

func (s *streamStore) Events(ctx context.Context, run string) ([]*protos.Event, error) {
	st, err := s.open(ctx, run)
	if err != nil {
		return nil, err
	}

	var out []*protos.Event
	for from := int64(0); ; {
		recs, err := st.Read(ctx, from, 512)
		if err != nil {
			return nil, fmt.Errorf("flow: read %s at %d: %w", streamName(run), from, err)
		}
		if len(recs) == 0 {
			return out, nil
		}
		for _, r := range recs {
			out = append(out, r.Record)
			from = r.Offset + 1
		}
	}
}

type streamSink struct {
	mu     sync.Mutex
	stream *dsclient.Stream[*protos.Event]
}

// Append is serialised because forked threads append concurrently and a stream
// handle carries a position. The lock is not contended in practice: an append
// is the tail of a call that just took milliseconds at least.
func (s *streamSink) Append(ctx context.Context, ev *protos.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.stream.Append(ctx, []*protos.Event{ev})
	return err
}

// MemStore keeps history in memory.
//
// A run on one of these still replays within a process — a retry after a
// failed call costs nothing it already paid for — it just does not survive
// the process. Right for tests, and for work short enough that a crash means
// starting over anyway.
type MemStore struct {
	mu     sync.Mutex
	events map[string][]*protos.Event
}

// NewMemStore returns a Store that keeps history in memory only.
func NewMemStore() *MemStore { return &MemStore{events: map[string][]*protos.Event{}} }

func (m *MemStore) Sink(ctx context.Context, run string) (Sink, error) {
	return sinkFunc(func(ctx context.Context, ev *protos.Event) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.events[run] = append(m.events[run], ev)
		return nil
	}), nil
}

func (m *MemStore) Events(ctx context.Context, run string) ([]*protos.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events[run], nil
}

type sinkFunc func(ctx context.Context, ev *protos.Event) error

func (f sinkFunc) Append(ctx context.Context, ev *protos.Event) error { return f(ctx, ev) }

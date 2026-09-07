package flow

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"uuid"

	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow/protos"
)

// Sink is where one thread's events are written down. Append-only.
type Sink interface {
	Append(ctx context.Context, ev *protos.Event) error
}

// Store is where histories live, one per thread of a run: it hands out a Sink to
// write a thread's events, reads them back so the thread can resume, and drops
// them once the thread is over. Per thread because the thread is the unit that
// moves; a Store is expected to be local and broker-less.
type Store interface {
	// Sink returns the destination for one thread's events. Resolved once
	// per attempt of the thread rather than per event.
	Sink(ctx context.Context, run, thread string) (Sink, error)

	// Events returns everything recorded for a thread, oldest first. A
	// thread that has never started yields no events and no error.
	Events(ctx context.Context, run, thread string) ([]*protos.Event, error)

	// Drop discards a thread's history, once the thread has been joined and its
	// result lives in the parent's history. Dropping a thread with no history is
	// not an error.
	Drop(ctx context.Context, run, thread string) error
}

// NewName mints a run name nothing else will have. Prefer a name derived from
// what the work is about, so a resumed run finds its history.
func NewName() string { return uuid.New().String() }

func streamName(run, thread string) string {
	return "flow.thread." + run + "." + thread
}

// streamStore keeps each thread's history on its own durable stream, so resuming
// a thread replays exactly its own events.
type streamStore struct {
	client *dsclient.Client
}

// NewStore returns a Store backed by a durable-streams client.
func NewStore(client *dsclient.Client) Store { return &streamStore{client: client} }

func (s *streamStore) open(ctx context.Context, name string, create bool) (*dsclient.Stream[*protos.Event], error) {
	ok, err := s.client.StreamExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("flow: check %s: %w", name, err)
	}
	if !ok {
		if !create {
			return nil, nil
		}
		if err := s.client.CreateStream(ctx, name, nil); err != nil {
			return nil, fmt.Errorf("flow: create %s: %w", name, err)
		}
	}
	// New is required: *protos.Event is a pointer type, and a codec with no way
	// to allocate one can encode but not decode on resume.
	st, err := s.client.OpenStream[*protos.Event](name, dsclient.WithCodec[*protos.Event](
		dswire.ReflectCodec[*protos.Event]{New: func() *protos.Event { return &protos.Event{} }},
	))
	if err != nil {
		return nil, fmt.Errorf("flow: open %s: %w", name, err)
	}
	return st, nil
}

// Sink opens the thread's stream on first use, so a thread that records nothing
// leaves nothing behind.
func (s *streamStore) Sink(ctx context.Context, run, thread string) (Sink, error) {
	return &streamSink{store: s, name: streamName(run, thread)}, nil
}

func (s *streamStore) Events(ctx context.Context, run, thread string) ([]*protos.Event, error) {
	name := streamName(run, thread)
	st, err := s.open(ctx, name, false)
	if err != nil || st == nil {
		return nil, err
	}

	var out []*protos.Event
	for from := int64(0); ; {
		recs, err := st.Read(ctx, from, 512)
		if err != nil {
			return nil, fmt.Errorf("flow: read %s at %d: %w", name, from, err)
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

func (s *streamStore) Drop(ctx context.Context, run, thread string) error {
	name := streamName(run, thread)
	ok, err := s.client.StreamExists(ctx, name)
	if err != nil {
		return fmt.Errorf("flow: check %s: %w", name, err)
	}
	if !ok {
		return nil
	}
	if err := s.client.DeleteStream(ctx, name); err != nil {
		return fmt.Errorf("flow: drop %s: %w", name, err)
	}
	return nil
}

type streamSink struct {
	store *streamStore
	name  string

	mu     sync.Mutex
	stream *dsclient.Stream[*protos.Event]
}

// Append is serialised because a thread's events can come from more than one
// goroutine and a stream handle carries a position.
func (s *streamSink) Append(ctx context.Context, ev *protos.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stream == nil {
		st, err := s.store.open(ctx, s.name, true)
		if err != nil {
			return err
		}
		s.stream = st
	}
	_, err := s.stream.Append(ctx, []*protos.Event{ev})
	return err
}

// MemStore keeps history in memory: a run still replays within a process but
// does not survive it. Right for tests.
type MemStore struct {
	mu     sync.Mutex
	events map[string]map[string][]*protos.Event // run → thread → events
}

// NewMemStore returns a Store that keeps history in memory only.
func NewMemStore() *MemStore { return &MemStore{events: map[string]map[string][]*protos.Event{}} }

func (m *MemStore) Sink(ctx context.Context, run, thread string) (Sink, error) {
	return sinkFunc(func(ctx context.Context, ev *protos.Event) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		threads := m.events[run]
		if threads == nil {
			threads = map[string][]*protos.Event{}
			m.events[run] = threads
		}
		threads[thread] = append(threads[thread], ev)
		return nil
	}), nil
}

func (m *MemStore) Events(ctx context.Context, run, thread string) ([]*protos.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events[run][thread], nil
}

func (m *MemStore) Drop(ctx context.Context, run, thread string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.events[run], thread)
	return nil
}

// Threads names the threads of a run that have a history, sorted.
func (m *MemStore) Threads(run string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for thread := range m.events[run] {
		out = append(out, thread)
	}
	slices.Sort(out)
	return out
}

type sinkFunc func(ctx context.Context, ev *protos.Event) error

func (f sinkFunc) Append(ctx context.Context, ev *protos.Event) error { return f(ctx, ev) }

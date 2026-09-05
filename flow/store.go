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

// Sink is where one thread's events are written down.
//
// One method, because a thread's history is append-only and nothing in the
// engine ever rewrites it. That is the shape this design gained by moving off
// a relational store: the engine it comes from rewrote the whole run on every
// event — cloning the run, all its threads and all their events, per call —
// which is quadratic in the length of a run. Appending one record is not.
type Sink interface {
	Append(ctx context.Context, ev *protos.Event) error
}

// Store is where histories live, one per thread of a run: it hands out a
// Sink to write a thread's events, reads them back so the thread can resume
// where it stopped, and drops them once the thread is over.
//
// Per THREAD rather than per run because the thread is the unit that moves.
// A run's main thread is one stream; each thread it forks is another, which
// can be written on whichever machine runs that thread and read back on
// whichever runs it next, without touching the rest of the run. A thread's
// history is the business of the process running it — nothing else reads it
// and nothing connects to it — so a Store is expected to be local and
// broker-less.
type Store interface {
	// Sink returns the destination for one thread's events. Resolved once
	// per attempt of the thread rather than per event.
	Sink(ctx context.Context, run, thread string) (Sink, error)

	// Events returns everything recorded for a thread, oldest first. A
	// thread that has never started yields no events and no error.
	Events(ctx context.Context, run, thread string) ([]*protos.Event, error)

	// Drop discards a thread's history. Called once the thread has been
	// joined: its result is in its parent's history from then on, and a
	// replay of the parent never runs it again. Dropping a thread that has
	// no history is not an error.
	Drop(ctx context.Context, run, thread string) error
}

// NewName mints a run name nothing else will have. Use it when a run has no
// natural identity of its own; prefer a name derived from what the work is
// about, since that is what makes a resumed run find its history.
func NewName() string { return uuid.New().String() }

func streamName(run, thread string) string {
	return "flow.thread." + run + "." + thread
}

// streamStore keeps each thread's history on its own durable stream.
//
// Per thread rather than one stream for everything because a stream is the
// unit of reading here: resuming a thread means replaying exactly its own
// events, and a shared stream would mean reading everyone else's to find
// them.
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

// Sink opens the thread's stream on first use rather than here: a thread that
// records nothing should leave nothing behind.
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
// goroutine — the thread's own and the attempt's bookkeeping — and a stream
// handle carries a position. The lock is not contended in practice: an append
// is the tail of a call that just took milliseconds at least.
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

// MemStore keeps history in memory.
//
// A run on one of these still replays within a process — a retry after a
// failed call costs nothing it already paid for — it just does not survive
// the process. Right for tests, and for work short enough that a crash means
// starting over anyway.
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

// Threads names the threads of a run that have a history, sorted. A thread
// that was joined has none: its history was dropped with the join.
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

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

// EventAt pairs an event with its offset on a thread's stream, so a value
// dropped when the history was loaded can be read back by offset later.
type EventAt struct {
	Event  *protos.Event
	Offset int64
}

// Store is where histories live, one per thread of a run: it hands out a Sink to
// write a thread's events, reads them back so the thread can resume, and drops
// them once the thread is over. Per thread because the thread is the unit that
// moves; a Store is expected to be local and broker-less.
type Store interface {
	// Sink returns the destination for one thread's events. Resolved once
	// per attempt of the thread rather than per event.
	Sink(ctx context.Context, run, thread string) (Sink, error)

	// Read returns up to n of a thread's events starting at offset, oldest
	// first, each paired with its offset. Fewer than n (including none) means the
	// history ends there. Reading a thread that never started yields nothing and
	// no error. Replay both streams the history in (offset 0 on) and reads back a
	// single value it dropped (its offset, n=1).
	Read(ctx context.Context, run, thread string, offset int64, n int) ([]EventAt, error)

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

// ListRuns implements [Lister] by reading the broker's stream catalog.
func (s *streamStore) ListRuns(ctx context.Context) ([]string, error) {
	names, err := s.client.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("flow: list streams: %w", err)
	}
	seen := map[string]bool{}
	var out []string
	for _, name := range names {
		if run, _, ok := ParseThreadStream(name); ok && !seen[run] {
			seen[run] = true
			out = append(out, run)
		}
	}
	slices.Sort(out)
	return out, nil
}

// ListThreads implements [Lister] by reading the broker's stream catalog.
func (s *streamStore) ListThreads(ctx context.Context, run string) ([]string, error) {
	names, err := s.client.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("flow: list streams: %w", err)
	}
	var out []string
	for _, name := range names {
		if r, thread, ok := ParseThreadStream(name); ok && r == run {
			out = append(out, thread)
		}
	}
	slices.Sort(out)
	return out, nil
}

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

func (s *streamStore) Read(ctx context.Context, run, thread string, offset int64, n int) ([]EventAt, error) {
	name := streamName(run, thread)
	st, err := s.open(ctx, name, false)
	if err != nil || st == nil {
		return nil, err
	}
	recs, err := st.Read(ctx, offset, n)
	if err != nil {
		return nil, fmt.Errorf("flow: read %s at %d: %w", name, offset, err)
	}
	out := make([]EventAt, len(recs))
	for i, r := range recs {
		out[i] = EventAt{Event: r.Record, Offset: r.Offset}
	}
	return out, nil
}

func (s *streamStore) Tail(ctx context.Context, run, thread string, n int) ([]EventAt, error) {
	if n <= 0 {
		return nil, nil
	}
	name := streamName(run, thread)
	st, err := s.open(ctx, name, false)
	if err != nil || st == nil {
		return nil, err
	}
	info, err := st.Info(ctx)
	if err != nil {
		return nil, fmt.Errorf("flow: info %s: %w", name, err)
	}
	if info.NewestCommitted < 0 {
		return nil, nil
	}
	from := info.NewestCommitted - int64(n) + 1
	if from < 0 {
		from = 0
	}
	var out []EventAt
	for from <= info.NewestCommitted {
		recs, err := st.Read(ctx, from, n)
		if err != nil {
			return nil, fmt.Errorf("flow: read %s at %d: %w", name, from, err)
		}
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			out = append(out, EventAt{Event: r.Record, Offset: r.Offset})
			from = r.Offset + 1
		}
	}
	return out, nil
}

func (s *streamStore) Events(ctx context.Context, run, thread string) ([]*protos.Event, error) {
	var out []*protos.Event
	for from := int64(0); ; {
		batch, err := s.Read(ctx, run, thread, from, 512)
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return out, nil
		}
		for _, e := range batch {
			out = append(out, e.Event)
			from = e.Offset + 1
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

func (m *MemStore) Read(ctx context.Context, run, thread string, offset int64, n int) ([]EventAt, error) {
	m.mu.Lock()
	evs := m.events[run][thread]
	m.mu.Unlock()
	if offset < 0 {
		offset = 0
	}
	var out []EventAt
	for i := offset; i < int64(len(evs)) && len(out) < n; i++ {
		out = append(out, EventAt{Event: evs[i], Offset: i})
	}
	return out, nil
}

func (m *MemStore) Tail(ctx context.Context, run, thread string, n int) ([]EventAt, error) {
	m.mu.Lock()
	evs := m.events[run][thread]
	m.mu.Unlock()
	if n <= 0 || len(evs) == 0 {
		return nil, nil
	}
	from := len(evs) - n
	if from < 0 {
		from = 0
	}
	out := make([]EventAt, 0, len(evs)-from)
	for i := from; i < len(evs); i++ {
		out = append(out, EventAt{Event: evs[i], Offset: int64(i)})
	}
	return out, nil
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

// ListRuns implements [Lister].
func (m *MemStore) ListRuns(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for run := range m.events {
		out = append(out, run)
	}
	slices.Sort(out)
	return out, nil
}

// ListThreads implements [Lister].
func (m *MemStore) ListThreads(ctx context.Context, run string) ([]string, error) {
	return m.Threads(run), nil
}

type sinkFunc func(ctx context.Context, ev *protos.Event) error

func (f sinkFunc) Append(ctx context.Context, ev *protos.Event) error { return f(ctx, ev) }

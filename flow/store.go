package flow

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
	"uuid"

	"github.com/ligustah/commitlog/blockv3"
	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow/protos"
)

// EventAt pairs an event with its offset on a stream, so a value dropped when
// the history was loaded can be read back by offset later.
type EventAt struct {
	Event  *protos.Event
	Offset int64
}

// Store keeps a run's streams — the append-only logs a run is made of. A
// thread's history is one stream, addressed by (run, thread) so the store maps
// it to a name of its own (a worker keeps it under the attempt). A shared
// channel's records are streams too, named globally by [ChannelValueStream] and
// [ChannelConsumeStream] and reached by name. Writes go through a per-thread
// [Tx] so a thread's history event and any channel record it makes commit
// together. A Store is expected to be local and broker-less; carrying a stream
// between machines is the engine's concern, not the store's.
type Store interface {
	// Begin returns the transaction one thread's writes go through, resolved once
	// per attempt of the thread. Appends to it — the thread's own history and any
	// channel stream — commit together.
	Begin(ctx context.Context, run, thread string) (Tx, error)

	// Read returns up to n of a thread's history events at or after offset, oldest
	// first, each with its offset. Fewer than n (including none) means the history
	// ends there. Reading a thread that never started yields nothing and no error.
	Read(ctx context.Context, run, thread string, offset int64, n int) ([]EventAt, error)

	// Events returns everything recorded for a thread, oldest first.
	Events(ctx context.Context, run, thread string) ([]*protos.Event, error)

	// Drop discards a thread's history, once the thread has been joined and its
	// result lives in the parent's history.
	Drop(ctx context.Context, run, thread string) error

	// Follow delivers a named stream's events in order from offset, calling yield
	// for each; it waits for the stream to appear and for new records, returning
	// when yield returns false or ctx ends. A shared channel's reader and a
	// replay's value read-back consume the channel's stream this way.
	Follow(ctx context.Context, stream string, from int64, yield func(EventAt) bool) error
}

// Tx is one thread's transaction over the store: appends — to the thread's own
// history or to a named channel stream — commit together, so a channel send's
// record and the event that justifies it are never torn apart. A Tx is resolved
// once per thread and reused across its attempts.
type Tx interface {
	// Append writes an event to the thread's own history stream.
	Append(ctx context.Context, ev *protos.Event) error
	// AppendTo writes an event to a named stream — a shared channel's — inside this
	// thread's transaction, so it commits with the history event that justifies it.
	AppendTo(ctx context.Context, stream string, ev *protos.Event) error
	// Commit commits what has been written, coalescing under a commit interval a
	// backing store may keep. A store that commits each append on its own does
	// nothing here.
	Commit(ctx context.Context) error
	// Flush force-commits now, as a thread is about to wait, so a value coalesced
	// under a commit interval reaches whoever the thread waits on. A store that
	// commits eagerly does nothing here.
	Flush(ctx context.Context) error
}

// NewName mints a run name nothing else will have. Prefer a name derived from
// what the work is about, so a resumed run finds its history.
func NewName() string { return uuid.New().String() }

const (
	// ChannelValuePrefix and ChannelConsumePrefix begin a shared channel's value
	// and consume stream names. A channel is named by its id alone — no run,
	// thread or attempt — so a writer moved to a new attempt keeps appending to
	// the same stream. The values match the engine's output naming so its pull
	// recognizes and carries these streams home.
	ChannelValuePrefix   = "wings.chanval."
	ChannelConsumePrefix = "wings.chancons."
)

func streamName(run, thread string) string {
	return threadStreamPrefix + run + "." + thread
}

// ChannelValueStream names a channel's value stream: the writer's values and
// closes. ChannelConsumeStream names its consume stream: the reader's consume
// reports and its link marker.
func ChannelValueStream(id string) string   { return ChannelValuePrefix + StreamPart(id) }
func ChannelConsumeStream(id string) string { return ChannelConsumePrefix + StreamPart(id) }

// StreamPart maps everything outside [A-Za-z0-9_-] to an underscore, so an id is
// safe to join into a stream name with dots. The engine's own stream naming
// defers to this, so a channel's streams have one name everywhere.
func StreamPart(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// followBatch is how many records a follow reads at once; followPoll bounds one
// blocking read so a follow re-opens the stream — catching a moved writer's
// first append to a stream a stale handle bound to empty — and re-checks ctx.
const (
	followBatch = 256
	followPoll  = 2 * time.Second
)

// streamStore keeps each stream on its own durable stream, so resuming a thread
// replays exactly its own events and a channel's records are read back by name.
type streamStore struct {
	client      *dsclient.Client
	compression dswire.Compression
}

// StoreOption configures a [NewStore].
type StoreOption func(*streamStore)

// WithStoreCompression sets the storage codec for the streams the store creates.
// Defaults to [dswire.CompressionZstd]; pass [dswire.CompressionNone] to store
// uncompressed.
func WithStoreCompression(c dswire.Compression) StoreOption {
	return func(s *streamStore) { s.compression = c }
}

// NewStore returns a Store backed by a durable-streams client. Streams are
// Zstd-compressed unless [WithStoreCompression] says otherwise.
func NewStore(client *dsclient.Client, opts ...StoreOption) Store {
	s := &streamStore{client: client, compression: dswire.CompressionZstd}
	for _, o := range opts {
		o(s)
	}
	return s
}

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
		if err := s.client.CreateStream(ctx, name, &dsclient.StreamConfig{Compression: s.compression, BlockFormat: int(blockv3.Version)}); err != nil {
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

// Begin opens the thread's transaction. Appends are written straight through —
// this store commits each on its own — so Commit and Flush are no-ops; the
// engine's transactional store is where a channel record and its event coalesce.
func (s *streamStore) Begin(ctx context.Context, run, thread string) (Tx, error) {
	return &streamTx{store: s, hist: streamName(run, thread), streams: map[string]*dsclient.Stream[*protos.Event]{}}, nil
}

func (s *streamStore) Read(ctx context.Context, run, thread string, offset int64, n int) ([]EventAt, error) {
	return s.readStream(ctx, streamName(run, thread), offset, n)
}

func (s *streamStore) readStream(ctx context.Context, name string, offset int64, n int) ([]EventAt, error) {
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

// Follow delivers a named stream's records from offset, waiting for the stream
// to appear and for new records, re-opening each pass so a moved writer's first
// append to a stream a stale handle bound to empty is seen.
func (s *streamStore) Follow(ctx context.Context, name string, from int64, yield func(EventAt) bool) error {
	for ctx.Err() == nil {
		ok, err := s.client.StreamExists(ctx, name)
		if err != nil {
			return fmt.Errorf("flow: check %s: %w", name, err)
		}
		if !ok {
			if err := pause(ctx, 200*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		st, err := s.open(ctx, name, false)
		if err != nil {
			return err
		}
		if st == nil {
			if err := pause(ctx, 200*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		readCtx, cancel := context.WithTimeout(ctx, followPoll)
		recs, err := st.ReadBlocking(readCtx, from, followBatch)
		expired := readCtx.Err() != nil
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !expired {
				if err := pause(ctx, time.Second); err != nil {
					return err
				}
			}
			continue
		}
		for _, r := range recs {
			from = r.Offset + 1
			if !yield(EventAt{Event: r.Record, Offset: r.Offset}) {
				return nil
			}
		}
	}
	return ctx.Err()
}

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// streamTx writes a thread's events straight through to their streams: this
// store is not transactional, so each append is durable at once. It opens each
// stream on first write, so a thread that records nothing leaves nothing behind.
type streamTx struct {
	store *streamStore
	hist  string

	mu      sync.Mutex
	streams map[string]*dsclient.Stream[*protos.Event]
}

func (tx *streamTx) Append(ctx context.Context, ev *protos.Event) error {
	return tx.AppendTo(ctx, tx.hist, ev)
}

// AppendTo is serialised because a thread's events can come from more than one
// goroutine and a stream handle carries a position.
func (tx *streamTx) AppendTo(ctx context.Context, name string, ev *protos.Event) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	st := tx.streams[name]
	if st == nil {
		var err error
		if st, err = tx.store.open(ctx, name, true); err != nil {
			return err
		}
		tx.streams[name] = st
	}
	_, err := st.Append(ctx, []*protos.Event{ev})
	return err
}

func (tx *streamTx) Commit(ctx context.Context) error { return nil }
func (tx *streamTx) Flush(ctx context.Context) error  { return nil }

// MemStore keeps every stream in memory: a run still replays within a process,
// and a shared channel's records pass through its named streams in-process, but
// nothing survives the process. Right for tests and single-program runs.
type MemStore struct {
	mu      sync.Mutex
	streams map[string][]*protos.Event // by stream name
	changed chan struct{}
}

// NewMemStore returns a Store that keeps every stream in memory only.
func NewMemStore() *MemStore {
	return &MemStore{streams: map[string][]*protos.Event{}, changed: make(chan struct{})}
}

func (m *MemStore) appendStream(name string, ev *protos.Event) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.streams[name] = append(m.streams[name], ev)
	close(m.changed)
	m.changed = make(chan struct{})
}

func (m *MemStore) Begin(ctx context.Context, run, thread string) (Tx, error) {
	return &memTx{m: m, hist: streamName(run, thread)}, nil
}

func (m *MemStore) Read(ctx context.Context, run, thread string, offset int64, n int) ([]EventAt, error) {
	return m.readStream(streamName(run, thread), offset, n), nil
}

func (m *MemStore) readStream(name string, offset int64, n int) []EventAt {
	m.mu.Lock()
	evs := m.streams[name]
	m.mu.Unlock()
	if offset < 0 {
		offset = 0
	}
	var out []EventAt
	for i := offset; i < int64(len(evs)) && len(out) < n; i++ {
		out = append(out, EventAt{Event: evs[i], Offset: i})
	}
	return out
}

func (m *MemStore) Tail(ctx context.Context, run, thread string, n int) ([]EventAt, error) {
	m.mu.Lock()
	evs := m.streams[streamName(run, thread)]
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
	return m.streams[streamName(run, thread)], nil
}

func (m *MemStore) Drop(ctx context.Context, run, thread string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.streams, streamName(run, thread))
	return nil
}

// Follow delivers a named stream's records from offset, blocking until more
// arrive or ctx ends.
func (m *MemStore) Follow(ctx context.Context, name string, from int64, yield func(EventAt) bool) error {
	for {
		m.mu.Lock()
		evs := m.streams[name]
		wait := m.changed
		var batch []EventAt
		for i := from; i < int64(len(evs)); i++ {
			batch = append(batch, EventAt{Event: evs[i], Offset: i})
		}
		m.mu.Unlock()
		for _, ea := range batch {
			from = ea.Offset + 1
			if !yield(ea) {
				return nil
			}
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Threads names the threads of a run that have a history, sorted.
func (m *MemStore) Threads(run string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for name := range m.streams {
		if r, thread, ok := ParseThreadStream(name); ok && r == run {
			out = append(out, thread)
		}
	}
	slices.Sort(out)
	return out
}

// ListRuns implements [Lister].
func (m *MemStore) ListRuns(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for name := range m.streams {
		if run, _, ok := ParseThreadStream(name); ok && !seen[run] {
			seen[run] = true
			out = append(out, run)
		}
	}
	slices.Sort(out)
	return out, nil
}

// ListThreads implements [Lister].
func (m *MemStore) ListThreads(ctx context.Context, run string) ([]string, error) {
	return m.Threads(run), nil
}

type memTx struct {
	m    *MemStore
	hist string
}

func (tx *memTx) Append(ctx context.Context, ev *protos.Event) error {
	tx.m.appendStream(tx.hist, ev)
	return nil
}

func (tx *memTx) AppendTo(ctx context.Context, name string, ev *protos.Event) error {
	tx.m.appendStream(name, ev)
	return nil
}

func (tx *memTx) Commit(ctx context.Context) error { return nil }
func (tx *memTx) Flush(ctx context.Context) error  { return nil }

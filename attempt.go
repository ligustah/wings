package wings

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// attemptWorkloadPrefix begins an attempt's transactional id; job stream part
// and attempt number follow.
const attemptWorkloadPrefix = "wings.job."

// Everything one attempt writes — its run history, recordings, byte streams —
// goes under one transactional producer and becomes visible together, so a
// moved job's leftovers cannot disagree. Committed at each heartbeat and step
// (so progress the coordinator is told is never ahead of what it can copy), on
// return, and by age before the backend times the transaction out.

// historyPrefix is the history of the run an attempt executes as; its Name part
// is always "history".
const historyPrefix = "wings.history."

// historyName is the stream a job attempt's history is kept on.
func historyName(job string, attempt int) string {
	return outputName{Prefix: historyPrefix, Job: job, Attempt: attempt, Name: "history"}.String()
}

// valuesPrefix holds the channel receive values one thread took, moved off its
// history so the big bytes can be reclaimed on return while the metadata stays.
// One stream per thread; the Name part is the thread.
const valuesPrefix = "wings.values."

// valuesName is the stream a job attempt keeps one thread's received channel
// values on, alongside the attempt's history.
func valuesName(job string, attempt int, thread string) string {
	return outputName{Prefix: valuesPrefix, Job: job, Attempt: attempt, Name: thread}.String()
}

// attemptOutputs is one attempt's transactional producer and its open transaction.
type attemptOutputs struct {
	node    *workerNode
	job     string
	attempt int
	// budget is how long one transaction may stay open; zero takes the backend default.
	budget time.Duration

	mu       sync.Mutex
	producer dsclient.Producer
	tx       dsclient.Tx
	opened   time.Time
	// err is sticky: after a failed commit the attempt fails rather than write a
	// record with a hole, and its retry resumes from the last commit that took.
	err error

	// flushers hold records in memory (a Recorder's batch) that a commit must
	// push out first.
	flushers map[int]func() error
	nextFl   int
}

func newAttemptOutputs(n *workerNode, job jobEnvelope) *attemptOutputs {
	a := &attemptOutputs{node: n, job: job.ID, attempt: job.Attempt}
	// Budget of 2H keeps a transaction from being reaped under a function that
	// heartbeats every H.
	if bounds, ok := flow.BoundsOf(job.Func); ok && bounds.Heartbeat > 0 {
		a.budget = 2 * bounds.Heartbeat
	}
	return a
}

// producerID is per attempt, so a job moved while its old attempt still lives
// does not fence the new one; the old transaction is abandoned and reaped.
func (a *attemptOutputs) producerID() string {
	return attemptWorkloadPrefix + streamPart(a.job) + "." + strconv.Itoa(a.attempt)
}

// begin opens a transaction if none is open. Call with mu held.
func (a *attemptOutputs) begin(ctx context.Context) error {
	if a.err != nil {
		return a.err
	}
	if a.tx != nil {
		return nil
	}
	if a.producer == nil {
		p, err := a.node.client.Producer(ctx, a.producerID())
		if err != nil {
			a.err = fmt.Errorf("wings: open a producer for job %s: %w", a.job, err)
			return a.err
		}
		a.producer = p
		if a.budget <= 0 {
			a.budget = p.TransactionTimeout()
		}
	}
	tx, err := a.producer.BeginTimeout(ctx, a.budget)
	if err != nil {
		a.err = fmt.Errorf("wings: begin a transaction for job %s: %w", a.job, err)
		return a.err
	}
	a.tx, a.opened = tx, time.Now()
	return nil
}

// append writes values to s inside the attempt's transaction.
func (a *attemptOutputs) append[T any](ctx context.Context, s *dsclient.Stream[T], values []T) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.begin(ctx); err != nil {
		return err
	}
	if _, err := dsclient.Output(a.tx, s).Append(ctx, values); err != nil {
		a.err = fmt.Errorf("wings: write output of job %s: %w", a.job, err)
		return a.err
	}
	// Commit by age, so the backend does not time the transaction out and abort it.
	if time.Since(a.opened) > a.budget/2 {
		return a.commitLocked(ctx)
	}
	return nil
}

func (a *attemptOutputs) commitLocked(ctx context.Context) error {
	if a.tx == nil {
		return a.err
	}
	tx := a.tx
	a.tx = nil
	if err := tx.Commit(ctx); err != nil {
		a.err = fmt.Errorf("wings: commit what job %s wrote: %w", a.job, err)
		return a.err
	}
	return nil
}

// commit flushes every writer's held records and commits the open transaction.
func (a *attemptOutputs) commit(ctx context.Context) error {
	a.mu.Lock()
	flushers := slices.Collect(maps.Values(a.flushers))
	a.mu.Unlock()
	// Outside the lock: a flush appends, which takes it.
	for _, flush := range flushers {
		if err := flush(); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.commitLocked(ctx)
}

// register adds a writer a commit must flush first and returns how to remove it.
func (a *attemptOutputs) register(flush func() error) func() {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.flushers == nil {
		a.flushers = map[int]func() error{}
	}
	id := a.nextFl
	a.nextFl++
	a.flushers[id] = flush
	return func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		delete(a.flushers, id)
	}
}

// finish commits what is left once the function returns, on its own bounded
// context rather than the attempt's, which may be why it stopped.
func (a *attemptOutputs) finish(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), outputAppend)
	defer cancel()
	return a.commit(ctx)
}

// historyStore is the [flow.Store] an attempt's run is kept on. Reading returns
// the previous attempt's history, hydrated onto this worker under this attempt's
// name (see hydrateHistory), so a moved job replays to where its predecessor got.
type historyStore struct {
	a    *attemptOutputs
	name string
}

func (h *historyStore) stream(ctx context.Context) (*dsclient.Stream[*protos.Event], error) {
	st, err := h.a.node.client.OpenStream[*protos.Event](h.name, dsclient.WithCodec[*protos.Event](
		dswire.ReflectCodec[*protos.Event]{New: func() *protos.Event { return &protos.Event{} }},
	))
	if err != nil {
		return nil, fmt.Errorf("wings: open history %s: %w", h.name, err)
	}
	return st, nil
}

// Events reads one thread's events out of the attempt's stream, which holds
// every thread of the run in one order.
func (h *historyStore) Events(ctx context.Context, _, thread string) ([]*protos.Event, error) {
	run := h.name
	ok, err := h.a.node.client.StreamExists(ctx, run)
	if err != nil {
		return nil, fmt.Errorf("wings: look for history %s: %w", run, err)
	}
	if !ok {
		return nil, nil
	}
	st, err := h.stream(ctx)
	if err != nil {
		return nil, err
	}
	var events []*protos.Event
	var from int64
	for {
		recs, err := st.Read(ctx, from, recordBatch)
		if err != nil {
			return nil, fmt.Errorf("wings: read history %s: %w", run, err)
		}
		if len(recs) == 0 {
			return events, nil
		}
		for _, r := range recs {
			if r.Record.GetThreadId() == thread {
				events = append(events, r.Record)
			}
			from = r.Offset + 1
		}
	}
}

// Read returns up to n of one thread's events at or after offset in the
// attempt's combined stream, each with its offset there. Because threads share
// the stream, offsets are sparse per thread; paging by the last offset plus one
// carries on scanning, and n=1 at a known event's offset reads it back.
func (h *historyStore) Read(ctx context.Context, _, thread string, offset int64, n int) ([]flow.EventAt, error) {
	run := h.name
	ok, err := h.a.node.client.StreamExists(ctx, run)
	if err != nil {
		return nil, fmt.Errorf("wings: look for history %s: %w", run, err)
	}
	if !ok {
		return nil, nil
	}
	st, err := h.stream(ctx)
	if err != nil {
		return nil, err
	}
	from := offset
	if from < 0 {
		from = 0
	}
	var out []flow.EventAt
	for len(out) < n {
		recs, err := st.Read(ctx, from, recordBatch)
		if err != nil {
			return nil, fmt.Errorf("wings: read history %s: %w", run, err)
		}
		if len(recs) == 0 {
			return out, nil
		}
		for _, r := range recs {
			from = r.Offset + 1
			if r.Record.GetThreadId() == thread {
				out = append(out, flow.EventAt{Event: r.Record, Offset: r.Offset})
				if len(out) >= n {
					break
				}
			}
		}
	}
	return out, nil
}

func (h *historyStore) Sink(ctx context.Context, _, thread string) (flow.Sink, error) {
	return &historySink{h: h, thread: thread}, nil
}

// Drop is a no-op: a joined thread's events stay in the attempt's stream, which
// goes as a whole when the job settles.
func (h *historyStore) Drop(ctx context.Context, _, _ string) error { return nil }

// valueStream opens one thread's value stream, where its received channel values
// live apart from its history.
func (h *historyStore) valueStream(ctx context.Context, thread string) (*dsclient.Stream[[]byte], error) {
	st, err := h.a.node.client.OpenStream[[]byte](valuesName(h.a.job, h.a.attempt, thread),
		dsclient.WithCodec[[]byte](dswire.RawCodec{}))
	if err != nil {
		return nil, fmt.Errorf("wings: open value stream for thread %s of job %s: %w", thread, h.a.job, err)
	}
	return st, nil
}

// ReadValues implements [flow.ValueReader], reading back the receive values this
// attempt moved off the thread's history. index counts the thread's moved values
// from zero, which is their offset on the stream.
func (h *historyStore) ReadValues(ctx context.Context, _, thread string, index int64, n int) ([][]byte, error) {
	name := valuesName(h.a.job, h.a.attempt, thread)
	ok, err := h.a.node.client.StreamExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("wings: look for value stream %s: %w", name, err)
	}
	if !ok {
		return nil, nil
	}
	st, err := h.valueStream(ctx, thread)
	if err != nil {
		return nil, err
	}
	recs, err := st.Read(ctx, index, n)
	if err != nil {
		return nil, fmt.Errorf("wings: read value stream %s at %d: %w", name, index, err)
	}
	out := make([][]byte, len(recs))
	for i, r := range recs {
		out[i] = r.Record
	}
	return out, nil
}

// historySink appends a run's events inside the attempt's transaction, standing
// the stream up lazily on the first event that belongs to a thread — so a
// function that forks, sleeps and calls nothing leaves nothing behind. A
// received channel value is moved off the event onto the thread's own value
// stream, written in the same transaction so the two stay consistent on a move.
type historySink struct {
	h      *historyStore
	thread string

	mu     sync.Mutex
	stream *dsclient.Stream[*protos.Event]
	values *dsclient.Stream[[]byte]
	held   []*protos.Event
}

func (s *historySink) Append(ctx context.Context, ev *protos.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if rv := ev.GetChannelRecv(); rv != nil && !rv.GetClosed() && rv.GetValue() != nil {
		if s.values == nil {
			name := valuesName(s.h.a.job, s.h.a.attempt, s.thread)
			if err := ensureStream(context.WithoutCancel(ctx), s.h.a.node.client, name); err != nil {
				return err
			}
			vs, err := s.h.valueStream(ctx, s.thread)
			if err != nil {
				return err
			}
			s.values = vs
		}
		if err := s.h.a.append(ctx, s.values, [][]byte{rv.GetValue().GetSerialized()}); err != nil {
			return err
		}
		ev = strippedRecvEvent(ev)
	}

	if s.stream == nil {
		if ev.GetRunStart() != nil || ev.GetRunEnd() != nil {
			// Hold the attempt markers until something worth a stream arrives.
			s.held = append(s.held, ev)
			return nil
		}
		// Not the attempt's context: a half-made stream is the next attempt's problem.
		if err := ensureStream(context.WithoutCancel(ctx), s.h.a.node.client, s.h.name); err != nil {
			return err
		}
		st, err := s.h.stream(ctx)
		if err != nil {
			return err
		}
		s.stream = st
	}
	batch := append(s.held, ev)
	s.held = nil
	return s.h.a.append(ctx, s.stream, batch)
}

// strippedRecvEvent rebuilds a receive event without its value, which has been
// moved to the value stream. Rebuilt field by field rather than cloned so the
// big value bytes are not copied only to be dropped.
func strippedRecvEvent(ev *protos.Event) *protos.Event {
	rv := ev.GetChannelRecv()
	return &protos.Event{
		Timestamp: ev.GetTimestamp(),
		Serial:    ev.GetSerial(),
		Attempt:   ev.GetAttempt(),
		ThreadId:  ev.GetThreadId(),
		Payload: &protos.Event_ChannelRecv{ChannelRecv: &protos.ChannelRecvEvent{
			Channel:      rv.GetChannel(),
			FromThreadId: rv.GetFromThreadId(),
			FromSeq:      rv.GetFromSeq(),
			Closed:       rv.GetClosed(),
		}},
	}
}

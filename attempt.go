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

func (h *historyStore) Sink(ctx context.Context, _, _ string) (flow.Sink, error) {
	return &historySink{h: h}, nil
}

// Drop is a no-op: a joined thread's events stay in the attempt's stream, which
// goes as a whole when the job settles.
func (h *historyStore) Drop(ctx context.Context, _, _ string) error { return nil }

// historySink appends a run's events inside the attempt's transaction, standing
// the stream up lazily on the first event that belongs to a thread — so a
// function that forks, sleeps and calls nothing leaves nothing behind.
type historySink struct {
	h *historyStore

	mu     sync.Mutex
	stream *dsclient.Stream[*protos.Event]
	held   []*protos.Event
}

func (s *historySink) Append(ctx context.Context, ev *protos.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

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

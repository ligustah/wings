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

// One attempt of one job, on the worker running it, writes several streams:
// the history of the run it executes as, the recordings it opens, the files
// it produces. They are written under ONE transactional producer, and they
// become visible together.
//
// That is what makes a moved job's leftovers consistent. A retry is handed
// what its predecessor wrote, and the predecessor's history says which
// recordings it had made by the point it reached; if the two could disagree —
// a recording copied further than the history that explains it — the retry
// would resume from one and replay the other. Committing them as one
// transaction, and copying them home by whole transactions, is what removes
// that. The copy is the mirror's business; this file is the commit.
//
// A transaction is committed at the points that mean something: before every
// heartbeat and step report (so what the coordinator is told about progress
// is never ahead of what it can copy), when the function returns, and — as a
// net under a function that reports nothing for a long time — when the open
// transaction is older than half its budget, since a transaction the backend
// times out takes everything in it along.

// historyPrefix is the history of the run an attempt executes as. See
// attemptOutputs and the worker's runOne. Its Name part is always "history".
const historyPrefix = "wings.history."

// historyName is the stream a job attempt's history is kept on, on the worker
// that runs it and on the coordinator alike.
func historyName(job string, attempt int) string {
	return outputName{Prefix: historyPrefix, Job: job, Attempt: attempt, Name: "history"}.String()
}

// attemptOutputs is one attempt's transactional producer and the transaction
// currently open on it.
type attemptOutputs struct {
	node    *workerNode
	job     string
	attempt int
	// budget is how long one transaction may stay open. Zero takes the
	// backend's default, read once the producer exists.
	budget time.Duration

	mu       sync.Mutex
	producer dsclient.Producer
	tx       dsclient.Tx
	opened   time.Time
	// err is sticky: once a commit has failed, what was in it is gone, and
	// letting the attempt carry on writing would produce a record with a hole
	// in it. The attempt fails instead, and its retry resumes from the last
	// commit that took.
	err error

	// flushers are the writers holding records in memory — a Recorder's
	// batch — which a commit point has to push out first, or the boundary
	// falls in the middle of what the function considers written.
	flushers map[int]func() error
	nextFl   int
}

func newAttemptOutputs(n *workerNode, job jobEnvelope) *attemptOutputs {
	a := &attemptOutputs{node: n, job: job.ID, attempt: job.Attempt}
	// A function that must heartbeat every H has a commit every H, so a
	// transaction that may live 2H is never reaped under it.
	if bounds, ok := flow.BoundsOf(job.Func); ok && bounds.Heartbeat > 0 {
		a.budget = 2 * bounds.Heartbeat
	}
	return a
}

// producerID names the attempt's transactional identity. Per attempt, so a
// job moved while its old attempt is still alive does not fence it: the old
// one's open transaction is abandoned and reaped, and never becomes visible.
func (a *attemptOutputs) producerID() string {
	return "wings.job." + streamPart(a.job) + "." + strconv.Itoa(a.attempt)
}

// begin opens a transaction if none is open. Called with mu held.
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
	// Committed by age rather than left to the backend's timeout, which would
	// abort it — and everything in it — instead.
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

// commit pushes out what every writer is holding and commits the open
// transaction: a point at which everything the attempt has written is visible
// together, and nothing after it is.
func (a *attemptOutputs) commit(ctx context.Context) error {
	a.mu.Lock()
	flushers := slices.Collect(maps.Values(a.flushers))
	a.mu.Unlock()
	// Outside the lock: a flush appends, and appending takes it.
	for _, flush := range flushers {
		if err := flush(); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.commitLocked(ctx)
}

// register adds a writer whose held records a commit must push out first, and
// returns how to take it off again.
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

// finish commits whatever is left once the function has returned, whether it
// succeeded or not: a failed attempt's record is exactly what its retry
// resumes from.
//
// Not on the attempt's context, which by now may be the reason it stopped;
// bounded on its own instead.
func (a *attemptOutputs) finish(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), outputAppend)
	defer cancel()
	return a.commit(ctx)
}

// historyStore is the [flow.Store] an attempt's run is kept on: the history
// stream for this attempt, written inside the attempt's transaction.
//
// Reading is the coordinator's copy of the PREVIOUS attempt's history, put on
// this worker under this attempt's name before the job arrived (see
// hydrateHistory) — so a moved job replays to where its predecessor got, and
// carries on from there under its own name.
type historyStore struct {
	a *attemptOutputs
}

func (h *historyStore) stream(ctx context.Context, run string) (*dsclient.Stream[*protos.Event], error) {
	st, err := h.a.node.client.OpenStream[*protos.Event](run, dsclient.WithCodec[*protos.Event](
		dswire.ReflectCodec[*protos.Event]{New: func() *protos.Event { return &protos.Event{} }},
	))
	if err != nil {
		return nil, fmt.Errorf("wings: open history %s: %w", run, err)
	}
	return st, nil
}

func (h *historyStore) Events(ctx context.Context, run string) ([]*protos.Event, error) {
	ok, err := h.a.node.client.StreamExists(ctx, run)
	if err != nil {
		return nil, fmt.Errorf("wings: look for history %s: %w", run, err)
	}
	if !ok {
		return nil, nil
	}
	st, err := h.stream(ctx, run)
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
			events = append(events, r.Record)
			from = r.Offset + 1
		}
	}
}

func (h *historyStore) Sink(ctx context.Context, run string) (flow.Sink, error) {
	return &historySink{h: h, run: run}, nil
}

// historySink appends a run's events inside the attempt's transaction.
//
// It stands the stream up LAZILY, on the first event that belongs to a
// thread. A function that forks nothing, sleeps never and calls nothing
// produces a history of two markers — started, finished — which says nothing
// a retry needs, and creating a stream and a producer for every such job
// would charge the common case for the rare one. The markers are held until
// something worth keeping comes, and dropped if nothing does.
type historySink struct {
	h   *historyStore
	run string

	mu     sync.Mutex
	stream *dsclient.Stream[*protos.Event]
	held   []*protos.Event
}

func (s *historySink) Append(ctx context.Context, ev *protos.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stream == nil {
		if ev.GetThreadId() == "" {
			s.held = append(s.held, ev)
			return nil
		}
		// Not the attempt's context: a stream half-made when a deadline
		// expires is the next attempt's problem.
		if err := ensureStream(context.WithoutCancel(ctx), s.h.a.node.client, s.run); err != nil {
			return err
		}
		st, err := s.h.stream(ctx, s.run)
		if err != nil {
			return err
		}
		s.stream = st
	}
	batch := append(s.held, ev)
	s.held = nil
	return s.h.a.append(ctx, s.stream, batch)
}

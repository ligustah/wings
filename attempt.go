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

// Each thread of one attempt writes its run history, recordings, and
// shared-channel outbox under its own transactional producer, which it alone
// commits, so a moved job's leftovers cannot disagree and no sibling's commit
// tears a send from its event. Committed at clear boundaries between whole
// events: the event's own append (coalesced under a CommitInterval), a channel
// wait, the thread's heartbeat, and its end. A thread that runs long without
// writing keeps its transaction alive through [flow.Context.Blocking].

// historyPrefix is the history of the run an attempt executes as; its Name part
// is always "history".
const historyPrefix = "wings.history."

// historyName is the stream one thread of a job attempt's history is kept on.
// The thread id is the stream's Name component, so each thread has its own stream.
func historyName(job string, attempt int, thread string) string {
	return outputName{Prefix: historyPrefix, Job: job, Attempt: attempt, Name: thread}.String()
}

// attemptOutputs is one thread of an attempt's transactional producer and its
// open transaction. Each thread writes its own history, recordings, and
// shared-channel outbox into this one, so a send's record and the event that
// justifies it commit together and no sibling thread's commit can tear them apart.
type attemptOutputs struct {
	node    *workerNode
	job     string
	attempt int
	thread  string
	// budget is how long one transaction may stay open; zero takes the backend default.
	budget time.Duration

	mu       sync.Mutex
	producer dsclient.Producer
	tx       dsclient.Tx
	opened   time.Time
	// logStream is the thread's durable log, opened once and written inside the
	// same transaction as its history so a line and its marker commit together and
	// the pull carries them home as one.
	logStream *dsclient.Stream[*protos.LogRecord]
	// err is sticky: after a failed commit the attempt fails rather than write a
	// record with a hole, and its retry resumes from the last commit that took.
	err error

	// flushers hold records in memory (a Recorder's batch) that a commit must
	// push out first.
	flushers map[int]func() error
	nextFl   int
}

func newAttemptOutputs(n *workerNode, job string, attempt int, thread string, budget time.Duration) *attemptOutputs {
	return &attemptOutputs{node: n, job: job, attempt: attempt, thread: thread, budget: budget}
}

// producerID is per attempt and thread, so a job moved while its old attempt
// still lives does not fence the new one (the old transaction is abandoned and
// reaped), and each thread commits independently. pullWanted recovers the job
// and attempt from it (pull.go).
func (a *attemptOutputs) producerID() string {
	return attemptWorkloadPrefix + streamPart(a.job) + "." + streamPart(a.thread) + "." + strconv.Itoa(a.attempt)
}

// begin opens a transaction if none is open. Call with mu held.
func (a *attemptOutputs) begin(ctx context.Context) error {
	if a.err != nil {
		return a.err
	}
	if a.tx != nil {
		return nil
	}
	if a.budget <= 0 {
		a.budget = txBudget(a.node.commitInterval)
	}
	p, tx, err := beginProducerTx(ctx, a.node.client, a.producerID(), a.budget, a.producer)
	if err != nil {
		a.producer = p
		a.err = fmt.Errorf("wings: begin a transaction for job %s: %w", a.job, err)
		return a.err
	}
	a.producer, a.tx, a.opened = p, tx, time.Now()
	return nil
}

// append writes values to s inside the thread's transaction. It never commits: a
// value precedes the send event that justifies it, so the commit is left to the
// event ([historySink.Append]), which keeps the two whole in one transaction.
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
	return nil
}

// appendLog writes a durable log line into the thread's open transaction, opening
// the thread's log stream once. It never commits: the [protos.LogEvent] marker the
// caller records next drives the commit, so a line and its marker are whole in one
// transaction and the pull carries both home (see [nodeLogs.Log], [pulledStream]).
func (a *attemptOutputs) appendLog(ctx context.Context, name string, rec *protos.LogRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.begin(ctx); err != nil {
		return err
	}
	if a.logStream == nil {
		if err := ensureLogStream(context.WithoutCancel(ctx), a.node.client, name); err != nil {
			return err
		}
		st, err := a.node.client.OpenStream[*protos.LogRecord](name, dsclient.WithCodec[*protos.LogRecord](
			dswire.ReflectCodec[*protos.LogRecord]{New: func() *protos.LogRecord { return &protos.LogRecord{} }},
		))
		if err != nil {
			return fmt.Errorf("wings: open log %s: %w", name, err)
		}
		a.logStream = st
	}
	if _, err := dsclient.Output(a.tx, a.logStream).Append(ctx, []*protos.LogRecord{rec}); err != nil {
		a.err = fmt.Errorf("wings: write log of job %s: %w", a.job, err)
		return a.err
	}
	return nil
}

// appendEvents writes a thread's events into its transaction — after any value a
// send announced just before, so the two are whole. It never commits; the caller
// ([historySink]) decides when, so a coalescing interval holds several events in
// one transaction.
func (a *attemptOutputs) appendEvents(ctx context.Context, s *dsclient.Stream[*protos.Event], values []*protos.Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.begin(ctx); err != nil {
		return err
	}
	if _, err := dsclient.Output(a.tx, s).Append(ctx, values); err != nil {
		a.err = fmt.Errorf("wings: write output of job %s: %w", a.job, err)
		return a.err
	}
	return nil
}

func (a *attemptOutputs) commitLocked(ctx context.Context) error {
	if a.tx == nil {
		return a.err
	}
	tx := a.tx
	a.tx = nil
	// A commit decided past its point of no return is done: its records are
	// durable and the sweep delivers them. The producer keeps its identity; the
	// next begin finishes the predecessor inline (see beginTx).
	if err := tx.Commit(ctx); err != nil && !decided(err) {
		a.err = fmt.Errorf("wings: commit what job %s wrote: %w", a.job, err)
		return a.err
	}
	return nil
}

// commitDue reports whether the open transaction is old enough to commit under
// the worker's commitInterval. Call with mu held. A zero interval is always due,
// so every event commits on its own.
func (a *attemptOutputs) commitDue() bool {
	ci := a.node.commitInterval
	return ci <= 0 || time.Since(a.opened) >= ci
}

// commitIfDue commits only once the open transaction has aged past the worker's
// commitInterval, so a run of channel sends between commits coalesces into one.
// What must not wait for the interval — a value someone is blocked on, a reported
// checkpoint — is flushed by [historySink.CommitBoundary] and [progressOf.Heartbeat].
func (a *attemptOutputs) commitIfDue(ctx context.Context) error {
	a.mu.Lock()
	if !a.commitDue() {
		err := a.err
		a.mu.Unlock()
		return err
	}
	a.mu.Unlock()
	return a.commit(ctx)
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

// attemptTxns is one attempt's per-thread transactional producers, created on
// first use and keyed by thread id. Each thread of the run writes its history,
// recordings, and shared-channel outbox through its own, so their commits are
// independent and a send's record commits with the event that justifies it.
type attemptTxns struct {
	node    *workerNode
	job     string
	attempt int
	budget  time.Duration

	mu       sync.Mutex
	byThread map[string]*attemptOutputs
}

func newAttemptTxns(n *workerNode, job jobEnvelope) *attemptTxns {
	t := &attemptTxns{node: n, job: job.ID, attempt: job.Attempt, byThread: map[string]*attemptOutputs{}}
	// Budget of 2H keeps a transaction from being reaped under a function that
	// heartbeats every H.
	if bounds, ok := flow.BoundsOf(job.Func); ok && bounds.Heartbeat > 0 {
		t.budget = 2 * bounds.Heartbeat
	}
	return t
}

// For returns the thread's producer, creating it on first use.
func (t *attemptTxns) For(thread string) *attemptOutputs {
	t.mu.Lock()
	defer t.mu.Unlock()
	a := t.byThread[thread]
	if a == nil {
		a = newAttemptOutputs(t.node, t.job, t.attempt, thread, t.budget)
		t.byThread[thread] = a
	}
	return a
}

// finishAll commits what every thread has left once the attempt is over, on its
// own bounded context. It finishes every thread even if one fails, returning the
// first error, so no transaction is left open.
func (t *attemptTxns) finishAll(ctx context.Context) error {
	t.mu.Lock()
	outs := slices.Collect(maps.Values(t.byThread))
	t.mu.Unlock()
	var err error
	for _, a := range outs {
		if e := a.finish(ctx); e != nil && err == nil {
			err = e
		}
	}
	return err
}

// historyStore is the [flow.Store] an attempt's run is kept on, one stream per
// thread. Reading returns the previous attempt's history, hydrated onto this
// worker under this attempt's names (see hydrateHistory), so a moved job replays
// to where its predecessor got.
type historyStore struct {
	txns    *attemptTxns
	job     string
	attempt int
}

// open returns the thread's history stream, or ok=false when it does not exist.
func (h *historyStore) open(ctx context.Context, thread string) (*dsclient.Stream[*protos.Event], bool, error) {
	name := historyName(h.job, h.attempt, thread)
	exists, err := h.txns.node.client.StreamExists(ctx, name)
	if err != nil {
		return nil, false, fmt.Errorf("wings: look for history %s: %w", name, err)
	}
	if !exists {
		return nil, false, nil
	}
	st, err := eventStream[*protos.Event](h.txns.node.client, name)
	if err != nil {
		return nil, false, err
	}
	return st, true, nil
}

// Events reads one thread's events out of its own stream.
func (h *historyStore) Events(ctx context.Context, _, thread string) ([]*protos.Event, error) {
	st, ok, err := h.open(ctx, thread)
	if err != nil || !ok {
		return nil, err
	}
	var events []*protos.Event
	var from int64
	for {
		recs, err := st.Read(ctx, from, recordBatch)
		if err != nil {
			return nil, fmt.Errorf("wings: read history %s: %w", historyName(h.job, h.attempt, thread), err)
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

// Read returns up to n of one thread's events at or after offset in the thread's
// own stream, each with its offset there; n=1 at a known offset reads it back.
func (h *historyStore) Read(ctx context.Context, _, thread string, offset int64, n int) ([]flow.EventAt, error) {
	st, ok, err := h.open(ctx, thread)
	if err != nil || !ok {
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
			return nil, fmt.Errorf("wings: read history %s: %w", historyName(h.job, h.attempt, thread), err)
		}
		if len(recs) == 0 {
			return out, nil
		}
		for _, r := range recs {
			from = r.Offset + 1
			out = append(out, flow.EventAt{Event: r.Record, Offset: r.Offset})
			if len(out) >= n {
				break
			}
		}
	}
	return out, nil
}

func (h *historyStore) Begin(ctx context.Context, _, thread string) (flow.Tx, error) {
	return &workerTx{
		out:     h.txns.For(thread),
		client:  h.txns.node.client,
		hist:    historyName(h.job, h.attempt, thread),
		streams: map[string]*dsclient.Stream[*protos.Event]{},
	}, nil
}

// Follow delivers a named stream's records from offset, waiting for the stream to
// appear and for new records. The relay mirrors a shared channel's streams onto
// this node under their global names, so a receiver here follows the same name a
// coordinator or another worker wrote.
func (h *historyStore) Follow(ctx context.Context, name string, from int64, yield func(flow.EventAt) bool) error {
	for ctx.Err() == nil {
		exists, err := flow.StreamAvailable(ctx, h.txns.node.client, name)
		if err != nil {
			return fmt.Errorf("wings: check %s: %w", name, err)
		}
		if !exists {
			if err := pause(ctx, 200*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		st, err := eventStream[*protos.Event](h.txns.node.client, name)
		if err != nil {
			return err
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
			if !yield(flow.EventAt{Event: r.Record, Offset: r.Offset}) {
				return nil
			}
		}
	}
	return ctx.Err()
}

// Drop is a no-op: a joined thread's stream stays until the job settles, when
// dropOutputsOf reclaims it; dropping it live would race the pull that copies it.
func (h *historyStore) Drop(ctx context.Context, _, _ string) error { return nil }

// followBatch is how many records a Follow reads at once.
const followBatch = 256

// workerTx appends one thread's events inside that thread's transaction, standing
// the history stream up lazily on the first real event — so a thread that forks,
// sleeps and calls nothing leaves nothing behind. It also writes the channel
// streams a send touches, so a value and the send event that justifies it commit
// together (pull.go). It resets nothing: a worker attempt writes its own names.
type workerTx struct {
	out    *attemptOutputs
	client *dsclient.Client
	hist   string

	mu      sync.Mutex
	streams map[string]*dsclient.Stream[*protos.Event]
	held    []*protos.Event
}

// streamLocked returns the named stream, opening it once. Call with mu held.
func (tx *workerTx) streamLocked(ctx context.Context, name string) (*dsclient.Stream[*protos.Event], error) {
	if st := tx.streams[name]; st != nil {
		return st, nil
	}
	// Not the attempt's context: a half-made stream is the next attempt's problem.
	if err := ensureStream(context.WithoutCancel(ctx), tx.client, name); err != nil {
		return nil, err
	}
	st, err := eventStream[*protos.Event](tx.client, name)
	if err != nil {
		return nil, err
	}
	tx.streams[name] = st
	return st, nil
}

// Append writes an event to the thread's history stream and commits unless a
// CommitInterval is holding the transaction open to coalesce. What must be durable
// at once — a value someone is blocked on, or a thread as it parks — is flushed by
// Flush; between those, work in flight coalesces and a restart replays whatever the
// last commit did not cover.
func (tx *workerTx) Append(ctx context.Context, ev *protos.Event) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if _, open := tx.streams[tx.hist]; !open {
		if ev.GetRunStart() != nil || ev.GetRunEnd() != nil {
			// Hold the attempt markers until something worth a stream arrives.
			tx.held = append(tx.held, ev)
			return nil
		}
	}
	st, err := tx.streamLocked(ctx, tx.hist)
	if err != nil {
		return err
	}
	batch := append(tx.held, ev)
	tx.held = nil
	if err := tx.out.appendEvents(ctx, st, batch); err != nil {
		return err
	}
	return tx.out.commitIfDue(ctx)
}

// AppendTo writes an event to a named channel stream in the thread's transaction.
// It never commits: a value precedes its send event, and a consume report or close
// rides the event it pairs with, so the event's commit takes both home together.
func (tx *workerTx) AppendTo(ctx context.Context, name string, ev *protos.Event) error {
	tx.mu.Lock()
	st, err := tx.streamLocked(ctx, name)
	tx.mu.Unlock()
	if err != nil {
		return err
	}
	return tx.out.appendEvents(ctx, st, []*protos.Event{ev})
}

func (tx *workerTx) Commit(ctx context.Context) error { return tx.out.commitIfDue(ctx) }
func (tx *workerTx) Flush(ctx context.Context) error  { return tx.out.commit(ctx) }

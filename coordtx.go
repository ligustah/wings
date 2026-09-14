package wings

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	streams "github.com/ligustah/durable_streams"
	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// A coordinator thread's history and the shared-channel outboxes it writes go
// through one transactional producer per thread, so a send's event and its
// outbox record commit together and no sibling thread's commit can tear them
// apart (see [flow.Committer]). One goroutine — the thread that owns it — ever
// touches the producer, so its writes are fully serialized. The coordinator's
// counterpart to a worker's [attemptOutputs]. A coordinator run retries in place
// over reused producers, so each thread's producer resets when its own history
// shows a higher attempt (see [coordOutputs.resetTo]); a thread's RunStart marker
// resets it before the thread sends anything new.

const coordPrefix = "wings.coord."

// defaultTxBudget is how long a thread's transaction may stay open before the
// backend reaps it. Generous because a stream has a single producer, so a
// long-held transaction blocks no one; a thread that runs long without writing
// keeps its transaction alive through [flow.Context.Blocking] rather than by a
// tighter bound.
const defaultTxBudget = 5 * time.Minute

// coordTxBudget forces the coordinator's per-transaction timeout, overriding the
// default. Zero uses the default; a var so a test can shrink it to reproduce a
// reap without a long wait.
var coordTxBudget time.Duration

// txBudget is the per-transaction timeout a thread opens with: generous by
// default, widened to outlast a large commit interval so coalescing is not cut
// short by a reap. This is the reap ceiling, not how often a thread commits — a
// thread that runs long without writing keeps its transaction alive through
// [flow.Context.Blocking].
func txBudget(commitInterval time.Duration) time.Duration {
	b := defaultTxBudget
	if want := 3 * commitInterval; want > b {
		b = want
	}
	return b
}

// coordBudget is txBudget for the coordinator, forced by coordTxBudget for a test.
func coordBudget(commitInterval time.Duration) time.Duration {
	if coordTxBudget > 0 {
		return coordTxBudget
	}
	return txBudget(commitInterval)
}

// blockingBeat is how often [flow.Context.Blocking] commits the calling thread's
// transaction and reports liveness while its work runs: a fraction of the
// transaction budget, so it commits well before a reap, and never longer than the
// commit interval, so coalesced writes still land on time.
func blockingBeat(budget, commitInterval time.Duration) time.Duration {
	if budget <= 0 {
		budget = defaultTxBudget
	}
	beat := budget / 4
	if commitInterval > 0 && commitInterval < beat {
		beat = commitInterval
	}
	return beat
}

// coordOutputs is one coordinator thread's transactional producer and its open
// transaction, shared by the thread's history sink and the shared-channel links
// it writes through.
type coordOutputs struct {
	client *dsclient.Client
	// id is the qualified "<run>/<thread>" this producer writes for.
	id     string
	budget time.Duration
	// commitInterval coalesces plain history commits: a transaction older than this
	// commits on the next append, so events between commit at most once per interval.
	// Zero commits every event. A parked or blocking thread flushes regardless.
	commitInterval time.Duration

	mu       sync.Mutex
	producer dsclient.Producer
	tx       dsclient.Tx
	// logStream is the thread's durable log, opened once and written inside the
	// same transaction as its history so a line and its marker commit together.
	logStream *dsclient.Stream[*protos.LogRecord]
	// opened is when the current transaction began, for commitInterval.
	opened  time.Time
	attempt uint64
	// err is sticky within an attempt: after a failed commit the attempt fails
	// rather than write a record with a hole. A retry clears it (see resetTo).
	err error
}

func newCoordOutputs(client *dsclient.Client, id string, commitInterval time.Duration) *coordOutputs {
	return &coordOutputs{client: client, id: id, commitInterval: commitInterval}
}

func (a *coordOutputs) producerID() string { return coordPrefix + streamPart(a.id) }

// begin opens a transaction if none is open. Call with mu held.
func (a *coordOutputs) begin(ctx context.Context) error {
	if a.err != nil {
		return a.err
	}
	if a.tx != nil {
		return nil
	}
	if a.producer == nil {
		p, err := openProducer(ctx, a.client, a.producerID())
		if err != nil {
			a.err = fmt.Errorf("wings: open a producer for %s: %w", a.id, err)
			return a.err
		}
		a.producer = p
		if a.budget <= 0 {
			a.budget = coordBudget(a.commitInterval)
		}
	}
	tx, err := beginTx(ctx, a.producer, a.budget)
	if err != nil {
		a.err = fmt.Errorf("wings: begin a transaction for %s: %w", a.id, err)
		return a.err
	}
	a.tx = tx
	a.opened = time.Now()
	return nil
}

// commitDue reports whether an open transaction is old enough to commit under
// commitInterval. Call with mu held. A zero interval is always due, so every
// event commits on its own.
func (a *coordOutputs) commitDue() bool {
	return a.commitInterval <= 0 || time.Since(a.opened) >= a.commitInterval
}

// appendLog writes a durable log line into the thread's open transaction, opening
// the thread's log stream once. It never commits: the [protos.LogEvent] marker the
// caller records next drives the commit, so a line and its marker are whole in one
// transaction (see [clusterLogs.Log]). The log stream is not a parseOutput stream,
// so a settled job's history drop leaves it, and the lines outlive the history.
func (a *coordOutputs) appendLog(ctx context.Context, name string, rec *protos.LogRecord) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.begin(ctx); err != nil {
		return err
	}
	if a.logStream == nil {
		if err := ensureLogStream(context.WithoutCancel(ctx), a.client, name); err != nil {
			return err
		}
		st, err := a.client.OpenStream[*protos.LogRecord](name, dsclient.WithCodec[*protos.LogRecord](
			dswire.ReflectCodec[*protos.LogRecord]{New: func() *protos.LogRecord { return &protos.LogRecord{} }},
		))
		if err != nil {
			return fmt.Errorf("wings: open log %s: %w", name, err)
		}
		a.logStream = st
	}
	if _, err := dsclient.Output(a.tx, a.logStream).Append(ctx, []*protos.LogRecord{rec}); err != nil {
		a.err = fmt.Errorf("wings: write log of %s: %w", a.id, err)
		return a.err
	}
	return nil
}

// appendEvent writes a thread's event into its transaction. It never commits; the
// caller ([coordSink]) decides when, so a coalescing interval holds several events
// in one transaction.
func (a *coordOutputs) appendEvent(ctx context.Context, s *dsclient.Stream[*protos.Event], ev *protos.Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.begin(ctx); err != nil {
		return err
	}
	if _, err := dsclient.Output(a.tx, s).Append(ctx, []*protos.Event{ev}); err != nil {
		a.err = fmt.Errorf("wings: write output of %s: %w", a.id, err)
		return a.err
	}
	return nil
}

// decided reports whether a commit error in fact left the transaction committed:
// the broker refuses a commit taken past its point of no return with
// [dswire.ErrCommitDecided], its records are durable, and the reaper's sweep makes
// them visible. Replaying such a transaction writes its output twice, so a caller
// reads this as done, not as a failure to retry.
func decided(err error) bool { return errors.Is(err, dswire.ErrCommitDecided) }

const (
	txBeginTries   = 40
	txBeginBackoff = 150 * time.Millisecond
)

// openProducer opens a producer for id, retrying while the broker that
// coordinates the id is still adopting its transaction-state partition. The
// broker waits out its own bounded window and then refuses with the retryable
// [streams.ErrTxStateUnreachable]; the refusal clears once leadership of the
// partition settles, so the same open tries again.
func openProducer(ctx context.Context, client *dsclient.Client, id string) (dsclient.Producer, error) {
	var err error
	for try := 0; try < txBeginTries; try++ {
		var p dsclient.Producer
		if p, err = client.Producer(ctx, id); err == nil || !errors.Is(err, streams.ErrTxStateUnreachable) {
			return p, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(txBeginBackoff):
		}
	}
	return nil, err
}

// beginTx opens a transaction on p, waiting out the window after a decided commit
// where the identity's previous transaction is still being finalized. The broker
// finishes that predecessor inline on the next Begin, but a completion still in
// flight comes back as [dswire.ErrTransactionNotFinalized] — a "retry shortly"
// that keeps the identity — so the same producer keeps its nonce and tries again.
func beginTx(ctx context.Context, p dsclient.Producer, budget time.Duration) (dsclient.Tx, error) {
	var err error
	for try := 0; try < txBeginTries; try++ {
		var tx dsclient.Tx
		if tx, err = p.BeginTimeout(ctx, budget); err == nil || !errors.Is(err, dswire.ErrTransactionNotFinalized) {
			return tx, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(txBeginBackoff):
		}
	}
	return nil, err
}

func (a *coordOutputs) commitLocked(ctx context.Context) error {
	if a.tx == nil {
		return a.err
	}
	tx := a.tx
	a.tx = nil
	// A commit decided past its point of no return is done: its records are
	// durable and the sweep delivers them. The producer keeps its identity; the
	// next begin finishes the predecessor inline (see beginTx).
	if err := tx.Commit(ctx); err != nil && !decided(err) {
		a.err = fmt.Errorf("wings: commit what %s wrote: %w", a.id, err)
		return a.err
	}
	return nil
}

func (a *coordOutputs) commit(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.commitLocked(ctx)
}

// commitIfDue commits only once the open transaction has aged past
// commitInterval, so plain history events between commits coalesce into one.
func (a *coordOutputs) commitIfDue(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.commitDue() {
		return a.commitLocked(ctx)
	}
	return a.err
}

// resetTo drops a failed attempt's transaction and clears its error once a higher
// attempt is seen, so the retry writes afresh over the reused producer. The
// thread's RunStart marker carries the new attempt, so the reset happens before
// the thread writes anything new.
func (a *coordOutputs) resetTo(ctx context.Context, attempt uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if attempt <= a.attempt {
		return
	}
	a.attempt = attempt
	if a.tx != nil {
		_ = a.tx.Abort(context.WithoutCancel(ctx))
		a.tx = nil
	}
	a.err = nil
}

// abandon aborts the open transaction of a thread that will not resume.
func (a *coordOutputs) abandon(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.tx != nil {
		_ = a.tx.Abort(context.WithoutCancel(ctx))
		a.tx = nil
	}
}

// finish commits what is left once the thread is over, on its own bounded context.
func (a *coordOutputs) finish(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), outputAppend)
	defer cancel()
	return a.commit(ctx)
}

// coordStore is the coordinator's [flow.Store]: it reads, follows and drops
// through the plain store but writes each thread's events inside that thread's
// transaction, so a shared-channel send's event commits with its channel record.
type coordStore struct {
	flow.Store
	c      *Cluster
	client *dsclient.Client
}

func newCoordStore(c *Cluster, client *dsclient.Client) *coordStore {
	return &coordStore{
		Store:  flow.NewStore(client, flow.WithStoreCompression(streamCompression)),
		c:      c,
		client: client,
	}
}

// Drop commits the thread's tail before its history stream goes. A coalesced log
// line lives in the same transaction as the history marker but on the log stream,
// which outlives the history; without this flush a join that drops a short-lived
// forked thread's history under a CommitInterval would abandon the transaction —
// and the log line with it — before the run's end commits it.
func (s *coordStore) Drop(ctx context.Context, run, thread string) error {
	if out, err := s.c.coordOutputsFor(run + "/" + thread); err == nil {
		if err := out.commit(context.WithoutCancel(ctx)); err != nil {
			return err
		}
	}
	return s.Store.Drop(ctx, run, thread)
}

// Begin returns the thread's transaction. A retried coordinator run reuses the
// producer, reset when the thread's history shows a higher attempt.
func (s *coordStore) Begin(ctx context.Context, run, thread string) (flow.Tx, error) {
	out, err := s.c.coordOutputsFor(run + "/" + thread)
	if err != nil {
		return nil, err
	}
	return &coordTx{
		out:     out,
		client:  s.client,
		hist:    flow.ThreadStream(run, thread),
		streams: map[string]*dsclient.Stream[*protos.Event]{},
	}, nil
}

// RetireChannels implements [flow.ChannelRetirer]: the run body's in-process call
// that created these channels has returned, so retire them. Done off the caller so
// the call is not held for storage work; the run's end retires whatever is left.
func (s *coordStore) RetireChannels(_ context.Context, _ string, ids []string) {
	s.c.wg.Go(func() { s.c.retireChannels(ids) })
}

// coordTx appends a thread's events — its history and any channel stream it writes
// — inside the thread's own transaction, so a send's value and the event that
// justifies it commit together (pull.go). It resets the producer when it sees a
// higher attempt, so a retried run writes afresh (see [coordOutputs.resetTo]).
type coordTx struct {
	out    *coordOutputs
	client *dsclient.Client
	hist   string

	mu      sync.Mutex
	streams map[string]*dsclient.Stream[*protos.Event]
}

func (tx *coordTx) stream(ctx context.Context, name string) (*dsclient.Stream[*protos.Event], error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	if st := tx.streams[name]; st != nil {
		return st, nil
	}
	// Not the run's context: a half-made stream is the next attempt's problem.
	if err := ensureStream(context.WithoutCancel(ctx), tx.client, name); err != nil {
		return nil, err
	}
	st, err := eventStream[*protos.Event](tx.client, name)
	if err != nil {
		return nil, fmt.Errorf("wings: open %s: %w", name, err)
	}
	tx.streams[name] = st
	return st, nil
}

// Append writes an event to the thread's history stream and commits unless a
// CommitInterval is holding the transaction open to coalesce. What must be durable
// at once — a channel value, or a thread's writes as it parks — is flushed by
// Flush; between those, work in flight coalesces and a restart replays whatever the
// last commit did not cover.
func (tx *coordTx) Append(ctx context.Context, ev *protos.Event) error {
	tx.out.resetTo(ctx, ev.GetAttempt())
	st, err := tx.stream(ctx, tx.hist)
	if err != nil {
		return err
	}
	if err := tx.out.appendEvent(ctx, st, ev); err != nil {
		return err
	}
	return tx.out.commitIfDue(ctx)
}

// AppendTo writes an event to a named channel stream in the thread's transaction.
// It never commits: a value precedes its send event, and a consume report or close
// rides the event it pairs with, so the event's commit takes both home together.
func (tx *coordTx) AppendTo(ctx context.Context, name string, ev *protos.Event) error {
	st, err := tx.stream(ctx, name)
	if err != nil {
		return err
	}
	return tx.out.appendEvent(ctx, st, ev)
}

func (tx *coordTx) Commit(ctx context.Context) error { return tx.out.commitIfDue(ctx) }
func (tx *coordTx) Flush(ctx context.Context) error  { return tx.out.commit(ctx) }

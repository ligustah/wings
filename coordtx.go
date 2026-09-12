package wings

import (
	"context"
	"fmt"
	"sync"
	"time"

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
		p, err := a.client.Producer(ctx, a.producerID())
		if err != nil {
			a.err = fmt.Errorf("wings: open a producer for %s: %w", a.id, err)
			return a.err
		}
		a.producer = p
		if a.budget <= 0 {
			a.budget = coordBudget(a.commitInterval)
		}
	}
	tx, err := a.producer.BeginTimeout(ctx, a.budget)
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

// append writes a shared-channel record into the thread's open transaction. It
// never commits: a value precedes the send event that justifies it, so the commit
// is left to the event ([coordSink.Append]), which keeps the two whole in one
// transaction.
func (a *coordOutputs) append(ctx context.Context, s *dsclient.Stream[flow.ChannelItem], it flow.ChannelItem) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.begin(ctx); err != nil {
		return err
	}
	if _, err := dsclient.Output(a.tx, s).Append(ctx, []flow.ChannelItem{it}); err != nil {
		a.err = fmt.Errorf("wings: write output of %s: %w", a.id, err)
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

func (a *coordOutputs) commitLocked(ctx context.Context) error {
	if a.tx == nil {
		return a.err
	}
	tx := a.tx
	a.tx = nil
	if err := tx.Commit(ctx); err != nil {
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

// coordStore is the coordinator's [flow.Store]: it reads and drops through the
// plain store but writes each thread's events inside that thread's transaction, so
// a shared-channel send's event commits with its outbox record (see
// [clusterChannels.Link], [flow.Committer]).
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

func (s *coordStore) Sink(ctx context.Context, run, thread string) (flow.Sink, error) {
	out, err := s.c.coordOutputsFor(run + "/" + thread)
	if err != nil {
		return nil, err
	}
	return &coordSink{
		out:    out,
		client: s.client,
		name:   flow.ThreadStream(run, thread),
	}, nil
}

// coordSink appends a thread's events inside the thread's own transaction,
// resetting the producer when it sees a higher attempt so a retried run writes
// afresh (see [coordOutputs.resetTo]).
type coordSink struct {
	out    *coordOutputs
	client *dsclient.Client
	name   string

	mu     sync.Mutex
	stream *dsclient.Stream[*protos.Event]
}

func (s *coordSink) Append(ctx context.Context, ev *protos.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.out.resetTo(ctx, ev.GetAttempt())
	if s.stream == nil {
		// Not the run's context: a half-made stream is the next attempt's problem.
		if err := ensureStream(context.WithoutCancel(ctx), s.client, s.name); err != nil {
			return err
		}
		st, err := s.client.OpenStream[*protos.Event](s.name, dsclient.WithCodec[*protos.Event](
			dswire.ReflectCodec[*protos.Event]{New: func() *protos.Event { return &protos.Event{} }},
		))
		if err != nil {
			return fmt.Errorf("wings: open history %s: %w", s.name, err)
		}
		s.stream = st
	}
	// Append the event — after any value a send announced just before it, so the two
	// are whole in one transaction — then commit unless a CommitInterval is holding
	// the transaction open to coalesce. What must be durable at once — a channel
	// send's value, or a thread's writes as it parks — is flushed by [coordSink.Commit]
	// and [coordSink.CommitBoundary]; between those, work in flight coalesces and a
	// coordinator restart replays whatever the last commit did not cover.
	if err := s.out.appendEvent(ctx, s.stream, ev); err != nil {
		return err
	}
	return s.out.commitIfDue(ctx)
}

// Commit implements [flow.Committer]: it commits a channel send's event with the
// value it announced, or a consume report with the receive that justified it.
// Under a CommitInterval it coalesces, committing only once the transaction has
// aged past the interval; a parked thread's [coordSink.CommitBoundary] flushes what
// is left open so nothing waits on it forever.
func (s *coordSink) Commit(ctx context.Context) error { return s.out.commitIfDue(ctx) }

// CommitBoundary implements [flow.BoundaryCommitter]: it force-commits as the
// thread waits, so a value coalesced under a CommitInterval reaches whoever the
// thread is about to wait on. Without an interval the sink commits eagerly, so
// there is nothing held open.
func (s *coordSink) CommitBoundary(ctx context.Context) error {
	if s.out.commitInterval <= 0 {
		return nil
	}
	return s.out.commit(ctx)
}

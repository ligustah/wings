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

// A coordinator run's history and its shared-channel outboxes go through one
// transactional producer per run, so a send's event and its outbox record commit
// together (see [flow.Committer]). The coordinator's counterpart to a worker's
// [attemptOutputs]. Unlike a worker, whose run is one attempt per process, a
// coordinator run retries in place over a reused sink, so a higher attempt resets
// the producer (see [coordOutputs.resetTo]).

const coordPrefix = "wings.coord."

// coordOutputs is one coordinator run's transactional producer and open
// transaction, shared by the run's history sink and its shared-channel links.
type coordOutputs struct {
	client *dsclient.Client
	run    string
	budget time.Duration

	mu       sync.Mutex
	producer dsclient.Producer
	tx       dsclient.Tx
	attempt  uint64
	// err is sticky within an attempt: after a failed commit the attempt fails
	// rather than write a record with a hole. A retry clears it (see resetTo).
	err error
}

func newCoordOutputs(client *dsclient.Client, run string) *coordOutputs {
	return &coordOutputs{client: client, run: run}
}

func (a *coordOutputs) producerID() string { return coordPrefix + streamPart(a.run) }

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
			a.err = fmt.Errorf("wings: open a producer for run %s: %w", a.run, err)
			return a.err
		}
		a.producer = p
		if a.budget <= 0 {
			a.budget = p.TransactionTimeout()
		}
	}
	tx, err := a.producer.BeginTimeout(ctx, a.budget)
	if err != nil {
		a.err = fmt.Errorf("wings: begin a transaction for run %s: %w", a.run, err)
		return a.err
	}
	a.tx = tx
	return nil
}

// stage writes values to s inside the run's open transaction without committing,
// so a shared-channel outbox record waits there for the send event that pairs
// with it (see [coordSink.Append]).
func (a *coordOutputs) stage[T any](ctx context.Context, s *dsclient.Stream[T], values []T) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.begin(ctx); err != nil {
		return err
	}
	if _, err := dsclient.Output(a.tx, s).Append(ctx, values); err != nil {
		a.err = fmt.Errorf("wings: write output of run %s: %w", a.run, err)
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
		a.err = fmt.Errorf("wings: commit what run %s wrote: %w", a.run, err)
		return a.err
	}
	return nil
}

func (a *coordOutputs) commit(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.commitLocked(ctx)
}

// resetTo drops a failed attempt's transaction and clears its error once a higher
// run attempt is seen, so the retry writes afresh over the reused producer. Every
// thread of a run shares one producer, so the failure stops them all (the sticky
// error) and one reset — driven by the run's own main thread — reopens it.
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

// abandon aborts the open transaction of a run that will not resume.
func (a *coordOutputs) abandon(ctx context.Context) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.tx != nil {
		_ = a.tx.Abort(context.WithoutCancel(ctx))
		a.tx = nil
	}
}

// finish commits what is left once the run is over, on its own bounded context.
func (a *coordOutputs) finish(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), outputAppend)
	defer cancel()
	return a.commit(ctx)
}

// coordStore is the coordinator's [flow.Store]: it reads and drops through the
// plain store but writes each thread's events inside the run's transaction, so a
// shared-channel send's event commits with its outbox record (see
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
	out, err := s.c.coordOutputsFor(run)
	if err != nil {
		return nil, err
	}
	return &coordSink{
		out:    out,
		client: s.client,
		name:   flow.ThreadStream(run, thread),
		main:   thread == flow.MainThread,
	}, nil
}

// coordSink appends a thread's events inside the run's transaction. The main
// thread's sink resets the transaction when it sees a higher attempt, so a
// retried run writes afresh (see [coordOutputs.resetTo]).
type coordSink struct {
	out    *coordOutputs
	client *dsclient.Client
	name   string
	main   bool

	mu     sync.Mutex
	stream *dsclient.Stream[*protos.Event]
}

func (s *coordSink) Append(ctx context.Context, ev *protos.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.main {
		s.out.resetTo(ctx, ev.GetAttempt())
	}
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
	// Stage the event and commit: a history event is durable at once, as it was
	// before the coordinator kept a transaction (so a restart loses nothing), and
	// the commit flushes any shared-channel outbox record staged just before it —
	// a send's outbox record and its event thus commit together (see
	// [clusterChannels.Link]).
	if err := s.out.stage(ctx, s.stream, []*protos.Event{ev}); err != nil {
		return err
	}
	return s.out.commit(ctx)
}

// Commit implements [flow.Committer].
func (s *coordSink) Commit(ctx context.Context) error { return s.out.commit(ctx) }

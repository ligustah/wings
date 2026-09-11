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
// apart (see [flow.Committer]). The coordinator's counterpart to a worker's
// [attemptOutputs]. A coordinator run retries in place over reused producers, so
// each thread's producer resets when its own history shows a higher attempt (see
// [coordOutputs.resetTo]); a thread's RunStart marker resets it before the thread
// sends anything new.

const coordPrefix = "wings.coord."

// coordOutputs is one coordinator thread's transactional producer and its open
// transaction, shared by the thread's history sink and the shared-channel links
// it writes through.
type coordOutputs struct {
	client *dsclient.Client
	// id is the qualified "<run>/<thread>" this producer writes for.
	id     string
	budget time.Duration

	mu       sync.Mutex
	producer dsclient.Producer
	tx       dsclient.Tx
	attempt  uint64
	// err is sticky within an attempt: after a failed commit the attempt fails
	// rather than write a record with a hole. A retry clears it (see resetTo).
	err error
}

func newCoordOutputs(client *dsclient.Client, id string) *coordOutputs {
	return &coordOutputs{client: client, id: id}
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
			a.budget = p.TransactionTimeout()
		}
	}
	tx, err := a.producer.BeginTimeout(ctx, a.budget)
	if err != nil {
		a.err = fmt.Errorf("wings: begin a transaction for %s: %w", a.id, err)
		return a.err
	}
	a.tx = tx
	return nil
}

// stage writes a shared-channel record into the thread's open transaction without
// committing. A value announced before its send event waits for that event's
// commit; a consume report sent after its receive commits under the receive's own
// [coordSink.Commit]. Only the writing thread touches this producer, so a record
// is never torn from the event that justifies it.
func (a *coordOutputs) stage(ctx context.Context, s *dsclient.Stream[flow.ChannelItem], it flow.ChannelItem) error {
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

// stageEvent stages a thread's event into its transaction; the caller commits. A
// value staged earlier in the same transaction goes home with it.
func (a *coordOutputs) stageEvent(ctx context.Context, s *dsclient.Stream[*protos.Event], ev *protos.Event) error {
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

// resetTo drops a failed attempt's transaction and clears its error once a higher
// attempt is seen, so the retry writes afresh over the reused producer. The
// thread's RunStart marker carries the new attempt, so the reset happens before
// the thread stages anything new.
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
	// Stage the event and commit: a history event is durable at once, as it was
	// before the coordinator kept a transaction (so a restart loses nothing), and a
	// value announced earlier in this thread's transaction goes home in the same
	// commit as the event that justifies it (see [clusterChannels.Link]).
	if err := s.out.stageEvent(ctx, s.stream, ev); err != nil {
		return err
	}
	return s.out.commit(ctx)
}

// Commit implements [flow.Committer]: it commits a consume report this thread
// staged after recording its receive, so the report commits with or after the
// receive that justified it.
func (s *coordSink) Commit(ctx context.Context) error { return s.out.commit(ctx) }

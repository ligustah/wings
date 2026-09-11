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
	// pending holds each sending thread's announced shared-channel records, by
	// qualified thread id, until that thread's own next event stages and commits
	// them — so a sibling thread's commit cannot flush a record ahead of its event.
	pending map[string][]pendingSend
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

// stage writes a shared-channel record into the run's open transaction without
// committing, for a record that needs no torn-pair protection: a close (a
// duplicate of which is idempotent) or a consume report (never resent on a
// replay). It commits with the event it is paired with, under that event's
// commit. A value is held instead (see [coordOutputs.buffer]).
func (a *coordOutputs) stage(ctx context.Context, s *dsclient.Stream[flow.ChannelItem], it flow.ChannelItem) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.begin(ctx); err != nil {
		return err
	}
	if _, err := dsclient.Output(a.tx, s).Append(ctx, []flow.ChannelItem{it}); err != nil {
		a.err = fmt.Errorf("wings: write output of run %s: %w", a.run, err)
		return a.err
	}
	return nil
}

// buffer holds a sending thread's shared-channel value until that thread's own
// next event stages and commits it (see [coordSink.Append], [coordSink.Commit]).
func (a *coordOutputs) buffer(from string, s *dsclient.Stream[flow.ChannelItem], it flow.ChannelItem) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pending == nil {
		a.pending = map[string][]pendingSend{}
	}
	a.pending[from] = append(a.pending[from], pendingSend{stream: s, item: it})
}

// flushPendingLocked stages a thread's buffered records into the open transaction.
// Call with mu held and a transaction open.
func (a *coordOutputs) flushPendingLocked(ctx context.Context, from string) error {
	for _, p := range a.pending[from] {
		if _, err := dsclient.Output(a.tx, p.stream).Append(ctx, []flow.ChannelItem{p.item}); err != nil {
			a.err = fmt.Errorf("wings: write output of run %s: %w", a.run, err)
			return a.err
		}
	}
	delete(a.pending, from)
	return nil
}

// stageEvent stages a thread's buffered shared-channel records and then its event
// into the run's transaction under one lock hold, so the two never tear: no
// sibling thread can commit between a send's record and the event that justifies
// it, and a record is never durable without its event (which a replay would
// otherwise resend). The caller commits.
func (a *coordOutputs) stageEvent(ctx context.Context, from string, s *dsclient.Stream[*protos.Event], ev *protos.Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.begin(ctx); err != nil {
		return err
	}
	if err := a.flushPendingLocked(ctx, from); err != nil {
		return err
	}
	if _, err := dsclient.Output(a.tx, s).Append(ctx, []*protos.Event{ev}); err != nil {
		a.err = fmt.Errorf("wings: write output of run %s: %w", a.run, err)
		return a.err
	}
	return nil
}

// flushAndCommit stages a thread's buffered records — a consume report sent after
// its receive was recorded — and commits, so the report commits with or after the
// receive that justified it, never before.
func (a *coordOutputs) flushAndCommit(ctx context.Context, from string) error {
	a.mu.Lock()
	if len(a.pending[from]) > 0 {
		if err := a.begin(ctx); err != nil {
			a.mu.Unlock()
			return err
		}
		if err := a.flushPendingLocked(ctx, from); err != nil {
			a.mu.Unlock()
			return err
		}
	}
	a.mu.Unlock()
	return a.commit(ctx)
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
	a.pending = nil
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
	a.pending = nil
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
		from:   run + "/" + thread,
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
	// from is the qualified id of this sink's thread (run/thread), by which its
	// announced shared-channel records are held until this event stages them.
	from string
	main bool

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
	// Stage this thread's buffered shared-channel records with the event, then
	// commit: a history event is durable at once, as it was before the coordinator
	// kept a transaction (so a restart loses nothing), and a send's outbox record
	// goes home in the same commit as the event that justifies it, never torn from
	// it by a sibling thread (see [coordOutputs.stageEvent], [clusterChannels.Link]).
	if err := s.out.stageEvent(ctx, s.from, s.stream, ev); err != nil {
		return err
	}
	return s.out.commit(ctx)
}

// Commit implements [flow.Committer]: it flushes a consume report this thread sent
// after recording its receive, then commits (see [coordOutputs.flushAndCommit]).
func (s *coordSink) Commit(ctx context.Context) error { return s.out.flushAndCommit(ctx, s.from) }

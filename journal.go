package wings

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/wings/flow"
)

// journalStream is where the coordinator records what it did.
//
// It lives on the cluster's OWN embedded instance — the broker-less one, with
// no listener and no port — because this is not something a worker reads. It is
// the coordinator's account of its own decisions, and the whole point of
// putting it on a durable stream rather than in a map is that the map dies with
// the process and this does not.
const journalStream = "wings.coordinator"

// Journal entry kinds.
const (
	journalClusterStart = "start"       // a coordinator came up
	journalSubmitted    = "submitted"   // a job was accepted and sent to a worker
	journalAttached     = "attached"    // a workflow retry rejoined a job already running
	journalRedispatch   = "redispatch"  // a lost worker's job was sent somewhere else
	journalHeld         = "held"        // no worker was live; the job waits for one
	journalYielded      = "yielded"     // the job let its worker go, until something happens
	journalRecovered    = "recovered"   // a restarted coordinator took the job back from its record
	journalCompleted    = "completed"   // a result came back
	journalFailed       = "failed"      // the coordinator gave up on the job
	journalWorkerUp     = "worker-up"   // a worker entered service
	journalWorkerGone   = "worker-gone" // a worker left, one way or another
	journalClusterStop  = "stop"        // Stop was called
)

const (
	// journalBuffer is how many entries may be queued for the writer. Deep
	// enough that an ordinary burst of submits never touches the bottom,
	// shallow enough that a wedged writer cannot pin much memory.
	journalBuffer = 4096

	// journalWait is how long an entry that matters waits for room in a full
	// queue before it is dropped. The writer drains thousands of entries a
	// second, so this is many batches' worth; an entry still waiting after it
	// is one the writer is not going to take.
	journalWait = time.Second
)

// journalEntry is one line in that account.
//
// Deliberately flat and self-contained: an entry is meant to be legible on its
// own to somebody reading the stream after a crash, without holding the rest of
// the log in their head.
type journalEntry struct {
	At   time.Time `json:"at"`
	Kind string    `json:"kind"`
	// Epoch names the run of the coordinator that wrote this line. The record
	// is append-only and survives the process, so several runs share it; this
	// is what lets a reader tell them apart without parsing names.
	Epoch   string `json:"epoch,omitempty"`
	Job     string `json:"job,omitempty"`
	Func    string `json:"fn,omitempty"`
	Worker  string `json:"worker,omitempty"`
	Attempt int    `json:"attempt,omitempty"`
	Err     string `json:"err,omitempty"`

	// Run, Thread and Step are set when the job was a call inside a run rather
	// than a bare call, and they are what make the record answerable at the
	// level someone actually asks at: not "job 3f went to remote-2" but "the
	// second call of that run went to remote-2, and never came back". Absent
	// for a call made outside a run, which belongs to nothing larger and needs
	// no such column.
	Run    string `json:"run,omitempty"`
	Thread string `json:"thread,omitempty"`
	Step   uint64 `json:"step,omitempty"`

	// Yield is what a yielded job waits for, on a yielded entry: what a
	// coordinator that restarts needs to wake it.
	Yield *yieldEnvelope `json:"yield,omitempty"`
}

// from copies a call's origin onto an entry.
func (e journalEntry) from(o flow.Origin) journalEntry {
	e.Run, e.Thread, e.Step = o.Run, o.Thread, o.Step
	return e
}

// journal appends entries off the hot path.
//
// Recording must never become backpressure on the work itself, so writes go
// through a buffered channel drained by one goroutine. What happens when the
// buffer is full depends on what the entry is. A completion is the flood — one
// per job, and the one line that is also implied by the result reaching its
// caller — and is dropped on the spot. Everything else is what a post-mortem
// is read for: which job went where, what was moved, which worker was lost.
// Those wait, briefly, for room; only an entry the writer will not take within
// that wait is dropped. Drops are counted and reported either way, so a gap is
// never silent.
type journal struct {
	stream *dsclient.Stream[journalEntry]
	log    *slog.Logger
	epoch  string

	ch chan journalEntry
	// quit is closed by close, and is what a recorder checks rather than a
	// closed ch: a send on a closed channel panics, select or no select, and an
	// entry can arrive after close — a goroutine that was moving a job when
	// Stop began finishes its move against a journal already drained. Those
	// are counted, not written, and never a crash.
	quit chan struct{}
	done chan struct{}
	once sync.Once

	dropped atomic.Int64
	late    atomic.Int64
}

// openJournal declares the stream and starts the writer.
func openJournal(ctx context.Context, client *dsclient.Client, log *slog.Logger, epoch string) (*journal, error) {
	ok, err := client.StreamExists(ctx, journalStream)
	if err != nil {
		return nil, fmt.Errorf("wings: check %s: %w", journalStream, err)
	}
	if !ok {
		if err := client.CreateStream(ctx, journalStream, nil); err != nil {
			return nil, fmt.Errorf("wings: create %s: %w", journalStream, err)
		}
	}
	s, err := client.OpenStream[journalEntry](journalStream)
	if err != nil {
		return nil, fmt.Errorf("wings: open %s: %w", journalStream, err)
	}

	j := newJournal(s, log, epoch, journalBuffer)
	go j.write()
	return j, nil
}

// newJournal is a journal with its queue, and no writer running yet.
func newJournal(s *dsclient.Stream[journalEntry], log *slog.Logger, epoch string, buffer int) *journal {
	return &journal{
		stream: s,
		log:    log,
		epoch:  epoch,
		ch:     make(chan journalEntry, buffer),
		quit:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// record queues one entry. Safe after close, and never blocks for a
// completion; anything else may wait up to journalWait for room.
func (j *journal) record(e journalEntry) {
	if j == nil {
		return
	}
	e.At = time.Now()
	e.Epoch = j.epoch

	select {
	case <-j.quit:
		j.late.Add(1)
		return
	default:
	}
	select {
	case j.ch <- e:
		return
	default:
	}
	if e.Kind == journalCompleted {
		j.dropped.Add(1)
		return
	}
	t := time.NewTimer(journalWait)
	defer t.Stop()
	select {
	case j.ch <- e:
	case <-t.C:
		j.dropped.Add(1)
	case <-j.quit:
		j.late.Add(1)
	}
}

// write drains the queue, batching whatever has piled up.
//
// Batching is what makes the journal affordable: a Map of a thousand jobs
// produces two thousand entries, and appending them one at a time would cost a
// round trip through the engine per entry for a record nobody is reading yet.
func (j *journal) write() {
	defer close(j.done)

	batch := make([]journalEntry, 0, 256)
	for {
		select {
		case e := <-j.ch:
			batch = append(batch[:0], e)
		case <-j.quit:
			// What is still queued was recorded before close and is written.
			// An entry that lands in the queue after this drain was sent by a
			// recorder that saw the queue open, and is the one kind of entry
			// this cannot count; the window is a channel operation wide.
			j.flush(j.drain(batch[:0]))
			return
		}
		// Take everything else already waiting, up to the batch size.
		for len(batch) < cap(batch) {
			select {
			case e := <-j.ch:
				batch = append(batch, e)
				continue
			default:
			}
			break
		}
		j.flush(batch)
	}
}

// drain takes everything queued right now, without waiting.
func (j *journal) drain(batch []journalEntry) []journalEntry {
	for {
		select {
		case e := <-j.ch:
			batch = append(batch, e)
		default:
			return batch
		}
	}
}

func (j *journal) flush(batch []journalEntry) {
	if len(batch) == 0 {
		return
	}
	// Background, not the cluster context: this runs during shutdown too, and
	// the record of the shutdown is exactly the part worth keeping.
	if _, err := j.stream.Append(context.Background(), batch); err != nil {
		j.dropped.Add(int64(len(batch)))
		if j.log != nil {
			j.log.Warn("wings: journal append failed", "entries", len(batch), "err", err)
		}
	}
}

// close stops the writer once everything already queued is written. Safe to
// call twice.
func (j *journal) close() {
	if j == nil {
		return
	}
	j.once.Do(func() {
		close(j.quit)
		<-j.done

		if dropped := j.dropped.Load(); dropped > 0 && j.log != nil {
			j.log.Warn("wings: journal entries dropped", "count", dropped,
				"why", "the journal fell behind; the record of this run has gaps")
		}
		if late := j.late.Load(); late > 0 && j.log != nil {
			j.log.Warn("wings: journal entries arrived after the journal closed", "count", late,
				"why", "something was still moving or failing a job as the cluster stopped")
		}
	})
}

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

// journalStream is where the coordinator records its own decisions. It lives on
// the cluster's broker-less embedded instance and survives the process, which a
// map would not.
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
	journalBuffer = 4096
	// journalWait is how long an entry that matters waits for room before it is dropped.
	journalWait = time.Second
)

// journalEntry is one line in the coordinator's account, flat and self-contained
// so it is legible on its own after a crash.
type journalEntry struct {
	At   time.Time `json:"at"`
	Kind string    `json:"kind"`
	// Epoch names the coordinator run that wrote this line; several runs share
	// the append-only stream.
	Epoch   string `json:"epoch,omitempty"`
	Job     string `json:"job,omitempty"`
	Func    string `json:"fn,omitempty"`
	Worker  string `json:"worker,omitempty"`
	Attempt int    `json:"attempt,omitempty"`
	Err     string `json:"err,omitempty"`

	// Run, Thread and Step are set when the job was a call inside a run, so the
	// record is answerable at the level asked at. Absent for a bare call.
	Run    string `json:"run,omitempty"`
	Thread string `json:"thread,omitempty"`
	Step   uint64 `json:"step,omitempty"`

	// Yield is what a yielded job waits for — what a restarted coordinator needs
	// to wake it.
	Yield *yieldEnvelope `json:"yield,omitempty"`
}

func (e journalEntry) from(o flow.Origin) journalEntry {
	e.Run, e.Thread, e.Step = o.Run, o.Thread, o.Step
	return e
}

// journal appends entries off the hot path: recording must never backpressure
// the work, so writes go through a buffered channel drained by one goroutine. A
// full buffer drops completions (one per job, implied by the result anyway) on
// the spot; everything else waits up to journalWait. Drops are counted and reported.
type journal struct {
	stream *dsclient.Stream[journalEntry]
	log    *slog.Logger
	epoch  string

	ch chan journalEntry
	// quit is what a recorder checks rather than a closed ch, since a send on a
	// closed channel panics and an entry can arrive after close.
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

// record queues one entry. Safe after close, never blocks for a completion.
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

// write drains the queue in batches, since appending one at a time would cost a
// round trip per entry for a record nobody is reading yet.
func (j *journal) write() {
	defer close(j.done)

	batch := make([]journalEntry, 0, 256)
	for {
		select {
		case e := <-j.ch:
			batch = append(batch[:0], e)
		case <-j.quit:
			j.flush(j.drain(batch[:0]))
			return
		}
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
	// Background context: this runs during shutdown too, when the record matters most.
	if _, err := j.stream.Append(context.Background(), batch); err != nil {
		j.dropped.Add(int64(len(batch)))
		if j.log != nil {
			j.log.Warn("wings: journal append failed", "entries", len(batch), "err", err)
		}
	}
}

// close stops the writer once everything queued is written. Safe to call twice.
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

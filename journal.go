package wings

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/dsclient"
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
	journalSubmitted   = "submitted"   // a job was accepted and sent to a worker
	journalRedispatch  = "redispatch"  // a lost worker's job was sent somewhere else
	journalCompleted   = "completed"   // a result came back
	journalFailed      = "failed"      // the coordinator gave up on the job
	journalWorkerUp    = "worker-up"   // a worker entered service
	journalWorkerGone  = "worker-gone" // a worker left, one way or another
	journalClusterStop = "stop"        // Stop was called
)

// journalEntry is one line in that account.
//
// Deliberately flat and self-contained: an entry is meant to be legible on its
// own to somebody reading the stream after a crash, without holding the rest of
// the log in their head.
type journalEntry struct {
	At      time.Time `json:"at"`
	Kind    string    `json:"kind"`
	Job     string    `json:"job,omitempty"`
	Func    string    `json:"fn,omitempty"`
	Worker  string    `json:"worker,omitempty"`
	Attempt int       `json:"attempt,omitempty"`
	Err     string    `json:"err,omitempty"`
}

// journal appends entries off the hot path.
//
// Recording must never become backpressure on the work itself, so writes go
// through a buffered channel drained by one goroutine, and a full buffer DROPS
// entries rather than blocking a submit. That is the right trade for a record
// kept for diagnosis: an incomplete account of a run is bad, an account that
// makes the run slower is worse. Drops are counted and reported, so a gap is
// never silent.
type journal struct {
	stream *dsclient.Stream[journalEntry]
	log    *slog.Logger

	ch   chan journalEntry
	done chan struct{}

	mu      sync.Mutex
	dropped int
}

// openJournal declares the stream and starts the writer.
func openJournal(ctx context.Context, client *dsclient.Client, log *slog.Logger) (*journal, error) {
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

	j := &journal{
		stream: s,
		log:    log,
		// Deep enough that an ordinary burst of submits never touches the
		// bottom, shallow enough that a wedged writer cannot pin much memory.
		ch:   make(chan journalEntry, 4096),
		done: make(chan struct{}),
	}
	go j.write()
	return j, nil
}

// record queues one entry. Never blocks.
func (j *journal) record(e journalEntry) {
	if j == nil {
		return
	}
	e.At = time.Now()
	select {
	case j.ch <- e:
	default:
		j.mu.Lock()
		j.dropped++
		j.mu.Unlock()
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
		e, ok := <-j.ch
		if !ok {
			j.flush(batch)
			return
		}
		batch = append(batch[:0], e)

		// Take everything else already waiting, up to the batch size.
		for len(batch) < cap(batch) {
			select {
			case e, ok := <-j.ch:
				if !ok {
					j.flush(batch)
					return
				}
				batch = append(batch, e)
				continue
			default:
			}
			break
		}
		j.flush(batch)
	}
}

func (j *journal) flush(batch []journalEntry) {
	if len(batch) == 0 {
		return
	}
	// Background, not the cluster context: this runs during shutdown too, and
	// the record of the shutdown is exactly the part worth keeping.
	if _, err := j.stream.Append(context.Background(), batch); err != nil {
		j.mu.Lock()
		j.dropped += len(batch)
		j.mu.Unlock()
		if j.log != nil {
			j.log.Warn("wings: journal append failed", "entries", len(batch), "err", err)
		}
	}
}

// close stops the writer once everything already queued is written.
func (j *journal) close() {
	if j == nil {
		return
	}
	close(j.ch)
	<-j.done

	j.mu.Lock()
	dropped := j.dropped
	j.mu.Unlock()
	if dropped > 0 && j.log != nil {
		j.log.Warn("wings: journal entries dropped", "count", dropped,
			"why", "the journal fell behind; the record of this run has gaps")
	}
}

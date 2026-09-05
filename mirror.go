package wings

import (
	"context"
	"fmt"

	"github.com/ligustah/durable_streams/dsclient"
)

// mirrorPrefix is where a worker's results are copied to on the coordinator's
// own durable streams.
const mirrorPrefix = "wings.mirror.results."

func mirrorStreamFor(workerID string) string { return mirrorPrefix + workerID }

// mirroredResult is one result, plus where it sat in the worker's stream.
//
// The source offset is carried rather than inferred from this stream's own
// offsets because the two can drift: a mirror stream's tail includes records
// belonging to in-flight or aborted transactions, so its length is not a
// position anyone may resume from. The source offset written INSIDE the record
// is a fact, and facts are what a recovery needs.
type mirroredResult struct {
	Source int64          `json:"source"`
	Result resultEnvelope `json:"result"`
	Worker string         `json:"worker,omitempty"`
}

// mirror copies one worker's results onto the coordinator's own streams as they
// are read.
//
// A worker owns its queue and its results — that is deliberate, and it is why a
// worker whose link drops keeps working — but everything the coordinator has
// LEARNED lives in a map in its memory. Mirroring writes that learning down on
// the coordinator's side too, so a run that died has left an account of every
// result it saw and a position to read the worker from.
//
// A restarted coordinator reads a mirror for the offset to continue from, and
// for the results its predecessor saw: a job the journal still shows
// outstanding whose result is here is over (see recover.go). What it cannot
// do is resume a bare call: the goroutine that was waiting for the answer
// died with the process, and a result for a job nothing forked again is
// dropped as nobody's (see deliver). A workflow is different, because its
// history is the durable thing and a rerun forks again what had not
// returned, and the fork rejoins the job the predecessor left running.
//
// It is store-and-forward rather than replication in the durable-streams sense:
// a reader that tails one stream and appends to another, with a position it can
// resume from. That shape tolerates the link being down, which is the whole
// point; the engine's own replication is raft-based HA for a small stable set of
// brokers, and wings' workers are neither small in number nor stable.
type mirror struct {
	stream *dsclient.Stream[mirroredResult]
	worker string

	// next is the source offset to read from. Held in memory during a run and
	// recovered from the stream when a worker is first attached.
	next int64
}

// openMirror attaches to a worker's mirror stream and works out where reading
// should resume.
func (c *Cluster) openMirror(ctx context.Context, workerID string) (*mirror, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	name := mirrorStreamFor(workerID)

	ok, err := client.StreamExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("wings: check %s: %w", name, err)
	}
	if !ok {
		if err := client.CreateStream(ctx, name, nil); err != nil {
			return nil, fmt.Errorf("wings: create %s: %w", name, err)
		}
	}
	stream, err := client.OpenStream[mirroredResult](name)
	if err != nil {
		return nil, fmt.Errorf("wings: open %s: %w", name, err)
	}

	m := &mirror{stream: stream, worker: workerID}
	if m.next, err = m.resume(ctx); err != nil {
		return nil, err
	}
	return m, nil
}

// resume reads the mirror to its end and returns the source offset to continue
// from.
//
// Reading the whole thing is deliberate and happens once per attach: the last
// record is the only one that matters, but a stream is read forwards, and this
// is the one moment where being certain beats being quick.
func (m *mirror) resume(ctx context.Context) (int64, error) {
	var next int64
	for from := int64(0); ; {
		recs, err := m.stream.Read(ctx, from, 512)
		if err != nil {
			return 0, fmt.Errorf("wings: read %s at %d: %w", m.stream.Name(), from, err)
		}
		if len(recs) == 0 {
			return next, nil
		}
		for _, r := range recs {
			if r.Record.Source >= next {
				next = r.Record.Source + 1
			}
			from = r.Offset + 1
		}
	}
}

// append records one result and advances the resume point.
//
// Called before the result is handed to whoever is waiting for it, so a result
// that was delivered is always one that was written down. The other order would
// leave a job that a caller saw completed and a recovery would see outstanding.
func (m *mirror) append(ctx context.Context, source int64, res resultEnvelope) error {
	if _, err := m.stream.Append(ctx, []mirroredResult{{
		Source: source,
		Result: res,
		Worker: m.worker,
	}}); err != nil {
		return fmt.Errorf("wings: mirror result %s from %s: %w", res.ID, m.worker, err)
	}
	m.next = source + 1
	return nil
}

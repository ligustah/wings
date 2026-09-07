package wings

import (
	"context"
	"fmt"

	"github.com/ligustah/durable_streams/dsclient"
)

// mirrorPrefix is where a worker's results are copied on the coordinator's own streams.
const mirrorPrefix = "wings.mirror.results."

func mirrorStreamFor(workerID string) string { return mirrorPrefix + workerID }

// mirroredResult is one result plus its offset in the worker's stream. The
// source offset is carried in the record because a mirror stream's own length is
// not a resumable position — its tail includes in-flight and aborted transactions.
type mirroredResult struct {
	Source int64          `json:"source"`
	Result resultEnvelope `json:"result"`
	Worker string         `json:"worker,omitempty"`
}

// mirror copies one worker's results onto the coordinator's own streams as they
// are read, so a restarted coordinator has an offset to resume the worker from
// and the results its predecessor already saw (see recover.go). Store-and-forward,
// not the engine's raft replication, because it must tolerate the link being down
// and wings' workers are neither few nor stable.
type mirror struct {
	stream *dsclient.Stream[mirroredResult]
	worker string

	// next is the source offset to read from, recovered from the stream on attach.
	next int64
}

// openMirror attaches to a worker's mirror stream and works out where to resume.
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

// resume reads the mirror to its end for the source offset to continue from.
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

// append records one result and advances the resume point. Called before the
// result is delivered, so a result a caller saw is always one that was written
// down — the other order would leave recovery seeing a delivered job as outstanding.
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

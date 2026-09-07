package wings

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"

	"github.com/ligustah/durable_streams/dsclient"
)

// machineStream is the coordinator's record of every machine it has asked for.
const machineStream = "wings.machines"

// Machine record kinds.
const (
	machineIntent   = "intent"   // we are about to create this machine
	machineReady    = "ready"    // it exists and a worker is running on it
	machineReleased = "released" // it has been destroyed, or was never there
	machineFailed   = "failed"   // creation failed; it may or may not exist
)

// machineRecord is one line in that record.
type machineRecord struct {
	Kind   string    `json:"kind"`
	Lease  string    `json:"lease"`
	Worker string    `json:"worker,omitempty"`
	At     time.Time `json:"at"`
	Err    string    `json:"err,omitempty"`
}

// machineLog is the write-ahead record of provisioned machines: the intent is
// written before creation, so a coordinator that dies mid-creation still left a
// note naming the machine it was about to make.
type machineLog struct {
	stream *dsclient.Stream[machineRecord]
}

func (c *Cluster) openMachineLog(ctx context.Context) (*machineLog, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	ok, err := client.StreamExists(ctx, machineStream)
	if err != nil {
		return nil, fmt.Errorf("wings: check %s: %w", machineStream, err)
	}
	if !ok {
		if err := client.CreateStream(ctx, machineStream, nil); err != nil {
			return nil, fmt.Errorf("wings: create %s: %w", machineStream, err)
		}
	}
	stream, err := client.OpenStream[machineRecord](machineStream)
	if err != nil {
		return nil, fmt.Errorf("wings: open %s: %w", machineStream, err)
	}
	return &machineLog{stream: stream}, nil
}

// write appends one record synchronously and returns its error: an intent that
// was not recorded must not be acted on.
func (l *machineLog) write(ctx context.Context, rec machineRecord) error {
	if l == nil {
		return nil
	}
	rec.At = time.Now()
	if _, err := l.stream.Append(ctx, []machineRecord{rec}); err != nil {
		return fmt.Errorf("wings: record machine %s as %s: %w", rec.Lease, rec.Kind, err)
	}
	return nil
}

// outstanding returns the leases that may still name a live machine, oldest
// first. A lease with an intent and nothing after it may or may not have been
// created; only reattachment, by asking the cloud, can say.
func (l *machineLog) outstanding(ctx context.Context) ([]string, error) {
	if l == nil {
		return nil, nil
	}

	// A lease's last word counts; the stream gives them in order.
	var order []string
	state := map[string]string{}

	for from := int64(0); ; {
		recs, err := l.stream.Read(ctx, from, 512)
		if err != nil {
			return nil, fmt.Errorf("wings: read %s at %d: %w", machineStream, from, err)
		}
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			rec := r.Record
			if _, seen := state[rec.Lease]; !seen {
				order = append(order, rec.Lease)
			}
			state[rec.Lease] = rec.Kind
			from = r.Offset + 1
		}
	}

	var live []string
	for _, lease := range order {
		switch state[lease] {
		case machineIntent, machineReady, machineFailed:
			live = append(live, lease)
		}
	}
	return live, nil
}

// workerFor returns the worker id last recorded as running on a lease, or empty
// if none got that far.
func (l *machineLog) workerFor(ctx context.Context, lease string) (string, error) {
	if l == nil {
		return "", nil
	}
	var worker string
	for from := int64(0); ; {
		recs, err := l.stream.Read(ctx, from, 512)
		if err != nil {
			return "", fmt.Errorf("wings: read %s at %d: %w", machineStream, from, err)
		}
		if len(recs) == 0 {
			return worker, nil
		}
		for _, r := range recs {
			if r.Record.Lease == lease && r.Record.Worker != "" {
				worker = r.Record.Worker
			}
			from = r.Offset + 1
		}
	}
}

// newLease mints an identity for a machine that does not exist yet.
func newLease() string { return newToken(10) }

// newEpoch mints an identity for one run of the coordinator.
func newEpoch() string { return newToken(6) }

// newToken returns n random lowercase alphanumeric characters beginning with a
// letter, so it is safe to embed in any cloud's machine name.
func newToken(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	b[0] = alphabet[int(b[0])%26]
	return string(b)
}

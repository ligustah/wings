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

// machineLog is the write-ahead record of provisioned machines.
//
// Write-ahead is the whole point, and it is why the coordinator mints the
// identity rather than the cloud: the intent is written BEFORE anything is
// created, so a coordinator that dies mid-creation still left a note saying
// what it was about to do. The alternative — record the machine once the API
// returns — has a window in which a billed VM exists that nothing on earth
// knows about, and that window is exactly the one a crash likes.
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

// write appends one record.
//
// Synchronous and its error returned, unlike the journal's fire-and-forget: an
// intent that was not written down is an intent that must not be acted on, so
// the caller has to be able to refuse.
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
// first.
//
// "May" is doing real work there. A lease with an intent and nothing after it
// is the interesting case: the machine might exist, might have half-existed, or
// might never have been created — the record cannot say, because the crash it
// is designed for happens precisely in that gap. Resolving it means asking the
// cloud, which is what reattachment does.
func (l *machineLog) outstanding(ctx context.Context) ([]string, error) {
	if l == nil {
		return nil, nil
	}

	// Ordered, because a lease's last word is what counts and a stream gives
	// them in order.
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

// workerFor returns the worker id last recorded as running on a lease.
//
// Empty when the record never got that far, which is exactly the case where the
// machine exists and has nothing useful on it.
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
//
// Shorter than a lease because it is a prefix on names people read, and its job
// is only to differ from the last run rather than to be globally unique.
func newEpoch() string { return newToken(6) }

// newToken returns n random lowercase alphanumeric characters, beginning with a
// letter.
//
// Lowercase alphanumeric because a token has to survive being embedded in
// whatever a cloud calls a machine name — GCE wants RFC1035, and the provider
// is the one that knows that, so what it gets handed must be safe everywhere.
// A leading letter for the same reason: several clouds refuse a name that
// starts with a digit.
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

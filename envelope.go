package wings

import (
	"time"

	"github.com/ligustah/wings/flow"
)

// Stream names. Each worker gets its own pair, named per worker so the same
// code serves one shared in-process engine and per-worker brokers alike.
const (
	jobStreamPrefix    = "wings.jobs."
	resultStreamPrefix = "wings.results."
	beatStreamPrefix   = "wings.beats."
	// controlStreamPrefix carries word about a job already on a worker; the job
	// queue cannot, since a worker reads that only as slots free.
	controlStreamPrefix = "wings.control."
	// nestedStreamPrefix is the queue of threads forked by jobs. See nested.go.
	nestedStreamPrefix = "wings.nested."
)

func jobStreamFor(workerID string) string     { return jobStreamPrefix + workerID }
func resultStreamFor(workerID string) string  { return resultStreamPrefix + workerID }
func beatStreamFor(workerID string) string    { return beatStreamPrefix + workerID }
func controlStreamFor(workerID string) string { return controlStreamPrefix + workerID }
func nestedStreamFor(workerID string) string  { return nestedStreamPrefix + workerID }

// controlEnvelope tells a worker to stop one attempt of one job, or carries the
// answer to a call that job made.
type controlEnvelope struct {
	Job     string          `json:"job"`
	Attempt int             `json:"attempt,omitempty"`
	Why     string          `json:"why,omitempty"`
	Answer  *answerEnvelope `json:"answer,omitempty"`
}

// jobEnvelope is one unit of work on the wire. Payload is the input already
// encoded by the Func's codec.
type jobEnvelope struct {
	ID      string `json:"id"`
	Func    string `json:"fn"`
	Payload []byte `json:"payload,omitempty"`
	// Run and Thread name the thread this job is, when it is one; empty for a
	// bare call on [Cluster.Bind].
	Run    string `json:"run,omitempty"`
	Thread string `json:"thread,omitempty"`
	// Root and Lineage make the job a thread of run code (Func empty): the
	// worker replays the ancestors from Root down to reach the body. See
	// lineage.go.
	Root    flow.Root `json:"root,omitempty"`
	Lineage []string  `json:"lineage,omitempty"`
	// Attempt counts prior dispatches, from 0.
	Attempt int `json:"attempt,omitempty"`
	// Checkpoint is the last progress a previous attempt reported through
	// [flow.Context.Heartbeat], for a retry to resume from.
	Checkpoint []byte `json:"checkpoint,omitempty"`
	// Priors are earlier attempts' event logs, oldest first, read with [Priors].
	Priors []Recording `json:"priors,omitempty"`
	// Nested routes a job forked by another job to the nested queue. See nested.go.
	Nested bool `json:"nested,omitempty"`
	// Capacity is the fleet parallelism handed to the thread through
	// [flow.Context.MaxParallelism]; zero falls back to the worker's own.
	Capacity int `json:"capacity,omitempty"`
}

// answerEnvelope is the outcome of a call a job made. Thread and Step name the
// call as the job's history does, so the worker knows which wait to end.
type answerEnvelope struct {
	Thread  string `json:"thread"`
	Step    uint64 `json:"step"`
	Payload []byte `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
}

// beatEnvelope reports that a job is still running, and how far it has got.
type beatEnvelope struct {
	Job string `json:"job"`
	// Leaving says the worker itself is about to go; the coordinator moves its
	// work now rather than on a dead connection.
	Leaving bool `json:"leaving,omitempty"`
	// Attempt is which dispatch this is from, so a moved job's stale attempt is
	// not mistaken for the retry.
	Attempt int `json:"attempt,omitempty"`
	// Started says the job has been taken off the queue and begun, so the
	// coordinator's clocks run from then rather than from dispatch.
	Started    bool   `json:"started,omitempty"`
	Checkpoint []byte `json:"checkpoint,omitempty"`
	// Wait names what a thread of the job is waiting on; Woke says its last
	// waiting thread runs again. A waiting job is not counted to its worker's
	// load. See slots.go.
	Wait string `json:"wait,omitempty"`
	Woke bool   `json:"woke,omitempty"`
}

// resultEnvelope is one outcome; exactly one of Payload and Error is set. A
// failing work function is a normal result carried in Error, not a transport
// failure.
type resultEnvelope struct {
	ID string `json:"id"`
	// Attempt is which dispatch produced this; only the current attempt's
	// result counts.
	Attempt int    `json:"attempt,omitempty"`
	Payload []byte `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
	// Yield says the attempt ended without an answer, to be run again. See yield.go.
	Yield *yieldEnvelope `json:"yield,omitempty"`
}

// yieldEnvelope is why an attempt gave up its place and what brings it back: a
// deadline for a long sleep, or a wait on a thread or channel.
type yieldEnvelope struct {
	Until   time.Time `json:"until,omitempty"`
	Wait    string    `json:"wait,omitempty"`
	Channel string    `json:"channel,omitempty"`
	Seq     uint64    `json:"seq,omitempty"`
}

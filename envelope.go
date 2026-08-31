package wings

// Stream names on a worker's broker. Each worker owns exactly these two: the
// coordinator writes jobs and reads results, and never the reverse. Workers do
// not read each other's anything — there is no stream in this package that two
// workers both touch.
const (
	jobStream    = "wings.jobs"
	resultStream = "wings.results"

	// consumerGroup is the offset key a worker commits its progress under.
	// One worker per broker, so it never needs to be more specific than this.
	consumerGroup = "wings"
)

// jobEnvelope is one unit of work on the wire.
//
// Payload is the user's input already encoded by the Func's codec, so this
// layer never sees In and never needs to know what it was.
type jobEnvelope struct {
	ID      string `json:"id"`
	Func    string `json:"fn"`
	Payload []byte `json:"payload,omitempty"`
	// Attempt counts prior dispatches of this job, starting at 0. Carried so a
	// work function that cares can tell a retry from a first run, and so logs
	// on the worker say which it was.
	Attempt int `json:"attempt,omitempty"`
}

// resultEnvelope is one outcome.
//
// Exactly one of Payload and Error is meaningful. A failing work function is a
// NORMAL result here, not a transport failure: it rides back as Error and is
// re-raised on the coordinator. Only infrastructure trouble aborts the worker's
// batch, which is what keeps one bad input from stalling a worker forever.
type resultEnvelope struct {
	ID      string `json:"id"`
	Payload []byte `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
}

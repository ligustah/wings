package wings

// Stream names. Each worker gets its own pair: the coordinator writes jobs to a
// NAMED worker and reads that worker's results. Workers do not read each other's
// anything — there is no stream in this package that two workers both touch.
//
// Naming them per worker rather than per broker is what lets the same code
// serve both shapes. In process, every worker's pair lives on ONE shared
// engine; distributed, each worker's pair lives on that worker's own broker and
// the names simply do not collide with anyone. Nothing above has to know which
// it is looking at.
const (
	jobStreamPrefix    = "wings.jobs."
	resultStreamPrefix = "wings.results."
	// beatStreamPrefix is where a worker reports progress on jobs still
	// running. Separate from results because the result stream is
	// transactional — exactly one record per job, committed with the offset
	// that consumed it — and a heartbeat is neither one per job nor something
	// worth a transaction.
	beatStreamPrefix = "wings.beats."

	// consumerGroup is the offset key a worker commits its progress under. It
	// need not be more specific: a worker consumes only its own job stream.
	consumerGroup = "wings"
)

func jobStreamFor(workerID string) string    { return jobStreamPrefix + workerID }
func resultStreamFor(workerID string) string { return resultStreamPrefix + workerID }
func beatStreamFor(workerID string) string   { return beatStreamPrefix + workerID }

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
	// Checkpoint is the last progress a previous attempt reported through
	// [Heartbeat], and is what makes a retry cheap: the work resumes from it
	// rather than starting over. Empty on a first attempt, and on a retry of
	// something that never heartbeated.
	Checkpoint []byte `json:"checkpoint,omitempty"`
	// Steps are the [Step] calls a previous attempt completed, in order. A
	// retry replays them from here instead of running them again.
	Steps []stepRecord `json:"steps,omitempty"`
	// Priors are the event logs earlier attempts of this job left behind,
	// oldest attempt first. A retry reads them with [Priors] to pick up where
	// one of them stopped instead of starting over.
	Priors []Recording `json:"priors,omitempty"`
}

// stepRecord is one completed [Step]: where it sat in the job, what it was
// called, and what it produced.
//
// The index is carried rather than implied by position because these arrive one
// at a time over a best-effort channel, and one that goes missing must leave a
// detectable hole rather than a silently shifted list — a step log off by one is
// a retry that skips work it never did.
type stepRecord struct {
	Index int    `json:"i"`
	Name  string `json:"name"`
	Value []byte `json:"value,omitempty"`
}

// beatEnvelope is one report that a job is still running, and how far it has
// got.
//
// Job rather than offset because a worker runs its whole batch at once, so
// beats from several jobs interleave on one stream and each has to say which it
// belongs to.
type beatEnvelope struct {
	Job string `json:"job"`
	// Started says the worker has taken this job off its queue and begun it.
	// Sent once, first, so the coordinator's clocks run from when the work
	// began rather than from when it was sent: a job can sit behind others on
	// a busy worker for longer than its own bound without a moment of it being
	// the work's fault. Any beat implies it, so a lost one costs nothing.
	Started    bool   `json:"started,omitempty"`
	Checkpoint []byte `json:"checkpoint,omitempty"`
	// Step is one newly completed [Step], if this beat reports one. Sent one at
	// a time rather than as a growing log, so the cost of a step does not climb
	// with how many came before it; the coordinator does the accumulating.
	Step *stepRecord `json:"step,omitempty"`
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

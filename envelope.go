package wings

import "github.com/ligustah/wings/flow"

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
	// controlStreamPrefix is the coordinator's word to a worker about a job
	// already on it. The jobs stream cannot carry it: a worker takes jobs off
	// that queue only as slots free, and the message that matters most is
	// about the job holding the slot.
	controlStreamPrefix = "wings.control."
	// nestedStreamPrefix is the queue of calls made BY jobs, one per worker.
	// Apart from the job queue on purpose: see the worker's serveNested.
	nestedStreamPrefix = "wings.nested."

	// consumerGroup is the offset key a worker commits its progress under. It
	// need not be more specific: a worker consumes only its own job stream.
	consumerGroup = "wings"
)

func jobStreamFor(workerID string) string     { return jobStreamPrefix + workerID }
func resultStreamFor(workerID string) string  { return resultStreamPrefix + workerID }
func beatStreamFor(workerID string) string    { return beatStreamPrefix + workerID }
func controlStreamFor(workerID string) string { return controlStreamPrefix + workerID }
func nestedStreamFor(workerID string) string  { return nestedStreamPrefix + workerID }

// controlEnvelope tells a worker to stop one attempt of one job.
//
// Sent when nobody wants the answer any more: the last caller gave up, the job
// was moved elsewhere, or the coordinator failed it. The worker was already
// credited back for the job, and without this it went on running it, so the
// next job sent there waited behind work that had no reader.
type controlEnvelope struct {
	Job     string `json:"job"`
	Attempt int    `json:"attempt,omitempty"`
	Why     string `json:"why,omitempty"`
	// Answer, when set, makes this the other thing the coordinator says to a
	// worker about a job already on it: a call that job made has an outcome.
	// Same stream, because both are about an attempt in flight here and both
	// must get past the queue.
	Answer *answerEnvelope `json:"answer,omitempty"`
}

// jobEnvelope is one unit of work on the wire.
//
// Payload is the user's input already encoded by the Func's codec, so this
// layer never sees In and never needs to know what it was.
type jobEnvelope struct {
	ID      string `json:"id"`
	Func    string `json:"fn"`
	Payload []byte `json:"payload,omitempty"`
	// Run and Thread say which thread of which run this job is, when it is
	// one: the worker runs it as that thread, from that thread's history,
	// and the threads it forks are named under it. Empty for a bare call
	// made on [Cluster.Bind], which belongs to no run.
	Run    string `json:"run,omitempty"`
	Thread string `json:"thread,omitempty"`
	// Attempt counts prior dispatches of this job, starting at 0. Carried so a
	// work function that cares can tell a retry from a first run, and so logs
	// on the worker say which it was.
	Attempt int `json:"attempt,omitempty"`
	// Checkpoint is the last progress a previous attempt reported through
	// [flow.Context.Heartbeat], and is what makes a retry cheap: the work resumes from
	// it rather than starting over. Empty on a first attempt, and on a retry
	// of something that never heartbeated.
	Checkpoint []byte `json:"checkpoint,omitempty"`
	// Steps are the [flow.Context.Step] calls a previous attempt completed, in order.
	// A retry replays them from here instead of running them again. The index
	// each carries is what lets one that went missing leave a detectable hole
	// rather than a silently shifted list.
	Steps []flow.StepRecord `json:"steps,omitempty"`
	// Priors are the event logs earlier attempts of this job left behind,
	// oldest attempt first. A retry reads them with [Priors] to pick up where
	// one of them stopped instead of starting over.
	Priors []Recording `json:"priors,omitempty"`
	// Nested says this job is a thread forked BY another job, and goes on
	// the worker's nested queue rather than its job queue. See serveNested.
	Nested bool `json:"nested,omitempty"`
}

// answerEnvelope is the outcome of a call a job made, sent back to the worker
// running it. Thread and Step name the call the way the job's own history
// does, which is how the worker knows which wait to end.
type answerEnvelope struct {
	Thread  string `json:"thread"`
	Step    uint64 `json:"step"`
	Payload []byte `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
}

// beatEnvelope is one report that a job is still running, and how far it has
// got.
//
// Job rather than offset because a worker runs its whole batch at once, so
// beats from several jobs interleave on one stream and each has to say which it
// belongs to.
type beatEnvelope struct {
	Job string `json:"job"`
	// Leaving says the WORKER is about to go — its machine is being taken
	// back — and names no job. The coordinator treats it as the worker's
	// death, announced early: everything outstanding on it is moved now, in
	// the time the cloud gave, rather than when the connection is found dead.
	Leaving bool `json:"leaving,omitempty"`
	// Attempt is which dispatch of the job this report is from. A job that was
	// moved has an attempt still running where it was left, and that one may
	// wake up and report: without this the coordinator could not tell its
	// beats from the retry's, and would reset the retry's clock and hand it a
	// checkpoint from behind where it already is.
	Attempt int `json:"attempt,omitempty"`
	// Started says the worker has taken this job off its queue and begun it.
	// Sent once, first, so the coordinator's clocks run from when the work
	// began rather than from when it was sent: a job can sit behind others on
	// a busy worker for longer than its own bound without a moment of it being
	// the work's fault. Any beat implies it, so a lost one costs nothing.
	Started    bool   `json:"started,omitempty"`
	Checkpoint []byte `json:"checkpoint,omitempty"`
	// Step is one newly completed flow.Context.Step, if this beat reports one. Sent one at
	// a time rather than as a growing log, so the cost of a step does not climb
	// with how many came before it; the coordinator does the accumulating.
	Step *flow.StepRecord `json:"step,omitempty"`
}

// resultEnvelope is one outcome.
//
// Exactly one of Payload and Error is meaningful. A failing work function is a
// NORMAL result here, not a transport failure: it rides back as Error and is
// re-raised on the coordinator. Only infrastructure trouble aborts the worker's
// batch, which is what keeps one bad input from stalling a worker forever.
type resultEnvelope struct {
	ID string `json:"id"`
	// Attempt is which dispatch produced this. A moved job's abandoned attempt
	// can finish after the move, and a result from it is not the answer: its
	// outputs are the ones the coordinator drops when the job settles, so
	// delivering it would hand the caller handles to streams about to be
	// deleted. Only the current attempt's result counts.
	Attempt int    `json:"attempt,omitempty"`
	Payload []byte `json:"payload,omitempty"`
	Error   string `json:"error,omitempty"`
}

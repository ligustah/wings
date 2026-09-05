package wings

import (
	"strconv"
	"testing"
	"time"
)

// THE POINT: what a job commits on a worker with an engine of its own comes
// home as the transactions it committed. The coordinator's engine knows each
// writer it has applied and how far — which no copy made a stream at a time
// could tell it — and the copy is readable as the same recording.
func TestWhatAJobCommitsComesHomeAsTransactions(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := start(t, Config{Target: LocalProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	rec, err := simulate(ctx, 50)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	var got int
	for _, err := range Replay[Tick](ctx, rec) {
		if err != nil {
			t.Fatalf("Replay: %v", err)
		}
		got++
	}
	if got != 50 {
		t.Fatalf("replayed %d events, want 50", got)
	}

	// The attempt's transactional id, as the worker's engine named the
	// writer: the job's, from the journal, and attempt 0.
	entries := awaitJournal(t, c, func(es []journalEntry) bool { return countKind(es, journalCompleted) >= 1 })
	var job string
	for _, e := range entries {
		if e.Kind == journalSubmitted {
			job = e.Job
		}
	}
	if job == "" {
		t.Fatalf("no submitted job in the journal: %+v", entries)
	}
	workload := "wings.job." + streamPart(job) + "." + strconv.Itoa(0)
	deadline := time.Now().Add(5 * time.Second)
	for {
		mark, known, err := c.engine.Coordinator().AppliedThrough(workload)
		if err != nil {
			t.Fatalf("AppliedThrough: %v", err)
		}
		if known {
			if mark.Epoch != 1 || mark.Sequence == 0 {
				t.Fatalf("the mark for %s is %+v", workload, mark)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the coordinator's engine has applied nothing from writer %s; the recording was copied some other way", workload)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// THE POINT: the puller is offered every transaction a worker committed,
// and declines the ones nobody wants — an attempt the job has moved on
// from, a job that has settled — rather than stopping over one it cannot
// copy whole: the coordinator discards such attempts' streams on the
// worker, and their late transactions are exactly the ones with records
// missing.
func TestThePullerDeclinesWhatNoAttemptWants(t *testing.T) {
	c := &Cluster{pending: map[string]*pendingJob{}}
	c.pending["ep-1"] = &pendingJob{job: jobEnvelope{ID: "ep-1", Attempt: 2}}
	for _, tc := range []struct {
		workload string
		want     bool
	}{
		{"wings.job.ep-1.2", true},          // the attempt the job is on
		{"wings.job.ep-1.3", true},          // one the coordinator has yet to hear of
		{"wings.job.ep-1.1", false},         // an attempt the job moved on from
		{"wings.job.ep-1.0", false},         // ditto
		{"wings.job.ep-2.0", false},         // a job that settled
		{"wings.lineage.some.stream", true}, // not an attempt's at all
		{"wings.job.odd", true},             // not the shape expected; the engine's to judge
	} {
		if got := c.pullWanted(tc.workload); got != tc.want {
			t.Errorf("pullWanted(%q) = %v, want %v", tc.workload, got, tc.want)
		}
	}
}

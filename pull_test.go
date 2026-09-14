package wings

import (
	"strconv"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// THE POINT: the pull's progress watch reconnects a pull that is behind and
// applying nothing, then declares the worker lost if that does not clear it —
// but a pull that keeps advancing, however slowly, resets the clock and trips
// neither, so a slow or paging pull is never mistaken for a wedged one.
func TestPullWatchEscalatesOnlyWhenFrozenAndBehind(t *testing.T) {
	base := time.Now()
	at := func(d time.Duration) time.Time { return base.Add(d) }

	t.Run("caught up never trips", func(t *testing.T) {
		var pw pullWatch
		for s := time.Duration(0); s < 10*time.Minute; s += pullWatchInterval {
			if r, l := pw.observe(false, 100, at(s)); r || l {
				t.Fatalf("caught up at %s escalated: reconnect=%v lost=%v", s, r, l)
			}
		}
	})

	t.Run("slow but advancing never trips", func(t *testing.T) {
		var pw pullWatch
		var applied int64
		for s := time.Duration(0); s < 10*time.Minute; s += pullWatchInterval {
			applied++ // behind the whole time, yet always applying more
			if r, l := pw.observe(true, applied, at(s)); r || l {
				t.Fatalf("advancing pull at %s escalated: reconnect=%v lost=%v", s, r, l)
			}
		}
	})

	t.Run("frozen and behind reconnects once then faults", func(t *testing.T) {
		var pw pullWatch
		var reconnects int
		var firstReconnect, faultAt time.Duration = -1, -1
		for s := time.Duration(0); s <= pullFaultGrace+pullWatchInterval; s += pullWatchInterval {
			r, l := pw.observe(true, 500, at(s)) // behind and frozen throughout
			if r {
				reconnects++
				if firstReconnect < 0 {
					firstReconnect = s
				}
			}
			if l {
				faultAt = s
				break
			}
		}
		// The first reading sets the baseline, so the stall is measured from the
		// second; each escalation then falls in the interval it is first observed.
		if reconnects != 1 {
			t.Fatalf("reconnected %d times, want exactly once", reconnects)
		}
		if firstReconnect < pullInterruptGrace || firstReconnect > pullInterruptGrace+2*pullWatchInterval {
			t.Fatalf("reconnected at %s, want near the interrupt grace %s", firstReconnect, pullInterruptGrace)
		}
		if faultAt < pullFaultGrace || faultAt > pullFaultGrace+2*pullWatchInterval {
			t.Fatalf("faulted at %s, want near the fault grace %s", faultAt, pullFaultGrace)
		}
	})

	t.Run("progress after a stall clears it", func(t *testing.T) {
		var pw pullWatch
		pw.observe(true, 500, at(0))                  // baseline
		pw.observe(true, 500, at(pullInterruptGrace)) // frozen, but not yet reconnecting off one baseline
		if r, l := pw.observe(true, 900, at(pullInterruptGrace+pullWatchInterval)); r || l {
			t.Fatalf("progress did not clear the stall: reconnect=%v lost=%v", r, l)
		}
		// The clock is reset, so a fresh freeze must run the full grace again.
		if r, l := pw.observe(true, 900, at(pullFaultGrace)); r || l {
			t.Fatalf("stall clock was not reset by progress: reconnect=%v lost=%v", r, l)
		}
	})
}

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
	// writer: the job's, from the journal, the main thread, and attempt 0.
	// Each thread commits under its own producer now, so the writer carries
	// the thread id between the job and the attempt.
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
	workload := "wings.job." + streamPart(job) + "." + streamPart(flow.MainThread) + "." + strconv.Itoa(0)
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
// missing. A settled job is still wanted: its last transaction is pulled
// after it finishes.
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
		{"wings.job.ep-2.0", true},          // a settled job: its output may still be arriving home
		{"wings.lineage.some.stream", true}, // not an attempt's at all
		{"wings.job.odd", true},             // not the shape expected; the engine's to judge
	} {
		if got := c.pullWanted(tc.workload); got != tc.want {
			t.Errorf("pullWanted(%q) = %v, want %v", tc.workload, got, tc.want)
		}
	}
}

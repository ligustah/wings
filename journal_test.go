package wings

import (
	"context"
	"testing"
	"time"

	"github.com/ligustah/durable_streams/dsclient"
)

// readJournal reads the coordinator's record back off the stream it was written
// to — through dsclient, the same way anything else reads it, so this proves the
// entries really landed rather than that a struct in memory was populated.
func readJournal(t *testing.T, c *Cluster) []journalEntry {
	t.Helper()

	s, err := c.shared.OpenStream[journalEntry](journalStream)
	if err != nil {
		t.Fatalf("open %s: %v", journalStream, err)
	}

	var out []journalEntry
	for from := int64(0); ; {
		recs, err := s.Read(context.Background(), from, 512)
		if err != nil {
			t.Fatalf("read %s at %d: %v", journalStream, from, err)
		}
		if len(recs) == 0 {
			return out
		}
		for _, r := range recs {
			out = append(out, r.Record)
			from = r.Offset + 1
		}
	}
}

// awaitJournal waits for the async writer to catch up. The journal is
// deliberately off the hot path, so a result reaching the caller does not mean
// its entry is on disk yet.
func awaitJournal(t *testing.T, c *Cluster, want func([]journalEntry) bool) []journalEntry {
	t.Helper()

	var last []journalEntry
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		last = readJournal(t, c)
		if want(last) {
			return last
		}
		time.Sleep(20 * time.Millisecond)
	}
	return last
}

func countKind(entries []journalEntry, kind string) int {
	n := 0
	for _, e := range entries {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

// THE POINT: the coordinator's bookkeeping is durable, not just in a map. Every
// job it accepted and every job it answered is on a stream that outlives the
// process, which is what makes a post-mortem possible at all.
func TestCoordinatorJournalsEveryJob(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Concurrency: 2})

	in := []int{1, 2, 3, 4, 5, 6, 7, 8}
	if _, err := Map(c.Bind(t.Context()), double, in); err != nil {
		t.Fatalf("Map: %v", err)
	}

	entries := awaitJournal(t, c, func(es []journalEntry) bool {
		return countKind(es, journalSubmitted) >= len(in) && countKind(es, journalCompleted) >= len(in)
	})

	submitted := map[string]journalEntry{}
	completed := map[string]journalEntry{}
	for _, e := range entries {
		switch e.Kind {
		case journalSubmitted:
			submitted[e.Job] = e
		case journalCompleted:
			completed[e.Job] = e
		}
	}

	if len(submitted) != len(in) {
		t.Errorf("journal records %d submissions, want %d", len(submitted), len(in))
	}
	for id, e := range submitted {
		done, ok := completed[id]
		if !ok {
			t.Errorf("job %s was submitted but never recorded as completed", id)
			continue
		}
		if done.Err != "" {
			t.Errorf("job %s recorded an error: %s", id, done.Err)
		}
		if e.Func != "test.double" || done.Func != "test.double" {
			t.Errorf("job %s recorded fn %q/%q, want \"test.double\"", id, e.Func, done.Func)
		}
		if e.Worker == "" {
			t.Errorf("job %s recorded no worker", id)
		}
	}

	if countKind(entries, journalWorkerUp) == 0 {
		t.Error("no worker was recorded as entering service")
	}
}

// A failing work function is a normal outcome, and the record must say so — an
// entry that only ever means success is not a record of what happened.
func TestJournalCarriesTheFailure(t *testing.T) {
	c := start(t, Config{Target: InProcess()})

	if _, err := boom(c.Bind(t.Context()), "kaboom"); err == nil {
		t.Fatal("want an error from the failing work function")
	}

	entries := awaitJournal(t, c, func(es []journalEntry) bool {
		return countKind(es, journalCompleted) >= 1
	})

	var found bool
	for _, e := range entries {
		if e.Kind == journalCompleted && e.Err != "" {
			found = true
		}
	}
	if !found {
		t.Errorf("the journal has no completed entry carrying an error; got %d entries", len(entries))
	}
}

// The whole point of the shared instance: in process, the coordinator and every
// worker are on ONE engine, so there is exactly one to close and no per-worker
// directory to leak a lock.
func TestInProcessWorkersShareOneEngine(t *testing.T) {
	c := start(t, Config{
		Target:  InProcess(),
		Workers: 3,
	})

	if got := c.Workers(); got != 3 {
		t.Fatalf("started %d workers, want 3", got)
	}

	var clients []*dsclient.Client
	c.mu.Lock()
	for _, w := range c.workers {
		clients = append(clients, w.client)
		if w.ownsClient {
			t.Errorf("in-process worker %s claims to own the shared client; closing it would take the engine down under the others", w.id)
		}
	}
	c.mu.Unlock()

	for _, cl := range clients {
		if cl != c.shared {
			t.Fatal("an in-process worker is on a client of its own, not the cluster's shared instance")
		}
	}
}

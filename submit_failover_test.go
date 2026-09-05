package wings

import (
	"strings"
	"testing"
	"time"
)

// THE POINT: a job handed to a worker that died a moment ago — picked
// before the coordinator knew — is moved to another, as its jobs are about
// to be, rather than failed. A forked thread that failed this way was its
// parent's to notice at the join, and a parent waiting on a channel the
// thread was to feed never got there.
func TestAJobHandedToAWorkerThatJustDiedIsMoved(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := start(t, Config{Target: LocalProcess(), Workers: 2, Concurrency: 1, ReconnectTimeout: 2 * time.Second})
	ctx := c.Bind(t.Context())

	// The first worker is the one an idle fleet's pick lands on. Killed
	// without a word, so the coordinator finds out only by trying.
	c.mu.Lock()
	first := c.workers[0]
	c.mu.Unlock()
	if first.proc == nil {
		t.Fatal("no process to kill")
	}
	if err := first.proc.Kill(); err != nil {
		t.Fatalf("kill: %v", err)
	}
	_, _ = first.proc.Wait()

	got, err := double(ctx, 21)
	if err != nil {
		t.Fatalf("double: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	entries := awaitJournal(t, c, func(es []journalEntry) bool { return countKind(es, journalCompleted) >= 1 })
	var moved bool
	for _, e := range entries {
		if strings.Contains(e.Err, "could not hand it to "+first.id) {
			moved = true
		}
	}
	if !moved {
		t.Fatalf("the job was not moved off the dead worker it was handed to; journal: %+v", entries)
	}
}

package flow_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings"
	"github.com/ligustah/wings/flow"
)

// long is a work function that takes a while and counts how many times it was
// actually started. Started, not finished: the question is whether a workflow
// retry set a second copy of it going.
var long struct {
	starts  atomic.Int64
	release chan struct{}
	once    sync.Once
}

var longWork = wings.Define("rejoin.long", func(ctx context.Context, in int) (int, error) {
	long.starts.Add(1)
	select {
	case <-long.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return in * 2, nil
})

// THE POINT: a workflow that is retried while one of its activities is still
// running must rejoin that activity, not start another. Two copies of an hour
// of work would be waste on its own; worse, the copy starts from nothing while
// the original is most of the way through, holding the checkpoint and the steps
// that make it cheap to move. Nothing a long activity records about its own
// progress survives being duplicated.
func TestARetriedWorkflowRejoinsTheActivityStillRunning(t *testing.T) {
	ctx := cluster(t)

	long.release = make(chan struct{})
	long.starts.Store(0)
	t.Cleanup(func() { long.once.Do(func() { close(long.release) }) })

	var attempts atomic.Int64
	wf := flow.Define("t.rejoin", func(ctx context.Context, n int) (int, error) {
		fut := flow.Go(ctx, longWork, n)

		// The first attempt walks away while the activity is still going. Its
		// call is recorded and its return is not, which is exactly the state a
		// coordinator crash or a failing workflow leaves behind.
		if attempts.Add(1) == 1 {
			return 0, errors.New("abandoned while the activity ran")
		}
		return fut.Await(ctx)
	}, flow.Backoff(time.Millisecond, time.Millisecond))

	done := make(chan struct{})
	var (
		got int
		err error
	)
	go func() {
		defer close(done)
		got, err = flow.Run(ctx, wf, flow.NewInstance(), 21, flow.WithStore(flow.NewMemStore()))
	}()

	// Give the second attempt time to reach the call and rejoin, then let the
	// one running activity finish.
	waitFor(t, func() bool { return attempts.Load() >= 2 })
	time.Sleep(300 * time.Millisecond)
	long.once.Do(func() { close(long.release) })

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the workflow never finished")
	}
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}

	if n := long.starts.Load(); n != 1 {
		t.Fatalf("the activity was started %d times; a retried workflow must rejoin the one already running, not add another", n)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition never became true")
}

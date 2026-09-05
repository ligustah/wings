package flow_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ligustah/wings/flow"
)

// long is a function that takes a while and counts how many times it was
// actually started. Started, not finished: the question is whether a run's
// retry set a second copy of it going.
var long struct {
	starts  atomic.Int64
	release chan struct{}
}

var longWork = flow.Define("rejoin.long", func(ctx context.Context, in int) (int, error) {
	long.starts.Add(1)
	select {
	case <-long.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	return in * 2, nil
})

// rejoining is an executor that recognises a call still in flight by its
// origin and hands the second asker the first one's answer — what a cluster
// does with the jobs it has outstanding.
type rejoining struct {
	mu       sync.Mutex
	inflight map[string]*pending
}

// pending is one call in flight: done closes when its answer is in.
type pending struct {
	done chan struct{}
	out  []byte
	err  error
}

func (r *rejoining) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	key := flow.OriginFrom(ctx).Key()

	r.mu.Lock()
	if r.inflight == nil {
		r.inflight = map[string]*pending{}
	}
	p, ok := r.inflight[key]
	if !ok {
		p = &pending{done: make(chan struct{})}
		r.inflight[key] = p
		go func() {
			p.out, p.err = flow.Execute(context.WithoutCancel(ctx), name, payload)
			close(p.done)
		}()
	}
	r.mu.Unlock()

	select {
	case <-p.done:
		return p.out, p.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// THE POINT: a run that is retried while one of its calls is still running
// must rejoin that call, not start another. Two copies of an hour of work
// would be waste on its own; worse, the copy starts from nothing while the
// original is most of the way through, holding the checkpoint and the steps
// that make it cheap to move. The run's part is to hand the executor the same
// origin on the retry; the executor's is to recognise it.
func TestARetriedRunRejoinsTheCallStillRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		long.release = make(chan struct{})
		long.starts.Store(0)

		var attempts atomic.Int64
		var got int
		done := make(chan error, 1)
		go func() {
			done <- flow.Run(t.Context(), flow.NewName(), func(ctx context.Context) error {
				fut := flow.Go(ctx, longWork, 21)

				// The first attempt walks away while the call is still going. Its
				// call is recorded and its return is not, which is exactly the
				// state a crash or a failing body leaves behind.
				if attempts.Add(1) == 1 {
					return errors.New("abandoned while the call ran")
				}
				var err error
				got, err = fut.Await(ctx)
				return err
			}, flow.WithStore(flow.NewMemStore()), flow.WithExecutor(&rejoining{}), quick)
		}()

		// A second on the bubble's clock is the first attempt walking away,
		// its backoff, and the second attempt reaching the call — after which
		// everything is waiting on the one call still running.
		synctest.Sleep(time.Second)
		if n := attempts.Load(); n != 2 {
			t.Fatalf("the body ran %d times, want 2 by now", n)
		}
		if n := long.starts.Load(); n != 1 {
			t.Fatalf("the call was started %d times; a retried run must rejoin the one already running, not add another", n)
		}
		close(long.release)

		if err := <-done; err != nil {
			t.Fatalf("Run: %v", err)
		}
		if got != 42 {
			t.Fatalf("got %d, want 42", got)
		}
	})
}

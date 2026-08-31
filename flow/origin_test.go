package flow_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings"
	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/internal/invoke"
)

// spy is a host that watches what a workflow asks of the thing underneath it.
//
// A real cluster would do too — the origin ends up in its journal — but reading
// it back there means reaching into wings' unexported record from a package
// wings itself imports, which is a cycle. The seam is where the claim lives
// anyway: what the workflow host stamps on a dispatch is exactly what a
// coordinator has to write down.
type spy struct {
	inner invoke.Host

	mu   sync.Mutex
	seen []invoke.Origin
}

func (s *spy) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	s.mu.Lock()
	s.seen = append(s.seen, invoke.OriginFrom(ctx))
	s.mu.Unlock()
	return s.inner.Invoke(ctx, name, payload)
}

func (s *spy) Parallel(ctx context.Context, n int, body func(context.Context, int) error) []error {
	return s.inner.Parallel(ctx, n, body)
}

func (s *spy) Streams() *dsclient.Client { return s.inner.Streams() }

func (s *spy) origins() []invoke.Origin {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]invoke.Origin(nil), s.seen...)
}

// watched wraps a cluster context so every dispatch out of a workflow is
// recorded.
func watched(t *testing.T) (context.Context, *spy) {
	t.Helper()
	ctx := cluster(t)
	s := &spy{inner: invoke.From(ctx)}
	return invoke.With(ctx, s), s
}

// THE POINT: a coordinator's record of what it sent where has to be answerable
// at the level someone asks at. "Job 3f went to remote-2" is useless on its own;
// "the second step of run order-77 went to remote-2 and never came back" is the
// question. Only the workflow knows which run a call belongs to, so it is the
// workflow that has to say.
func TestDispatchedWorkNamesTheRunItBelongsTo(t *testing.T) {
	ctx, spy := watched(t)

	chain := flow.Define("origin.chain", func(ctx context.Context, n int) (int, error) {
		for range 3 {
			var err error
			if n, err = double(ctx, n); err != nil {
				return 0, err
			}
		}
		return n, nil
	})

	if got, err := flow.Run(ctx, chain, "order-77", 1, flow.WithStore(flow.NewMemStore())); err != nil {
		t.Fatalf("Run: %v", err)
	} else if got != 8 {
		t.Fatalf("got %d, want 8", got)
	}

	origins := spy.origins()
	if len(origins) != 3 {
		t.Fatalf("%d calls were dispatched, want 3", len(origins))
	}
	for i, o := range origins {
		if o.Flow != "origin.chain" {
			t.Errorf("call %d says it belongs to flow %q, want \"origin.chain\"", i, o.Flow)
		}
		if o.Run != "order-77" {
			t.Errorf("call %d says it belongs to run %q, want \"order-77\"", i, o.Run)
		}
		if o.Thread != "main" {
			t.Errorf("call %d ran on thread %q, want \"main\"", i, o.Thread)
		}
	}

	// The steps must be distinct and in order, or the record cannot say WHICH
	// call of the three a worker was given.
	for i, o := range origins {
		if i > 0 && o.Step <= origins[i-1].Step {
			t.Errorf("call %d is at step %d, which is not past the %d of the call before it",
				i, o.Step, origins[i-1].Step)
		}
	}
}

// A fan-out is where the run alone stops being enough: three calls of the same
// workflow at the same position, told apart only by the thread they are on.
func TestForkedWorkNamesItsThread(t *testing.T) {
	ctx, spy := watched(t)

	fan := flow.Define("origin.fan", func(ctx context.Context, n int) (int, error) {
		outs, err := wings.Map(ctx, double, []int{1, 2, 3})
		if err != nil {
			return 0, err
		}
		return outs[0] + outs[1] + outs[2], nil
	})

	if got, err := flow.Run(ctx, fan, "batch-1", 0, flow.WithStore(flow.NewMemStore())); err != nil {
		t.Fatalf("Run: %v", err)
	} else if got != 12 {
		t.Fatalf("got %d, want 12", got)
	}

	threads := map[string]bool{}
	for _, o := range spy.origins() {
		if o.Run != "batch-1" {
			t.Errorf("a forked call says it belongs to run %q, want \"batch-1\"", o.Run)
		}
		threads[o.Thread] = true
	}
	if len(threads) != 3 {
		t.Fatalf("three parallel calls report %d distinct threads: %v", len(threads), threads)
	}
	for i := range 3 {
		want := fmt.Sprintf("main.%d", i)
		if !threads[want] {
			t.Errorf("no call was recorded on thread %q; the names must be the deterministic ones replay uses", want)
		}
	}
}

// Work called outside a workflow belongs to nothing larger, and must not claim
// to — an origin invented for a bare call would put a run id in the record that
// names no run.
func TestABareCallHasNoOrigin(t *testing.T) {
	ctx, spy := watched(t)

	if got, err := double(ctx, 21); err != nil {
		t.Fatalf("double: %v", err)
	} else if got != 42 {
		t.Fatalf("got %d", got)
	}

	origins := spy.origins()
	if len(origins) != 1 {
		t.Fatalf("%d calls were dispatched, want 1", len(origins))
	}
	if !origins[0].Zero() {
		t.Errorf("a call made outside a workflow reports origin %+v, want none", origins[0])
	}
}

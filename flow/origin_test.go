package flow_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ligustah/wings/flow"
)

// spy is an executor that watches what a run asks of it.
//
// The seam is where the claim lives: what the run stamps on a dispatch is
// exactly what an executor that keeps a record — a cluster, say — has to
// write down.
type spy struct {
	inner flow.Executor

	mu   sync.Mutex
	seen []flow.Origin
}

func (s *spy) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	s.mu.Lock()
	s.seen = append(s.seen, flow.OriginFrom(ctx))
	s.mu.Unlock()
	return s.inner.Invoke(ctx, name, payload)
}

func (s *spy) origins() []flow.Origin {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]flow.Origin(nil), s.seen...)
}

// THE POINT: an executor's record of what it ran where has to be answerable
// at the level someone asks at. "Job 3f went to remote-2" is useless on its
// own; "the second call of run order-77 went to remote-2 and never came back"
// is the question. Only the run knows which run a call belongs to, so it is
// the run that has to say.
func TestDispatchedWorkNamesTheRunItBelongsTo(t *testing.T) {
	s := &spy{inner: flow.Local()}

	var got int
	err := flow.Run(t.Context(), "order-77", func(ctx flow.Context) error {
		n := 1
		for range 3 {
			var err error
			if n, err = double(ctx, n); err != nil {
				return err
			}
		}
		got = n
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.WithExecutor(s))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 8 {
		t.Fatalf("got %d, want 8", got)
	}

	origins := s.origins()
	if len(origins) != 3 {
		t.Fatalf("%d calls were dispatched, want 3", len(origins))
	}
	for i, o := range origins {
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

// placements is a placer that watches what a run forks and runs it here.
type placements struct {
	mu   sync.Mutex
	seen []flow.Thread
}

func (p *placements) Place(ctx context.Context, th flow.Thread, body func(flow.Context) ([]byte, error)) ([]byte, error) {
	p.mu.Lock()
	p.seen = append(p.seen, th)
	p.mu.Unlock()
	return flow.InProcess().Place(ctx, th, body)
}

// A fan-out is where the run alone stops being enough: three threads of the
// same run running the same function, told apart only by their names — and
// the placer is told everything it needs to run each one elsewhere: the run,
// the thread, the function and its input.
func TestForkedWorkNamesItsThread(t *testing.T) {
	p := &placements{}

	var got int
	err := flow.Run(t.Context(), "batch-1", func(ctx flow.Context) error {
		outs, err := ctx.Map(double, []int{1, 2, 3})
		if err != nil {
			return err
		}
		got = outs[0] + outs[1] + outs[2]
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.WithPlacer(p))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 12 {
		t.Fatalf("got %d, want 12", got)
	}

	threads := map[string]bool{}
	for _, th := range p.seen {
		if th.Run != "batch-1" {
			t.Errorf("a forked thread says it belongs to run %q, want \"batch-1\"", th.Run)
		}
		if th.Parent != "main" {
			t.Errorf("thread %s says its parent is %q, want \"main\"", th.ID, th.Parent)
		}
		if th.Fn != "flow.double" {
			t.Errorf("thread %s runs %q, want the function the fan-out was over", th.ID, th.Fn)
		}
		if len(th.Input) == 0 {
			t.Errorf("thread %s carries no input; a placer running it elsewhere would have nothing to run it on", th.ID)
		}
		threads[th.ID] = true
	}
	if len(threads) != 3 {
		t.Fatalf("three parallel calls report %d distinct threads: %v", len(threads), threads)
	}
	for i := range 3 {
		want := fmt.Sprintf("main.%d", i)
		if !threads[want] {
			t.Errorf("no thread was placed as %q; the names must be the deterministic ones replay uses", want)
		}
	}
}

// Work called outside a run belongs to nothing larger, and must not claim
// to — an origin invented for a bare call would put a run name in the record
// that names no run.
func TestABareCallHasNoOrigin(t *testing.T) {
	s := &spy{inner: flow.Local()}

	if got, err := double(flow.Bind(context.Background(), s), 21); err != nil {
		t.Fatalf("double: %v", err)
	} else if got != 42 {
		t.Fatalf("got %d", got)
	}

	origins := s.origins()
	if len(origins) != 1 {
		t.Fatalf("%d calls were dispatched, want 1", len(origins))
	}
	if !origins[0].Zero() {
		t.Errorf("a call made outside a run reports origin %+v, want none", origins[0])
	}
}

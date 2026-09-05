package flow_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// placingElsewhere is a placer that sends every forked thread to "another
// process": a function thread runs as a run of its own, and a thread of run
// code is reached by replaying its lineage — both against the same store
// and host, which is what a worker would have been given copies of.
type placingElsewhere struct {
	store flow.Store
	host  flow.ChannelHost

	mu       sync.Mutex
	lineages [][]string
	roots    []flow.Root
}

func (p *placingElsewhere) opts() []flow.RunOption {
	return []flow.RunOption{flow.WithStore(p.store), flow.WithChannelHost(p.host), flow.WithPlacer(p), flow.Once()}
}

func (p *placingElsewhere) Place(ctx context.Context, th flow.Thread, body func(flow.Context) ([]byte, error)) ([]byte, error) {
	if th.Fn != "" {
		return flow.RunThread(ctx, th.Run, th.ID, th.Fn, th.Input, p.opts()...)
	}
	if err := flow.Share(ctx); err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.lineages = append(p.lineages, slices.Clone(th.Lineage))
	p.roots = append(p.roots, th.Root)
	p.mu.Unlock()
	out, err := flow.RunLineage(ctx, th.Run, th.Root, th.Lineage, p.opts()...)
	if errors.Is(err, flow.ErrUnknownRoot) {
		return flow.InProcess().Place(ctx, th, body)
	}
	return out, err
}

var spawnsAProducer = flow.DefineWorkflow("test.spawnsAProducer", func(ctx flow.Context, base int) error {
	ch := ctx.NewChannel[int]()
	producer := ctx.Spawn(func(ctx flow.Context) (int, error) {
		for i := 1; i <= 3; i++ {
			if err := ch.Send(ctx, base+i); err != nil {
				return 0, err
			}
		}
		return 3, ch.Close(ctx)
	})
	total := 0
	for {
		v, ok, err := ch.Recv(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		total += v
	}
	n, err := producer.Await(ctx)
	if err != nil {
		return err
	}
	spawned.total, spawned.count = total, n
	return nil
})

var spawned struct{ total, count int }

// THE POINT: a thread of run code can run in another process. The process
// replays the workflow from its history to the fork, is handed the closure
// there, and runs it — sharing the run's channels with the workflow that
// stayed home.
func TestAThreadOfRunCodeRunsElsewhereByItsLineage(t *testing.T) {
	p := &placingElsewhere{store: flow.NewMemStore(), host: flow.NewMemChannelHost()}
	spawned.total, spawned.count = 0, 0
	if err := spawnsAProducer.Run(t.Context(), 10, p.opts()...); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if spawned.total != 36 || spawned.count != 3 {
		t.Fatalf("received a total of %d from %d sends, want 36 from 3", spawned.total, spawned.count)
	}
	if len(p.lineages) != 1 || !slices.Equal(p.lineages[0], []string{"main", "main.0"}) {
		t.Fatalf("placed lineages %v, want one: main, main.0", p.lineages)
	}
}

var spawnsWithin = flow.DefineWorkflow("test.spawnsWithin", func(ctx flow.Context, base int) error {
	results := ctx.NewBufferedChannel[int](4)
	outer := ctx.Spawn(func(ctx flow.Context) (int, error) {
		// A thread of run code forked by one that was itself placed
		// elsewhere: its lineage is three long.
		inner := ctx.Spawn(func(ctx flow.Context) (int, error) {
			return base * 2, results.Send(ctx, base*2)
		})
		v, err := inner.Await(ctx)
		if err != nil {
			return 0, err
		}
		return v + 1, results.Send(ctx, v+1)
	})
	v, err := outer.Await(ctx)
	if err != nil {
		return err
	}
	a, _, err := results.Recv(ctx)
	if err != nil {
		return err
	}
	b, _, err := results.Recv(ctx)
	if err != nil {
		return err
	}
	within.awaited, within.received = v, a+b
	return nil
})

var within struct{ awaited, received int }

// THE POINT: lineages nest. A thread of run code forked by a thread that was
// itself placed elsewhere is reached by replaying both, and a channel made
// before either left reaches all three processes.
func TestLineagesNest(t *testing.T) {
	p := &placingElsewhere{store: flow.NewMemStore(), host: flow.NewMemChannelHost()}
	within.awaited, within.received = 0, 0
	if err := spawnsWithin.Run(t.Context(), 5, p.opts()...); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if within.awaited != 11 || within.received != 21 {
		t.Fatalf("awaited %d and received %d, want 11 and 21", within.awaited, within.received)
	}
	want := [][]string{{"main", "main.0"}, {"main", "main.0", "main.0.0"}}
	if len(p.lineages) != 2 || !slices.Equal(p.lineages[0], want[0]) || !slices.Equal(p.lineages[1], want[1]) {
		t.Fatalf("placed lineages %v, want %v", p.lineages, want)
	}
}

// A run started with a bare body has no root another process could start
// from, and its threads of run code stay home.
func TestAThreadOfABareRunStaysHome(t *testing.T) {
	p := &placingElsewhere{store: flow.NewMemStore(), host: flow.NewMemChannelHost()}
	var got int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		var err error
		got, err = ctx.Spawn(func(ctx flow.Context) (int, error) { return 7, nil }).Await(ctx)
		return err
	}, p.opts()...)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 7 {
		t.Fatalf("got %d, want 7", got)
	}
	if len(p.roots) != 1 || p.roots[0].Workflow != "" || p.roots[0].Function != "" {
		t.Fatalf("a bare run's thread was offered with root %+v, want none", p.roots)
	}
}

var spawnsSlowly = flow.DefineWorkflow("test.spawnsSlowly", func(ctx flow.Context, base int) error {
	// A thread off the lineage, not joined by the time the target is
	// forked: the root's replay, reaching its join and finding none, is
	// over at once.
	other := ctx.Spawn(func(ctx flow.Context) (int, error) {
		time.Sleep(400 * time.Millisecond)
		return 0, nil
	})
	outer := ctx.Spawn(func(ctx flow.Context) (int, error) {
		// Computation between the events of a history is done again by a
		// replay, and takes as long: the process replaying this thread to
		// the fork below is still here while the root's replay, which has
		// nothing to do but read, runs out of history.
		time.Sleep(150 * time.Millisecond)
		return ctx.Spawn(func(ctx flow.Context) (int, error) { return base + 1, nil }).Await(ctx)
	})
	if _, err := other.Await(ctx); err != nil {
		return err
	}
	var err error
	slowly, err = outer.Await(ctx)
	return err
})

var slowly int

// THE POINT: a root whose replay ends before an ancestor's has reached the
// target's fork has not outrun the histories, only the goroutine replaying
// the ancestor. RunLineage waits for every thread on the path.
func TestALineageWaitsForAnAncestorStillReplaying(t *testing.T) {
	p := &placingElsewhere{store: flow.NewMemStore(), host: flow.NewMemChannelHost()}
	slowly = 0
	if err := spawnsSlowly.Run(t.Context(), 41, p.opts()...); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if slowly != 42 {
		t.Fatalf("got %d, want 42", slowly)
	}
}

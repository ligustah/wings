package flow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// parking is a Parker that writes down every park and every resume.
type parking struct {
	mu      sync.Mutex
	parked  []flow.Wait
	resumed []flow.Wait
}

func (p *parking) Park(_ context.Context, w flow.Wait) func(context.Context) error {
	p.mu.Lock()
	p.parked = append(p.parked, w)
	p.mu.Unlock()
	return func(context.Context) error {
		p.mu.Lock()
		p.resumed = append(p.resumed, w)
		p.mu.Unlock()
		return nil
	}
}

func (p *parking) waits(of []flow.Wait) map[string][]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := map[string][]string{}
	for _, w := range of {
		out[w.On] = append(out[w.On], w.Thread)
	}
	return out
}

// THE POINT: a thread that waits says so, once per wait, and says when the
// wait is over — which is what lets a process that bounds its running
// threads not count the waiting ones. A wait that is over before it began
// is no wait at all.
func TestAThreadIsParkedWhileItWaits(t *testing.T) {
	p := &parking{}
	err := flow.Run(t.Context(), "parked", func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		release := make(chan struct{})
		producer := ctx.Spawn(func(ctx flow.Context) (int, error) {
			<-release
			// An unbuffered send with the receiver not yet there: waits.
			return 0, ch.Send(ctx, 1)
		})

		// A receive with nothing to take: waits, for the producer.
		go func() {
			time.Sleep(20 * time.Millisecond)
			close(release)
		}()
		if _, _, err := ch.Recv(ctx); err != nil {
			return err
		}
		if err := ctx.Sleep(10 * time.Millisecond); err != nil {
			return err
		}
		// The producer has sent and returned by the time this waits — or
		// nearly; either way its join is a wait the parker may or may not
		// see, so it is not asserted on.
		if _, err := producer.Await(ctx); err != nil {
			return err
		}

		// A thread that is already over is not waited for.
		done := ctx.Spawn(func(ctx flow.Context) (int, error) { return 7, nil })
		time.Sleep(20 * time.Millisecond)
		_, err := done.Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()), flow.WithParker(p))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	parked := p.waits(p.parked)
	if got := parked[flow.WaitRecv]; len(got) != 1 || got[0] != "main" {
		t.Errorf("receives parked: %v, want main once", got)
	}
	if got := parked[flow.WaitSleep]; len(got) != 1 || got[0] != "main" {
		t.Errorf("sleeps parked: %v, want main once", got)
	}
	if got := parked[flow.WaitSend]; len(got) != 1 || got[0] != "main.0" {
		t.Errorf("sends parked: %v, want main.0 once", got)
	}
	if got := parked[flow.WaitJoin]; len(got) > 1 {
		t.Errorf("joins parked: %v, want at most one — a thread already over is not waited for", got)
	}
	if len(p.resumed) != len(p.parked) {
		t.Errorf("%d parks and %d resumes; every wait that ends must resume", len(p.parked), len(p.resumed))
	}
}

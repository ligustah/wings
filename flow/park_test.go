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

// only reports that names is non-empty and every entry is want.
func only(names []string, want string) bool {
	if len(names) == 0 {
		return false
	}
	for _, n := range names {
		if n != want {
			return false
		}
	}
	return true
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

// has reports whether any thread has parked on a wait of this kind yet, so a
// test can wait for a park to be recorded instead of racing a fixed sleep.
func (p *parking) has(on string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.parked {
		if w.On == on {
			return true
		}
	}
	return false
}

// THE POINT: a thread that waits says so, once per wait, and says when the
// wait is over — which is what lets a process that bounds its running
// threads not count the waiting ones. A wait that is over before it began
// is no wait at all.
func TestAThreadIsParkedWhileItWaits(t *testing.T) {
	p := &parking{}
	err := flow.Run(t.Context(), "parked", func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int](flow.WithCapacity(1))
		release := make(chan struct{})
		producer := ctx.Spawn(func(ctx flow.Context) (int, error) {
			<-release // held until the receive below is parked with nothing to take
			for _, v := range []int{1, 2, 3} {
				if err := w.Send(ctx, v); err != nil {
					return 0, err
				}
			}
			return 0, nil
		})

		// A receive with nothing to take waits, for the producer; release the
		// producer only once that wait is on record, so the receive park is certain.
		go func() {
			for !p.has(flow.WaitRecv) {
				time.Sleep(time.Millisecond)
			}
			close(release)
		}()
		if _, _, err := r.Recv(ctx); err != nil { // parks (WaitRecv), then takes 1
			return err
		}

		// With the first value taken the producer sends the next two: the second
		// fills the one-place buffer and the third waits for room while nothing is
		// receiving. Drain only once that send wait is on record, so it is certain.
		for !p.has(flow.WaitSend) {
			time.Sleep(time.Millisecond)
		}
		for range 2 {
			if _, _, err := r.Recv(ctx); err != nil {
				return err
			}
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

	// A buffered channel decouples sender and receiver, so a receive may park
	// once or twice depending on whether the next value is already buffered when
	// it looks; the point is that each wait is reported by the right thread and
	// every one resumes. The sleep parks exactly once.
	parked := p.waits(p.parked)
	if got := parked[flow.WaitRecv]; !only(got, "main") {
		t.Errorf("receives parked: %v, want main", got)
	}
	if got := parked[flow.WaitSleep]; len(got) != 1 || got[0] != "main" {
		t.Errorf("sleeps parked: %v, want main once", got)
	}
	if got := parked[flow.WaitSend]; !only(got, "main.0") {
		t.Errorf("sends parked: %v, want main.0", got)
	}
	if got := parked[flow.WaitJoin]; len(got) > 1 {
		t.Errorf("joins parked: %v, want at most one — a thread already over is not waited for", got)
	}
	if len(p.resumed) != len(p.parked) {
		t.Errorf("%d parks and %d resumes; every wait that ends must resume", len(p.parked), len(p.resumed))
	}
}

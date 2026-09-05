package flow_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// givesUp is a body that bounds every kind of wait with a timeout of its own
// and goes on when it fires, then fails on its first attempt so that a
// second replays it. Each wait's error goes into seen, in order.
func givesUp(seen *[]error, attempts *int) func(ctx flow.Context) error {
	return func(ctx flow.Context) error {
		*seen = (*seen)[:0]
		const bound = 50 * time.Millisecond

		slow := ctx.Spawn(func(ctx flow.Context) (int, error) {
			if err := ctx.Sleep(2 * time.Second); err != nil {
				return 0, err
			}
			return 1, nil
		})
		tctx, cancel := ctx.WithTimeout(bound)
		_, err := slow.Await(tctx)
		cancel()
		*seen = append(*seen, err)

		quiet := ctx.NewChannel[int]()
		tctx, cancel = ctx.WithTimeout(bound)
		_, _, err = quiet.Recv(tctx)
		cancel()
		*seen = append(*seen, err)

		tctx, cancel = ctx.WithTimeout(bound)
		err = quiet.Send(tctx, 1)
		cancel()
		*seen = append(*seen, err)

		cctx, stop := ctx.WithCancel()
		go func() { time.Sleep(bound); stop() }()
		err = cctx.Sleep(2 * time.Second)
		*seen = append(*seen, err)

		tctx, cancel = ctx.WithTimeout(bound)
		err = tctx.Sleep(2 * time.Second)
		cancel()
		*seen = append(*seen, err)

		*attempts++
		if *attempts == 1 {
			return errors.New("fail once, to be retried")
		}
		return nil
	}
}

// THE POINT: a wait the body cut short with a timeout or cancel of its own
// is on record, and the next attempt is cut short at the same place with the
// same error, at once — rather than waiting again, or finding this time
// that the wait completes and the body diverges from its history.
func TestAWaitTheBodyGaveUpOnIsGivenUpOnAgain(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()
	var seen []error
	attempts := 0
	body := givesUp(&seen, &attempts)

	err := flow.Run(t.Context(), name, body, flow.WithStore(store), flow.Once())
	if err == nil {
		t.Fatal("the first attempt was meant to fail")
	}
	first := append([]error(nil), seen...)
	if len(first) != 5 {
		t.Fatalf("the first attempt saw %d waits, want 5", len(first))
	}

	started := time.Now()
	if err := flow.Run(t.Context(), name, body, flow.WithStore(store), flow.Once()); err != nil {
		t.Fatalf("second attempt: %v", err)
	}
	if took := time.Since(started); took > 500*time.Millisecond {
		t.Fatalf("the second attempt took %v; it must not wait for what the first gave up on", took)
	}
	if len(seen) != 5 {
		t.Fatalf("the second attempt saw %d waits, want 5", len(seen))
	}
	for i := range first {
		if !errors.Is(seen[i], first[i]) || seen[i] == nil {
			t.Fatalf("wait %d: the first attempt saw %v, the second %v", i, first[i], seen[i])
		}
	}
	if !errors.Is(first[0], context.DeadlineExceeded) || !errors.Is(first[3], context.Canceled) {
		t.Fatalf("the waits ended with %v", first)
	}
}

// THE POINT: what the thread's own context cuts short is not on record. The
// attempt is over; the next one waits again, and gets the answer.
func TestAWaitTheAttemptCutShortIsWaitedForAgain(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()
	release := make(chan struct{})
	body := func(ctx flow.Context) error {
		v, err := ctx.Spawn(func(ctx flow.Context) (int, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			return 7, nil
		}).Await(ctx)
		if err != nil {
			return err
		}
		if v != 7 {
			return errors.New("not seven")
		}
		return nil
	}

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	err := flow.Run(ctx, name, body, flow.WithStore(store), flow.Once())
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the first attempt ended with %v, want its context's deadline", err)
	}

	close(release)
	if err := flow.Run(t.Context(), name, body, flow.WithStore(store), flow.Once()); err != nil {
		t.Fatalf("second attempt: %v; the join was never given up on by the body", err)
	}
}

var givesUpThenSpawns = flow.DefineWorkflow("test.givesUpThenSpawns", func(ctx flow.Context, base int) error {
	slow := ctx.Spawn(func(ctx flow.Context) (int, error) {
		if err := ctx.Sleep(2 * time.Second); err != nil {
			return 0, err
		}
		return 0, nil
	})
	tctx, cancel := ctx.WithTimeout(50 * time.Millisecond)
	_, err := slow.Await(tctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		return flow.Permanent(errors.New("the wait ended with something else"))
	}
	v, err := ctx.Spawn(func(ctx flow.Context) (int, error) { return base + 1, nil }).Await(ctx)
	if err != nil {
		return err
	}
	if v != base+1 {
		return flow.Permanent(errors.New("wrong value"))
	}
	return nil
})

// THE POINT: a replay toward a thread of run code passes the wait its
// parent gave up on the way the parent did — at once, with the same error —
// and does not stand where the parent gave up, nor take the thread it
// gave up on for one to join.
func TestAReplayToAForkPassesAWaitGivenUpOn(t *testing.T) {
	p := &placingElsewhere{store: flow.NewMemStore(), host: flow.NewMemChannelHost()}
	started := time.Now()
	if err := givesUpThenSpawns.Run(t.Context(), 3, p.opts()...); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if took := time.Since(started); took > 2*time.Second {
		t.Fatalf("the run took %v", took)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var replayed bool
	for _, l := range p.lineages {
		if strings.Join(l, "/") == "main/main.1" {
			replayed = true
		}
	}
	if !replayed {
		t.Fatalf("the lineages placed were %v; the second thread was to be reached by replay", p.lineages)
	}
}

// THE POINT: a send on a channel that is already closed is refused, with an
// error rather than a panic, and refused again on replay; what a send under
// way offered is drained by the receiver that comes, closed or not.
func TestASendOnAClosedChannelIsRefused(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()
	attempts := 0
	var seen []error
	body := func(ctx flow.Context) error {
		seen = seen[:0]
		ch := ctx.NewChannel[int]()
		if err := ch.Close(ctx); err != nil {
			return err
		}
		seen = append(seen, ch.Send(ctx, 2))
		attempts++
		if attempts == 1 {
			return errors.New("fail once, to be retried")
		}
		return nil
	}
	if err := flow.Run(t.Context(), name, body, flow.WithStore(store), flow.Once()); err == nil {
		t.Fatal("the first attempt was meant to fail")
	}
	if !errors.Is(seen[0], flow.ErrChannelClosed) {
		t.Fatalf("the send after the close returned %v", seen[0])
	}
	if err := flow.Run(t.Context(), name, body, flow.WithStore(store), flow.Once()); err != nil {
		t.Fatalf("second attempt: %v", err)
	}
	if !errors.Is(seen[0], flow.ErrChannelClosed) {
		t.Fatalf("the replayed send after the close returned %v", seen[0])
	}
}

// THE POINT: a send under way when the channel closes is not refused: what
// it offered is drained by the receiver that comes, which finds the channel
// closed only after.
func TestASendUnderWayAtTheCloseIsDrained(t *testing.T) {
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		sender := ctx.Spawn(func(ctx flow.Context) (int, error) {
			return 1, ch.Send(ctx, 1)
		})
		time.Sleep(50 * time.Millisecond)
		if err := ch.Close(ctx); err != nil {
			return err
		}
		v, more, err := ch.Recv(ctx)
		if err != nil || !more || v != 1 {
			return fmt.Errorf("the receive got %d, %v, %v; want the value the sender offered", v, more, err)
		}
		if _, more, err := ch.Recv(ctx); err != nil || more {
			return fmt.Errorf("the second receive got %v, %v; want the channel closed", more, err)
		}
		_, err = sender.Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()), flow.Once())
	if err != nil {
		t.Fatal(err)
	}
}

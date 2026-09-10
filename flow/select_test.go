package flow_test

import (
	"errors"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

var selFast = flow.Define(func(ctx flow.Context, n int) (int, error) { return n, nil })

// THE POINT: AwaitAny returns the future that finishes, not the one that blocks,
// and does not touch the loser.
func TestAwaitAnyReturnsTheFinisher(t *testing.T) {
	var idx, got int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		block, _ := ctx.NewChannel[int]()
		fast := ctx.Go(selFast, 42)
		slow := ctx.Spawn(func(ctx flow.Context) (int, error) {
			v, _, err := block.Recv(ctx) // nothing is ever sent
			return v, err
		})
		var err error
		idx, got, err = flow.AwaitAny(ctx, fast, slow)
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if idx != 0 || got != 42 {
		t.Fatalf("AwaitAny returned (%d, %d), want the fast future (0, 42)", idx, got)
	}
}

// THE POINT: which case won is recorded, so a retry replays the same choice and
// value rather than racing again.
func TestSelectReplaysTheSameCase(t *testing.T) {
	var wins []int
	var vals []int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		block, _ := ctx.NewChannel[int]()
		a := ctx.Go(selFast, 7)
		slow := ctx.Spawn(func(ctx flow.Context) (int, error) {
			v, _, err := block.Recv(ctx)
			return v, err
		})
		win, v, err := flow.AwaitAny(ctx, a, slow)
		if err != nil {
			return err
		}
		wins = append(wins, win)
		vals = append(vals, v)
		if len(wins) == 1 {
			return errors.New("fail once")
		}
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.Backoff(0, 0))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(wins) != 2 || wins[0] != wins[1] || vals[0] != vals[1] {
		t.Fatalf("attempts chose %v with %v; the replay must repeat the recorded case", wins, vals)
	}
}

// THE POINT: a timeout is a case; it wins when nothing else is ready, and the
// win is recorded so a retry does not wait again.
func TestSelectAfterWins(t *testing.T) {
	var fired []string
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch, _ := ctx.NewChannel[int]() // nothing is ever sent
		err := ctx.Select().
			Recv(ch, func(int, bool, error) error { fired = append(fired, "recv"); return nil }).
			After(20*time.Millisecond, func() error { fired = append(fired, "timeout"); return nil }).
			Do(ctx)
		if err != nil {
			return err
		}
		if len(fired) == 1 {
			return errors.New("fail once")
		}
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.Backoff(0, 0))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(fired) != 2 || fired[0] != "timeout" || fired[1] != "timeout" {
		t.Fatalf("fired %v, want the timeout case both times", fired)
	}
}

// THE POINT: a select recv wins when a value is waiting, and hands it over as
// Channel.Recv would.
func TestSelectRecvTakesAValue(t *testing.T) {
	var got int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int](flow.WithCapacity(1))
		producer := ctx.Spawn(func(ctx flow.Context) (flow.None, error) {
			return flow.None{}, w.Send(ctx, 99)
		})
		if _, err := producer.Await(ctx); err != nil {
			return err
		}
		return ctx.Select().
			Recv(r, func(v int, ok bool, err error) error { got = v; return err }).
			After(time.Second, func() error { return errors.New("timed out waiting for the value") }).
			Do(ctx)
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 99 {
		t.Fatalf("got %d, want the sent value 99", got)
	}
}

// THE POINT: a select send wins when the buffer has room, puts the value, and a
// later receive takes it — the write side is selectable like the read side.
func TestSelectSendWhenThereIsRoom(t *testing.T) {
	var got int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int](flow.WithCapacity(1))
		if err := ctx.Select().
			Send(w, 99, func(err error) error { return err }).
			After(time.Second, func() error { return errors.New("had room but the send never won") }).
			Do(ctx); err != nil {
			return err
		}
		v, _, err := r.Recv(ctx)
		got = v
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 99 {
		t.Fatalf("got %d, want the sent value 99", got)
	}
}

// THE POINT: with the buffer full the send case is not ready, so back-pressure
// hands the win to another case instead of blocking the whole select.
func TestSelectSendYieldsWhenFull(t *testing.T) {
	var fired string
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		_, w := ctx.NewChannel[int](flow.WithCapacity(1))
		if err := w.Send(ctx, 1); err != nil { // fills the one place
			return err
		}
		return ctx.Select().
			Send(w, 2, func(error) error { fired = "send"; return nil }).
			After(20*time.Millisecond, func() error { fired = "timeout"; return nil }).
			Do(ctx)
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if fired != "timeout" {
		t.Fatalf("fired %q, want the timeout: a full channel's send case must not win", fired)
	}
}

func TestSelectOutsideARunIsAnError(t *testing.T) {
	var ctx flow.Context
	if err := ctx.Select().After(time.Second, func() error { return nil }).Do(ctx); err == nil {
		t.Fatal("Select outside a run must fail")
	}
}

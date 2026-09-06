package flow_test

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/ligustah/wings/flow"
)

var napThenDouble = flow.Define(func(ctx flow.Context, in int) (int, error) {
	if err := ctx.Sleep(2 * time.Minute); err != nil {
		return 0, err
	}
	return in * 2, nil
}, flow.WithName("flow.napThenDouble"))

// THE POINT: a thread run on somebody else's behalf hands a long sleep back
// rather than waiting it out in place. The caller learns when to run the
// thread again; run again after that, the thread replays a sleep that is
// over and carries on.
func TestOnceReturnsALongSleepToTheCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := flow.NewMemStore()
		start := time.Now()

		_, err := flow.RunThread(context.Background(), "nap", "main.0", "flow.napThenDouble", encodeInt(t, 21),
			flow.WithStore(store), flow.Once())
		ok, until := flow.IsSuspended(err)
		if !ok {
			t.Fatalf("got %v, want a suspension", err)
		}
		if want := start.Add(2 * time.Minute); !until.Equal(want) {
			t.Fatalf("suspended until %s, want %s", until, want)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("the caller waited %s; a suspension is returned, not served", elapsed)
		}

		time.Sleep(2 * time.Minute)
		out, err := flow.RunThread(context.Background(), "nap", "main.0", "flow.napThenDouble", encodeInt(t, 21),
			flow.WithStore(store), flow.Once())
		if err != nil {
			t.Fatalf("second RunThread: %v", err)
		}
		if got := decodeInt(t, out); got != 42 {
			t.Fatalf("got %d, want 42", got)
		}
	})
}

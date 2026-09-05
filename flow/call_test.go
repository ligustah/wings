package flow_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow"
)

var (
	forkingRuns atomic.Int32

	// forking is a function whose body does what a run's body may do: it forks
	// and joins. Under Execute that is an error; under RunCall it is a run.
	forking = flow.Define("call.forking", func(ctx flow.Context, in int) (int, error) {
		forkingRuns.Add(1)
		a := ctx.Go(double, in)
		b := ctx.Go(double, in+1)
		x, err := a.Await(ctx)
		if err != nil {
			return 0, err
		}
		y, err := b.Await(ctx)
		if err != nil {
			return 0, err
		}
		return x + y, nil
	})

	failsOnce = flow.Define("call.failsOnce", func(ctx flow.Context, _ int) (int, error) {
		return 0, errors.New("not this time")
	})
)

func encodeInt(t *testing.T, v int) []byte {
	t.Helper()
	b, err := dswire.EncodeRecord(dswire.ReflectCodec[int]{}, v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func decodeInt(t *testing.T, b []byte) int {
	t.Helper()
	v, err := dswire.DecodeRecord(dswire.ReflectCodec[int]{}, b)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// THE POINT: a function run through RunCall is run code. It may fork, and
// what it produced is kept with the run, so entering the run again hands back
// the answer without running anything.
func TestRunCallMakesAFunctionARun(t *testing.T) {
	store := flow.NewMemStore()
	forkingRuns.Store(0)

	out, err := flow.RunCall(context.Background(), "call-1", "call.forking", encodeInt(t, 3), flow.WithStore(store))
	if err != nil {
		t.Fatalf("RunCall: %v", err)
	}
	if got := decodeInt(t, out); got != 3*2+4*2 {
		t.Fatalf("got %d, want 14", got)
	}

	again, err := flow.RunCall(context.Background(), "call-1", "call.forking", encodeInt(t, 3), flow.WithStore(store))
	if err != nil {
		t.Fatalf("second RunCall: %v", err)
	}
	if string(again) != string(out) {
		t.Fatalf("the finished run returned %q, want the recorded %q", again, out)
	}
	if n := forkingRuns.Load(); n != 1 {
		t.Fatalf("the function ran %d times; a finished run must return its recorded output", n)
	}

	// The same function under plain Execute has no run to fork in.
	if _, err := flow.Execute(context.Background(), "call.forking", encodeInt(t, 3)); err == nil ||
		!strings.Contains(err.Error(), "outside a Run") {
		t.Fatalf("Execute of a forking function: got %v, want an outside-a-Run error", err)
	}
}

// Once hands the body's error back as it was: the caller is the one deciding
// about retries, and wants the function's own words.
func TestOnceReturnsTheBodysErrorUnwrapped(t *testing.T) {
	_, err := flow.RunCall(context.Background(), "call-2", "call.failsOnce", encodeInt(t, 0),
		flow.WithStore(flow.NewMemStore()), flow.Once())
	if err == nil || err.Error() != "not this time" {
		t.Fatalf("got %v, want the body's own error", err)
	}
}

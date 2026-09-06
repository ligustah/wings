package flow_test

import (
	"errors"
	"runtime"
	"strings"
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: MaxParallelism reports the capacity an executor injected, so a run
// body can size a fan-out to the fleet without knowing its shape.
func TestMaxParallelismReportsInjectedCapacity(t *testing.T) {
	ctx := flow.WithMaxParallelism(t.Context(), 12)
	var got int
	err := flow.Run(ctx, flow.NewName(), func(ctx flow.Context) error {
		n, err := ctx.MaxParallelism()
		got = n
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 12 {
		t.Fatalf("MaxParallelism = %d, want the injected 12", got)
	}
}

// THE POINT: no injection falls back to the process's own parallelism, so a
// standalone run still gets a sensible number rather than zero.
func TestMaxParallelismDefaultsToProcessParallelism(t *testing.T) {
	var got int
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		n, err := ctx.MaxParallelism()
		got = n
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := runtime.GOMAXPROCS(0); got != want {
		t.Fatalf("MaxParallelism = %d, want GOMAXPROCS %d", got, want)
	}
}

// THE POINT: the value is fixed by the first read. A retry under a fleet that
// has since grown or shrunk still sees what the history recorded — anything
// else would contradict decisions the first attempt already made from it.
func TestMaxParallelismIsRecordedNotRereadOnRetry(t *testing.T) {
	store := flow.NewMemStore()
	name := flow.NewName()

	err := flow.Run(flow.WithMaxParallelism(t.Context(), 8), name, func(ctx flow.Context) error {
		if _, err := ctx.MaxParallelism(); err != nil {
			return err
		}
		return errors.New("fail once")
	}, flow.WithStore(store), flow.Once())
	if err == nil {
		t.Fatal("the first attempt was meant to fail")
	}

	var got int
	err = flow.Run(flow.WithMaxParallelism(t.Context(), 99), name, func(ctx flow.Context) error {
		n, err := ctx.MaxParallelism()
		got = n
		return err
	}, flow.WithStore(store), flow.Once())
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if got != 8 {
		t.Fatalf("MaxParallelism = %d on the retry, want the recorded 8 despite 99 injected now", got)
	}
}

func TestMaxParallelismOutsideARunIsAnError(t *testing.T) {
	var ctx flow.Context
	if _, err := ctx.MaxParallelism(); err == nil || !strings.Contains(err.Error(), "outside a Run") {
		t.Fatalf("got %v, want an error saying there is no run", err)
	}
}

package flow_test

import (
	"strings"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// THE POINT: the zero Context must not panic. A struct that wraps an interface
// has a zero value whether we like it or not, and code that reaches one has a
// bug — which is worth an error naming what is missing, not a nil dereference
// pointing at the wrong line. Nothing is swallowed: every operation still
// fails, and says why.
func TestTheZeroContextIsSafeAndSaysWhatIsMissing(t *testing.T) {
	var ctx flow.Context

	// The context.Context half behaves as Background.
	if _, ok := ctx.Deadline(); ok {
		t.Error("zero Context has a deadline")
	}
	if ctx.Done() != nil {
		t.Error("zero Context has a Done channel")
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("zero Context is already done: %v", err)
	}
	if v := ctx.Value("anything"); v != nil {
		t.Errorf("zero Context carries a value: %v", v)
	}
	derived, cancel := ctx.WithTimeout(time.Hour)
	cancel()
	if derived.Err() == nil {
		t.Error("a context derived from the zero Context did not cancel")
	}
	if flow.From(ctx).Attempt() != 0 {
		t.Error("zero Context reports an attempt")
	}

	// The flow half reports the absence rather than pretending.
	wantsRun := func(what string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s on the zero Context succeeded; it must fail, there is no run", what)
			return
		}
		if !strings.Contains(err.Error(), "outside a Run") && !strings.Contains(err.Error(), "outside a running function") && !strings.Contains(err.Error(), "not inside a Run") {
			t.Errorf("%s on the zero Context: %v; the error must say there is no run here", what, err)
		}
	}

	_, err := ctx.Now()
	wantsRun("Now", err)
	wantsRun("Sleep", ctx.Sleep(time.Millisecond))
	_, err = ctx.Go(double, 1).Await(ctx)
	wantsRun("Go", err)
	_, err = ctx.Spawn(func(ctx flow.Context) (int, error) { return 1, nil }).Await(ctx)
	wantsRun("Spawn", err)
	_, err = ctx.Map(double, []int{1, 2})
	wantsRun("Map", err)
	_, err = double(ctx, 1)
	wantsRun("a defined function", err)
	wantsRun("Heartbeat", ctx.Heartbeat(1))
	if _, ok, err := ctx.Checkpoint[int](); ok || err != nil {
		t.Errorf("Checkpoint on the zero Context: ok=%v err=%v; want no checkpoint and no error", ok, err)
	}
	_, w := ctx.NewChannel[int]()
	if err := w.Send(ctx, 1); err == nil {
		t.Error("Send on a channel made from the zero Context succeeded")
	}
}

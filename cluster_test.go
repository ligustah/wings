package wings

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ligustah/durable_streams/dswire"
)

// TestMain lets the test binary be its own worker.
//
// This is what makes the LocalProcess target testable at all: wings starts a
// worker by re-executing os.Executable(), which under `go test` is this binary,
// so it has to recognise the worker environment before it starts running tests.
func TestMain(m *testing.M) {
	if isWorkerProcess() {
		// Never returns.
		_, _ = Start(context.Background(), Config{})
	}
	os.Exit(m.Run())
}

type point struct {
	X, Y int
}

var double = Define("test.double", func(ctx context.Context, in int) (int, error) {
	return in * 2, nil
})

var sum = Define("test.sum", func(ctx context.Context, in point) (int, error) {
	return in.X + in.Y, nil
})

var boom = Define("test.boom", func(ctx context.Context, in string) (string, error) {
	return "", errors.New("deliberate failure: " + in)
})

var panics = Define("test.panics", func(ctx context.Context, in int) (int, error) {
	panic("deliberate panic")
})

var slow = Define("test.slow", func(ctx context.Context, d time.Duration) (string, error) {
	select {
	case <-time.After(d):
		return "finished", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
})

func start(t *testing.T, cfg Config) *Cluster {
	t.Helper()
	ctx := t.Context()
	cfg.Dir = t.TempDir()
	c, err := Start(ctx, cfg)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	return c
}

// firstWorker returns a worker under the cluster's lock.
//
// The lock is not ceremony: the watchdog reaps dead workers out of this slice
// once a second, whatever the target and whether or not autoscaling is on, so
// reading it bare is a race the detector will find.
func firstWorker(t *testing.T, c *Cluster) *workerConn {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.workers) == 0 {
		t.Fatal("the cluster has no workers")
	}
	return c.workers[0]
}

func TestInProcessCall(t *testing.T) {
	c := start(t, Config{Target: InProcess()})

	got, err := double(c.Bind(t.Context()), 21)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

// A struct input proves the codec carries more than an int, and that the
// coordinator never has to know what the type was.
func TestStructInput(t *testing.T) {
	c := start(t, Config{Target: InProcess()})

	got, err := sum(c.Bind(t.Context()), point{X: 3, Y: 4})
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got != 7 {
		t.Fatalf("got %d, want 7", got)
	}
}

// THE POINT: Map's results are positional. Work is spread across workers and
// completes out of order, so anything that returned results in completion order
// would pass a single-worker test and silently scramble a real one.
func TestMapPreservesOrder(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 3, Concurrency: 4})

	in := make([]int, 100)
	want := make([]int, 100)
	for i := range in {
		in[i] = i
		want[i] = i * 2
	}

	got, err := Map(c.Bind(t.Context()), double, in)
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("Map returned results out of order:\n got %v\nwant %v", got[:10], want[:10])
	}
}

// A work function's error is a normal result, not a transport failure. If it
// were the latter the worker would abort and redeliver the batch, and a
// deterministic failure would loop forever.
func TestWorkFunctionErrorPropagates(t *testing.T) {
	c := start(t, Config{Target: InProcess()})

	_, err := boom(c.Bind(t.Context()), "input")
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	if !strings.Contains(err.Error(), "deliberate failure: input") {
		t.Fatalf("error lost its message: %v", err)
	}
}

// THE POINT: one panicking job costs one job. A worker that died on a panic
// would take every other job it held with it.
func TestPanicDoesNotKillTheWorker(t *testing.T) {
	c := start(t, Config{Target: InProcess()})

	if _, err := panics(c.Bind(t.Context()), 1); err == nil {
		t.Fatal("want an error from the panicking function, got nil")
	} else if !strings.Contains(err.Error(), "panicked") {
		t.Fatalf("error should say it panicked: %v", err)
	}

	// The worker must still be serving.
	got, err := double(c.Bind(t.Context()), 5)
	if err != nil {
		t.Fatalf("worker died with the panic: %v", err)
	}
	if got != 10 {
		t.Fatalf("got %d, want 10", got)
	}
}

func TestJobTimeout(t *testing.T) {
	c := start(t, Config{Target: InProcess(), JobTimeout: 100 * time.Millisecond})

	_, err := slow(c.Bind(t.Context()), 10*time.Second)
	if err == nil {
		t.Fatal("want a timeout error, got nil")
	}
	// Named, not "context deadline exceeded": the worker knows which function
	// and which bound, and it is the only place that does.
	if !strings.Contains(err.Error(), "test.slow exceeded its 100ms timeout") {
		t.Fatalf("want the timeout named, got: %v", err)
	}
}

// THE POINT: the whole promise of this package is that the target is the only
// thing that changes. Running the identical assertions across process boundaries
// is what proves it.
func TestLocalProcessTargetMatchesInProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := start(t, Config{Target: LocalProcess(), Workers: 2, Concurrency: 2})

	in := make([]int, 20)
	want := make([]int, 20)
	for i := range in {
		in[i] = i
		want[i] = i * 2
	}

	got, err := Map(c.Bind(t.Context()), double, in)
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	if _, err := boom(c.Bind(t.Context()), "remote"); err == nil ||
		!strings.Contains(err.Error(), "deliberate failure: remote") {
		t.Fatalf("error did not survive the process boundary: %v", err)
	}
}

// An unregistered name must produce a legible error rather than a hang. It is
// the signature of a worker built from different source than the coordinator.
func TestUnknownFunctionIsReported(t *testing.T) {
	c := start(t, Config{Target: InProcess()})

	// Built directly rather than through Define, because Define would register
	// it — and then it would not be missing.
	ghost := &def[int, int]{
		name:     "test.not-registered",
		inCodec:  dswire.ReflectCodec[int]{},
		outCodec: dswire.ReflectCodec[int]{},
	}
	_, err := ghost.dispatch(c.Bind(t.Context()), 1)
	if err == nil {
		t.Fatal("want an error for an unregistered function")
	}
	if !strings.Contains(err.Error(), "test.not-registered") {
		t.Fatalf("error should name the missing function: %v", err)
	}
	// It should also say what the worker does have, so the mismatch is visible.
	if !strings.Contains(err.Error(), "test.double") {
		t.Fatalf("error should list what is registered: %v", err)
	}
}

func TestMapEmptyInput(t *testing.T) {
	c := start(t, Config{Target: InProcess()})

	got, err := Map(c.Bind(t.Context()), double, nil)
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %v, want empty", got)
	}
}

func TestDuplicateDefinePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("want a panic on duplicate Define")
		}
	}()
	_ = Define("test.double", func(ctx context.Context, in int) (int, error) { return in, nil })
}

func TestStopIsIdempotent(t *testing.T) {
	c, err := Start(t.Context(), Config{Target: InProcess(), Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
}

// Concurrency inside a worker should actually overlap. Ten 200ms jobs on one
// worker with concurrency 10 must not take two seconds.
func TestWorkerConcurrencyOverlaps(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 10})

	in := make([]time.Duration, 10)
	for i := range in {
		in[i] = 200 * time.Millisecond
	}

	started := time.Now()
	got, err := Map(c.Bind(t.Context()), slow, in)
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	for i, g := range got {
		if g != "finished" {
			t.Fatalf("result %d: got %q", i, g)
		}
	}
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("10 concurrent 200ms jobs took %s; they ran serially", elapsed)
	}
}

func ExampleDefine() {
	// Defined at package scope in real code, so a worker process has it too.
	greet := Define("example.greet", func(ctx context.Context, name string) (string, error) {
		return "hello, " + name, nil
	})

	c, err := Start(context.Background(), Config{Target: InProcess()})
	if err != nil {
		panic(err)
	}
	defer c.Stop(context.Background())

	out, err := Map(c.Bind(context.Background()), greet, []string{"ada", "alan"})
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output: [hello, ada hello, alan]
}

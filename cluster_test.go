package wings

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow"
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

var double = flow.Define(func(ctx flow.Context, in int) (int, error) {
	return in * 2, nil
}, flow.WithName("test.double"))

var sum = flow.Define(func(ctx flow.Context, in point) (int, error) {
	return in.X + in.Y, nil
}, flow.WithName("test.sum"))

var boom = flow.Define(func(ctx flow.Context, in string) (string, error) {
	return "", errors.New("deliberate failure: " + in)
}, flow.WithName("test.boom"))

var panics = flow.Define(func(ctx flow.Context, in int) (int, error) {
	panic("deliberate panic")
}, flow.WithName("test.panics"))

var slow = flow.Define(func(ctx flow.Context, d time.Duration) (string, error) {
	select {
	case <-time.After(d):
		return "finished", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}, flow.WithName("test.slow"))

// runSeq names each test run distinctly: a run's name is its history, and a
// second run under a finished one's name would be that run, already done.
var runSeq atomic.Uint64

// mapOn fans f out over ins as a run on c, the way a workflow would, and
// returns what Map returned. Map is only callable inside a run — it forks a
// thread per input — so this is how a test gets a fan-out out of a cluster.
func mapOn[In, Out any](ctx context.Context, c *Cluster, f flow.Func[In, Out], ins []In) ([]Out, error) {
	var (
		outs   []Out
		mapErr error
	)
	name := "test-map-" + strconv.FormatUint(runSeq.Add(1), 36)
	err := c.Run(ctx, name, func(ctx flow.Context) error {
		outs, mapErr = ctx.Map(f, ins)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return outs, mapErr
}

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

	got, err := mapOn(t.Context(), c, double, in)
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

	got, err := mapOn(t.Context(), c, double, in)
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

	// Sent by name rather than through a Func, because Define would register
	// it — and then it would not be missing.
	payload, err := dswire.EncodeRecord(dswire.ReflectCodec[int]{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = clusterExecutor{c}.Invoke(c.Bind(t.Context()), "test.not-registered", payload)
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

// THE POINT: a Map dispatches every input at once, and each used to be an
// append of its own to the worker's queue — a round trip per job, through the
// engine or over the network. Submissions that arrive while one append is in
// flight now go together in the next.
func TestJobsThatArriveTogetherGoInOneAppend(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 4})

	in := make([]int, 400)
	want := make([]int, len(in))
	for i := range in {
		in[i], want[i] = i, i*2
	}
	got, err := mapOn(t.Context(), c, double, in)
	if err != nil {
		t.Fatalf("Map: %v", err)
	}
	if !slices.Equal(got, want) {
		t.Fatal("batched submissions came back wrong")
	}

	w := c.fleet()[0]
	if n := w.appends.Load(); n >= int64(len(in)) {
		t.Fatalf("%d jobs took %d appends; nothing arriving together was sent together", len(in), n)
	} else {
		t.Logf("%d jobs went in %d appends", len(in), n)
	}
}

// THE POINT: a worker's streams are named after it, and a name is never
// reused, so a worker that has gone used to leave its mirror — and in process
// its queue, results and beats; as a child process its whole broker directory
// — on a persistent Dir forever, with every autoscale cycle minting more.
func TestAWorkerThatIsGoneLeavesNothingBehind(t *testing.T) {
	for _, target := range []Target{InProcess(), LocalProcess()} {
		t.Run(fmt.Sprint(target.kind), func(t *testing.T) {
			if target.kind == targetLocalProcess && testing.Short() {
				t.Skip("spawns child processes")
			}
			dir := t.TempDir()
			c, err := Start(t.Context(), Config{Target: target, Workers: 2, Dir: dir})
			if err != nil {
				t.Fatalf("Start: %v", err)
			}
			if _, err := mapOn(t.Context(), c, double, []int{1, 2, 3, 4}); err != nil {
				t.Fatalf("Map: %v", err)
			}
			var ids []string
			for _, w := range c.fleet() {
				ids = append(ids, w.id)
			}
			if err := c.Stop(context.Background()); err != nil {
				t.Fatalf("Stop: %v", err)
			}

			// Look with a fresh coordinator over the same Dir: what it can see
			// is what was left.
			again, err := Start(t.Context(), Config{Target: InProcess(), Dir: dir})
			if err != nil {
				t.Fatalf("Start again: %v", err)
			}
			defer again.Stop(context.Background())
			names, err := again.shared.ListStreams(t.Context())
			if err != nil {
				t.Fatalf("ListStreams: %v", err)
			}
			for _, name := range names {
				for _, id := range ids {
					if strings.Contains(name, id) {
						t.Errorf("stream %s belongs to worker %s, which is gone", name, id)
					}
				}
			}
			for _, id := range ids {
				if _, err := os.Stat(filepath.Join(dir, id)); err == nil {
					t.Errorf("worker %s left its directory behind", id)
				}
			}
		})
	}
}

// THE POINT: a caller that gave up stopped waiting and the coordinator
// credited the worker back — but the worker kept running the job, so the next
// job sent to that worker waited behind work nobody wanted, and a scaler that
// saw an idle worker could retire it mid-job. Giving up now stops the job
// where it runs.
func TestGivingUpOnAJobStopsItOnTheWorker(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1, Concurrency: 1})

	ctx, giveUp := c.Bind(t.Context()).WithCancel()
	defer giveUp()
	abandoned := make(chan error, 1)
	go func() {
		_, err := slow(ctx, 30*time.Second)
		abandoned <- err
	}()
	waitFor(t, "the job to start on the worker", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, p := range c.pending {
			return !p.started.IsZero()
		}
		return false
	})
	giveUp()
	if err := <-abandoned; err == nil {
		t.Fatal("the abandoned call returned no error")
	}

	// The one slot on the one worker is free again, or is about to be.
	began := time.Now()
	got, err := double(c.Bind(t.Context()), 21)
	if err != nil {
		t.Fatalf("the job after it: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	if took := time.Since(began); took > 10*time.Second {
		t.Fatalf("the next job waited %s behind a job nobody wanted", took)
	}
}

func TestMapEmptyInput(t *testing.T) {
	c := start(t, Config{Target: InProcess()})

	got, err := mapOn(t.Context(), c, double, nil)
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
	_ = flow.Define(func(ctx flow.Context, in int) (int, error) { return in, nil }, flow.WithName("test.double"))
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
	got, err := mapOn(t.Context(), c, slow, in)
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

var huge = flow.Define(func(ctx flow.Context, n int) (string, error) {
	return strings.Repeat("x", n), nil
}, flow.WithName("test.huge"))

// THE POINT: a result too large to carry is one job's mistake. It used to look
// like a dropped connection to the coordinator, which declared the worker dead,
// redispatched everything it held, and on a cloud target destroyed the machine
// — then the retry produced the same result somewhere else. The worker knows
// the size before anything is sent, and the job is what fails.
func TestAResultTooLargeFailsTheJobNotTheWorker(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	_, err := huge(ctx, maxResult+1)
	if err == nil {
		t.Fatal("want an error from a result too large to carry")
	}
	if !strings.Contains(err.Error(), "Channel") {
		t.Fatalf("the error should say what to do instead: %v", err)
	}

	// The worker is untouched: still serving, never suspected.
	if got, err := double(ctx, 21); err != nil || got != 42 {
		t.Fatalf("the worker did not survive an oversized result: %v, %d", err, got)
	}
	if firstWorker(t, c).dead.Load() {
		t.Fatal("the worker was declared dead over one job's result")
	}
	entries := awaitJournal(t, c, func(es []journalEntry) bool {
		return countKind(es, journalCompleted) >= 2
	})
	if n := countKind(entries, journalWorkerGone); n != 0 {
		t.Errorf("the record says a worker was lost %d times; none was", n)
	}
	if n := countKind(entries, journalRedispatch); n != 0 {
		t.Errorf("the job was moved %d times; a result too large is the same everywhere", n)
	}
}

// A caller that gives up on a job while moveJob has taken its worker away and
// not yet failed it finds a pending job with no worker. Releasing nothing is
// nothing, not a crash.
func TestGivingUpOnAJobWithNoWorkerIsHarmless(t *testing.T) {
	c := &Cluster{pending: map[string]*pendingJob{}}
	p := &pendingJob{job: jobEnvelope{ID: "j"}, done: make(chan struct{}), waiters: 1}
	c.pending["j"] = p
	c.ctx, c.cancel = context.WithCancel(context.Background())
	defer c.cancel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.await(ctx, p); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want the caller's cancellation", err)
	}
	if _, still := c.pending["j"]; still {
		t.Fatal("the abandoned job is still pending")
	}
}

func ExampleDefine() {
	// Defined at package scope in real code, so a worker process has it too.
	greet := flow.Define(func(ctx flow.Context, name string) (string, error) {
		return "hello, " + name, nil
	}, flow.WithName("example.greet"))

	c, err := Start(context.Background(), Config{Target: InProcess()})
	if err != nil {
		panic(err)
	}
	defer c.Stop(context.Background())

	out, err := mapOn(context.Background(), c, greet, []string{"ada", "alan"})
	if err != nil {
		panic(err)
	}
	fmt.Println(out)
	// Output: [hello, ada hello, alan]
}

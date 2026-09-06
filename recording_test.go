package wings

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire/protos"

	"github.com/ligustah/wings/flow"
)

// Tick is a made-up simulation event, standing in for whatever a long job
// actually produces.
type Tick struct {
	At   int    `json:"at"`
	What string `json:"what"`
}

// simulate produces a long event log and returns only a handle to it. The point
// of the whole feature: the log never travels as a value.
var simulate = flow.Define(func(ctx flow.Context, steps int) (Recording, error) {
	rec, err := Record[Tick](ctx, "replay")
	if err != nil {
		return Recording{}, err
	}
	for i := range steps {
		if err := rec.Record(Tick{At: i, What: fmt.Sprintf("step %d", i)}); err != nil {
			return Recording{}, err
		}
	}
	if err := rec.Close(); err != nil {
		return Recording{}, err
	}
	return rec.Recording(), nil
}, flow.WithName("test.simulate"))

// THE POINT: a job whose output is measured in megabytes cannot return it as a
// value — one record on one stream, held whole in memory at both ends, and past
// a few megabytes not carried by the transport at all. It streams out event by
// event as it is produced, lands on the coordinator, and comes back as events.
func TestALongJobsEventsComeHomeWithoutTravellingAsAValue(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	// A process boundary on purpose: in process the two halves share an engine,
	// and it is the crossing that this is about.
	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 1})
	ctx := c.Bind(t.Context())

	const steps = 20_000
	rec, err := simulate(ctx, steps)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if rec.Zero() {
		t.Fatal("no recording came back")
	}
	if rec.Events != steps {
		t.Fatalf("the handle says %d events, want %d", rec.Events, steps)
	}
	if !rec.Complete {
		t.Fatal("the handle says the log was never closed")
	}

	seen := 0
	for ev, err := range Replay[Tick](ctx, rec) {
		if err != nil {
			t.Fatalf("replay at event %d: %v", seen, err)
		}
		if ev.At != seen {
			t.Fatalf("event %d says it is %d; the order must be the order they were recorded", seen, ev.At)
		}
		if ev.What != fmt.Sprintf("step %d", seen) {
			t.Fatalf("event %d is %q", seen, ev.What)
		}
		seen++
	}
	if seen != steps {
		t.Fatalf("replayed %d events, want %d", seen, steps)
	}
}

// Replay hands events out one at a time, so a reader that has seen enough stops
// reading. A resume that wants the first ten minutes of a two-hour log should
// not pay for the other hundred and ten.
func TestReplayStopsWhenTheReaderDoes(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	rec, err := simulate(ctx, 20_000)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}

	seen := 0
	for ev, err := range Replay[Tick](ctx, rec) {
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		if ev.At != seen {
			t.Fatalf("event %d says it is %d", seen, ev.At)
		}
		seen++
		if seen == 5 {
			break
		}
	}
	if seen != 5 {
		t.Fatalf("read %d events after breaking at 5", seen)
	}

	// And the log is still whole afterwards: stopping is not consuming.
	again := 0
	for range Replay[Tick](ctx, rec) {
		again++
	}
	if again != 20_000 {
		t.Fatalf("a second pass read %d events, want 20000", again)
	}
}

// A log lives on the coordinator's disk until somebody says it can go, and a run
// that never says so fills one.
func TestDiscardRemovesARecording(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	rec, err := simulate(ctx, 16)
	if err != nil {
		t.Fatalf("simulate: %v", err)
	}
	if err := rec.Discard(ctx); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	if ok, err := c.shared.StreamExists(t.Context(), rec.ID); err != nil {
		t.Fatalf("StreamExists: %v", err)
	} else if ok {
		t.Fatal("the recording's storage is still there after it was discarded")
	}

	// Discarding twice is not an error: a caller that cleans up in a defer and
	// again on the happy path should not have to care which ran.
	if err := rec.Discard(ctx); err != nil {
		t.Fatalf("second Discard: %v", err)
	}
}

// Outside a work function there is no job for a recording to belong to.
func TestRecordingOutsideAJobIsAnError(t *testing.T) {
	if _, err := Record[Tick](context.Background(), "x"); err == nil {
		t.Fatal("want an error from Record with no job")
	}
}

// resume lets a test hold the first attempt of a recording job still while the
// second one runs, and reports what each attempt saw.
//
// The counters are only readable when the worker is in this process. What the
// function RETURNS is what a cross-process test can check, so the number that
// matters — how many of its predecessor's events the retry replayed — is the
// result rather than a variable.
var resume struct {
	release chan struct{}
	once    sync.Once

	mu       sync.Mutex
	replayed []int // events the retry read back, per attempt
	priors   []int // how many priors each attempt was handed
}

// resumable records events, goes quiet part-way through on its first attempt,
// and on any later one replays what its predecessor left before carrying on.
//
// That is the whole feature in one function: durable execution for the job, and
// a log the job keeps for itself alongside it.
var resumeSim = flow.Define(func(ctx flow.Context, total int) (int, error) {

	attempt := ctx.Attempt()

	from := 0
	priors := Priors(ctx)

	resume.mu.Lock()
	resume.priors = append(resume.priors, len(priors))
	resume.mu.Unlock()

	if len(priors) > 0 {
		read := 0
		for ev, err := range Replay[Tick](ctx, priors[len(priors)-1]) {
			if err != nil {

				break
			}
			if ev.At != read {
				return 0, fmt.Errorf("prior event %d says it is %d", read, ev.At)
			}
			read++
		}
		from = read
		resume.mu.Lock()
		resume.replayed = append(resume.replayed, read)
		resume.mu.Unlock()
	}

	rec, err := Record[Tick](ctx, "replay")
	if err != nil {
		return 0, err
	}
	for i := from; i < total; i++ {
		if attempt == 0 && i == from+4 {
			if err := rec.Flush(); err != nil {
				return 0, err
			}

			select {
			case <-resume.release:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			return 0, errors.New("test.resume: first attempt was abandoned")
		}
		if err := rec.Record(Tick{At: i, What: fmt.Sprintf("step %d", i)}); err != nil {
			return 0, err
		}
		if err := ctx.Heartbeat(i + 1); err != nil {
			return 0, err
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := rec.Close(); err != nil {
		return 0, err
	}
	return from, nil
}, flow.WithName("test.resume"),

	flow.WithHeartbeatTimeout(300*time.Millisecond))

// THE POINT: a job that is moved must be able to replay what the attempt before
// it recorded. Without that the whole feature is half a feature — a simulation
// streams a hundred megabytes of events to the coordinator, dies at minute
// fifty, and the retry starts from zero anyway, so nothing the events cost
// bought anything.
//
// Four things have to line up for this to work at all, and each was missing:
// the retry's context must reach storage; it must be handed a handle to what its
// predecessor wrote; that log must be readable although nobody closed it; and it
// must still exist when the retry looks.
func TestARetryReplaysWhatItsPredecessorRecorded(t *testing.T) {
	resetResume(t)

	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 2})

	replayed, err := resumeSim(c.Bind(t.Context()), 12)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	// The first attempt recorded four events before going quiet, and flushed
	// them. Whatever the retry read back has to be those.
	if replayed != 4 {
		t.Fatalf("the retry replayed %d events; its predecessor recorded 4 before it stalled", replayed)
	}

	resume.mu.Lock()
	priors := resume.priors
	resume.mu.Unlock()

	if len(priors) < 2 {
		t.Fatalf("the job ran %d times; this test needs it to be moved", len(priors))
	}
	if priors[0] != 0 {
		t.Fatalf("the first attempt was handed %d priors; it has no predecessor", priors[0])
	}
	if priors[1] == 0 {
		t.Fatal("the retry was handed no priors; it cannot resume what it cannot see")
	}
}

// The same thing across a process boundary, which is the case the feature is
// actually for.
//
// In process a worker and the coordinator share one engine, so the log the first
// attempt wrote is ALREADY where the retry will look and nothing has to move. A
// worker in another process has storage of its own, so the coordinator's copy of
// what attempt 0 recorded has to be put onto the machine attempt 1 will run on
// before the job is sent there. That path exists only here.
func TestARetryOnAnotherMachineIsGivenItsPredecessorsRecording(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	resetResume(t)

	c := start(t, Config{Target: LocalProcess(), Workers: 2, Concurrency: 1})

	replayed, err := resumeSim(c.Bind(t.Context()), 12)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if replayed != 4 {
		t.Fatalf("the retry replayed %d events on a machine that never saw them written; "+
			"its predecessor recorded 4 before it stalled", replayed)
	}

	// And once the job settles, what the abandoned attempt recorded goes — from
	// the worker that wrote it as well as from the coordinator that kept it.
	// Nobody holds a handle to either, so keeping them would fill two disks one
	// retry at a time.
	waitFor(t, "the abandoned attempt's recording to be discarded everywhere",
		func() bool { return len(abandoned(t, c)) == 0 })

	// And STAYS gone. The worker that wrote it is still up, so a copy the mirror
	// restarted would put the coordinator's back — and put it back holding only
	// whatever arrived after the delete, because the position lives at the
	// destination and deleting a stream does not roll it back.
	for range 8 {
		time.Sleep(100 * time.Millisecond)
		if got := abandoned(t, c); len(got) != 0 {
			t.Fatalf("an abandoned attempt's recording came back after being discarded: %v", got)
		}
	}
}

// abandoned is every copy of what a job's first attempt recorded, anywhere in
// the cluster — the coordinator's, the writer's, and the prior put on the
// worker that ran the retry alike.
func abandoned(t *testing.T, c *Cluster) []string {
	t.Helper()

	clients := map[string]*dsclient.Client{"coordinator": c.shared}
	for _, w := range c.fleet() {
		clients[w.id] = w.client
	}

	var out []string
	for who, client := range clients {
		names, err := client.ListStreams(t.Context())
		if err != nil {
			continue // a worker on its way out is not a leak
		}
		for _, n := range names {
			if o, ok := parseOutput(n); ok && o.Attempt == 0 && (o.Prefix == recordingPrefix || o.Prefix == priorPrefix) {
				out = append(out, who+":"+n)
			}
		}
	}
	return out
}

// A retry can land back on the worker it came off — pick prefers anywhere else,
// but on a small or busy cluster there is nowhere else — and when it does, the
// two attempts must not want the same storage.
//
// This used to fail before the work began: the second attempt asked for the name
// its predecessor already held, was refused, and the error the caller saw was
// about a recording rather than about whatever had actually gone wrong.
func TestTwoAttemptsOfOneJobRecordToDifferentPlaces(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})

	n, err := newWorkerNode(t.Context(), c.shared, "attempts", 1, 0, nil)
	if err != nil {
		t.Fatalf("newWorkerNode: %v", err)
	}

	first := outputName{Prefix: recordingPrefix, Job: "job-1", Attempt: 0, Name: "replay"}.String()
	second := outputName{Prefix: recordingPrefix, Job: "job-1", Attempt: 1, Name: "replay"}.String()
	if first == second {
		t.Fatalf("both attempts want %s; a retry would write into its predecessor's log", first)
	}

	if _, err := n.declareOutput(t.Context(), first, "replay"); err != nil {
		t.Fatalf("first attempt: %v", err)
	}
	if _, err := n.declareOutput(t.Context(), second, "replay"); err != nil {
		t.Fatalf("a retry on the same worker was refused its own storage: %v", err)
	}

	// Within one attempt the name is still exclusive: opening it twice is a
	// mistake, not two logs quietly sharing one handle.
	if _, err := n.declareOutput(t.Context(), first, "replay"); err == nil {
		t.Fatal("want an error from opening the same recording twice in one attempt")
	}
}

func resetResume(t *testing.T) {
	t.Helper()
	resume.release = make(chan struct{})
	resume.once = sync.Once{}
	resume.mu.Lock()
	resume.replayed, resume.priors = nil, nil
	resume.mu.Unlock()
	t.Cleanup(func() { resume.once.Do(func() { close(resume.release) }) })
}

// Beat is a pointer event, which is what a job whose events are protobuf
// actually has: generated message types are pointers, and so is anything with
// an UnmarshalBinary of its own.
type Beat struct {
	At   int    `json:"at"`
	What string `json:"what"`
}

var pulse = flow.Define(func(ctx flow.Context, steps int) (Recording, error) {
	rec, err := Record[*Beat](ctx, "replay")
	if err != nil {
		return Recording{}, err
	}
	for i := range steps {
		if err := rec.Record(&Beat{At: i, What: fmt.Sprintf("beat %d", i)}); err != nil {
			return Recording{}, err
		}
	}
	if err := rec.Close(); err != nil {
		return Recording{}, err
	}
	return rec.Recording(), nil
}, flow.WithName("test.pulse"))

// A recording of pointers has to work, because the events worth recording are
// usually pointers. A codec cannot allocate the thing a pointer points at unless
// it is given something that can, and every other typed boundary in wings hands
// it one.
func TestARecordingOfPointerEvents(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	const steps = 32
	rec, err := pulse(ctx, steps)
	if err != nil {
		t.Fatalf("pulse: %v", err)
	}

	seen := 0
	for ev, err := range Replay[*Beat](ctx, rec) {
		if err != nil {
			t.Fatalf("replay at event %d: %v", seen, err)
		}
		if ev == nil {
			t.Fatalf("event %d came back nil", seen)
		}
		if ev.At != seen || ev.What != fmt.Sprintf("beat %d", seen) {
			t.Fatalf("event %d is %+v", seen, ev)
		}
		seen++
	}
	if seen != steps {
		t.Fatalf("replayed %d events, want %d", seen, steps)
	}
}

// Note is a pointer event with marshalling of its own, which is the other kind
// that needs allocating: UnmarshalBinary on a nil receiver writes nowhere.
type Note struct {
	At   int
	What string
}

func (n *Note) MarshalBinary() ([]byte, error) {
	return fmt.Appendf(nil, "%d:%s", n.At, n.What), nil
}

func (n *Note) UnmarshalBinary(b []byte) error {
	_, err := fmt.Sscanf(string(b), "%d:%s", &n.At, &n.What)
	return err
}

// emit records protobuf events, which is what a simulation's replay actually
// is. A generated message type is a pointer, always.
var emit = flow.Define(func(ctx flow.Context, steps int) (Recording, error) {
	rec, err := Record[*protos.Data](ctx, "replay")
	if err != nil {
		return Recording{}, err
	}
	for i := range steps {
		if err := rec.Record(&protos.Data{Serialized: fmt.Appendf(nil, "tick %d", i)}); err != nil {
			return Recording{}, err
		}
	}
	if err := rec.Close(); err != nil {
		return Recording{}, err
	}
	return rec.Recording(), nil
}, flow.WithName("test.emit"))

// scribble records a pointer event with its own binary marshalling.
var scribble = flow.Define(func(ctx flow.Context, steps int) (Recording, error) {
	rec, err := Record[*Note](ctx, "replay")
	if err != nil {
		return Recording{}, err
	}
	for i := range steps {
		if err := rec.Record(&Note{At: i, What: fmt.Sprintf("note-%d", i)}); err != nil {
			return Recording{}, err
		}
	}
	if err := rec.Close(); err != nil {
		return Recording{}, err
	}
	return rec.Recording(), nil
}, flow.WithName("test.scribble"))

// THE POINT: the events worth recording are pointers. A generated protobuf type
// is one and there is no value form of it; so is anything carrying marshalling
// of its own. A codec handed a pointer type has a nil pointer and no way to
// allocate what it should point at unless it is given one — protobuf says so and
// refuses outright, and a custom unmarshaller is quietly called on a nil
// receiver, which is worse.
//
// Every other typed boundary in wings passes that allocator. A recording is the
// one place it matters most, since a replay of a long simulation is exactly a
// stream of generated messages.
func TestARecordingOfPointerEventsThatMustBeAllocated(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 1})
	ctx := c.Bind(t.Context())

	const steps = 32

	t.Run("protobuf", func(t *testing.T) {
		rec, err := emit(ctx, steps)
		if err != nil {
			t.Fatalf("emit: %v", err)
		}
		seen := 0
		for ev, err := range Replay[*protos.Data](ctx, rec) {
			if err != nil {
				t.Fatalf("replay at event %d: %v", seen, err)
			}
			if ev == nil {
				t.Fatalf("event %d came back nil", seen)
			}
			if got, want := string(ev.GetSerialized()), fmt.Sprintf("tick %d", seen); got != want {
				t.Fatalf("event %d is %q, want %q", seen, got, want)
			}
			seen++
		}
		if seen != steps {
			t.Fatalf("replayed %d events, want %d", seen, steps)
		}
	})

	t.Run("binary", func(t *testing.T) {
		rec, err := scribble(ctx, steps)
		if err != nil {
			t.Fatalf("scribble: %v", err)
		}
		seen := 0
		for ev, err := range Replay[*Note](ctx, rec) {
			if err != nil {
				t.Fatalf("replay at event %d: %v", seen, err)
			}
			if ev == nil {
				t.Fatalf("event %d came back nil", seen)
			}
			if ev.At != seen || ev.What != fmt.Sprintf("note-%d", seen) {
				t.Fatalf("event %d is %+v", seen, ev)
			}
			seen++
		}
		if seen != steps {
			t.Fatalf("replayed %d events, want %d", seen, steps)
		}
	})
}

// THE POINT: a machine is destroyed the moment its work is done — that is what
// makes a cloud target worth having, and it is the reason a recording is copied
// off the worker at all. Retiring one must not take with it the events it wrote
// half a second earlier.
//
// The window is real and narrow: the job finishes, its result comes home, the
// scaler sees an idle machine and releases it. Between the last record and the
// copy of it there is a moment when those events exist only on a machine that is
// about to stop existing.
func TestARetiredMachineTakesNothingWithIt(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	c := start(t, Config{
		Target:      LocalProcess(),
		Concurrency: 1,
		Scaling: Scaling{
			Min:           1,
			Max:           3,
			JobsPerWorker: 1,
			Interval:      20 * time.Millisecond,
			IdleTimeout:   30 * time.Millisecond, // retire the moment it is idle
		},
	})
	ctx := c.Bind(t.Context())

	// Several at once, so the fleet scales up and every extra machine it starts
	// is retired again as soon as its job is done.
	const jobs, steps = 4, 500
	recs := make([]Recording, jobs)
	errs := make([]error, jobs)
	var wg sync.WaitGroup
	for i := range jobs {
		wg.Go(func() {
			recs[i], errs[i] = simulate(ctx, steps)
		})
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("simulate %d: %v", i, err)
		}
	}

	// Give the scaler time to take the extra machines away, so this reads the
	// coordinator's copies rather than anything still on a worker.
	waitFor(t, "the fleet to scale back down", func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.workers) <= 1
	})

	for i, rec := range recs {
		seen := 0
		for ev, err := range Replay[Tick](ctx, rec) {
			if err != nil {
				t.Fatalf("job %d: replay at event %d: %v; the machine that wrote it is gone, "+
					"and its events had to be kept before it went", i, seen, err)
			}
			if ev.At != seen {
				t.Fatalf("job %d: event %d says it is %d", i, seen, ev.At)
			}
			seen++
		}
		if seen != steps {
			t.Fatalf("job %d: replayed %d events, want %d", i, seen, steps)
		}
	}
}

// late stages an attempt that was moved away and then finishes anyway.
var late struct {
	release0 chan struct{} // lets attempt 0 finish
	release1 chan struct{} // lets attempt 1 finish
	started1 chan struct{} // closed once attempt 1 is running
	once     sync.Once
}

// lateSim records events, then on its first attempt goes quiet until released;
// on its second it stays alive until released. Either attempt, once released,
// completes the recording and returns a handle to it — which is the situation
// the coordinator has to get right: two attempts of one job, both finishing,
// only one of them the job.
var lateSim = flow.Define(func(ctx flow.Context, steps int) (Recording, error) {
	rec, err := Record[Tick](ctx, "replay")
	if err != nil {
		return Recording{}, err
	}
	for i := range steps {
		if err := rec.Record(Tick{At: i}); err != nil {
			return Recording{}, err
		}
	}
	if err := rec.Flush(); err != nil {
		return Recording{}, err
	}

	switch ctx.Attempt() {
	case 0:

		select {
		case <-late.release0:
		case <-ctx.Done():
			return Recording{}, ctx.Err()
		}
	default:
		late.once.Do(func() { close(late.started1) })

		for {
			select {
			case <-late.release1:
			case <-ctx.Done():
				return Recording{}, ctx.Err()
			case <-time.After(50 * time.Millisecond):
				_ = ctx.Heartbeat(0)
				continue
			}
			break
		}
	}
	if err := rec.Close(); err != nil {
		return Recording{}, err
	}
	return rec.Recording(), nil
}, flow.WithName("test.late"),

	flow.WithHeartbeatTimeout(300*time.Millisecond))

// THE POINT: a job that was moved because its worker went quiet may still
// finish there. Its result carries no more authority than its beats do: the job
// is now the retry. Delivering the stale result used to hand the caller a handle
// to attempt 0's recording and then, in the same breath, delete every attempt
// but attempt 1's — which is to say, exactly that recording.
func TestAMovedAttemptThatFinishesAnywayIsNotTheAnswer(t *testing.T) {
	late.release0 = make(chan struct{})
	late.release1 = make(chan struct{})
	late.started1 = make(chan struct{})
	late.once = sync.Once{}

	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 1})
	ctx := c.Bind(t.Context())

	const steps = 8
	type outcome struct {
		rec Recording
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		rec, err := lateSim(ctx, steps)
		done <- outcome{rec, err}
	}()

	// Wait for the move, and for the retry to be running.
	select {
	case <-late.started1:
	case o := <-done:
		t.Fatalf("the job finished before it could be moved: %+v", o)
	case <-time.After(20 * time.Second):
		t.Fatal("the retry never started")
	}

	// Now the abandoned attempt finishes. Its result must not be the caller's.
	close(late.release0)
	select {
	case o := <-done:
		t.Fatalf("the call returned with the moved attempt's result (attempt %d, err %v); "+
			"the job is the retry now", o.rec.Attempt, o.err)
	case <-time.After(500 * time.Millisecond):
	}

	close(late.release1)
	var got outcome
	select {
	case got = <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the retry never delivered")
	}
	if got.err != nil {
		t.Fatalf("lateSim: %v", got.err)
	}
	if got.rec.Attempt != 1 {
		t.Fatalf("the result is from attempt %d, want 1", got.rec.Attempt)
	}

	// And its handle must open, now and after the abandoned attempt's output
	// has been cleaned up.
	waitFor(t, "the abandoned attempt's recording to be discarded",
		func() bool { return len(abandoned(t, c)) == 0 })
	seen := 0
	for ev, err := range Replay[Tick](ctx, got.rec) {
		if err != nil {
			t.Fatalf("replay at event %d: %v", seen, err)
		}
		if ev.At != seen {
			t.Fatalf("event %d says it is %d", seen, ev.At)
		}
		seen++
	}
	if seen != steps {
		t.Fatalf("replayed %d events, want %d", seen, steps)
	}
}

// A beat from an attempt the job has moved on from says nothing about the
// attempt now running, and must not touch its clock or its checkpoint.
func TestABeatFromAMovedAttemptIsIgnored(t *testing.T) {
	c := &Cluster{pending: map[string]*pendingJob{}}
	p := &pendingJob{job: jobEnvelope{ID: "j", Attempt: 1}, checkpoint: []byte("new")}
	c.pending["j"] = p

	c.onBeat(beatEnvelope{Job: "j", Attempt: 0, Checkpoint: []byte("old")})
	if !p.started.IsZero() || !p.beat.IsZero() {
		t.Fatal("a beat from the abandoned attempt started the retry's clocks")
	}
	if string(p.checkpoint) != "new" {
		t.Fatalf("the checkpoint is %q; the abandoned attempt's beat replaced the retry's", p.checkpoint)
	}

	c.onBeat(beatEnvelope{Job: "j", Attempt: 1, Checkpoint: []byte("newer")})
	if p.started.IsZero() || p.beat.IsZero() {
		t.Fatal("the retry's own beat was ignored")
	}
	if string(p.checkpoint) != "newer" {
		t.Fatalf("the checkpoint is %q, want the retry's", p.checkpoint)
	}
}

// waitFor blocks until cond holds, and names what it was waiting for when it
// does not.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

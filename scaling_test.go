package wings

import (
	"slices"
	"testing"
	"time"
)

func TestScalingWantClampsToBounds(t *testing.T) {
	s := Scaling{Min: 2, Max: 6, JobsPerWorker: 4}.withDefaults()

	for _, tc := range []struct {
		outstanding, want int
		why               string
	}{
		{0, 2, "an empty queue still holds the floor"},
		{1, 2, "one job cannot go below the floor"},
		{8, 2, "8/4 is 2, which is the floor anyway"},
		{9, 3, "9/4 rounds UP to 3; a partial batch still needs a worker"},
		{12, 3, "12/4 is exactly 3"},
		{24, 6, "24/4 is 6, the ceiling"},
		{1000, 6, "the ceiling is a spend limit, not a suggestion"},
	} {
		if got := s.want(tc.outstanding); got != tc.want {
			t.Errorf("want(%d) = %d, want %d — %s", tc.outstanding, got, tc.want, tc.why)
		}
	}
}

// THE POINT: rounding down would leave a queue permanently unserved. With
// JobsPerWorker=10 and 9 jobs outstanding, 9/10 truncates to zero workers, and
// nothing would ever pick the work up.
func TestScalingRoundsUpSoAPartialBatchIsStillServed(t *testing.T) {
	s := Scaling{Min: 0, Max: 10, JobsPerWorker: 10}.withDefaults()

	if got := s.want(9); got != 1 {
		t.Fatalf("want(9) with JobsPerWorker=10 is %d; a partial batch must still get a worker", got)
	}
	if got := s.want(11); got != 2 {
		t.Fatalf("want(11) = %d, want 2", got)
	}
}

// Min below 1 must become 1: with zero workers there is nothing to send the
// first job to, and the cluster would refuse work until a tick happened to
// raise it.
func TestScalingFloorIsAtLeastOne(t *testing.T) {
	s := Scaling{Max: 4}.withDefaults()
	if s.Min != 1 {
		t.Errorf("Min defaulted to %d, want 1", s.Min)
	}
	if got := s.want(0); got != 1 {
		t.Errorf("want(0) = %d, want 1", got)
	}
}

func TestScalingDisabledByDefault(t *testing.T) {
	var s Scaling
	if s.enabled() {
		t.Error("the zero Scaling must be off")
	}
	if got := s.initialWorkers(7); got != 7 {
		t.Errorf("with scaling off the configured worker count must stand; got %d, want 7", got)
	}
}

func TestScalingValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		s       Scaling
		wantErr bool
	}{
		{"zero value is inert", Scaling{}, false},
		{"max below min", Scaling{Min: 5, Max: 2}, true},
		{"negative min", Scaling{Min: -1, Max: 4}, true},
		{"negative jobs per worker", Scaling{Max: 4, JobsPerWorker: -3}, true},
		{"sane", Scaling{Min: 1, Max: 4, JobsPerWorker: 2}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.s.validate()
			if tc.wantErr && err == nil {
				t.Error("want an error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("want no error, got %v", err)
			}
		})
	}
}

func TestStartRejectsImpossibleScaling(t *testing.T) {
	_, err := Start(t.Context(), Config{
		Target:  InProcess(),
		Dir:     t.TempDir(),
		Scaling: Scaling{Min: 4, Max: 2},
	})
	if err == nil {
		t.Fatal("want an error for Max below Min")
	}
}

// THE POINT: the policy is expressed in the queue, and carrying it out goes
// through the same launch path as the initial workers — so this test is about
// the whole mechanism, not the arithmetic.
func TestAutoscaleAddsWorkersUnderLoad(t *testing.T) {
	c := start(t, Config{
		Target:      InProcess(),
		Concurrency: 1,
		Scaling: Scaling{
			Min:           1,
			Max:           4,
			JobsPerWorker: 2,
			Interval:      50 * time.Millisecond,
			IdleTimeout:   time.Hour, // never scale down during this test
		},
	})

	if got := c.Workers(); got != 1 {
		t.Fatalf("started with %d workers, want the floor of 1", got)
	}

	// Enough slow work that the queue clearly calls for more than one worker.
	in := make([]time.Duration, 16)
	for i := range in {
		in[i] = 400 * time.Millisecond
	}

	done := make(chan []string, 1)
	go func() {
		got, err := Map(c.Bind(t.Context()), slow, in)
		if err != nil {
			t.Errorf("Map: %v", err)
		}
		done <- got
	}()

	if !eventually(t, 20*time.Second, func() bool { return c.Workers() > 1 }) {
		t.Fatalf("the cluster never scaled up; still %d worker(s) with %d outstanding",
			c.Workers(), c.Outstanding())
	}

	select {
	case got := <-done:
		if len(got) != len(in) {
			t.Fatalf("got %d results, want %d", len(got), len(in))
		}
		for i, g := range got {
			if g != "finished" {
				t.Fatalf("result %d is %q; scaling lost a job", i, g)
			}
		}
	case <-time.After(60 * time.Second):
		t.Fatal("Map never finished while autoscaling")
	}

	if got := c.Workers(); got > 4 {
		t.Errorf("scaled to %d workers, above the Max of 4", got)
	}
}

// Idle workers must be given back, or a burst leaves a cluster permanently at
// its high-water mark — which on a cloud target is a bill.
func TestAutoscaleRetiresIdleWorkers(t *testing.T) {
	c := start(t, Config{
		Target:      InProcess(),
		Concurrency: 1,
		Scaling: Scaling{
			Min:           1,
			Max:           4,
			JobsPerWorker: 1,
			Interval:      50 * time.Millisecond,
			IdleTimeout:   200 * time.Millisecond,
		},
	})

	in := make([]time.Duration, 8)
	for i := range in {
		in[i] = 150 * time.Millisecond
	}
	if _, err := Map(c.Bind(t.Context()), slow, in); err != nil {
		t.Fatalf("Map: %v", err)
	}

	if !eventually(t, 20*time.Second, func() bool { return c.Workers() == 1 }) {
		t.Fatalf("idle workers were never retired; still %d, want the floor of 1", c.Workers())
	}

	// And the cluster must still work afterwards.
	got, err := double(c.Bind(t.Context()), 21)
	if err != nil {
		t.Fatalf("Call after scale-down: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
}

// Scaling down must never drop a job. A worker is only retired while idle, and
// idle is decided under the same lock that assigns work.
func TestScaleDownNeverDropsWork(t *testing.T) {
	c := start(t, Config{
		Target:      InProcess(),
		Concurrency: 2,
		Scaling: Scaling{
			Min:           1,
			Max:           4,
			JobsPerWorker: 1,
			Interval:      20 * time.Millisecond,
			IdleTimeout:   30 * time.Millisecond, // aggressively retire
		},
	})

	// Several rounds, so scale-up and scale-down interleave with dispatch.
	for round := range 4 {
		in := make([]int, 25)
		want := make([]int, 25)
		for i := range in {
			in[i] = round*100 + i
			want[i] = in[i] * 2
		}
		got, err := Map(c.Bind(t.Context()), double, in)
		if err != nil {
			t.Fatalf("round %d: Map: %v", round, err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("round %d: results wrong while scaling:\n got %v\nwant %v", round, got, want)
		}
	}
}

// The local-process target must scale the same way, since the policy is
// supposed to be independent of what a worker actually is.
func TestAutoscaleWorksOnTheLocalTarget(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	c := start(t, Config{
		Target:      LocalProcess(),
		Concurrency: 1,
		Scaling: Scaling{
			Min:           1,
			Max:           3,
			JobsPerWorker: 2,
			Interval:      100 * time.Millisecond,
			IdleTimeout:   time.Hour,
		},
	})

	in := make([]time.Duration, 12)
	for i := range in {
		in[i] = 500 * time.Millisecond
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := Map(c.Bind(t.Context()), slow, in); err != nil {
			t.Errorf("Map: %v", err)
		}
	}()

	if !eventually(t, 40*time.Second, func() bool { return c.Workers() > 1 }) {
		t.Errorf("the local target never scaled up; still %d worker(s)", c.Workers())
	}

	select {
	case <-done:
	case <-time.After(90 * time.Second):
		t.Fatal("Map never finished")
	}
}

func eventually(t *testing.T, limit time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		select {
		case <-time.After(25 * time.Millisecond):
		case <-t.Context().Done():
			return false
		}
	}
	return cond()
}

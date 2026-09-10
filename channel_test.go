package wings

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// feed is a call's input carrying a channel: the handle travels, the values
// follow through the cluster.
type feed struct {
	Values *flow.Channel[int] `json:"values"`
	Count  int                `json:"count,omitempty"`
}

// sums receives everything on the channel it was handed and returns the total.
var sums = flow.Define(func(ctx flow.Context, in feed) (int, error) {
	total := 0
	for {
		v, ok, err := in.Values.Recv(ctx)
		if err != nil {
			return 0, err
		}
		if !ok {
			return total, nil
		}
		total += v
	}
}, flow.WithName("test.sums"))

// counts sends 1..Count on the channel it was handed, then closes it.
var counts = flow.Define(func(ctx flow.Context, in feed) (int, error) {
	for i := 1; i <= in.Count; i++ {
		if err := in.Values.Send(ctx, i); err != nil {
			return 0, err
		}
	}
	return in.Count, in.Values.Close(ctx)
}, flow.WithName("test.counts"))

// THE POINT: a finished run's channel data is reclaimed. A settled job's outbox
// is dropped once its output is home and merged; and once the run completes — so
// it will never resume and replay receives from them — its canonical streams are
// dropped too, rather than kept for the cluster's life.
func TestASettledJobsChannelOutboxIsDropped(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := start(t, Config{Target: LocalProcess(), Workers: 2, Concurrency: 1})

	var got int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		producer := ctx.Go(counts, feed{Values: ch, Count: 5})
		consumer := ctx.Go(sums, feed{Values: ch})
		if _, err := producer.Await(ctx); err != nil {
			return err
		}
		var err error
		got, err = consumer.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 15 {
		t.Fatalf("consumer summed %d, want 15", got)
	}

	client, err := c.sharedClient()
	if err != nil {
		t.Fatalf("shared client: %v", err)
	}

	// All channel streams go once the run completes: every outbox (the two forked
	// jobs' by the per-job settle path, the workflow's own export outbox by
	// forgetRun) and then each canonical stream, once its last feeding outbox is
	// gone. A completed run will not resume, so nothing reads them again.
	deadline := time.Now().Add(30 * time.Second)
	for {
		names, err := client.ListStreams(t.Context())
		if err != nil {
			t.Fatalf("list streams: %v", err)
		}
		outboxes, canonical := 0, 0
		for _, n := range names {
			if o, ok := parseOutput(n); ok && o.Prefix == chanoutPrefix {
				outboxes++
			}
			if strings.HasPrefix(n, chanPrefix) {
				canonical++
			}
		}
		if outboxes == 0 && canonical == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after the run completed, %d outboxes and %d canonical channel streams remain; not all reclaimed", outboxes, canonical)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// sumsSlow receives everything on its channel and returns the total, pausing
// between receives so its worker can be killed while it is partway through —
// leaving some receives recorded and the rest still to come.
var sumsSlow = flow.Define(func(ctx flow.Context, in feed) (int, error) {
	total := 0
	for {
		v, ok, err := in.Values.Recv(ctx)
		if err != nil {
			return 0, err
		}
		if !ok {
			return total, nil
		}
		total += v
		time.Sleep(300 * time.Millisecond)
	}
}, flow.WithName("test.sumsSlow"))

// THE POINT: received values live on a side stream apart from the history, and a
// job that moves carries both — so a receiver killed partway through replays the
// receives it had recorded by reading their values back from the side stream
// hydrated onto the new worker, then takes the rest from the channel.
func TestAMovedReceiverReplaysItsRecordedValues(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := start(t, Config{Target: LocalProcess(), Workers: 1, Concurrency: 2})

	var got int
	done := make(chan error, 1)
	go func() {
		done <- c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
			ch := ctx.NewChannel[int]()
			producer := ctx.Go(counts, feed{Values: ch, Count: 5})
			consumer := ctx.Go(sumsSlow, feed{Values: ch})
			if _, err := producer.Await(ctx); err != nil {
				return err
			}
			var err error
			got, err = consumer.Await(ctx)
			return err
		})
	}()

	// Let the receiver record a couple of values, then kill the worker it is on.
	time.Sleep(700 * time.Millisecond)
	victim := firstWorker(t, c)
	if victim.proc == nil {
		t.Fatal("expected a local worker with a process to kill")
	}
	if err := victim.proc.Kill(); err != nil {
		t.Fatalf("kill worker: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the run never finished after the receiver's worker was killed; its recorded values were not replayed")
	}
	if got != 15 {
		t.Fatalf("the moved receiver summed %d, want 15; a replay read the wrong values back", got)
	}
}

// fanSum creates a channel of its own, hands it to a producer and a consumer it
// forks, and returns the total. The channel is created on the worker running
// fanSum, not the coordinator — a worker-created shared channel.
var fanSum = flow.Define(func(ctx flow.Context, _ struct{}) (int, error) {
	ch := ctx.NewChannel[int]()
	producer := ctx.Go(counts, feed{Values: ch, Count: 5})
	consumer := ctx.Go(sums, feed{Values: ch})
	if _, err := producer.Await(ctx); err != nil {
		return 0, err
	}
	return consumer.Await(ctx)
}, flow.WithName("test.fanSum"))

// reclaimHold gates a workflow open after a forked activity has finished, so a
// test can observe that activity's channel being reclaimed before the run ends.
var reclaimHold struct{ release chan struct{} }

var heldOpen = flow.Define(func(ctx flow.Context, _ struct{}) (int, error) {
	select {
	case <-reclaimHold.release:
	case <-ctx.Done():
	}
	return 0, nil
}, flow.WithName("test.heldOpen"))

// THE POINT: a channel is reclaimed when the activity that created it returns,
// not only when the whole run ends — so a long run of short activities does not
// accumulate their channel data. fanSum creates and drains a channel and
// returns; its stream is gone while the run is still held open on another thread.
func TestAnActivitysChannelIsReclaimedWhenItReturns(t *testing.T) {
	reclaimHold.release = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(reclaimHold.release) }) }
	t.Cleanup(release)

	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 4})

	client, err := c.sharedClient()
	if err != nil {
		t.Fatalf("shared client: %v", err)
	}

	var got int
	done := make(chan error, 1)
	go func() {
		done <- c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
			var err error
			if got, err = ctx.Go(fanSum, struct{}{}).Await(ctx); err != nil {
				return err
			}
			// Hold the run open: fanSum has returned and its channel should be
			// reclaimed while this waits.
			_, err = ctx.Go(heldOpen, struct{}{}).Await(ctx)
			return err
		})
	}()

	canonicals := func() int {
		names, err := client.ListStreams(t.Context())
		if err != nil {
			t.Fatalf("list streams: %v", err)
		}
		n := 0
		for _, s := range names {
			if strings.HasPrefix(s, chanPrefix) {
				n++
			}
		}
		return n
	}

	// fanSum's channel stream should be reclaimed while the run is still held open
	// on heldOpen — proof the reclamation is per-activity, not only per-run.
	deadline := time.Now().Add(30 * time.Second)
	reclaimed := false
	for time.Now().Before(deadline) {
		if canonicals() == 0 {
			reclaimed = true
			break
		}
		select {
		case err := <-done:
			t.Fatalf("run finished before the channel was seen reclaimed mid-run: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if !reclaimed {
		t.Fatalf("fanSum's channel stream was not reclaimed while the run was still open")
	}

	release()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 15 {
		t.Fatalf("fanSum returned %d, want 15", got)
	}
}

// THE POINT: a channel created during an in-process activity call is reclaimed
// when the call returns, not only at run end. Calling fanSum directly (rather
// than forking it) creates its channel under the run body, which is never a job;
// the fix retires it on the call's return, so the direct-call shape — the one
// that avoids a round trip per receive — reclaims per activity too.
func TestAnInProcessCallsChannelIsReclaimedWhenItReturns(t *testing.T) {
	reclaimHold.release = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(reclaimHold.release) }) }
	t.Cleanup(release)

	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 4})

	client, err := c.sharedClient()
	if err != nil {
		t.Fatalf("shared client: %v", err)
	}

	var got int
	done := make(chan error, 1)
	go func() {
		done <- c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
			var err error
			if got, err = fanSum(ctx, struct{}{}); err != nil { // direct call: runs in place
				return err
			}
			// Hold the run open: fanSum has returned and its channel should be
			// reclaimed while this waits, though no job's thread ever owned it.
			_, err = ctx.Go(heldOpen, struct{}{}).Await(ctx)
			return err
		})
	}()

	canonicals := func() int {
		names, err := client.ListStreams(t.Context())
		if err != nil {
			t.Fatalf("list streams: %v", err)
		}
		n := 0
		for _, s := range names {
			if strings.HasPrefix(s, chanPrefix) {
				n++
			}
		}
		return n
	}

	deadline := time.Now().Add(30 * time.Second)
	reclaimed := false
	for time.Now().Before(deadline) {
		if canonicals() == 0 {
			reclaimed = true
			break
		}
		select {
		case err := <-done:
			t.Fatalf("run finished before the channel was seen reclaimed mid-run: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if !reclaimed {
		t.Fatalf("the in-process call's channel stream was not reclaimed while the run was open")
	}

	release()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 15 {
		t.Fatalf("fanSum returned %d, want 15", got)
	}
}

// recvFan forks a producer and receives on its own thread, like the trainer's
// Train where main receives the boards' sends. Called directly, the receiver is
// the run's own thread, so its wants go to the run's coordinator outbox
// (chanout.<run>.0.<id>) rather than a job's.
var recvFan = flow.Define(func(ctx flow.Context, _ struct{}) (int, error) {
	ch := ctx.NewChannel[int]()
	prod := ctx.Go(counts, feed{Values: ch, Count: 5})
	total := 0
	for {
		v, ok, err := ch.Recv(ctx)
		if err != nil {
			return 0, err
		}
		if !ok {
			break
		}
		total += v
	}
	if _, err := prod.Await(ctx); err != nil {
		return 0, err
	}
	return total, nil
}, flow.WithName("test.recvFan"))

// THE POINT: a channel the caller thread itself receives on is reclaimed when the
// call returns, not only at run end — even though the receiver's wants live in the
// run's own coordinator outbox, which is not a job that ever settles. This is the
// trainer's shape (main receives the boards' decisions inside a called activity).
func TestACallersReceiveChannelIsReclaimedWhenTheCallReturns(t *testing.T) {
	reclaimHold.release = make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(reclaimHold.release) }) }
	t.Cleanup(release)

	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 4})

	var got int
	done := make(chan error, 1)
	go func() {
		done <- c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
			var err error
			if got, err = recvFan(ctx, struct{}{}); err != nil {
				return err
			}
			_, err = ctx.Go(heldOpen, struct{}{}).Await(ctx)
			return err
		})
	}()

	// The channel must first appear — its wants live in the run's own outbox — and
	// then be reclaimed while the run is still held open on heldOpen.
	waitForChannel := func(want bool, deadline time.Duration, what string) {
		t.Helper()
		end := time.Now().Add(deadline)
		for time.Now().Before(end) {
			if (streamsWithPrefix(t, c, chanPrefix) > 0) == want {
				return
			}
			select {
			case err := <-done:
				t.Fatalf("run ended while waiting for %s: %v", what, err)
			case <-time.After(100 * time.Millisecond):
			}
		}
		t.Fatalf("timed out waiting for %s", what)
	}
	waitForChannel(true, 15*time.Second, "the caller's receive channel to appear")
	waitForChannel(false, 20*time.Second, "the channel to be reclaimed mid-run")

	release()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 15 {
		t.Fatalf("recvFan returned %d, want 15", got)
	}
}

func streamsWithPrefix(t *testing.T, c *Cluster, prefix string) int {
	t.Helper()
	client, err := c.sharedClient()
	if err != nil {
		t.Fatalf("shared client: %v", err)
	}
	names, err := client.ListStreams(t.Context())
	if err != nil {
		t.Fatalf("list streams: %v", err)
	}
	n := 0
	for _, s := range names {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}

// THE POINT: RetainChannelData keeps a returned activity's channel data — the
// canonical stream that holds the one copy of its values — for replaying it step
// by step while debugging, rather than dropping it when the activity returns.
func TestRetainChannelDataKeepsReturnedValues(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 4, RetainChannelData: true})

	var got int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		var err error
		got, err = ctx.Go(fanSum, struct{}{}).Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 15 {
		t.Fatalf("fanSum returned %d, want 15", got)
	}
	if streamsWithPrefix(t, c, chanPrefix) == 0 {
		t.Fatal("RetainChannelData was set but the channel data was dropped anyway")
	}
}

// THE POINT: a channel a worker created (not the coordinator) is attributed to
// its run through the job that owns it, so the run's completion reclaims its
// canonical stream the same way it does the coordinator's own channels.
func TestAWorkerCreatedChannelStreamIsReclaimed(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	c := start(t, Config{Target: LocalProcess(), Workers: 2, Concurrency: 2})

	var got int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		var err error
		got, err = ctx.Go(fanSum, struct{}{}).Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 15 {
		t.Fatalf("fanSum returned %d, want 15", got)
	}

	client, err := c.sharedClient()
	if err != nil {
		t.Fatalf("shared client: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		names, err := client.ListStreams(t.Context())
		if err != nil {
			t.Fatalf("list streams: %v", err)
		}
		canonical := 0
		for _, n := range names {
			if strings.HasPrefix(n, chanPrefix) {
				canonical++
			}
		}
		if canonical == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d canonical channel streams remain after the run completed; the worker-created channel was not reclaimed", canonical)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// THE POINT: a channel is shared by handing it to a call. The workflow on the
// coordinator and the function on a worker — or two functions on two workers
// — use it the way two threads would, and the values cross machines.
func TestAChannelCrossesMachines(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target Target
	}{
		{"inprocess", InProcess()},
		{"local", LocalProcess()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "local" && testing.Short() {
				t.Skip("spawns child processes")
			}
			c := start(t, Config{Target: tc.target, Workers: 2, Concurrency: 1})

			t.Run("workflow to function", func(t *testing.T) {
				var got int
				err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
					ch := ctx.NewChannel[int]()
					fut := ctx.Go(sums, feed{Values: ch})
					for _, v := range []int{1, 2, 3, 4} {
						if err := ch.Send(ctx, v); err != nil {
							return err
						}
					}
					if err := ch.Close(ctx); err != nil {
						return err
					}
					var err error
					got, err = fut.Await(ctx)
					return err
				})
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				if got != 10 {
					t.Fatalf("the function summed %d, want 10", got)
				}
			})

			t.Run("function to workflow", func(t *testing.T) {
				var got []int
				err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
					ch := ctx.NewChannel[int]()
					fut := ctx.Go(counts, feed{Values: ch, Count: 3})
					for {
						v, ok, err := ch.Recv(ctx)
						if err != nil {
							return err
						}
						if !ok {
							break
						}
						got = append(got, v)
					}
					_, err := fut.Await(ctx)
					return err
				})
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
					t.Fatalf("the workflow received %v, want [1 2 3]", got)
				}
			})

			t.Run("function to function", func(t *testing.T) {
				var got int
				err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
					ch := ctx.NewChannel[int]()
					producer := ctx.Go(counts, feed{Values: ch, Count: 5})
					consumer := ctx.Go(sums, feed{Values: ch})
					if _, err := producer.Await(ctx); err != nil {
						return err
					}
					var err error
					got, err = consumer.Await(ctx)
					return err
				})
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				if got != 15 {
					t.Fatalf("the consumer summed %d, want 15", got)
				}
			})

			// Fan-out is N single-reader channels: each receiver has its own
			// channel, and the sender distributes across them. The totals still
			// account for every value once.
			t.Run("fan-out over two channels", func(t *testing.T) {
				var totals [2]int
				err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
					chs := [2]*flow.Channel[int]{ctx.NewChannel[int](), ctx.NewChannel[int]()}
					first := ctx.Go(sums, feed{Values: chs[0]})
					second := ctx.Go(sums, feed{Values: chs[1]})
					for v := 1; v <= 6; v++ {
						if err := chs[(v-1)%2].Send(ctx, v); err != nil {
							return err
						}
					}
					for i := range chs {
						if err := chs[i].Close(ctx); err != nil {
							return err
						}
					}
					var err error
					if totals[0], err = first.Await(ctx); err != nil {
						return err
					}
					totals[1], err = second.Await(ctx)
					return err
				})
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				if totals[0]+totals[1] != 21 {
					t.Fatalf("the receivers summed %d and %d, want 21 in all", totals[0], totals[1])
				}
			})
		})
	}
}

var movedRecv struct {
	attempts atomic.Int32
	release  chan struct{}
}

// receivesThenStalls takes three values, reports progress — which commits
// its history — and on its first attempt goes quiet until moved. The retry
// must see the same three values, in the same order, before the rest.
var receivesThenStalls = flow.Define(func(ctx flow.Context, in feed) ([]int, error) {
	movedRecv.attempts.Add(1)
	var got []int
	for range 3 {
		v, _, err := in.Values.Recv(ctx)
		if err != nil {
			return nil, err
		}
		got = append(got, v)
	}
	if err := ctx.Heartbeat(len(got)); err != nil {
		return nil, err
	}
	if ctx.Attempt() == 0 {
		select {
		case <-movedRecv.release:
		case <-ctx.Done():
		}
		return nil, errors.New("test.receivesThenStalls: the first attempt was abandoned")
	}
	for {
		v, ok, err := in.Values.Recv(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			return got, nil
		}
		got = append(got, v)
	}
}, flow.WithName("test.receivesThenStalls"),

	flow.WithHeartbeatTimeout(300*time.Millisecond))

// THE POINT: a moved function replays its receives from the channel's
// durable record, on a worker that never saw the sender, and gets exactly
// what its predecessor got.
func TestAMovedFunctionReplaysItsReceives(t *testing.T) {
	movedRecv.attempts.Store(0)
	movedRecv.release = make(chan struct{})
	t.Cleanup(func() { close(movedRecv.release) })

	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 1})
	var got []int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		fut := ctx.Go(receivesThenStalls, feed{Values: ch})
		for _, v := range []int{7, 8, 9, 10, 11} {
			if err := ch.Send(ctx, v); err != nil {
				return err
			}
		}
		if err := ch.Close(ctx); err != nil {
			return err
		}
		var err error
		got, err = fut.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []int{7, 8, 9, 10, 11}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if n := movedRecv.attempts.Load(); n != 2 {
		t.Fatalf("the function ran %d times, want 2: once stalled, once moved", n)
	}
}

package wings

import (
	"errors"
	"strings"
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

// THE POINT: a settled job's shared-channel outbox is dropped once its output is
// home and merged, rather than tailed for the cluster's life. The canonical
// stream stays, so a resume can still replay receives from it.
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

	// Every outbox is dropped once the run settles: the two forked jobs' by the
	// per-job settle path, and the workflow's own export outbox by forgetRun once
	// the run completes. The canonical stream must survive for a resume.
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
		if outboxes == 0 {
			if canonical == 0 {
				t.Fatal("the canonical channel stream was dropped; a resume could not replay")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d channel outboxes still present after the run settled; they were not all dropped", outboxes)
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

			// Each value to exactly one of two receivers on two workers: the
			// coordinator grants it, and the totals account for every value
			// once.
			t.Run("two functions receiving", func(t *testing.T) {
				var totals [2]int
				err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
					ch := ctx.NewChannel[int]()
					first := ctx.Go(sums, feed{Values: ch})
					second := ctx.Go(sums, feed{Values: ch})
					for v := 1; v <= 6; v++ {
						if err := ch.Send(ctx, v); err != nil {
							return err
						}
					}
					if err := ch.Close(ctx); err != nil {
						return err
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
					t.Fatalf("the receivers summed %d and %d, want 21 in all: each value to one of them", totals[0], totals[1])
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

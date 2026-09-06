package flow_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// consumer receives everything on a shared channel and returns the sum. The
// handle arrives in its input, the way a work function on a worker would get
// it.
type feed struct {
	Values *flow.Channel[int] `json:"values"`
}

var consumer = flow.Define(func(ctx flow.Context, in feed) (int, error) {
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
}, flow.WithName("test.consumer"))

// sharedRuns is an executor that runs each call as a run of its own on the
// same host, the way a worker does — so a channel in the input really
// crosses from one run to another.
type sharedRuns struct {
	store flow.Store
	host  flow.ChannelHost
}

func (e sharedRuns) Invoke(ctx context.Context, name string, payload []byte) ([]byte, error) {
	o := flow.OriginFrom(ctx)
	return flow.RunCall(ctx, "child:"+o.Key(), name, payload,
		flow.WithStore(e.store), flow.WithExecutor(e), flow.WithChannelHost(e.host), flow.Once())
}

// THE POINT: a channel handed to another run in a call's input carries values
// between the two runs, and what was sent before the handle left goes too.
func TestAChannelReachesAnotherRun(t *testing.T) {
	host := flow.NewMemChannelHost()
	store := flow.NewMemStore()
	var got int
	err := flow.Run(t.Context(), "parent", func(ctx flow.Context) error {
		ch := ctx.NewBufferedChannel[int](1)
		if err := ch.Send(ctx, 1); err != nil { // before the channel is shared
			return err
		}
		fut := ctx.Go(consumer, feed{Values: ch})
		for _, v := range []int{2, 3, 4} {
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
	}, flow.WithStore(store), flow.WithExecutor(sharedRuns{store, host}), flow.WithChannelHost(host))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 10 {
		t.Fatalf("the other run received a total of %d, want 10", got)
	}
}

// producer sends into a channel it was handed and closes it.
var producer = flow.Define(func(ctx flow.Context, in feed) (int, error) {
	for i := 1; i <= 3; i++ {
		if err := in.Values.Send(ctx, i*10); err != nil {
			return 0, err
		}
	}
	return 3, in.Values.Close(ctx)
}, flow.WithName("test.producer"))

var replayedRecv struct {
	attempts atomic.Int32
	seen     [][]int
}

// THE POINT: the receiving side of a shared channel replays like any other
// receive — a retry is handed the same values in the same order, from the
// host rather than from a sender that is long finished.
func TestAReplayedRunReceivesTheSameValuesFromASharedChannel(t *testing.T) {
	host := flow.NewMemChannelHost()
	store := flow.NewMemStore()
	replayedRecv.attempts.Store(0)
	replayedRecv.seen = nil
	err := flow.Run(t.Context(), "receiver", func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		fut := ctx.Go(producer, feed{Values: ch})
		var seen []int
		for {
			v, ok, err := ch.Recv(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			seen = append(seen, v)
		}
		if _, err := fut.Await(ctx); err != nil {
			return err
		}
		replayedRecv.seen = append(replayedRecv.seen, seen)
		if replayedRecv.attempts.Add(1) == 1 {
			return errors.New("fail once")
		}
		return nil
	}, flow.WithStore(store), flow.WithExecutor(sharedRuns{store, host}), flow.WithChannelHost(host),
		flow.Backoff(0, 0))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(replayedRecv.seen) != 2 {
		t.Fatalf("ran %d times, want 2", len(replayedRecv.seen))
	}
	want := []int{10, 20, 30}
	for i, seen := range replayedRecv.seen {
		if len(seen) != 3 || seen[0] != want[0] || seen[1] != want[1] || seen[2] != want[2] {
			t.Fatalf("attempt %d received %v, want %v", i, seen, want)
		}
	}
}

// THE POINT: a channel cannot leave a run that has no host, and the error
// arrives at the call rather than as a hang somewhere else.
func TestAChannelCannotLeaveARunWithoutAHost(t *testing.T) {
	err := flow.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		_, err := consumer(ctx, feed{Values: ch})
		return err
	}, flow.WithStore(flow.NewMemStore()), flow.Once())
	if err == nil || !strings.Contains(err.Error(), "no channel host") {
		t.Fatalf("got %v, want an error naming the missing host", err)
	}
}

// takesTwoWhenTold waits to be told, then takes two values and returns their
// sum, noting when it took the first.
var takesTwoWhenTold = flow.Define(func(ctx flow.Context, in feed) (int, error) {
	select {
	case <-told.release:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	a, _, err := in.Values.Recv(ctx)
	if err != nil {
		return 0, err
	}
	told.firstTake.Store(time.Now().UnixNano())
	b, _, err := in.Values.Recv(ctx)
	if err != nil {
		return 0, err
	}
	return a + b, nil
}, flow.WithName("test.takesTwoWhenTold"))

var told struct {
	release   chan struct{}
	firstTake atomic.Int64
}

// THE POINT: a shared channel is a channel, not a queue. Its capacity holds
// across runs: a send with no room waits for a receive in the other run to
// make some, exactly as a send between threads waits.
func TestASharedChannelHonoursItsCapacity(t *testing.T) {
	host := flow.NewMemChannelHost()
	store := flow.NewMemStore()
	told.release = make(chan struct{})
	told.firstTake.Store(0)

	var secondSent time.Time
	var got int
	err := flow.Run(t.Context(), "capacity", func(ctx flow.Context) error {
		ch := ctx.NewBufferedChannel[int](1)
		fut := ctx.Go(takesTwoWhenTold, feed{Values: ch})
		if err := ch.Send(ctx, 1); err != nil { // the one place in the buffer
			return err
		}
		time.AfterFunc(100*time.Millisecond, func() { close(told.release) })
		if err := ch.Send(ctx, 2); err != nil { // no room until the other run takes one
			return err
		}
		secondSent = time.Now()
		var err error
		got, err = fut.Await(ctx)
		return err
	}, flow.WithStore(store), flow.WithExecutor(sharedRuns{store, host}), flow.WithChannelHost(host))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 3 {
		t.Fatalf("the other run summed %d, want 3", got)
	}
	first := time.Unix(0, told.firstTake.Load())
	if first.IsZero() || secondSent.Before(first) {
		t.Fatalf("the second send completed at %s, before the first take at %s: the capacity was not honoured",
			secondSent.Format(time.StampMicro), first.Format(time.StampMicro))
	}
}

// THE POINT: each value on a shared channel goes to ONE receiver, wherever
// it runs. Two runs receiving from the same channel split what is sent
// between them; nothing is seen twice and nothing is lost.
func TestEachValueOnASharedChannelGoesToOneReceiver(t *testing.T) {
	host := flow.NewMemChannelHost()
	store := flow.NewMemStore()
	var totals [2]int
	err := flow.Run(t.Context(), "split", func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		first := ctx.Go(consumer, feed{Values: ch})
		second := ctx.Go(consumer, feed{Values: ch})
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
	}, flow.WithStore(store), flow.WithExecutor(sharedRuns{store, host}), flow.WithChannelHost(host))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if totals[0]+totals[1] != 21 {
		t.Fatalf("the two receivers summed %d and %d, want a total of 21: each value to exactly one of them", totals[0], totals[1])
	}
}

// THE POINT: the rule for handing out values is one rule, and a host that
// starts with a record its predecessor wrote reaches the same state from it
// — and grants what the predecessor admitted but never got to grant.
func TestTheArbiterRestoresItsStateFromTheRecord(t *testing.T) {
	a := flow.NewArbiter()
	var record []flow.ChannelItem
	offer := func(it flow.ChannelItem) []flow.ChannelItem {
		out := a.Offer(it)
		record = append(record, out...)
		return out
	}
	value := func(seq uint64) flow.ChannelItem {
		return flow.ChannelItem{From: "p/main", Seq: seq, Data: []byte("v")}
	}
	want := func(from string, seq uint64) flow.ChannelItem {
		return flow.ChannelItem{Want: true, From: from, Seq: seq}
	}

	if out := offer(value(0)); len(out) != 1 {
		t.Fatalf("a value with nobody waiting: %d records, want 1", len(out))
	}
	out := offer(want("a/main", 0))
	if len(out) != 2 || out[1].To != "a/main" || out[1].From != "p/main" || out[1].Seq != 0 {
		t.Fatalf("a want with a value waiting: %+v, want the want and a grant of p/main#0 to a/main#0", out)
	}
	if out := offer(want("a/main", 0)); len(out) != 0 {
		t.Fatalf("the same want again: %d records, want none", len(out))
	}
	if out := offer(want("b/main", 0)); len(out) != 1 {
		t.Fatalf("a want with nothing to give: %d records, want 1", len(out))
	}
	out = offer(value(1))
	if len(out) != 2 || out[1].To != "b/main" || out[1].Seq != 1 {
		t.Fatalf("a value with a want waiting: %+v, want the value and a grant of p/main#1 to b/main#0", out)
	}
	if out := offer(value(1)); len(out) != 0 {
		t.Fatalf("the same value again: %d records, want none", len(out))
	}
	if out := offer(want("c/main", 0)); len(out) != 1 {
		t.Fatalf("a third want with nothing to give: %d records, want 1", len(out))
	}
	for _, tc := range []struct {
		name string
		it   flow.ChannelItem
		want bool
	}{
		{"a granted want", want("a/main", 0), true},
		{"a want still open", want("c/main", 0), false},
		{"a want never made", want("d/main", 0), false},
		{"a granted value", value(0), true},
		{"a value never sent", value(9), false},
	} {
		if got := a.Settled(tc.it); got != tc.want {
			t.Errorf("Settled(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
	if out := offer(flow.ChannelItem{Closed: true}); len(out) != 1 {
		t.Fatalf("a close: %d records, want 1", len(out))
	}
	if !a.Settled(want("c/main", 0)) {
		t.Errorf("a want still open on a closed channel is settled: the receiver is owed the close")
	}

	// A successor that reads the whole record owes nothing.
	b := flow.NewArbiter()
	for _, it := range record {
		b.Restore(it)
	}
	if owed := b.Grants(); len(owed) != 0 {
		t.Fatalf("restored from the whole record, the arbiter owes %+v, want nothing", owed)
	}
	// One that reads a record cut before a grant owes that grant.
	c := flow.NewArbiter()
	for _, it := range record {
		if it.To == "b/main" {
			continue
		}
		c.Restore(it)
	}
	owed := c.Grants()
	if len(owed) != 1 || owed[0].To != "b/main" || owed[0].Seq != 1 {
		t.Fatalf("restored from a record missing its last grant, the arbiter owes %+v, want that grant", owed)
	}
}

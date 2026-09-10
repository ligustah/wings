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

type writeFeed struct {
	Values *flow.Writer[int] `json:"values"`
}

// writerProducer is handed only the send side: it can send and close, not receive.
var writerProducer = flow.Define(func(ctx flow.Context, in writeFeed) (int, error) {
	for i := 1; i <= 3; i++ {
		if err := in.Values.Send(ctx, i*10); err != nil {
			return 0, err
		}
	}
	return 3, in.Values.Close(ctx)
}, flow.WithName("test.writerProducer"))

// THE POINT: a run handed only a Writer still delivers every value to the
// receiver, though that run drops each value's bytes once the host has them
// (it never receives, so nothing local reads them back). The receiver reads
// the values from the host's copy.
func TestAWriterOnlyRunDeliversEveryValue(t *testing.T) {
	host := flow.NewMemChannelHost()
	store := flow.NewMemStore()
	var got int
	err := flow.Run(t.Context(), "reader", func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		w := ch.Writer()
		fut := ctx.Go(writerProducer, writeFeed{Values: &w})
		total := 0
		for {
			v, ok, err := ch.Recv(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			total += v
		}
		if _, err := fut.Await(ctx); err != nil {
			return err
		}
		got = total
		return nil
	}, flow.WithStore(store), flow.WithExecutor(sharedRuns{store, host}), flow.WithChannelHost(host))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 60 {
		t.Fatalf("the reader received a total of %d, want 60 (10+20+30)", got)
	}
}

// THE POINT: Select reads over several hosted channels the way Go selects over
// channels — readiness is a value pumped from the host, no ledger involved — so
// fan-in is N single-reader channels drained by one selecting thread, and it
// works the same whether the producers run here or on other machines.
func TestSelectReadsOverHostedChannels(t *testing.T) {
	host := flow.NewMemChannelHost()
	store := flow.NewMemStore()
	var got int
	err := flow.Run(t.Context(), "fanin", func(ctx flow.Context) error {
		a, b := ctx.NewChannel[int](), ctx.NewChannel[int]()
		aw, bw := a.Writer(), b.Writer()
		fa := ctx.Go(writerProducer, writeFeed{Values: &aw})
		fb := ctx.Go(writerProducer, writeFeed{Values: &bw})

		total := 0
		done := [2]bool{}
		chans := [2]*flow.Channel[int]{a, b}
		for !done[0] || !done[1] {
			sel := ctx.Select()
			for i, ch := range chans {
				if done[i] {
					continue
				}
				i := i
				sel.Recv(ch, func(v int, ok bool, err error) error {
					switch {
					case err != nil:
						return err
					case !ok:
						done[i] = true // drained and closed: drop the case, like niling a channel
					default:
						total += v
					}
					return nil
				})
			}
			if err := sel.Do(ctx); err != nil {
				return err
			}
		}
		if _, err := fa.Await(ctx); err != nil {
			return err
		}
		if _, err := fb.Await(ctx); err != nil {
			return err
		}
		got = total
		return nil
	}, flow.WithStore(store), flow.WithExecutor(sharedRuns{store, host}), flow.WithChannelHost(host))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != 120 {
		t.Fatalf("selecting over two hosted channels received a total of %d, want 120 (60+60)", got)
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

// THE POINT: a shared channel has one reader. Fan-out to two runs is two
// channels, each drained by its own run; the sender distributes across them and
// every value is received once, wherever the run lives.
func TestFanOutIsOneChannelPerReceiver(t *testing.T) {
	host := flow.NewMemChannelHost()
	store := flow.NewMemStore()
	var totals [2]int
	err := flow.Run(t.Context(), "split", func(ctx flow.Context) error {
		chs := [2]*flow.Channel[int]{ctx.NewChannel[int](), ctx.NewChannel[int]()}
		first := ctx.Go(consumer, feed{Values: chs[0]})
		second := ctx.Go(consumer, feed{Values: chs[1]})
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
	}, flow.WithStore(store), flow.WithExecutor(sharedRuns{store, host}), flow.WithChannelHost(host))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if totals[0]+totals[1] != 21 {
		t.Fatalf("the two receivers summed %d and %d, want a total of 21", totals[0], totals[1])
	}
}

// THE POINT: a host that starts with a record its predecessor wrote reaches the
// same counts from it — values, consumes, and the close — so a restarted
// coordinator tells a parked receive or send the same thing the first one would.
func TestTheLedgerRestoresItsCountsFromTheRecord(t *testing.T) {
	record := []flow.ChannelItem{
		{From: "p/main", Seq: 0, Data: []byte("v")},
		{From: "p/main", Seq: 1, Data: []byte("v")},
		{Consumed: true, From: "p/main", Seq: 0},
		{From: "p/main", Seq: 2, Data: []byte("v")},
		{Closed: true},
	}

	a := flow.NewLedger()
	for _, it := range record {
		a.Restore(it)
	}
	if a.Values() != 3 || a.Consumed() != 1 || !a.Closed() {
		t.Fatalf("restored values=%d consumed=%d closed=%v, want 3/1/true", a.Values(), a.Consumed(), a.Closed())
	}
	// Re-offering what is already on the record changes nothing: a restarted
	// coordinator does not double-count its predecessor's writes.
	for _, it := range record {
		if out := a.Offer(it); len(out) != 0 {
			t.Fatalf("re-offering a restored record appended %+v, want nothing", out)
		}
	}
	if a.Values() != 3 || a.Consumed() != 1 {
		t.Fatalf("counts changed on re-offer: values=%d consumed=%d", a.Values(), a.Consumed())
	}
}

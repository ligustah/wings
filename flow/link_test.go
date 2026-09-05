package flow_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ligustah/wings/flow"
)

// consumer receives everything on a shared channel and returns the sum. The
// handle arrives in its input, the way a work function on a worker would get
// it.
type feed struct {
	Values *flow.Channel[int] `json:"values"`
}

var consumer = flow.Define("test.consumer", func(ctx flow.Context, in feed) (int, error) {
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
})

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
var producer = flow.Define("test.producer", func(ctx flow.Context, in feed) (int, error) {
	for i := 1; i <= 3; i++ {
		if err := in.Values.Send(ctx, i*10); err != nil {
			return 0, err
		}
	}
	return 3, in.Values.Close(ctx)
})

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

package flow_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// lossyHost is a channel host that never hears one announcement: the value
// a sender's first attempt announced just before the attempt ended, as a
// worker that died — or was moved on — between the record of a send and
// the copy of it reaching home.
type lossyHost struct {
	flow.ChannelHost
	mu   sync.Mutex
	lost bool
}

func (h *lossyHost) Link(ctx context.Context, run, id string) (flow.ChannelLink, error) {
	l, err := h.ChannelHost.Link(ctx, run, id)
	if err != nil {
		return nil, err
	}
	return &lossyLink{ChannelLink: l, host: h}, nil
}

type lossyLink struct {
	flow.ChannelLink
	host *lossyHost
}

func (l *lossyLink) Send(ctx context.Context, it flow.ChannelItem) error {
	if !it.Want && it.To == "" && !it.Closed && it.Seq == 1 {
		l.host.mu.Lock()
		first := !l.host.lost
		l.host.lost = true
		l.host.mu.Unlock()
		if first {
			return nil
		}
	}
	return l.ChannelLink.Send(ctx, it)
}

// lostAttempt is a placer that runs a function thread from its own history,
// as a worker does, but cuts the first attempt short once the sender says
// it has sent, and then resumes it — a worker's retry after the loss.
type lostAttempt struct {
	store flow.Store
	host  flow.ChannelHost
	sent  chan struct{}
}

func (e *lostAttempt) opts() []flow.RunOption {
	return []flow.RunOption{flow.WithStore(e.store), flow.WithPlacer(e), flow.WithChannelHost(e.host), flow.Once()}
}

func (e *lostAttempt) Place(ctx context.Context, th flow.Thread, body func(flow.Context) ([]byte, error)) ([]byte, error) {
	if th.Fn == "" {
		return flow.InProcess().Place(ctx, th, body)
	}
	actx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-e.sent:
			cancel()
		case <-ctx.Done():
		}
	}()
	out, err := flow.RunThread(actx, th.Run, th.ID, th.Fn, th.Input, e.opts()...)
	cancel()
	if err == nil || ctx.Err() != nil {
		return out, err
	}
	return flow.RunThread(ctx, th.Run, th.ID, th.Fn, th.Input, e.opts()...)
}

var sendsThenDies = flow.Define("test.sendsThenDies", func(ctx flow.Context, in feed) (int, error) {
	for i := 1; i <= 3; i++ {
		if err := in.Values.Send(ctx, i*10); err != nil {
			return 0, err
		}
	}
	if dyingRuns.Add(1) == 1 {
		close(dyingSent)
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return 3, in.Values.Close(ctx)
})

var (
	dyingRuns atomic.Int32
	dyingSent = make(chan struct{})
)

// THE POINT: a sender's history says it sent what nobody received, when
// the attempt ended between the record and the copy reaching the host. The
// replay announces every send again — the host drops the copies it has —
// so the one it never had arrives, and the receiver is not left waiting
// for a value that is on record as sent.
func TestAReplayedSendReachesAHostThatMissedIt(t *testing.T) {
	dyingRuns.Store(0)
	dyingSent = make(chan struct{})
	host := &lossyHost{ChannelHost: flow.NewMemChannelHost()}
	e := &lostAttempt{store: flow.NewMemStore(), host: host, sent: dyingSent}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var seen []int
	err := flow.Run(ctx, "receiver", func(ctx flow.Context) error {
		ch := ctx.NewBufferedChannel[int](3)
		fut := ctx.Go(sendsThenDies, feed{Values: ch})
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
		_, err := fut.Await(ctx)
		return err
	}, e.opts()...)
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("the receiver waited forever for a value its sender's history says was sent; got %v", seen)
	}
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	// In some order: the value that was lost arrives after the ones that
	// were not, whatever the sender's order was.
	slices.Sort(seen)
	if !slices.Equal(seen, []int{10, 20, 30}) {
		t.Fatalf("received %v, want 10, 20 and 30", seen)
	}
	if !host.lost {
		t.Fatal("the host lost nothing; the test proves nothing")
	}
}

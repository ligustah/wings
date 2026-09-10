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

// countingHost records how many times each value is announced, so a test can
// show a replay re-sends nothing: the host keeps what a send committed (a
// faithful ChannelLink.Send, as the transactional wings host is — see pull.go),
// and a replayed send reads the event back rather than announcing again.
type countingHost struct {
	flow.ChannelHost
	mu    sync.Mutex
	sends map[uint64]int // value seq → times announced
}

func (h *countingHost) Link(ctx context.Context, run, id string) (flow.ChannelLink, error) {
	l, err := h.ChannelHost.Link(ctx, run, id)
	if err != nil {
		return nil, err
	}
	return &countingLink{ChannelLink: l, host: h}, nil
}

func (h *countingHost) announces(seq uint64) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.sends[seq]
}

type countingLink struct {
	flow.ChannelLink
	host *countingHost
}

func (l *countingLink) Send(ctx context.Context, it flow.ChannelItem) error {
	if !it.Consumed && !it.Closed {
		l.host.mu.Lock()
		if l.host.sends == nil {
			l.host.sends = map[uint64]int{}
		}
		l.host.sends[it.Seq]++
		l.host.mu.Unlock()
	}
	return l.ChannelLink.Send(ctx, it)
}

// lostAttempt is a placer that runs a function thread from its own history,
// as a worker does, but cuts the first attempt short once the sender says
// it has sent, and then resumes it — a worker's retry after a move.
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

var sendsThenDies = flow.Define(func(ctx flow.Context, in writeFeed) (int, error) {
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
}, flow.WithName("test.sendsThenDies"))

var (
	dyingRuns atomic.Int32
	dyingSent = make(chan struct{})
)

// THE POINT: a sender's attempt ends after recording its sends and is moved on.
// The value it committed is durably on record with its host, so the replay reads
// each send back rather than announcing it again — no value is sent twice — and
// the receiver still sees every one. A replay has no side effect on the channel.
func TestAReplayedSendIsNotAnnouncedAgain(t *testing.T) {
	dyingRuns.Store(0)
	dyingSent = make(chan struct{})
	host := &countingHost{ChannelHost: flow.NewMemChannelHost()}
	e := &lostAttempt{store: flow.NewMemStore(), host: host, sent: dyingSent}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var seen []int
	err := flow.Run(ctx, "receiver", func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int](flow.WithCapacity(3))
		fut := ctx.Go(sendsThenDies, writeFeed{Values: w})
		for {
			v, ok, err := r.Recv(ctx)
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
	slices.Sort(seen)
	if !slices.Equal(seen, []int{10, 20, 30}) {
		t.Fatalf("received %v, want 10, 20 and 30", seen)
	}
	// Each value was announced exactly once across both attempts: the replay read
	// its sends back from history and announced nothing. Sends are 0-numbered, so
	// the three values are seq 0, 1 and 2.
	for seq := uint64(0); seq <= 2; seq++ {
		if n := host.announces(seq); n != 1 {
			t.Fatalf("value #%d was announced %d times, want exactly once (a replay must not re-send)", seq, n)
		}
	}
}

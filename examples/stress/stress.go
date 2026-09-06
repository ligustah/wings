// Package stress is a set of workflows that lean on the thread model from
// every side at once: pipelines of threads joined by bounded channels, many
// receivers on one channel, failures of every kind, lineages several deep,
// effects recorded before a fork and replayed on the worker that reaches it,
// hundreds of threads, and the things that are refused. Each workflow prints
// what it saw and ends with "OK <name>", or returns an error.
//
//	go run github.com/ligustah/wings/cmd/wings build -pkg ./examples/stress -o stress
//
//	./stress -target inprocess -workflow pipeline -input '{"n":20}'
//	./stress -target local -workers 3 -workflow fanin -input '{"n":50}'
package stress

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"time"

	"github.com/ligustah/wings/flow"
)

// Params is every workflow's input; each reads what it needs.
type Params struct {
	// N is a count: values through a pipeline, per producer, threads.
	N int `json:"n"`
	// Depth is how deep the deep workflow nests.
	Depth int `json:"depth"`
	// Long makes the sleepy workflow sleep past what a worker keeps in
	// memory, so waits are unloaded and woken by the coordinator.
	Long bool `json:"long"`
}

// pid is where a thread is running, as an effect: different on every
// worker, the same on replay.
func pid(ctx flow.Context) (int, error) {
	return ctx.Effect(func() (int, error) { return os.Getpid(), nil })
}

func ok(name string) error {
	fmt.Printf("OK %s\n", name)
	return nil
}

// --- pipeline: three threads of run code joined by bounded channels ---

var Pipeline = flow.DefineWorkflow("pipeline", func(ctx flow.Context, in Params) error {
	n := cmp.Or(in.N, 20)
	raw := ctx.NewBufferedChannel[int](2)
	squared := ctx.NewBufferedChannel[int](2)

	source := ctx.Spawn(func(ctx flow.Context) (int, error) {
		for i := 1; i <= n; i++ {
			if err := raw.Send(ctx, i); err != nil {
				return 0, err
			}
		}
		if err := raw.Close(ctx); err != nil {
			return 0, err
		}
		return pid(ctx)
	})
	transform := ctx.Spawn(func(ctx flow.Context) (int, error) {
		for {
			v, more, err := raw.Recv(ctx)
			if err != nil {
				return 0, err
			}
			if !more {
				break
			}
			if err := squared.Send(ctx, v*v); err != nil {
				return 0, err
			}
		}
		if err := squared.Close(ctx); err != nil {
			return 0, err
		}
		return pid(ctx)
	})
	sink := ctx.Spawn(func(ctx flow.Context) (int, error) {
		sum := 0
		for {
			v, more, err := squared.Recv(ctx)
			if err != nil {
				return 0, err
			}
			if !more {
				return sum, nil
			}
			sum += v
		}
	})

	sum, err := sink.Await(ctx)
	if err != nil {
		return err
	}
	want := n * (n + 1) * (2*n + 1) / 6
	if sum != want {
		return fmt.Errorf("pipeline: the sink summed %d, want %d", sum, want)
	}
	a, err := source.Await(ctx)
	if err != nil {
		return err
	}
	b, err := transform.Await(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("pipeline: %d values, sum of squares %d; source on pid %d, transform on pid %d, workflow on pid %d\n",
		n, sum, a, b, os.Getpid())
	return ok("pipeline")
})

// --- fanin: several producers, several consumers, one channel ---

type Feed struct {
	Values *flow.Channel[int] `json:"values"`
}

type Tally struct {
	Count int `json:"count"`
	Sum   int `json:"sum"`
	Pid   int `json:"pid"`
}

// Consume takes from the channel until it closes.
var Consume = flow.Define(func(ctx flow.Context, in Feed) (Tally, error) {
	var t Tally
	for {
		v, more, err := in.Values.Recv(ctx)
		if err != nil {
			return t, err
		}
		if !more {
			break
		}
		t.Count++
		t.Sum += v
	}
	var err error
	t.Pid, err = pid(ctx)
	return t, err
}, flow.WithName("consume"))

var Fanin = flow.DefineWorkflow("fanin", func(ctx flow.Context, in Params) error {
	n := cmp.Or(in.N, 50)
	const producers, consumers = 3, 4
	values := ctx.NewChannel[int]()

	var prods []*flow.Future[int]
	for p := range producers {
		prods = append(prods, ctx.Spawn(func(ctx flow.Context) (int, error) {
			for i := range n {
				if err := values.Send(ctx, p*n+i+1); err != nil {
					return 0, err
				}
			}
			return pid(ctx)
		}))
	}
	var cons []*flow.Future[Tally]
	for range consumers {
		cons = append(cons, ctx.Go(Consume, Feed{Values: values}))
	}
	for _, p := range prods {
		if _, err := p.Await(ctx); err != nil {
			return err
		}
	}
	if err := values.Close(ctx); err != nil {
		return err
	}
	total := Tally{}
	pids := map[int]int{}
	for _, c := range cons {
		t, err := c.Await(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("fanin: a consumer on pid %d took %d values\n", t.Pid, t.Count)
		total.Count += t.Count
		total.Sum += t.Sum
		pids[t.Pid]++
	}
	all := producers * n
	if total.Count != all || total.Sum != all*(all+1)/2 {
		return fmt.Errorf("fanin: consumers took %d values summing %d, want %d summing %d", total.Count, total.Sum, all, all*(all+1)/2)
	}
	return ok("fanin")
})

// --- flaky: every way a thread can go wrong, and the workflow going on ---

var ErrExpected = errors.New("expected failure")

var Fails = flow.Define(func(ctx flow.Context, in int) (int, error) {
	return 0, fmt.Errorf("%w: input %d", ErrExpected, in)
}, flow.WithName("fails"))

var Panics = flow.Define(func(ctx flow.Context, in int) (int, error) {
	panic(fmt.Sprintf("on purpose, with %d", in))
}, flow.WithName("panics"))

var Slow = flow.Define(func(ctx flow.Context, d time.Duration) (int, error) {
	select {
	case <-time.After(d):
		return int(d), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}, flow.WithName("slow"),

	flow.WithTimeout(500*time.Millisecond))

var Square = flow.Define(func(ctx flow.Context, in int) (int, error) {
	if in < 0 {
		return 0, fmt.Errorf("%w: cannot square %d", ErrExpected, in)
	}
	return in * in, nil
}, flow.WithName("square"))

var Flaky = flow.DefineWorkflow("flaky", func(ctx flow.Context, in Params) error {
	// A function that fails: the error comes back through Await, and the
	// workflow goes on.
	if _, err := ctx.Go(Fails, 1).Await(ctx); err == nil || !strings.Contains(err.Error(), "expected failure") {
		return fmt.Errorf("flaky: Fails returned %v, want the expected failure", err)
	}
	// One that panics: the worker survives, the error names the panic.
	if _, err := ctx.Go(Panics, 2).Await(ctx); err == nil || !strings.Contains(err.Error(), "on purpose") {
		return fmt.Errorf("flaky: Panics returned %v, want a panic report", err)
	}
	// One that exceeds its own timeout.
	if _, err := ctx.Go(Slow, 2*time.Second).Await(ctx); err == nil || !strings.Contains(err.Error(), "timeout") {
		return fmt.Errorf("flaky: Slow returned %v, want a timeout", err)
	}
	// And one that does not.
	if v, err := ctx.Go(Slow, 10*time.Millisecond).Await(ctx); err != nil || v != int(10*time.Millisecond) {
		return fmt.Errorf("flaky: a fast Slow returned %d, %v", v, err)
	}
	// A thread of run code that fails, on a worker.
	if _, err := ctx.Spawn(func(ctx flow.Context) (int, error) {
		p, err := pid(ctx)
		if err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("%w: from a closure on pid %d", ErrExpected, p)
	}).Await(ctx); err == nil || !strings.Contains(err.Error(), "from a closure") {
		return fmt.Errorf("flaky: the failing closure returned %v", err)
	}
	// A Map with one bad element fails as a whole.
	if _, err := ctx.Map(Square, []int{1, 2, -3, 4}); err == nil || !strings.Contains(err.Error(), "cannot square -3") {
		return fmt.Errorf("flaky: Map returned %v, want the bad element's error", err)
	}
	// A closure whose own fork fails, handled inside the closure.
	got, err := ctx.Spawn(func(ctx flow.Context) (string, error) {
		_, err := ctx.Go(Fails, 3).Await(ctx)
		if err == nil {
			return "", errors.New("the nested Fails did not fail")
		}
		out, err := ctx.Map(Square, []int{5, 6})
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("recovered; squares %v", out), nil
	}).Await(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("flaky: %s\n", got)
	// And after all of that, ordinary work still works.
	out, err := ctx.Map(Square, []int{1, 2, 3})
	if err != nil {
		return err
	}
	if out[0] != 1 || out[1] != 4 || out[2] != 9 {
		return fmt.Errorf("flaky: Map gave %v", out)
	}
	return ok("flaky")
})

// --- deep: lineages several long, channels at every level ---

type Level struct {
	Depth int                `json:"depth"`
	Pids  []int              `json:"pids"`
	Back  *flow.Channel[int] `json:"back"`
}

// descend is a thread of run code that forks another like it, until depth
// runs out. Each level makes a channel its child reports on, and the last
// makes one it hands back UP through its result.
func descend(ctx flow.Context, depth, max int) (Level, error) {
	p, err := pid(ctx)
	if err != nil {
		return Level{}, err
	}
	if depth == max {
		back := ctx.NewBufferedChannel[int](1)
		if err := back.Send(ctx, depth*100); err != nil {
			return Level{}, err
		}
		out, err := ctx.Map(Square, []int{depth, depth + 1})
		if err != nil {
			return Level{}, err
		}
		if out[0] != depth*depth {
			return Level{}, fmt.Errorf("deep: at depth %d Map gave %v", depth, out)
		}
		return Level{Depth: depth, Pids: []int{p}, Back: back}, nil
	}
	report := ctx.NewChannel[int]()
	child := ctx.Spawn(func(ctx flow.Context) (Level, error) {
		if err := report.Send(ctx, depth+1); err != nil {
			return Level{}, err
		}
		return descend(ctx, depth+1, max)
	})
	v, _, err := report.Recv(ctx)
	if err != nil {
		return Level{}, err
	}
	if v != depth+1 {
		return Level{}, fmt.Errorf("deep: level %d heard %d from its child", depth, v)
	}
	l, err := child.Await(ctx)
	if err != nil {
		return Level{}, err
	}
	l.Pids = append([]int{p}, l.Pids...)
	return l, nil
}

var Deep = flow.DefineWorkflow("deep", func(ctx flow.Context, in Params) error {
	depth := cmp.Or(in.Depth, 4)
	l, err := descend(ctx, 0, depth)
	if err != nil {
		return err
	}
	if l.Depth != depth || len(l.Pids) != depth+1 {
		return fmt.Errorf("deep: got %+v", l)
	}
	v, more, err := l.Back.Recv(ctx)
	if err != nil {
		return err
	}
	if !more || v != depth*100 {
		return fmt.Errorf("deep: the channel handed back up gave %d, %v", v, more)
	}
	fmt.Printf("deep: %d levels on pids %v; the deepest's channel gave %d\n", depth+1, l.Pids, v)
	return ok("deep")
})

// --- effects: what the workflow recorded before a fork, seen by the fork ---

var Effects = flow.DefineWorkflow("effects", func(ctx flow.Context, in Params) error {
	t0, err := ctx.Now()
	if err != nil {
		return err
	}
	host, err := ctx.Effect(os.Hostname)
	if err != nil {
		return err
	}
	var rolls []int
	for range 3 {
		r, err := ctx.Effect(func() (int, error) { return rand.IntN(1000), nil })
		if err != nil {
			return err
		}
		rolls = append(rolls, r)
	}
	if err := ctx.Sleep(50 * time.Millisecond); err != nil {
		return err
	}
	// A function called directly, on this thread, before the fork: replayed
	// from the history by the worker that reaches the closure.
	sq, err := Square(ctx, 12)
	if err != nil {
		return err
	}
	seen := fmt.Sprintf("%s %v %d %s", host, rolls, sq, t0.Format(time.RFC3339Nano))

	type view struct {
		Seen string
		Pid  int
		Host string
	}
	v, err := ctx.Spawn(func(ctx flow.Context) (view, error) {
		// The closure captured what the workflow computed; a worker
		// replaying the workflow to this fork must arrive at the same.
		p, err := pid(ctx)
		if err != nil {
			return view{}, err
		}
		h, err := ctx.Effect(os.Hostname)
		if err != nil {
			return view{}, err
		}
		t1, err := ctx.Now()
		if err != nil {
			return view{}, err
		}
		if !t1.After(t0) {
			return view{}, fmt.Errorf("effects: the closure's clock %v is not after the workflow's %v", t1, t0)
		}
		return view{Seen: fmt.Sprintf("%s %v %d %s", host, rolls, sq, t0.Format(time.RFC3339Nano)), Pid: p, Host: h}, nil
	}).Await(ctx)
	if err != nil {
		return err
	}
	if v.Seen != seen {
		return fmt.Errorf("effects: the closure saw %q, the workflow %q", v.Seen, seen)
	}
	fmt.Printf("effects: the closure on pid %d (%s) saw what the workflow recorded: %s\n", v.Pid, v.Host, seen)
	return ok("effects")
})

// --- wide: many threads of run code at once, on one channel ---

var Wide = flow.DefineWorkflow("wide", func(ctx flow.Context, in Params) error {
	n := cmp.Or(in.N, 50)
	const each = 20
	values := ctx.NewBufferedChannel[int](8)
	var threads []*flow.Future[int]
	for i := range n {
		threads = append(threads, ctx.Spawn(func(ctx flow.Context) (int, error) {
			for j := range each {
				if err := values.Send(ctx, i*each+j); err != nil {
					return 0, err
				}
			}
			return pid(ctx)
		}))
	}
	start := time.Now()
	seen := map[int]bool{}
	for range n * each {
		v, more, err := values.Recv(ctx)
		if err != nil {
			return err
		}
		if !more {
			return fmt.Errorf("wide: the channel closed after %d values", len(seen))
		}
		if seen[v] {
			return fmt.Errorf("wide: value %d received twice", v)
		}
		seen[v] = true
	}
	pids := map[int]int{}
	for _, t := range threads {
		p, err := t.Await(ctx)
		if err != nil {
			return err
		}
		pids[p]++
	}
	fmt.Printf("wide: %d threads sent %d values, received once each in %s; threads per pid: %v\n",
		n, n*each, time.Since(start).Round(time.Millisecond), pids)
	return ok("wide")
})

// --- sleepy: threads that sleep, and threads that wait on them ---

var Sleepy = flow.DefineWorkflow("sleepy", func(ctx flow.Context, in Params) error {
	naps := []time.Duration{300 * time.Millisecond, 600 * time.Millisecond, 900 * time.Millisecond}
	if in.Long {
		// Past flow.ShortSleep: a worker hands the thread back and the
		// coordinator wakes it. And a thread waiting on that one waits past
		// what a worker keeps in memory, so it is unloaded and woken too.
		naps = append(naps, 65*time.Second)
	}
	start := time.Now()
	var futs []*flow.Future[time.Duration]
	for _, d := range naps {
		futs = append(futs, ctx.Spawn(func(ctx flow.Context) (time.Duration, error) {
			// A thread that waits on a thread that sleeps.
			inner := ctx.Spawn(func(ctx flow.Context) (int, error) {
				if err := ctx.Sleep(d); err != nil {
					return 0, err
				}
				return pid(ctx)
			})
			t0, err := ctx.Now()
			if err != nil {
				return 0, err
			}
			if _, err := inner.Await(ctx); err != nil {
				return 0, err
			}
			t1, err := ctx.Now()
			if err != nil {
				return 0, err
			}
			return t1.Sub(t0), nil
		}))
	}
	for i, f := range futs {
		slept, err := f.Await(ctx)
		if err != nil {
			return err
		}
		if slept < naps[i] {
			return fmt.Errorf("sleepy: a nap of %s took %s", naps[i], slept)
		}
		fmt.Printf("sleepy: a nap of %s took %s\n", naps[i], slept.Round(time.Millisecond))
	}
	fmt.Printf("sleepy: all in %s\n", time.Since(start).Round(time.Millisecond))
	return ok("sleepy")
})

// --- refused: what a thread of run code cannot do ---

var Refused = flow.DefineWorkflow("refused", func(ctx flow.Context, in Params) error {
	// A Spawn inside a work function after a Step: steps are kept with the
	// call, not in the history, so the closure cannot be reached by replay.
	// It is refused with an error that says so, rather than run wrong.
	_, err := ctx.Go(StepsThenSpawns, 1).Await(ctx)
	if err == nil {
		return errors.New("refused: a Spawn after a Step was not refused")
	}
	fmt.Printf("refused: %v\n", err)
	return ok("refused")
})

var StepsThenSpawns = flow.Define(func(ctx flow.Context, in int) (int, error) {
	v, err := ctx.Step("first", func(ctx flow.Context) (int, error) { return in + 1, nil })
	if err != nil {
		return 0, err
	}
	return ctx.Spawn(func(ctx flow.Context) (int, error) { return v * 2, nil }).Await(ctx)
}, flow.WithName("stepsThenSpawns"))

// --- panicky: a closure that panics on a worker, and one that returns a
// permanent error, and the workflow going on ---

var Panicky = flow.DefineWorkflow("panicky", func(ctx flow.Context, in Params) error {
	_, err := ctx.Spawn(func(ctx flow.Context) (int, error) {
		p, _ := pid(ctx)
		panic(fmt.Sprintf("a closure panicking on pid %d", p))
	}).Await(ctx)
	if err == nil || !strings.Contains(err.Error(), "panicking on pid") {
		return fmt.Errorf("panicky: the panicking closure returned %v", err)
	}
	fmt.Printf("panicky: %v\n", err)
	_, err = ctx.Spawn(func(ctx flow.Context) (int, error) {
		return 0, flow.Permanent(errors.New("permanently wrong"))
	}).Await(ctx)
	if err == nil || !strings.Contains(err.Error(), "permanently wrong") {
		return fmt.Errorf("panicky: the permanent failure returned %v", err)
	}
	// A closure that fails on its first attempt only is retried where it
	// is, by the thread's own retry, and succeeds.
	v, err := ctx.Spawn(func(ctx flow.Context) (int, error) {
		n, err := ctx.Effect(func() (int, error) { return 0, nil })
		if err != nil {
			return 0, err
		}
		return n + 41, nil
	}).Await(ctx)
	if err != nil || v != 41 {
		return fmt.Errorf("panicky: after the failures a closure returned %d, %v", v, err)
	}
	return ok("panicky")
})

// --- bulky: values of some size through a channel that crosses machines ---

var Bulky = flow.DefineWorkflow("bulky", func(ctx flow.Context, in Params) error {
	n := cmp.Or(in.N, 12)
	const size = 256 << 10
	values := ctx.NewBufferedChannel[[]byte](2)
	producer := ctx.Spawn(func(ctx flow.Context) (int, error) {
		for i := range n {
			b := make([]byte, size)
			for j := range b {
				b[j] = byte(i + j)
			}
			if err := values.Send(ctx, b); err != nil {
				return 0, err
			}
		}
		return n, values.Close(ctx)
	})
	total, count := 0, 0
	for {
		b, more, err := values.Recv(ctx)
		if err != nil {
			return err
		}
		if !more {
			break
		}
		if len(b) != size || b[1] != byte(count+1) {
			return fmt.Errorf("bulky: value %d is %d bytes and starts %v", count, len(b), b[:2])
		}
		total += len(b)
		count++
	}
	if _, err := producer.Await(ctx); err != nil {
		return err
	}
	if count != n {
		return fmt.Errorf("bulky: received %d values, want %d", count, n)
	}
	fmt.Printf("bulky: %d values, %d MB\n", count, total>>20)
	return ok("bulky")
})

// --- cancelled: a thread of run code cut short by its parent's context,
// where it runs on another machine, and the workflow going on ---

var Cancelled = flow.DefineWorkflow("cancelled", func(ctx flow.Context, in Params) error {
	// A timeout on the wait, not on the thread: the thread is sleeping on
	// some worker and the parent stops waiting for it.
	tctx, cancel := ctx.WithTimeout(500 * time.Millisecond)
	slow := ctx.Spawn(func(ctx flow.Context) (int, error) {
		if err := ctx.Sleep(5 * time.Second); err != nil {
			return 0, err
		}
		return pid(ctx)
	})
	_, err := slow.Await(tctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("cancelled: the wait ended with %v, want a deadline", err)
	}
	fmt.Printf("cancelled: the wait ended with %v\n", err)
	// Cancelled outright, before it could finish.
	cctx, cancel := ctx.WithCancel()
	stopped := ctx.Spawn(func(ctx flow.Context) (int, error) {
		if err := ctx.Sleep(5 * time.Second); err != nil {
			return 0, err
		}
		return pid(ctx)
	})
	go func() { time.Sleep(200 * time.Millisecond); cancel() }()
	_, err = stopped.Await(cctx)
	if !errors.Is(err, context.Canceled) {
		return fmt.Errorf("cancelled: the cancelled wait ended with %v", err)
	}
	// And after both, a thread that finishes, awaited normally.
	v, err := ctx.Spawn(func(ctx flow.Context) (int, error) { return 7, nil }).Await(ctx)
	if err != nil || v != 7 {
		return fmt.Errorf("cancelled: the thread after returned %d, %v", v, err)
	}
	return ok("cancelled")
})

// --- stepped: a thread of run code that uses the progress API — steps,
// heartbeats, a checkpoint — where it runs ---

var Stepped = flow.DefineWorkflow("stepped", func(ctx flow.Context, in Params) error {
	v, err := ctx.Spawn(func(ctx flow.Context) (int, error) {
		a, err := ctx.Step("first", func(ctx flow.Context) (int, error) { return 20, nil })
		if err != nil {
			return 0, err
		}
		for i := range 5 {
			if err := ctx.Heartbeat(i); err != nil {
				return 0, err
			}
		}
		at, found, err := ctx.Checkpoint[int]()
		if err != nil {
			return 0, err
		}
		fmt.Printf("stepped: checkpoint %d found=%v\n", at, found)
		b, err := ctx.Step("second", func(ctx flow.Context) (int, error) { return a + 22, nil })
		if err != nil {
			return 0, err
		}
		return b, nil
	}).Await(ctx)
	if err != nil {
		return fmt.Errorf("stepped: %w", err)
	}
	if v != 42 {
		return fmt.Errorf("stepped: got %d, want 42", v)
	}
	// The same, inside a work function, which is where steps were made for.
	w, err := ctx.Go(Stepper, 1).Await(ctx)
	if err != nil || w != 42 {
		return fmt.Errorf("stepped: the stepping function returned %d, %v", w, err)
	}
	return ok("stepped")
})

var Stepper = flow.Define(func(ctx flow.Context, in int) (int, error) {
	a, err := ctx.Step("first", func(ctx flow.Context) (int, error) { return in + 19, nil })
	if err != nil {
		return 0, err
	}
	return ctx.Step("second", func(ctx flow.Context) (int, error) { return a + 22, nil })
}, flow.WithName("stepper"))

// --- orphaned: a receiver whose only sender fails without closing, and a
// sender whose channel was closed under it ---

var Orphaned = flow.DefineWorkflow("orphaned", func(ctx flow.Context, in Params) error {
	values := ctx.NewChannel[int]()
	producer := ctx.Spawn(func(ctx flow.Context) (int, error) {
		if err := values.Send(ctx, 1); err != nil {
			return 0, err
		}
		return 0, flow.Permanent(errors.New("the producer gives up"))
	})
	v, more, err := values.Recv(ctx)
	if err != nil || !more || v != 1 {
		return fmt.Errorf("orphaned: the first receive got %d, %v, %v", v, more, err)
	}
	// Nobody will ever send or close: a receive would wait forever, so the
	// wait is bounded, and reports the bound.
	tctx, cancel := ctx.WithTimeout(500 * time.Millisecond)
	_, _, err = values.Recv(tctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("orphaned: the orphaned receive ended with %v", err)
	}
	fmt.Printf("orphaned: the orphaned receive ended with %v\n", err)
	_, err = producer.Await(ctx)
	if err == nil || !strings.Contains(err.Error(), "gives up") {
		return fmt.Errorf("orphaned: the producer returned %v", err)
	}
	// Closed under a sender: the sender's send fails rather than hangs.
	closed := ctx.NewChannel[int]()
	if err := closed.Close(ctx); err != nil {
		return err
	}
	sender := ctx.Spawn(func(ctx flow.Context) (int, error) {
		err := closed.Send(ctx, 1)
		fmt.Printf("orphaned: sending on a closed channel: %v\n", err)
		if err == nil {
			return 0, errors.New("a send on a closed channel succeeded")
		}
		return 1, nil
	})
	if _, err := sender.Await(ctx); err != nil {
		return fmt.Errorf("orphaned: %w", err)
	}
	return ok("orphaned")
})

// --- crossed: a channel made by the workflow, sent into a work function on
// one worker whose spawned thread sends on it, and received on by a thread
// of the workflow's own on another ---

type Outlet struct {
	N   int
	Out *flow.Channel[int]
}

var Pumps = flow.Define(func(ctx flow.Context, in Outlet) (int, error) {
	p, _ := pid(ctx)
	inner := ctx.Spawn(func(ctx flow.Context) (int, error) {
		q, _ := pid(ctx)
		for i := range in.N {
			if err := in.Out.Send(ctx, i); err != nil {
				return 0, err
			}
		}
		return q, in.Out.Close(ctx)
	})
	q, err := inner.Await(ctx)
	if err != nil {
		return 0, err
	}
	fmt.Printf("crossed: pumped from pid %d, its thread on pid %d\n", p, q)
	return in.N, nil
}, flow.WithName("pumps"))

var Crossed = flow.DefineWorkflow("crossed", func(ctx flow.Context, in Params) error {
	n := cmp.Or(in.N, 25)
	values := ctx.NewBufferedChannel[int](3)
	pump := ctx.Go(Pumps, Outlet{N: n, Out: values})
	drain := ctx.Spawn(func(ctx flow.Context) (int, error) {
		p, _ := pid(ctx)
		sum := 0
		for {
			v, more, err := values.Recv(ctx)
			if err != nil {
				return 0, err
			}
			if !more {
				fmt.Printf("crossed: drained on pid %d\n", p)
				return sum, nil
			}
			sum += v
		}
	})
	sum, err := drain.Await(ctx)
	if err != nil {
		return fmt.Errorf("crossed: drain: %w", err)
	}
	if _, err := pump.Await(ctx); err != nil {
		return fmt.Errorf("crossed: pump: %w", err)
	}
	if want := n * (n - 1) / 2; sum != want {
		return fmt.Errorf("crossed: sum %d, want %d", sum, want)
	}
	return ok("crossed")
})

// --- mapped: a wide fan-out with Context.Map, results kept in order ---
//
// Map is a fork per input joined in order; a worker kill mid-Map redispatches
// the children that were on it, and this checks the results still come back
// complete, in order, and each computed exactly once — a value out of place or
// missing is a redispatch that lost or reordered a result.
var Mapped = flow.DefineWorkflow("mapped", func(ctx flow.Context, in Params) error {
	n := cmp.Or(in.N, 50)
	ins := make([]int, n)
	for i := range ins {
		ins[i] = i
	}
	out, err := ctx.Map(Square, ins)
	if err != nil {
		return fmt.Errorf("mapped: %w", err)
	}
	if len(out) != n {
		return fmt.Errorf("mapped: got %d results, want %d", len(out), n)
	}
	for i := range ins {
		if out[i] != i*i {
			return fmt.Errorf("mapped: out[%d] = %d, want %d", i, out[i], i*i)
		}
	}
	fmt.Printf("mapped: %d squares came back in order\n", n)
	return ok("mapped")
})

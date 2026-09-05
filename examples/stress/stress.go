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
var Consume = flow.Define("consume", func(ctx flow.Context, in Feed) (Tally, error) {
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
})

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

var Fails = flow.Define("fails", func(ctx flow.Context, in int) (int, error) {
	return 0, fmt.Errorf("%w: input %d", ErrExpected, in)
})

var Panics = flow.Define("panics", func(ctx flow.Context, in int) (int, error) {
	panic(fmt.Sprintf("on purpose, with %d", in))
})

var Slow = flow.Define("slow", func(ctx flow.Context, d time.Duration) (int, error) {
	select {
	case <-time.After(d):
		return int(d), nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}, flow.WithTimeout(500*time.Millisecond))

var Square = flow.Define("square", func(ctx flow.Context, in int) (int, error) {
	if in < 0 {
		return 0, fmt.Errorf("%w: cannot square %d", ErrExpected, in)
	}
	return in * in, nil
})

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

var StepsThenSpawns = flow.Define("stepsThenSpawns", func(ctx flow.Context, in int) (int, error) {
	v, err := ctx.Step("first", func(ctx flow.Context) (int, error) { return in + 1, nil })
	if err != nil {
		return 0, err
	}
	return ctx.Spawn(func(ctx flow.Context) (int, error) { return v * 2, nil }).Await(ctx)
})

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

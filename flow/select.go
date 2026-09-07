package flow

import (
	"errors"
	"reflect"
	"time"

	"github.com/ligustah/wings/flow/protos"
)

// Selector waits for the first of several cases to be ready and runs it. Build
// one with [Context.Select], add cases with [Selector.Await], [Selector.Recv]
// and [Selector.After], then call [Selector.Do]. Which case won is recorded, so
// a replay takes the same one rather than whatever is ready first.
type Selector struct {
	cases  []selCase
	timers []*time.Timer
	err    error
}

// selCase is one case's readiness, its wait channel, and how to run it. ready
// and waitCh must not consume anything: the winner is chosen, recorded, then
// fired.
type selCase struct {
	ready  func() bool
	waitCh func() reflect.Value
	fire   func(ctx Context) error
}

// Select starts a select over several futures, channels and a timeout. Add
// cases and call [Selector.Do].
func (c Context) Select() *Selector { return &Selector{} }

// Await adds a case that wins when fut has finished, then hands its result to
// handle. The other cases' futures keep running; await them elsewhere if you
// need them.
func (s *Selector) Await[T any](fut *Future[T], handle func(T, error) error) *Selector {
	s.cases = append(s.cases, selCase{
		ready:  func() bool { return closed(fut.done) },
		waitCh: func() reflect.Value { return reflect.ValueOf(fut.done) },
		fire: func(ctx Context) error {
			out, err := fut.Await(ctx)
			return handle(out, err)
		},
	})
	return s
}

// Recv adds a case that wins when a value can be received from ch — or ch is
// closed — then hands the outcome to handle, as [Channel.Recv] reports it. For
// the run's own channels.
func (s *Selector) Recv[T any](ch *Channel[T], handle func(v T, ok bool, err error) error) *Selector {
	s.cases = append(s.cases, selCase{
		ready: func() bool {
			cs := recvState(ch)
			return cs != nil && cs.readable()
		},
		waitCh: func() reflect.Value {
			cs := recvState(ch)
			if cs == nil {
				return reflect.Value{}
			}
			return reflect.ValueOf(cs.changedChan())
		},
		fire: func(ctx Context) error {
			v, ok, err := ch.Recv(ctx)
			return handle(v, ok, err)
		},
	})
	return s
}

// After adds a case that wins once d has passed with no other case ready, then
// calls handle — a timeout as a case.
func (s *Selector) After(d time.Duration, handle func() error) *Selector {
	timer := time.NewTimer(d)
	s.timers = append(s.timers, timer)
	deadline := time.Now().Add(d)
	s.cases = append(s.cases, selCase{
		ready:  func() bool { return !time.Now().Before(deadline) },
		waitCh: func() reflect.Value { return reflect.ValueOf(timer.C) },
		fire:   func(ctx Context) error { return handle() },
	})
	return s
}

// Do waits for the first ready case and runs it, returning what its handler
// returned. Ties go to the case added first. A context the body cut short ends
// the wait and is recorded, like any other wait.
func (s *Selector) Do(ctx Context) error {
	defer func() {
		for _, t := range s.timers {
			t.Stop()
		}
	}()
	if s.err != nil {
		return s.err
	}
	t := threadFrom(ctx)
	if t == nil {
		return errors.New("flow: Select called outside a Run")
	}
	if len(s.cases) == 0 {
		return errors.New("flow: Select has no cases")
	}

	if err, ok := t.interrupted("select"); ok {
		return err
	}
	ev, err := t.expect[*protos.SelectEvent]()
	if err != nil {
		return err
	}
	if ev != nil {
		chosen := int(ev.GetChosen())
		if chosen < 0 || chosen >= len(s.cases) {
			return continuityf("thread %q recorded select case %d, but the run now offers %d cases",
				t.id, chosen, len(s.cases))
		}
		return s.cases[chosen].fire(ctx)
	}

	chosen, err := s.waitReady(ctx, t)
	if err != nil {
		return err
	}
	t.record(&protos.SelectEvent{Chosen: uint64(chosen)})
	if err := t.err(); err != nil {
		return err
	}
	return s.cases[chosen].fire(ctx)
}

// waitReady blocks until a case is ready and returns its index, parking the
// thread while it waits.
func (s *Selector) waitReady(ctx Context, t *threadState) (int, error) {
	parked := false
	resume := noResume
	for {
		for i, c := range s.cases {
			if c.ready() {
				return i, resume(ctx)
			}
		}

		cases := make([]reflect.SelectCase, 0, len(s.cases)+1)
		for _, c := range s.cases {
			if ch := c.waitCh(); ch.IsValid() {
				cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: ch})
			}
		}
		done := len(cases)
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})

		if !parked {
			parked, resume = true, t.parkOn(ctx, WaitSelect, "", 0)
		}
		if chosen, _, _ := reflect.Select(cases); chosen == done {
			_ = resume(t.base())
			return 0, t.interrupt("select", ctx.Err())
		}
	}
}

// AwaitAny waits for the first of futs to finish and returns its index and
// result — a race with no case handlers. It does not await the losers.
func AwaitAny[T any](ctx Context, futs ...*Future[T]) (int, T, error) {
	var idx int
	var out T
	var ferr error
	sel := ctx.Select()
	for i := range futs {
		sel.Await(futs[i], func(v T, err error) error {
			idx, out, ferr = i, v, err
			return nil
		})
	}
	if err := sel.Do(ctx); err != nil {
		return 0, out, err
	}
	return idx, out, ferr
}

// closed reports whether a done-style channel has been closed, without
// consuming anything.
func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// recvState resolves a channel's runtime for a select recv case, or nil when it
// is not usable here yet.
func recvState[T any](ch *Channel[T]) *chanState {
	ch.mu.Lock()
	run, name := ch.run, ch.name
	ch.mu.Unlock()
	if run == nil {
		return nil
	}
	return run.channel(name)
}

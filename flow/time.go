package flow

import (
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ligustah/wings/flow/protos"
)

// ShortSleep is the longest a thread will simply wait in place. Beyond it, a
// sleep suspends the thread instead.
//
// The threshold exists because the two are not the same trade. Waiting in place
// keeps the thread's state in memory and its goroutine alive, which is cheap
// for a second and absurd for a day; suspending writes the wake-up time down
// and ends the attempt, which costs a replay when it resumes. A minute is
// where the replay stops being the expensive half. A variable so a process
// that would rather be told about every sleep — a worker whose coordinator
// schedules them — can lower it.
var ShortSleep = time.Minute

// Now returns the current time, recorded so that a replay sees the same instant.
//
// Use it instead of time.Now inside a run. A run that reads the real
// clock decides something different on every attempt, and the first retry then
// contradicts its own history.
func (c Context) Now() (time.Time, error) {
	t := threadFrom(c)
	if t == nil {
		return time.Time{}, errors.New("flow: Now called outside a Run")
	}

	ev, err := t.expect[*protos.GetTimeEvent]()
	if err != nil {
		return time.Time{}, err
	}
	if ev != nil {
		return ev.GetTime().AsTime(), nil
	}

	now := time.Now().Truncate(time.Microsecond)
	t.record(&protos.GetTimeEvent{Time: timestamppb.New(now)})
	return now, t.err()
}

// Sleep pauses the run for d.
//
// A short sleep waits in place. A long one suspends the run: the attempt ends,
// its history stays on disk, and the next attempt replays to this point and
// carries on. Either way the sleep is recorded, so it is not served twice — a
// run that slept an hour and then failed does not sleep another hour on
// its retry.
func (c Context) Sleep(d time.Duration) error {
	ctx := c
	t := threadFrom(ctx)
	if t == nil {
		return errors.New("flow: Sleep called outside a Run")
	}

	// The recorded event's timestamp is when the sleep STARTED, which is what
	// makes the deadline stable across attempts.
	start := time.Now()

	ev := t.peek()
	recorded, err := t.expect[*protos.SleepEvent]()
	if err != nil {
		return err
	}
	if recorded != nil {
		start = ev.GetTimestamp().AsTime()
		d = recorded.GetDuration().AsDuration()
	} else {
		t.record(&protos.SleepEvent{Duration: durationpb.New(d)})
		if err := t.err(); err != nil {
			return err
		}
	}

	// Cut short last time, by the body's own timeout or cancel: cut short
	// again, at once, whatever the clock says now.
	if err, ok := t.interrupted("sleep"); ok {
		return err
	}

	until := start.Add(d)
	remaining := time.Until(until)
	switch {
	case remaining <= 0:
		return nil
	case remaining < ShortSleep:
		resume := t.park(ctx, WaitSleep)
		if err := wait(ctx, remaining); err != nil {
			_ = resume(t.base())
			return t.interrupt("sleep", err)
		}
		return resume(ctx)
	default:
		return Suspend(until)
	}
}

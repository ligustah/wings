package flow

import (
	"context"
	"errors"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ligustah/wings/flow/protos"
)

// shortSleep is the longest a workflow will simply wait in place. Beyond it, a
// sleep suspends the run instead.
//
// The threshold exists because the two are not the same trade. Waiting in place
// keeps the run's state in memory and its goroutine alive, which is cheap for a
// second and absurd for a day; suspending writes the wake-up time down and ends
// the attempt, which costs a replay when it resumes. A minute is where the
// replay stops being the expensive half.
const shortSleep = time.Minute

// Now returns the current time, recorded so that a replay sees the same instant.
//
// Use it instead of time.Now inside a workflow. A workflow that reads the real
// clock decides something different on every attempt, and the first retry then
// contradicts its own history.
func Now(ctx context.Context) (time.Time, error) {
	t := threadFrom(ctx)
	if t == nil {
		return time.Time{}, errors.New("flow: Now called outside a workflow")
	}

	ev, err := expect[*protos.GetTimeEvent](t)
	if err != nil {
		return time.Time{}, err
	}
	if ev != nil {
		return ev.GetTime().AsTime(), nil
	}

	now := time.Now().Truncate(time.Microsecond)
	record(t, &protos.GetTimeEvent{Time: timestamppb.New(now)})
	return now, t.run.err()
}

// Sleep pauses the workflow for d.
//
// A short sleep waits in place. A long one suspends the run: the attempt ends,
// its history stays on disk, and the next attempt replays to this point and
// carries on. Either way the sleep is recorded, so it is not served twice — a
// workflow that slept an hour and then failed does not sleep another hour on
// its retry.
func Sleep(ctx context.Context, d time.Duration) error {
	t := threadFrom(ctx)
	if t == nil {
		return errors.New("flow: Sleep called outside a workflow")
	}

	// The recorded event's timestamp is when the sleep STARTED, which is what
	// makes the deadline stable across attempts.
	start := time.Now()

	ev := t.peek()
	recorded, err := expect[*protos.SleepEvent](t)
	if err != nil {
		return err
	}
	if recorded != nil {
		start = ev.GetTimestamp().AsTime()
		d = recorded.GetDuration().AsDuration()
	} else {
		record(t, &protos.SleepEvent{Duration: durationpb.New(d)})
		if err := t.run.err(); err != nil {
			return err
		}
	}

	until := start.Add(d)
	remaining := time.Until(until)
	switch {
	case remaining <= 0:
		return nil
	case remaining < shortSleep:
		return wait(ctx, remaining)
	default:
		return Suspend(until)
	}
}

package flow

import (
	"errors"
	"fmt"
	"time"
)

// Permanent marks an error as one that must not be retried.
//
// The default is the opposite — an error a workflow returns is assumed to be
// worth another attempt — because the errors that dominate distributed work are
// transient ones. Use this for the errors that are not: a malformed input, a
// rejected credential, anything where the next attempt fails identically and
// the only thing retrying buys is a longer wait before the same answer.
//
// Returns nil for a nil error, so it composes with the usual `return
// flow.Permanent(err)` at the end of a function.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// MakePermanent marks the error half of a (value, error) pair, so the result of
// a call can be passed straight through:
//
//	return flow.MakePermanent(parse(in))
func MakePermanent[T any](value T, err error) (T, error) {
	return value, Permanent(err)
}

// IsPermanent reports whether err will not be retried.
func IsPermanent(err error) bool {
	var e *permanentError
	return errors.As(err, &e)
}

type permanentError struct{ err error }

func (p *permanentError) Error() string { return "permanent error: " + p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Suspend stops the workflow until t, without holding a worker or a goroutine
// while it waits.
//
// This is the difference between a workflow and a long function call: a
// workflow that is waiting for tomorrow costs nothing today. The run ends, its
// events are durable, and the next attempt replays what already happened and
// carries on from here.
func Suspend(until time.Time) error { return &suspendError{Until: until} }

// IsSuspended reports whether err is a suspension, and until when.
func IsSuspended(err error) (bool, time.Time) {
	var s *suspendError
	if errors.As(err, &s) {
		return true, s.Until
	}
	return false, time.Time{}
}

type suspendError struct{ Until time.Time }

func (s *suspendError) Error() string { return "suspended until " + s.Until.Format(time.RFC3339) }

// continuityError means the workflow did something on this attempt that
// contradicts what the log says it did on the last one.
//
// It is always fatal and never retried, because a retry replays the same log
// against the same code and reaches the same contradiction. It nearly always
// means the workflow function was edited while runs of it were in flight —
// which is a deployment problem, not a runtime one, and the error says so.
type continuityError struct{ msg string }

func (c *continuityError) Error() string {
	return "workflow continuity error: " + c.msg + "\n" +
		"(the workflow did something that does not match its recorded history; " +
		"this usually means the function changed while a run of it was in flight — " +
		"change the workflow's name or version instead of editing one that is running)"
}

// IsContinuity reports whether err is a replay mismatch.
func IsContinuity(err error) bool {
	var e *continuityError
	return errors.As(err, &e)
}

func continuityf(format string, args ...any) error {
	return &continuityError{msg: fmt.Sprintf(format, args...)}
}

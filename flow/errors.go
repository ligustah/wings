package flow

import (
	"errors"
	"fmt"
	"time"
)

// Permanent marks an error as one that must not be retried, for failures where
// the next attempt fails identically — a malformed input, a rejected
// credential. Returns nil for a nil error. Run errors are retried by default.
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
	_, ok := errors.AsType[*permanentError](err)
	return ok
}

type permanentError struct{ err error }

func (p *permanentError) Error() string { return "permanent error: " + p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Suspend ends this attempt until t; the next attempt replays and carries on
// from here. By default [Run] waits in place and starts that attempt itself.
// Under [Once] it instead returns an error [IsSuspended] recognises, for a
// scheduler that wants to let the thread go and rerun it at t.
func Suspend(until time.Time) error { return &suspendError{Until: until} }

// IsSuspended reports whether err is a suspension, and until when.
func IsSuspended(err error) (bool, time.Time) {
	if s, ok := errors.AsType[*suspendError](err); ok {
		return true, s.Until
	}
	return false, time.Time{}
}

type suspendError struct{ Until time.Time }

func (s *suspendError) Error() string { return "suspended until " + s.Until.Format(time.RFC3339) }

// continuityError means an attempt contradicted its recorded history. Always
// fatal: a retry replays the same log against the same code and fails the same
// way.
type continuityError struct{ msg string }

func (c *continuityError) Error() string {
	return "continuity error: " + c.msg + "\n" +
		"(the run did something that does not match its recorded history; " +
		"this usually means the function changed while a run of it was in flight — " +
		"change the run's name or version instead of editing one that is running)"
}

// IsContinuity reports whether err is a replay mismatch.
func IsContinuity(err error) bool {
	_, ok := errors.AsType[*continuityError](err)
	return ok
}

func continuityf(format string, args ...any) error {
	return &continuityError{msg: fmt.Sprintf(format, args...)}
}

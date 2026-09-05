package flow

import (
	"errors"
	"fmt"
	"time"
)

// Permanent marks an error as one that must not be retried.
//
// The default is the opposite — an error a run's body returns is assumed to be
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
	_, ok := errors.AsType[*permanentError](err)
	return ok
}

type permanentError struct{ err error }

func (p *permanentError) Error() string { return "permanent error: " + p.err.Error() }
func (p *permanentError) Unwrap() error { return p.err }

// Suspend ends this attempt of the run until t.
//
// The attempt ends and its events are durable, so nothing of it stays in
// memory: no thread, no locals, no half-finished call. At t the next attempt
// replays what already happened and carries on from here. That is the
// difference between a run and a long function call, and it is what lets
// a run that waits for tomorrow keep nothing alive today.
//
// What it does NOT do is return from [Run]. Run waits in place
// until t and starts the next attempt itself, so the caller's goroutine is
// held for the duration and the process has to be running when t arrives. A
// scheduler that wakes runs on their own is not here yet.
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

// continuityError means the run did something on this attempt that
// contradicts what the log says it did on the last one.
//
// It is always fatal and never retried, because a retry replays the same log
// against the same code and reaches the same contradiction. It nearly always
// means the run's body was edited while runs of it were in flight —
// which is a deployment problem, not a runtime one, and the error says so.
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

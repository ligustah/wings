package flow

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ligustah/wings/flow/protos"
)

// Read-only inspection of a run's recorded history, for a UI or a debugging
// tool. Inspect decodes the event log a Store holds into plain values; nothing
// here runs or changes a run.

const threadStreamPrefix = "flow.thread."

// ThreadStream is the stream name [NewStore] keeps one thread's history under.
func ThreadStream(run, thread string) string { return streamName(run, thread) }

var threadPattern = regexp.MustCompile(`^main(\.\d+)*$`)

// ParseThreadStream is the inverse of [ThreadStream]: it splits a thread stream
// name back into its run and thread, or reports ok false for a name that is not
// one. For enumerating a store's runs and threads by listing stream names.
func ParseThreadStream(name string) (run, thread string, ok bool) {
	rest, ok := strings.CutPrefix(name, threadStreamPrefix)
	if !ok {
		return "", "", false
	}
	// The thread is the suffix matching main(\.\d+)*; the run is what precedes
	// it. Tested at each boundary so a run that itself contains ".main" splits
	// at the real thread, since a thread has only digits after "main".
	for i := range len(rest) {
		if rest[i] != '.' {
			continue
		}
		if cand := rest[i+1:]; threadPattern.MatchString(cand) {
			return rest[:i], cand, true
		}
	}
	return "", "", false
}

// Lister is an optional [Store] capability: enumerating the runs and threads
// that have recorded history, so inspection can discover what to read without
// being handed a thread list. A store that does not implement it can still be
// inspected by passing threads to [Inspect] explicitly.
type Lister interface {
	// ListRuns names the runs with recorded history, sorted.
	ListRuns(ctx context.Context) ([]string, error)
	// ListThreads names the threads of a run with recorded history, sorted.
	ListThreads(ctx context.Context, run string) ([]string, error)
}

// ListRuns names the runs with recorded history in store, which must implement
// [Lister].
func ListRuns(ctx context.Context, store Store) ([]string, error) {
	l, ok := store.(Lister)
	if !ok {
		return nil, fmt.Errorf("flow: store %T cannot enumerate runs", store)
	}
	return l.ListRuns(ctx)
}

// InspectRun reads the whole recorded history of a run — every thread the store
// still holds. The store must implement [Lister]; use [Inspect] with an explicit
// thread list for one that does not.
func InspectRun(ctx context.Context, store Store, run string) (Snapshot, error) {
	l, ok := store.(Lister)
	if !ok {
		return Snapshot{}, fmt.Errorf("flow: store %T cannot enumerate threads", store)
	}
	threads, err := l.ListThreads(ctx, run)
	if err != nil {
		return Snapshot{}, err
	}
	return Inspect(ctx, store, run, threads)
}

// Snapshot is a read-only view of one run's recorded history.
type Snapshot struct {
	Run     string       `json:"run"`
	Threads []ThreadView `json:"threads"`
}

// ThreadView is one thread of a run: what it runs, how it ended, and its events
// in order.
type ThreadView struct {
	ID       string      `json:"id"`
	Parent   string      `json:"parent,omitempty"`
	Fn       string      `json:"fn,omitempty"`
	Status   string      `json:"status"`
	Attempts uint64      `json:"attempts"`
	Events   []EventView `json:"events"`
}

// EventView is one recorded event, decoded to a kind and a short human-readable
// detail.
type EventView struct {
	Seq    uint64    `json:"seq"`
	At     time.Time `json:"at"`
	Kind   string    `json:"kind"`
	Detail string    `json:"detail,omitempty"`
	Error  string    `json:"error,omitempty"`
}

// Inspect reads the recorded history of run from store and returns a read-only
// view of the given threads, in the order passed. The store does not enumerate
// threads itself; list them from the stream names with [ParseThreadStream]. A
// thread with no history yields an empty view rather than an error.
func Inspect(ctx context.Context, store Store, run string, threads []string) (Snapshot, error) {
	snap := Snapshot{Run: run}
	for _, th := range threads {
		events, err := store.Events(ctx, run, th)
		if err != nil {
			return Snapshot{}, fmt.Errorf("flow: read thread %s of run %s: %w", th, run, err)
		}
		snap.Threads = append(snap.Threads, threadView(th, events))
	}
	return snap, nil
}

func threadView(id string, events []*protos.Event) ThreadView {
	tv := ThreadView{ID: id, Status: "running"}
	if i := strings.LastIndex(id, "."); i >= 0 {
		tv.Parent = id[:i]
	}
	for _, ev := range events {
		if ev.GetAttempt() > tv.Attempts {
			tv.Attempts = ev.GetAttempt()
		}
		if start := ev.GetRunStart(); start != nil {
			if start.GetFunction() != "" {
				tv.Fn = start.GetFunction()
			}
			continue
		}
		if end := ev.GetRunEnd(); end != nil {
			if s := statusName(end.GetStatus()); s != "" {
				tv.Status = s
			}
			continue
		}
		tv.Events = append(tv.Events, eventView(ev))
	}
	return tv
}

func eventView(ev *protos.Event) EventView {
	v := EventView{Seq: ev.GetSerial(), At: ev.GetTimestamp().AsTime()}
	switch p := protos.UnpackEventPayload(ev).(type) {
	case *protos.CallEvent:
		v.Kind, v.Detail = "call", p.GetName()
	case *protos.ReturnEvent:
		v.Kind = "return"
		if _, err := unpackResult(p.GetResult()); err != nil {
			v.Error = err.Error()
		}
	case *protos.ForkEvent:
		v.Kind, v.Detail = "fork", p.GetThreadId()
		if p.GetFunction() != "" {
			v.Detail += " (" + p.GetFunction() + ")"
		}
	case *protos.JoinEvent:
		v.Kind, v.Detail = "join", p.GetThreadId()
		if _, err := unpackResult(p.GetResult()); err != nil {
			v.Error = err.Error()
		}
	case *protos.SleepEvent:
		v.Kind, v.Detail = "sleep", p.GetDuration().AsDuration().String()
	case *protos.GetTimeEvent:
		v.Kind, v.Detail = "time", p.GetTime().AsTime().Format(time.RFC3339Nano)
	case *protos.EffectEvent:
		v.Kind = "effect"
		if _, err := unpackResult(p.GetResult()); err != nil {
			v.Error = err.Error()
		}
	case *protos.ChannelSendEvent:
		v.Kind = "send"
		switch {
		case p.GetClosed():
			v.Detail = "close " + p.GetChannel()
		case p.GetRefused():
			v.Detail = fmt.Sprintf("%s#%d refused (closed)", p.GetChannel(), p.GetSeq())
		default:
			v.Detail = fmt.Sprintf("%s#%d", p.GetChannel(), p.GetSeq())
		}
	case *protos.ChannelRecvEvent:
		v.Kind = "recv"
		if p.GetClosed() {
			v.Detail = p.GetChannel() + " closed"
		} else {
			v.Detail = fmt.Sprintf("%s from %s#%d", p.GetChannel(), p.GetFromThreadId(), p.GetFromSeq())
		}
	case *protos.SelectEvent:
		v.Kind, v.Detail = "select", fmt.Sprintf("case %d", p.GetChosen())
	case *protos.WaitInterruptedEvent:
		v.Kind, v.Detail = "interrupted", p.GetWait()
	default:
		v.Kind = protos.EventType(ev)
	}
	return v
}

func statusName(s protos.WorkflowStatus) string {
	switch s {
	case protos.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:
		return "completed"
	case protos.WorkflowStatus_WORKFLOW_STATUS_FAILED:
		return "failed"
	case protos.WorkflowStatus_WORKFLOW_STATUS_SUSPENDED:
		return "suspended"
	case protos.WorkflowStatus_WORKFLOW_STATUS_BACKOFF:
		return "backoff"
	default:
		return ""
	}
}

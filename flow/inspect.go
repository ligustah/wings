package flow

import (
	"context"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

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

// Tailer is an optional [Store] capability: reading a thread's most recent
// events without decoding its whole history. A status that lives at the tail —
// the last RunEnd — is then cheap to read for every run in a list.
type Tailer interface {
	// Tail returns up to the last n events of a thread, oldest first, each with
	// its offset. Fewer than n (including none) means the history is shorter.
	Tail(ctx context.Context, run, thread string, n int) ([]EventAt, error)
}

// statusWindow is how many events from the tail Status reads to find the last
// RunEnd. A thread's terminal or suspended RunEnd is its last recorded event
// (nothing is appended after it until the next attempt's RunStart), so a small
// window catches it; a running thread has none and reads as running.
const statusWindow = 8

// headWindow is how many events from the head InspectThread reads to find the
// RunStart that names a thread's function. The first attempt's RunStart is the
// thread's first event, so a small window catches it.
const headWindow = 4

// Status reports a thread's current status — the status of its last RunEnd, or
// "running" if it has not ended — reading only the tail when store is a [Tailer]
// and the whole history otherwise. For a run list that needs each run's status
// but not its events.
func Status(ctx context.Context, store Store, run, thread string) (string, error) {
	tail, err := tailEvents(ctx, store, run, thread, statusWindow)
	if err != nil {
		return "", err
	}
	return statusFromEvents(tail), nil
}

// ThreadInfo is a thread's header without its events: what it runs and how it
// stands, read from the ends of its history rather than the whole of it.
type ThreadInfo struct {
	ID       string `json:"id"`
	Parent   string `json:"parent,omitempty"`
	Fn       string `json:"fn,omitempty"`
	Status   string `json:"status"`
	Attempts uint64 `json:"attempts"`
}

// EventPage is a window of a thread's decoded events, with a cursor to the next.
// Done is set once the window reached the end of the history.
type EventPage struct {
	Events []EventView `json:"events"`
	Next   int64       `json:"next"`
	Done   bool        `json:"done"`
}

// InspectThread reads a thread's header — what it runs and its status — from the
// head and tail of its history, without decoding all of it. Its function comes
// from the first RunStart, its status from the last RunEnd, and its attempt count
// from the last event (attempts only ever climb). For a run view that lists a
// run's threads and pages their events with [ReadEvents] rather than holding them.
func InspectThread(ctx context.Context, store Store, run, thread string) (ThreadInfo, error) {
	info := ThreadInfo{ID: thread, Status: "running"}
	if i := strings.LastIndex(thread, "."); i >= 0 {
		info.Parent = thread[:i]
	}
	head, err := headEvents(ctx, store, run, thread, headWindow)
	if err != nil {
		return ThreadInfo{}, err
	}
	for _, ev := range head {
		if start := ev.GetRunStart(); start != nil && start.GetFunction() != "" {
			info.Fn = start.GetFunction()
			break
		}
	}
	tail, err := tailEvents(ctx, store, run, thread, statusWindow)
	if err != nil {
		return ThreadInfo{}, err
	}
	info.Status = statusFromEvents(tail)
	for _, ev := range tail {
		if a := ev.GetAttempt(); a > info.Attempts {
			info.Attempts = a
		}
	}
	return info, nil
}

// InspectRunHeaders lists a run's threads with their headers but not their
// events, reading only the ends of each thread's history. The store must
// implement [Lister].
func InspectRunHeaders(ctx context.Context, store Store, run string) ([]ThreadInfo, error) {
	l, ok := store.(Lister)
	if !ok {
		return nil, fmt.Errorf("flow: store %T cannot enumerate threads", store)
	}
	threads, err := l.ListThreads(ctx, run)
	if err != nil {
		return nil, err
	}
	out := make([]ThreadInfo, 0, len(threads))
	for _, th := range threads {
		info, err := InspectThread(ctx, store, run, th)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	return out, nil
}

// ReadEvents returns a page of a thread's decoded events starting at offset from,
// with a cursor (Next) to the page after it. A run's start and end are markers,
// not events: they are skipped but still advance the cursor. Read pages until
// the returned page has Done set.
func ReadEvents(ctx context.Context, store Store, run, thread string, from int64, limit int) (EventPage, error) {
	if limit <= 0 {
		limit = 200
	}
	batch, err := store.Read(ctx, run, thread, from, limit)
	if err != nil {
		return EventPage{}, fmt.Errorf("flow: read thread %s of run %s at %d: %w", thread, run, from, err)
	}
	page := EventPage{Next: from, Done: len(batch) < limit}
	for _, e := range batch {
		page.Next = e.Offset + 1
		if e.Event.GetRunStart() != nil || e.Event.GetRunEnd() != nil {
			continue // markers are not events; skip them but advance the cursor
		}
		page.Events = append(page.Events, eventView(e.Event))
	}
	return page, nil
}

// statusFromEvents returns the status the last RunEnd names, or "running" when
// none is present, matching how [threadView] decides a thread's status.
func statusFromEvents(events []*protos.Event) string {
	status := "running"
	for _, ev := range events {
		if end := ev.GetRunEnd(); end != nil {
			if s := statusName(end.GetStatus()); s != "" {
				status = s
			}
		}
	}
	return status
}

// headEvents returns up to the first n events of a thread.
func headEvents(ctx context.Context, store Store, run, thread string, n int) ([]*protos.Event, error) {
	batch, err := store.Read(ctx, run, thread, 0, n)
	if err != nil {
		return nil, fmt.Errorf("flow: read head of thread %s of run %s: %w", thread, run, err)
	}
	out := make([]*protos.Event, len(batch))
	for i, e := range batch {
		out[i] = e.Event
	}
	return out, nil
}

// tailEvents returns up to the last n events of a thread, from the [Tailer]
// capability when the store has it and the whole history sliced otherwise.
func tailEvents(ctx context.Context, store Store, run, thread string, n int) ([]*protos.Event, error) {
	if t, ok := store.(Tailer); ok {
		batch, err := t.Tail(ctx, run, thread, n)
		if err != nil {
			return nil, fmt.Errorf("flow: tail thread %s of run %s: %w", thread, run, err)
		}
		out := make([]*protos.Event, len(batch))
		for i, e := range batch {
			out[i] = e.Event
		}
		return out, nil
	}
	events, err := store.Events(ctx, run, thread)
	if err != nil {
		return nil, fmt.Errorf("flow: read thread %s of run %s: %w", thread, run, err)
	}
	if len(events) > n {
		events = events[len(events)-n:]
	}
	return events, nil
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
	// Value is the event's recorded value, formatted for display: a call or
	// effect's result, a join's child result. Text when the bytes are printable
	// UTF-8, a hex dump otherwise, capped so a large value does not flood the view.
	Value string `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
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
		if data, err := unpackResult(p.GetResult()); err != nil {
			v.Error = err.Error()
		} else {
			v.Value = formatValue(data)
		}
	case *protos.ForkEvent:
		v.Kind, v.Detail = "fork", p.GetThreadId()
		if p.GetFunction() != "" {
			v.Detail += " (" + p.GetFunction() + ")"
		}
	case *protos.JoinEvent:
		v.Kind, v.Detail = "join", p.GetThreadId()
		if data, err := unpackResult(p.GetResult()); err != nil {
			v.Error = err.Error()
		} else {
			v.Value = formatValue(data)
		}
	case *protos.SleepEvent:
		v.Kind, v.Detail = "sleep", p.GetDuration().AsDuration().String()
	case *protos.GetTimeEvent:
		v.Kind, v.Detail = "time", p.GetTime().AsTime().Format(time.RFC3339Nano)
	case *protos.EffectEvent:
		v.Kind = "effect"
		if data, err := unpackResult(p.GetResult()); err != nil {
			v.Error = err.Error()
		} else {
			v.Value = formatValue(data)
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

const valueCap = 2048

// formatValue renders a recorded value for display: printable UTF-8 as text,
// anything else as hex, each truncated near valueCap bytes with a "… (+N bytes)"
// marker.
func formatValue(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if printableUTF8(b) {
		if len(b) <= valueCap {
			return string(b)
		}
		cut := valueCap
		for cut > 0 && !utf8.RuneStart(b[cut]) {
			cut--
		}
		return string(b[:cut]) + fmt.Sprintf("… (+%d bytes)", len(b)-cut)
	}
	const hexCap = valueCap / 2
	if len(b) <= hexCap {
		return hex.EncodeToString(b)
	}
	return hex.EncodeToString(b[:hexCap]) + fmt.Sprintf("… (+%d bytes)", len(b)-hexCap)
}

func printableUTF8(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for _, r := range string(b) {
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
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

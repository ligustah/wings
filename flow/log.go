package flow

import (
	"context"
	"log/slog"

	"github.com/ligustah/wings/flow/protos"
)

// LogHost stores a run's durable log lines, one log apart from each thread's
// history so the lines outlive a history purge. A run given one with
// [WithLogHost] has [Context.Logger] write there.
type LogHost interface {
	// Log appends one line to the thread's log. Called from the thread's own
	// goroutine and only on the live path — a replay skips a line already written.
	Log(ctx context.Context, run, thread string, rec *protos.LogRecord) error
}

// WithLogHost sets where [Context.Logger] writes durable log lines. Without one,
// [Context.Logger] falls back to [slog.Default] and stores nothing.
func WithLogHost(h LogHost) RunOption { return func(o *runOptions) { o.logHost = h } }

// WithLogLevel sets the lowest level [Context.Logger] records. It is fixed for
// the life of the run — a run's config is immutable, so a replay filters exactly
// as the first attempt did. Defaults to [slog.LevelInfo].
func WithLogLevel(l slog.Level) RunOption { return func(o *runOptions) { o.logLevel = l } }

// Logger returns an [slog.Logger] whose lines are recorded durably to the run's
// log, so a finished or replayed run keeps what it logged; a replay does not
// write a line a previous attempt already wrote. It is bound to the calling
// thread. Outside a run it returns [slog.Default].
func (c Context) Logger() *slog.Logger {
	t := threadFrom(c)
	if t == nil {
		return slog.Default()
	}
	return slog.New(&logHandler{t: t, level: t.run.opts.logLevel, host: t.run.opts.logHost})
}

// logHandler is [Context.Logger]'s durable [slog.Handler]. groups and pre are the
// standard preformatted-attribute state: pre holds the attrs added by WithAttrs,
// already flattened under the groups open when they were added.
type logHandler struct {
	t      *threadState
	level  slog.Level
	host   LogHost
	groups []string
	pre    []*protos.LogAttr
}

func (h *logHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

// Handle records one line. On the live path it writes the line to the host (or
// [slog.Default] when there is none) and appends a [protos.LogEvent] marker to
// history; on a replay the marker at the cursor is consumed and the line is not
// written again. A continuity mismatch — the code changed under a live run — is
// stashed on the thread to surface at its next operation, since slog discards
// the error this returns.
func (h *logHandler) Handle(ctx context.Context, rec slog.Record) error {
	// A line written from within a Blocking step runs off the run thread, where the
	// history and transaction below are unsafe to touch; buffer it to flush with the
	// step's result (blockingLog), and record no marker of its own.
	if h.t.captureBlockingLog(h.line(rec)) {
		return nil
	}

	ev, err := h.t.expect[*protos.LogEvent]()
	if err != nil {
		h.t.fail(err)
		return err
	}
	if ev != nil {
		return nil
	}

	line := h.line(rec)
	if h.host == nil {
		slog.Default().LogAttrs(ctx, rec.Level, rec.Message, slogAttrs(line.GetAttrs())...)
	} else if err := h.host.Log(context.WithoutCancel(h.t.base()), h.t.run.name, h.t.id, line); err != nil {
		h.t.fail(err)
		return err
	}
	h.t.record(&protos.LogEvent{})
	return h.t.err()
}

// beginBlockingLog starts capturing lines written off-thread during a Blocking
// step; endBlockingLog stops it. Between the two, Handle buffers into blockingLog
// instead of recording a per-line marker and writing straight to the host.
func (t *threadState) beginBlockingLog() {
	t.blockingMu.Lock()
	t.blocking = true
	t.blockingMu.Unlock()
}

func (t *threadState) endBlockingLog() {
	t.blockingMu.Lock()
	t.blocking = false
	t.blockingMu.Unlock()
}

// captureBlockingLog buffers a line while a Blocking step is in flight, reporting
// true; otherwise it reports false and Handle takes the normal on-thread path.
func (t *threadState) captureBlockingLog(rec *protos.LogRecord) bool {
	t.blockingMu.Lock()
	defer t.blockingMu.Unlock()
	if !t.blocking {
		return false
	}
	t.blockingLog = append(t.blockingLog, rec)
	return true
}

// flushBlockingLog writes the lines a Blocking step buffered into the thread's
// transaction, so they commit with the result event recorded next — one dedup point
// for the whole step, since a replay skips f and never re-buffers them. With no host
// they go to [slog.Default], matching a hostless Logger. Called on the run thread
// once f has returned, so nothing appends to the buffer while it drains.
func (t *threadState) flushBlockingLog() error {
	t.blockingMu.Lock()
	lines := t.blockingLog
	t.blockingLog = nil
	t.blockingMu.Unlock()

	host := t.run.opts.logHost
	for _, line := range lines {
		if host == nil {
			slog.Default().LogAttrs(t.base(), slog.Level(line.GetLevel()), line.GetMessage(), slogAttrs(line.GetAttrs())...)
			continue
		}
		if err := host.Log(context.WithoutCancel(t.base()), t.run.name, t.id, line); err != nil {
			t.fail(err)
			return err
		}
	}
	return t.err()
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	prefix := groupPrefix(h.groups)
	pre := append([]*protos.LogAttr(nil), h.pre...)
	for _, a := range attrs {
		flattenAttr(&pre, prefix, a)
	}
	return &logHandler{t: h.t, level: h.level, host: h.host, groups: h.groups, pre: pre}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	groups := append(append([]string(nil), h.groups...), name)
	return &logHandler{t: h.t, level: h.level, host: h.host, groups: groups, pre: h.pre}
}

// line builds the durable record: the preformatted attrs, then the record's own,
// each flattened under the open groups, with values rendered to their string form.
func (h *logHandler) line(rec slog.Record) *protos.LogRecord {
	lr := &protos.LogRecord{
		TimeUnixNano: rec.Time.UnixNano(),
		Level:        int32(rec.Level),
		Message:      rec.Message,
	}
	lr.Attrs = append(lr.Attrs, h.pre...)
	prefix := groupPrefix(h.groups)
	rec.Attrs(func(a slog.Attr) bool {
		flattenAttr(&lr.Attrs, prefix, a)
		return true
	})
	return lr
}

func groupPrefix(groups []string) string {
	prefix := ""
	for _, g := range groups {
		prefix += g + "."
	}
	return prefix
}

// flattenAttr appends a, prefixed by the open groups, to dst — a group-valued
// attr recursively under its own name, a leaf as one dotted key and its rendered
// value. An empty attr is dropped, matching slog.
func flattenAttr(dst *[]*protos.LogAttr, prefix string, a slog.Attr) {
	a.Value = a.Value.Resolve()
	if a.Equal(slog.Attr{}) {
		return
	}
	if a.Value.Kind() == slog.KindGroup {
		group := a.Value.Group()
		if len(group) == 0 {
			return
		}
		inner := prefix
		if a.Key != "" {
			inner = prefix + a.Key + "."
		}
		for _, g := range group {
			flattenAttr(dst, inner, g)
		}
		return
	}
	*dst = append(*dst, &protos.LogAttr{Key: prefix + a.Key, Value: a.Value.String()})
}

func slogAttrs(attrs []*protos.LogAttr) []slog.Attr {
	out := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		out[i] = slog.String(a.GetKey(), a.GetValue())
	}
	return out
}

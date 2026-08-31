package wings

import (
	"context"
	"log/slog"
)

// streamLogger is the logger handed to the durable-streams machinery.
//
// That layer logs producer initialisation and every transaction begin and
// commit at INFO. For a stream store those are the interesting events; here
// they are one line per batch of work about plumbing the caller was promised
// they would never have to think about, and they bury wings' own messages. So
// the substrate is filtered to warnings and above while the caller's own logger
// keeps whatever level they set.
//
// Raise it deliberately when a worker is misbehaving — the transaction log is
// exactly what you want then.
func streamLogger(base *slog.Logger) *slog.Logger {
	return slog.New(&levelFilter{inner: base.Handler(), min: slog.LevelWarn})
}

type levelFilter struct {
	inner slog.Handler
	min   slog.Level
}

func (f *levelFilter) Enabled(ctx context.Context, l slog.Level) bool {
	return l >= f.min && f.inner.Enabled(ctx, l)
}

func (f *levelFilter) Handle(ctx context.Context, r slog.Record) error {
	return f.inner.Handle(ctx, r)
}

func (f *levelFilter) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelFilter{inner: f.inner.WithAttrs(attrs), min: f.min}
}

func (f *levelFilter) WithGroup(name string) slog.Handler {
	return &levelFilter{inner: f.inner.WithGroup(name), min: f.min}
}

package wings

import (
	"context"
	"log/slog"
)

// streamLogger filters the durable-streams substrate to warnings and above; its
// INFO is a line per transaction, which buries wings' own logging.
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

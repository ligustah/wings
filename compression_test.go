package wings

import (
	"testing"

	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow"
)

// THE POINT: the zero value means the default (Zstd), and each named codec maps
// to its dswire counterpart.
func TestCompressionResolve(t *testing.T) {
	cases := []struct {
		in   Compression
		want dswire.Compression
	}{
		{CompressionDefault, dswire.CompressionZstd}, // the zero value
		{CompressionNone, dswire.CompressionNone},
		{CompressionSnappy, dswire.CompressionSnappy},
		{CompressionS2, dswire.CompressionS2},
		{CompressionZstd, dswire.CompressionZstd},
	}
	for _, tc := range cases {
		if got := tc.in.resolve(); got != tc.want {
			t.Errorf("Compression(%d).resolve() = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// THE POINT: a run with compression disabled still creates its streams and a
// channel still round-trips — the override threads through to stream creation.
func TestRunWithCompressionDisabled(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Compression: CompressionNone})
	var got int
	err := c.Run(t.Context(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int]()
		if err := w.Send(ctx, 7); err != nil {
			return err
		}
		v, _, err := r.Recv(ctx)
		got = v
		return err
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if got != 7 {
		t.Fatalf("got %d, want the sent value 7", got)
	}
}

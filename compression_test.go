package wings

import (
	"testing"

	"github.com/ligustah/commitlog/blockv3"
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

// THE POINT: -compression names map to the codec (case-insensitively, empty is
// the default), and an unknown name is an error rather than a silent default.
func TestParseCompression(t *testing.T) {
	cases := []struct {
		in   string
		want Compression
	}{
		{"", CompressionDefault},
		{"default", CompressionDefault},
		{"none", CompressionNone},
		{"Snappy", CompressionSnappy},
		{" s2 ", CompressionS2},
		{"ZSTD", CompressionZstd},
	}
	for _, tc := range cases {
		got, err := parseCompression(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("parseCompression(%q) = %v, %v; want %v, nil", tc.in, got, err, tc.want)
		}
	}
	if _, err := parseCompression("lz4"); err == nil {
		t.Error("parseCompression(\"lz4\") = nil error, want an error for an unknown codec")
	}
}

// THE POINT: a run with compression disabled still creates its streams and a
// channel still round-trips — the override threads through to stream creation.
// THE POINT: wings stamps the v3 block layout on the streams it creates, so
// they store on the faster, tighter format rather than the engine's v2 default.
func TestStreamConfigWritesV3(t *testing.T) {
	if got := streamConfig().BlockFormat; got != int(blockv3.Version) {
		t.Errorf("streamConfig writes block format %d, want v3 (%d)", got, blockv3.Version)
	}
}

// THE POINT: the pull always states the v3 block layout for a destination, and
// adds the cluster's codec when compression is on — so a disabled cluster still
// pulls into v3 streams, just uncompressed.
func TestPulledOptionsFollowTheStorageCodec(t *testing.T) {
	defer func(prev dswire.Compression) { streamCompression = prev }(streamCompression)
	var c Cluster

	streamCompression = dswire.CompressionNone
	if opts := c.pulledOptions("wings.history.x"); len(opts) != 1 {
		t.Errorf("disabled compression should state only the block format, got %d options", len(opts))
	}

	streamCompression = dswire.CompressionZstd
	if opts := c.pulledOptions("wings.history.x"); len(opts) != 2 {
		t.Errorf("enabled compression should state block format and codec, got %d options", len(opts))
	}
}

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

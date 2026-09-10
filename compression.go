package wings

import (
	"github.com/ligustah/commitlog/blockv3"
	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"
)

// Compression selects the storage codec for the durable streams a [Cluster]
// creates — history, channels, journal, machine, output and worker streams. The
// zero value uses wings' default (Zstd); [CompressionNone] stores uncompressed.
type Compression uint8

const (
	// CompressionDefault uses wings' default, Zstd. The zero value.
	CompressionDefault Compression = iota
	// CompressionNone stores streams uncompressed.
	CompressionNone
	// CompressionSnappy is fast with a modest ratio.
	CompressionSnappy
	// CompressionS2 is a faster, higher-ratio Snappy successor.
	CompressionS2
	// CompressionZstd gives the best ratio; wings' default.
	CompressionZstd
)

func (c Compression) resolve() dswire.Compression {
	switch c {
	case CompressionNone:
		return dswire.CompressionNone
	case CompressionSnappy:
		return dswire.CompressionSnappy
	case CompressionS2:
		return dswire.CompressionS2
	default: // CompressionDefault, CompressionZstd
		return dswire.CompressionZstd
	}
}

// streamCompression is the codec this process stamps on every stream it creates.
// Set once at startup — by the coordinator from [Config.Compression], by a worker
// from the WINGS_COMPRESSION it was launched with — before any stream is made.
var streamCompression = dswire.CompressionZstd

// streamBlockFormat is the block layout wings writes: v3, which packs records
// more tightly than v2. Reads stay compatible; existing v2 streams keep theirs.
var streamBlockFormat = int(blockv3.Version)

func streamConfig() *dsclient.StreamConfig {
	return &dsclient.StreamConfig{Compression: streamCompression, BlockFormat: streamBlockFormat}
}

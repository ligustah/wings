// Package payload holds the gzipped worker binary compiled into a coordinator.
// The generated coordinator main embeds the blob with //go:embed and registers
// it here through [Set].
package payload

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync"
)

// ErrNoPayload means this binary carries no worker; a coordinator built with
// plain `go build` has none, and the caller falls back to compiling one.
var ErrNoPayload = errors.New("wings: no embedded worker")

// Meta describes the embedded worker.
type Meta struct {
	GOOS   string
	GOARCH string
	// Size is the uncompressed size in bytes.
	Size int64
}

func (m Meta) Platform() string { return m.GOOS + "/" + m.GOARCH }

var (
	mu         sync.RWMutex
	compressed []byte
	meta       Meta
)

// Set records the embedded worker. Called from the generated file's init.
func Set(gz []byte, goos, goarch string) {
	mu.Lock()
	defer mu.Unlock()
	compressed, meta = gz, Meta{GOOS: goos, GOARCH: goarch}
}

// Info reports what is embedded, if anything.
func Info() (Meta, bool) {
	mu.RLock()
	defer mu.RUnlock()
	if len(compressed) == 0 {
		return Meta{}, false
	}
	return meta, true
}

// Get decompresses the embedded worker and returns its bytes.
func Get() ([]byte, Meta, error) {
	mu.RLock()
	gz, m := compressed, meta
	mu.RUnlock()

	if len(gz) == 0 {
		return nil, Meta{}, ErrNoPayload
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		return nil, m, fmt.Errorf("wings: decompress embedded worker: %w", err)
	}
	defer zr.Close()

	worker, err := io.ReadAll(zr)
	if err != nil {
		return nil, m, fmt.Errorf("wings: decompress embedded worker: %w", err)
	}
	m.Size = int64(len(worker))
	return worker, m, nil
}

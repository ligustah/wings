// Package payload holds the worker binary compiled into a coordinator.
//
// The bytes get here through //go:embed, in the coordinator main that
// `wings build` generates. The directive has to live in that generated package:
// //go:embed resolves paths inside the package being compiled, and this library
// is normally read-only in the module cache, so an embed directive here could
// never find a file to point at.
//
// The blob is stored gzipped. It is a second copy of very nearly the same
// program, and compressing it is most of the difference between a coordinator
// that is awkward to move around and one that is merely large.
package payload

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// ErrNoPayload means this binary carries no worker. An ordinary answer rather
// than a failure: a coordinator built with plain `go build` has none, and the
// caller falls back to compiling one.
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

// Get decompresses the embedded worker.
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

// ExtractTo writes the embedded worker into dir and returns its path.
//
// Written to a file rather than streamed because it is uploaded once per
// machine, in parallel, and decompressing it once beats decompressing it n
// times.
func ExtractTo(dir string) (string, Meta, error) {
	worker, m, err := Get()
	if err != nil {
		return "", m, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", m, fmt.Errorf("wings: extract dir: %w", err)
	}
	out := filepath.Join(dir, "wings-worker")
	if err := os.WriteFile(out, worker, 0o755); err != nil {
		return "", m, fmt.Errorf("wings: write extracted worker: %w", err)
	}
	return out, m, nil
}

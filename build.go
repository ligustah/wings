package wings

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

// buildWorker cross-compiles the worker binary for a remote machine.
//
// The running process cannot ship itself: a Windows .exe is not a Linux worker,
// and even on Linux the coordinator's architecture need not be the VM's. So
// wings compiles the same package for the target and sends that.
//
// This means a remote run needs a Go toolchain and the module source on the
// coordinator. That is a real constraint and it is stated in [BuildConfig].
func buildWorker(ctx context.Context, cfg BuildConfig, outDir string, log *slog.Logger) (string, error) {
	pkg := cfg.Package
	if pkg == "" {
		pkg = "."
	}
	goos, goarch := cfg.GOOS, cfg.GOARCH
	if goos == "" {
		goos = "linux"
	}
	if goarch == "" {
		goarch = "amd64"
	}

	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", fmt.Errorf("wings: build dir: %w", err)
	}
	out := filepath.Join(outDir, "wings-worker")

	// Stripped and trimmed: this binary is uploaded once per machine and only
	// ever runs as a worker, so the debug info is bandwidth spent on something
	// nobody will attach a debugger to. It is most of the size.
	args := []string{"build", "-trimpath", "-ldflags", "-s -w", "-o", out}
	if cfg.Tags != "" {
		args = append(args, "-tags", cfg.Tags)
	}
	args = append(args, pkg)

	cmd := exec.CommandContext(ctx, "go", args...)
	// CGO_ENABLED=0 first so cfg.Env can override it. Cross-compiling with cgo
	// needs a target C toolchain the coordinator almost certainly lacks, and
	// the resulting error names a C compiler rather than the real cause.
	cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
	cmd.Env = append(cmd.Env, cfg.Env...)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	start := time.Now()
	log.Info("wings: cross-compiling worker", "pkg", pkg, "goos", goos, "goarch", goarch)
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("wings: cross-compile %s for %s/%s: %w\n%s", pkg, goos, goarch, err, stderr.String())
	}

	info, err := os.Stat(out)
	if err != nil {
		return "", fmt.Errorf("wings: built worker missing: %w", err)
	}
	log.Info("wings: worker built", "path", out, "bytes", info.Size(), "took", time.Since(start).Round(time.Millisecond))
	return out, nil
}

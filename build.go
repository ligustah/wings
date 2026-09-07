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

// buildWorker cross-compiles the worker for goos/goarch. It needs a Go toolchain
// and the module source on the coordinator.
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

	// -s -w strips debug info, which is most of the upload size.
	args := []string{"build", "-trimpath", "-ldflags", "-s -w", "-o", out}
	if cfg.Tags != "" {
		args = append(args, "-tags", cfg.Tags)
	}
	args = append(args, pkg)

	cmd := exec.CommandContext(ctx, "go", args...)
	// CGO_ENABLED=0 before cfg.Env so the caller can override it.
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

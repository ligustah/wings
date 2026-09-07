package wings

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// readyTimeout bounds how long we wait for a worker to announce its address; a
// cold machine may still be unpacking its binary.
const readyTimeout = 2 * time.Minute

// launchInProcess runs workers as goroutines on the cluster's own embedded
// instance. They still talk through dsclient over per-worker streams, the same
// as a remote worker; only the backend differs.
func (c *Cluster) launchInProcess(ctx context.Context, n int) ([]*workerConn, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}

	var out []*workerConn
	for range n {
		id := c.workerID("inproc")
		node, err := newWorkerNode(ctx, client, id, c.cfg.Concurrency, c.cfg.JobTimeout, c.log)
		if err != nil {
			return nil, closePartial(ctx, out, err)
		}

		// ownsClient false: the engine outlives any one worker.
		w, err := c.connect(id, client, false)
		if err != nil {
			return nil, closePartial(ctx, out, err)
		}
		w.node = node

		// Bound to the worker's context, so retiring one ends only its loop.
		w.wg.Go(func() {
			if err := node.run(w.ctx); err != nil && w.ctx.Err() == nil && !node.leaving.Load() {
				c.log.Error("wings: in-process worker stopped", "worker", id, "err", err)
			}
		})

		out = append(out, w)
	}
	return out, nil
}

// launchLocalProcess runs each worker as a child copy of this binary — no
// cross-compilation, which is what makes this the cheap way to test the process
// boundary.
func (c *Cluster) launchLocalProcess(ctx context.Context, n int) ([]*workerConn, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("wings: locate this executable: %w", err)
	}

	var out []*workerConn
	for range n {
		id := c.workerID("local")
		w, err := c.spawnLocal(ctx, exe, id, filepath.Join(c.dir, id))
		if err != nil {
			return nil, closePartial(ctx, out, err)
		}
		out = append(out, w)
	}
	return out, nil
}

func (c *Cluster) spawnLocal(ctx context.Context, exe, id, dir string) (*workerConn, error) {
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), workerEnv(id, "127.0.0.1:0", dir, c.cfg.Concurrency, c.cfg.JobTimeout)...)
	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("wings: worker %s stdout: %w", id, err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("wings: start worker %s: %w", id, err)
	}

	addr, err := awaitReady(ctx, stdout, id)
	if err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, err
	}

	backend, err := dialWorker(addr)
	if err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, fmt.Errorf("wings: dial worker %s at %s: %w", id, addr, err)
	}

	w, err := c.connectBackend(id, backend)
	if err != nil {
		_ = backend.Close()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, err
	}
	w.proc = cmd.Process
	w.dir = dir
	w.exited = make(chan struct{})
	// One owner for Wait, so the tail can ask whether this worker is gone.
	go func() {
		_, _ = cmd.Process.Wait()
		close(w.exited)
	}()

	c.log.Info("wings: local worker started", "worker", id, "addr", addr, "pid", cmd.Process.Pid)
	return w, nil
}

// workerEnv is the whole coordinator-to-worker contract, carried explicitly
// because a worker process shares nothing with the Config that set it.
func workerEnv(id, listen, dir string, concurrency int, jobTimeout time.Duration) []string {
	env := []string{
		envMode + "=" + modeWorker,
		envWorkerID + "=" + id,
		envListen + "=" + listen,
		envDir + "=" + dir,
	}
	if concurrency > 0 {
		env = append(env, envConcurrency+"="+strconv.Itoa(concurrency))
	}
	if jobTimeout > 0 {
		env = append(env, envJobTimeout+"="+jobTimeout.String())
	}
	return env
}

// awaitReady reads the worker's announced address off its stdout, then keeps
// draining stdout so the child never blocks on a full pipe.
func awaitReady(ctx context.Context, stdout io.ReadCloser, id string) (string, error) {
	type result struct {
		addr string
		err  error
	}
	ch := make(chan result, 1)
	scanner := bufio.NewScanner(stdout)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if addr, ok := strings.CutPrefix(line, readyPrefix); ok {
				ch <- result{addr: strings.TrimSpace(addr)}
				go func() { _, _ = io.Copy(io.Discard, stdout) }()
				return
			}
			fmt.Fprintf(os.Stdout, "[%s] %s\n", id, line)
		}
		err := scanner.Err()
		if err == nil {
			err = errors.New("worker exited before announcing its address")
		}
		ch <- result{err: err}
	}()

	timer := time.NewTimer(readyTimeout)
	defer timer.Stop()

	select {
	case r := <-ch:
		if r.err != nil {
			return "", fmt.Errorf("wings: worker %s: %w", id, r.err)
		}
		return r.addr, nil
	case <-timer.C:
		return "", fmt.Errorf("wings: worker %s did not become ready within %s", id, readyTimeout)
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// closePartial releases workers already brought up when a later one fails, so a
// half-built cluster never leaks a process or machine.
func closePartial(ctx context.Context, ws []*workerConn, cause error) error {
	for _, w := range ws {
		_ = w.close(ctx)
	}
	return cause
}

// launchRemote is implemented in remote.go.

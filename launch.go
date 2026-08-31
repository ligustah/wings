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

// readyTimeout bounds how long we wait for a worker to announce its address.
// Generous, because a cold machine may still be unpacking a binary, and a
// wrong answer here looks like a hang rather than a failure.
const readyTimeout = 2 * time.Minute

// launchInProcess runs workers as goroutines on the cluster's own embedded
// durable-streams instance.
//
// All of them on ONE instance, and the coordinator on the same one: there is no
// socket to cross and nothing to dial, because both halves are this process.
// What keeps that from being a special case is that they still talk through
// dsclient over their per-worker streams — the same loop, the same envelopes and
// the same encoding as a worker on a machine in another country. Only the
// backend under the client differs.
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

		// ownsClient is false: the engine belongs to the cluster and outlives
		// any one worker, so a retired worker must not close it.
		w, err := c.connect(id, client, false)
		if err != nil {
			return nil, closePartial(ctx, out, err)
		}
		w.node = node

		// Bound to the WORKER's context, so retiring one ends only its loop —
		// and w.close waits for it before releasing anything it reads through.
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			if err := node.run(w.ctx); err != nil && w.ctx.Err() == nil {
				c.log.Error("wings: in-process worker stopped", "worker", id, "err", err)
			}
		}()

		out = append(out, w)
	}
	return out, nil
}

// launchLocalProcess runs each worker as a child copy of this binary.
//
// No cross-compilation: the child is this exact executable on this exact
// machine, which is the whole reason this target is the cheap way to test the
// process boundary.
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
	w.exited = make(chan struct{})
	// One owner for Wait, so the tail can ask whether this worker is gone
	// without racing anyone for the answer.
	go func() {
		_, _ = cmd.Process.Wait()
		close(w.exited)
	}()

	c.log.Info("wings: local worker started", "worker", id, "addr", addr, "pid", cmd.Process.Pid)
	return w, nil
}

// workerEnv is the whole coordinator-to-worker contract.
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
	// Carried explicitly, because a worker in another process shares nothing
	// with the Config that set it. Leaving it out was a real bug: JobTimeout
	// bound in-process jobs and silently did nothing anywhere else, so the
	// guarantee changed with the target.
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
				// Keep the pipe moving; the worker logs to stdout for the rest
				// of its life.
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
// half-built cluster never leaks a process or a machine.
func closePartial(ctx context.Context, ws []*workerConn, cause error) error {
	for _, w := range ws {
		_ = w.close(ctx)
	}
	return cause
}

// launchRemote is implemented in remote.go.

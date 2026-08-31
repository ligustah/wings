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

	"github.com/ligustah/durable_streams/broker/client/dsremote"
)

// readyTimeout bounds how long we wait for a worker to announce its address.
// Generous, because a cold machine may still be unpacking a binary, and a
// wrong answer here looks like a hang rather than a failure.
const readyTimeout = 2 * time.Minute

// launchInProcess runs workers as goroutines over in-memory brokers.
//
// The coordinator holds each worker's backend directly, so a call crosses no
// socket and nothing is marshalled — but it is the same worker loop, the same
// streams and the same encoding as the other two targets.
func (c *Cluster) launchInProcess(ctx context.Context, dir string, n int) ([]*workerConn, error) {
	var out []*workerConn
	for i := range n {
		id := fmt.Sprintf("inproc-%d", i)
		node, err := startWorkerNode(ctx, workerConfig{
			id:          id,
			dir:         filepath.Join(dir, id),
			concurrency: c.cfg.Concurrency,
			timeout:     c.cfg.JobTimeout,
			log:         c.log,
		})
		if err != nil {
			return nil, closePartial(ctx, out, err)
		}

		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			if err := node.run(c.ctx); err != nil && c.ctx.Err() == nil {
				c.log.Error("wings: in-process worker stopped", "worker", id, "err", err)
			}
		}()

		// ownsClient is false: the node closes this backend itself, and closing
		// it twice takes the broker down under the half still using it.
		w, err := c.connect(id, node.backend(), false)
		if err != nil {
			_ = node.close()
			return nil, closePartial(ctx, out, err)
		}
		w.node = node
		out = append(out, w)
	}
	return out, nil
}

// launchLocalProcess runs each worker as a child copy of this binary.
//
// No cross-compilation: the child is this exact executable on this exact
// machine, which is the whole reason this target is the cheap way to test the
// process boundary.
func (c *Cluster) launchLocalProcess(ctx context.Context, dir string, n int) ([]*workerConn, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("wings: locate this executable: %w", err)
	}

	var out []*workerConn
	for i := range n {
		id := fmt.Sprintf("local-%d", i)
		w, err := c.spawnLocal(ctx, exe, id, filepath.Join(dir, id))
		if err != nil {
			return nil, closePartial(ctx, out, err)
		}
		out = append(out, w)
	}
	return out, nil
}

func (c *Cluster) spawnLocal(ctx context.Context, exe, id, dir string) (*workerConn, error) {
	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), workerEnv(id, "127.0.0.1:0", dir, c.cfg.Concurrency)...)
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

	backend, err := dsremote.Dial([]string{addr})
	if err != nil {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, fmt.Errorf("wings: dial worker %s at %s: %w", id, addr, err)
	}

	w, err := c.connect(id, backend, true)
	if err != nil {
		_ = backend.Close()
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
		return nil, err
	}
	w.proc = cmd.Process
	c.log.Info("wings: local worker started", "worker", id, "addr", addr, "pid", cmd.Process.Pid)
	return w, nil
}

// workerEnv is the whole coordinator-to-worker contract.
func workerEnv(id, listen, dir string, concurrency int) []string {
	env := []string{
		envMode + "=" + modeWorker,
		envWorkerID + "=" + id,
		envListen + "=" + listen,
		envDir + "=" + dir,
	}
	if concurrency > 0 {
		env = append(env, envConcurrency+"="+strconv.Itoa(concurrency))
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

package wings

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/ligustah/durable_streams/broker/client/dsremote"
	"github.com/ligustah/wings/internal/payload"
)

const (
	// remoteWorkDir is where the worker binary and its data live on a machine.
	remoteWorkDir = "/opt/wings"
	// remoteDialTimeout bounds waiting for a deployed worker's broker to answer
	// through the tunnel.
	remoteDialTimeout = 90 * time.Second
)

// launchRemote provisions machines, deploys this program to each, and connects
// to the broker every one of them starts.
//
// The binary is cross-compiled once and uploaded n times: the machines are
// identical by construction, so compiling per machine would be the same work
// repeated.
func (c *Cluster) launchRemote(ctx context.Context, n int) ([]*workerConn, error) {
	if c.cfg.Target.prov == nil {
		return nil, errors.New("wings: Remote target has no Provisioner")
	}

	binary, err := c.workerBinary(ctx)
	if err != nil {
		return nil, err
	}

	machines, err := c.cfg.Target.prov.Provision(ctx, n)
	if err != nil {
		return nil, fmt.Errorf("wings: provision %d machines: %w", n, err)
	}

	conns := make([]*workerConn, len(machines))
	errs := make([]error, len(machines))

	var wg sync.WaitGroup
	for i, m := range machines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conns[i], errs[i] = c.deploy(ctx, m, binary, fmt.Sprintf("remote-%d", i))
		}()
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		// Release every machine, including the ones that came up fine — a
		// cluster missing workers it was asked for is not the cluster the
		// caller asked for, and the rest would bill on unattended.
		release := context.WithoutCancel(ctx)
		for i, m := range machines {
			if conns[i] != nil {
				_ = conns[i].close(release)
				continue
			}
			_ = m.Close(release)
		}
		return nil, err
	}
	return conns, nil
}

// deploy puts the worker on one machine and connects to it.
func (c *Cluster) deploy(ctx context.Context, m Machine, binary, id string) (*workerConn, error) {
	remoteBin := path.Join(remoteWorkDir, "worker")

	c.log.Info("wings: uploading worker", "machine", m.ID())
	if err := m.Upload(ctx, binary, remoteBin); err != nil {
		return nil, fmt.Errorf("wings: upload to %s: %w", m.ID(), err)
	}

	env := map[string]string{
		envMode:     modeWorker,
		envWorkerID: id,
		// Loopback only. The broker authenticates nobody, so the tunnel is what
		// stands between it and the internet; binding 0.0.0.0 here would put an
		// open one on a public IP.
		envListen: fmt.Sprintf("127.0.0.1:%d", defaultRemotePort),
		envDir:    path.Join(remoteWorkDir, "data"),
	}
	if c.cfg.Concurrency > 0 {
		env[envConcurrency] = fmt.Sprint(c.cfg.Concurrency)
	}
	if c.cfg.JobTimeout > 0 {
		env[envJobTimeout] = c.cfg.JobTimeout.String()
	}

	c.log.Info("wings: starting worker", "machine", m.ID())
	if err := m.Start(ctx, remoteBin, env); err != nil {
		return nil, fmt.Errorf("wings: start worker on %s: %w", m.ID(), err)
	}

	local, err := m.Forward(ctx, defaultRemotePort)
	if err != nil {
		return nil, fmt.Errorf("wings: tunnel to %s: %w", m.ID(), err)
	}

	// The worker is still booting its broker behind the tunnel, so the first
	// dials legitimately fail. We poll rather than read a ready line: stdout
	// went to a log file on the machine when the process was detached, which is
	// what lets it outlive the SSH session that started it.
	backend, err := dialUntilReady(ctx, local, m.ID())
	if err != nil {
		return nil, err
	}

	w, err := c.connect(id, backend, true)
	if err != nil {
		_ = backend.Close()
		return nil, err
	}
	w.machine = m
	c.log.Info("wings: remote worker ready", "worker", id, "machine", m.ID(), "via", local)
	return w, nil
}

func dialUntilReady(ctx context.Context, addr, machineID string) (*dsremote.Client, error) {
	deadline, cancel := context.WithTimeout(ctx, remoteDialTimeout)
	defer cancel()

	var lastErr error
	for {
		backend, err := dsremote.Dial([]string{addr})
		if err == nil {
			// Dial may succeed against a tunnel whose far end is not serving
			// yet, so ask a question only a live broker can answer.
			if _, err = backend.ListStreams(deadline); err == nil {
				return backend, nil
			}
			_ = backend.Close()
		}
		lastErr = err

		select {
		case <-deadline.Done():
			return nil, fmt.Errorf("wings: worker on %s never answered at %s within %s: %w",
				machineID, addr, remoteDialTimeout, lastErr)
		case <-time.After(2 * time.Second):
		}
	}
}

// workerBinary produces the binary to deploy.
//
// A coordinator built by `wings build` carries its worker, so this is an
// extraction and the machine running it needs no Go toolchain and no source —
// which is the point of that command. A coordinator built by plain `go build`
// carries nothing, and cross-compiling one on the spot is the fallback: it
// still works, it just requires the toolchain and the module source to be
// present, and it says so.
func (c *Cluster) workerBinary(ctx context.Context) (string, error) {
	dir := c.buildDir()

	bin, meta, err := payload.ExtractTo(dir)
	if err == nil {
		c.log.Info("wings: using embedded worker", "platform", meta.Platform(), "bytes", meta.Size)
		return bin, nil
	}
	if !errors.Is(err, payload.ErrNoPayload) {
		return "", err
	}

	c.log.Info("wings: this binary carries no worker; cross-compiling one " +
		"(build with `wings build` to embed it and drop the toolchain requirement)")
	return buildWorker(ctx, c.cfg.Build, dir, c.log)
}

func (c *Cluster) buildDir() string {
	if c.tmpDir != "" {
		return filepath.Join(c.tmpDir, "build")
	}
	return filepath.Join(c.cfg.Dir, "build")
}

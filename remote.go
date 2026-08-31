package wings

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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

	image, err := c.workerImage(ctx)
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
			conns[i], errs[i] = c.deploy(ctx, m, image, c.workerID("remote"))
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
func (c *Cluster) deploy(ctx context.Context, m Machine, image *workerImage, id string) (*workerConn, error) {
	remoteBin := path.Join(remoteWorkDir, "worker")

	c.log.Info("wings: uploading worker", "machine", m.ID(), "bytes", image.size)
	src, err := image.open()
	if err != nil {
		return nil, err
	}
	err = m.Upload(ctx, src, image.size, remoteBin)
	src.Close()
	if err != nil {
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

	w, err := c.connectBackend(id, backend)
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

// workerImage is the worker to deploy, and where its bytes come from.
//
// Two sources, because there are two ways a coordinator can have a worker: one
// compiled into it, which is already in memory, or one cross-compiled on the
// spot, which go build wrote to a file. Neither is converted into the other —
// the embedded one is never written to disk just to have a path, and the built
// one is never slurped into memory just to have bytes.
type workerImage struct {
	blob []byte // embedded
	path string // cross-compiled
	size int64
}

// open returns a fresh reader over the image.
//
// Fresh per call, because machines are deployed to in parallel and each needs
// its own position in the stream. Safe to call concurrently: the byte slice is
// never written after it is set, and a file gets its own handle each time.
func (w *workerImage) open() (io.ReadCloser, error) {
	if w.blob != nil {
		return io.NopCloser(bytes.NewReader(w.blob)), nil
	}
	f, err := os.Open(w.path)
	if err != nil {
		return nil, fmt.Errorf("wings: open worker binary: %w", err)
	}
	return f, nil
}

// workerImage produces the worker to deploy.
//
// A coordinator built by `wings build` carries its worker, so this is a
// decompression into memory and the machine running it needs no Go toolchain
// and no source — which is the point of that command. A coordinator built by
// plain `go build` carries nothing, and cross-compiling one on the spot is the
// fallback: it still works, it just requires the toolchain and the module
// source to be present, and it says so.
func (c *Cluster) workerImage(ctx context.Context) (*workerImage, error) {
	blob, meta, err := payload.Get()
	if err == nil {
		c.log.Info("wings: using embedded worker", "platform", meta.Platform(), "bytes", meta.Size)
		return &workerImage{blob: blob, size: int64(len(blob))}, nil
	}
	if !errors.Is(err, payload.ErrNoPayload) {
		return nil, err
	}

	c.log.Info("wings: this binary carries no worker; cross-compiling one " +
		"(build with `wings build` to embed it and drop the toolchain requirement)")
	bin, err := buildWorker(ctx, c.cfg.Build, c.buildDir(), c.log)
	if err != nil {
		return nil, err
	}
	fi, err := os.Stat(bin)
	if err != nil {
		return nil, fmt.Errorf("wings: stat cross-compiled worker: %w", err)
	}
	return &workerImage{path: bin, size: fi.Size()}, nil
}

func (c *Cluster) buildDir() string {
	if c.tmpDir != "" {
		return filepath.Join(c.tmpDir, "build")
	}
	return filepath.Join(c.cfg.Dir, "build")
}

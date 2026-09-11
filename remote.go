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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ligustah/durable_streams/broker/client"
	"github.com/ligustah/durable_streams/broker/client/dsremote"
	"github.com/ligustah/durable_streams/broker/client/zstd"
	"github.com/ligustah/wings/internal/payload"
)

// dialWorker opens the coordinator's connection to one worker's broker, raising
// the gRPC message limit to maxMessage: an oversized message otherwise looks
// like a dropped connection rather than failing cleanly.
func dialWorker(addr string) (*dsremote.Client, error) {
	return dsremote.Dial([]string{addr},
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		client.WithCompression(zstd.Name),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessage),
			grpc.MaxCallSendMsgSize(maxMessage),
		))
}

const (
	// remoteWorkDir holds the worker binary and its data, relative to the SSH
	// user's home — the one directory every account can write without sudo.
	remoteWorkDir = "wings"
	// remoteDialTimeout bounds waiting for a deployed worker's broker to answer.
	remoteDialTimeout = 90 * time.Second
)

// launchRemote provisions n machines, deploys this program to each, and connects
// to the broker each starts. The binary is cross-compiled once and uploaded n
// times.
func (c *Cluster) launchRemote(ctx context.Context, n int) ([]*workerConn, error) {
	if c.cfg.Target.prov == nil {
		return nil, errors.New("wings: Remote target has no Provisioner")
	}

	image, err := c.workerImage(ctx)
	if err != nil {
		return nil, err
	}

	// Recorded before creation, so a crash mid-provision still leaves a note
	// naming the machine. A failure to record refuses the launch.
	leases := make([]string, n)
	for i := range leases {
		leases[i] = newLease()
		if err := c.machines.write(ctx, machineRecord{Kind: machineIntent, Lease: leases[i]}); err != nil {
			return nil, err
		}
	}

	machines, err := c.cfg.Target.prov.Provision(ctx, leases)
	if err != nil {
		// The intents stay: some machines may exist despite the error.
		for _, lease := range leases {
			_ = c.machines.write(context.WithoutCancel(ctx), machineRecord{
				Kind: machineFailed, Lease: lease, Err: err.Error(),
			})
		}
		return nil, fmt.Errorf("wings: provision %d machines: %w", n, err)
	}

	conns, err := c.deployAll(ctx, machines, image)
	if err != nil {
		return nil, err
	}
	return conns, nil
}

// deployAll puts the worker onto every machine and connects to each, releasing
// all of them if any fails.
func (c *Cluster) deployAll(ctx context.Context, machines []Machine, image *workerImage) ([]*workerConn, error) {
	conns := make([]*workerConn, len(machines))
	errs := make([]error, len(machines))

	var wg sync.WaitGroup
	for i, m := range machines {
		wg.Go(func() {
			id := c.workerID("remote")
			conns[i], errs[i] = c.deploy(ctx, m, image, id)
			if errs[i] == nil {
				conns[i].lease = m.ID()
				errs[i] = c.machines.write(ctx, machineRecord{
					Kind: machineReady, Lease: m.ID(), Worker: id,
				})
			}
		})
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		// Release every machine, including the ones that came up fine, or they
		// bill on unattended.
		release := context.WithoutCancel(ctx)
		for i, m := range machines {
			if conns[i] != nil {
				_ = conns[i].close(release)
			} else {
				_ = m.Close(release)
			}
			_ = c.machines.write(release, machineRecord{Kind: machineReleased, Lease: m.ID()})
		}
		return nil, err
	}
	return conns, nil
}

// reattach recovers the machines a previous coordinator left running: every
// unreleased lease is offered to the provisioner, and what comes back is resumed
// or destroyed. A machine whose worker has died is destroyed, not redeployed.
func (c *Cluster) reattach(ctx context.Context) ([]*workerConn, error) {
	if c.cfg.Target.kind != targetRemote || c.cfg.Target.prov == nil {
		return nil, nil
	}
	leases, err := c.machines.outstanding(ctx)
	if err != nil {
		return nil, err
	}
	if len(leases) == 0 {
		return nil, nil
	}

	re, ok := c.cfg.Target.prov.(Reattacher)
	if !ok {
		// Nothing can be recovered; say so loudly, since those machines may still
		// be billing, and close the record so it is not retried forever.
		c.log.Error("wings: machines were left running by a previous run and this provider "+
			"cannot reattach; they must be cleaned up by hand",
			"leases", leases, "provider", fmt.Sprintf("%T", c.cfg.Target.prov))
		for _, lease := range leases {
			_ = c.machines.write(ctx, machineRecord{
				Kind: machineReleased, Lease: lease, Err: "provider cannot reattach; not cleaned up",
			})
		}
		return nil, nil
	}

	c.log.Info("wings: looking for machines left by a previous run", "leases", len(leases))
	found, err := re.Reattach(ctx, leases)
	if err != nil {
		return nil, fmt.Errorf("wings: reattach: %w", err)
	}

	alive := map[string]Machine{}
	for _, m := range found {
		alive[m.ID()] = m
	}
	// A lease nothing came back for is gone; close its record.
	for _, lease := range leases {
		if _, ok := alive[lease]; !ok {
			_ = c.machines.write(ctx, machineRecord{
				Kind: machineReleased, Lease: lease, Err: "not found on reattach",
			})
		}
	}
	if len(found) == 0 {
		return nil, nil
	}

	// All at once: a dial to a worker that is not answering costs its whole
	// timeout, and serially those add up to minutes.
	conns := make([]*workerConn, len(found))
	release := context.WithoutCancel(ctx)
	var wg sync.WaitGroup
	for i, m := range found {
		wg.Go(func() {
			w, err := c.reconnect(ctx, m)
			if err != nil {
				c.log.Warn("wings: could not resume a recovered machine, destroying it",
					"lease", m.ID(), "err", err)
				_ = m.Close(release)
				_ = c.machines.write(release, machineRecord{
					Kind: machineReleased, Lease: m.ID(), Err: err.Error(),
				})
				return
			}
			conns[i] = w
		})
	}
	wg.Wait()

	var out []*workerConn
	for _, w := range conns {
		if w != nil {
			out = append(out, w)
		}
	}
	if len(out) > 0 {
		c.log.Info("wings: resumed machines from a previous run", "workers", len(out))
	}
	return out, nil
}

// reconnect opens a tunnel to a recovered machine and picks its worker back up.
// The binary is already there and running detached, so this only re-establishes
// access.
func (c *Cluster) reconnect(ctx context.Context, m Machine) (*workerConn, error) {
	// The worker's streams are named after its id, so recovering the name
	// recovers the queue. Asked first: a machine with no recorded worker has
	// nothing to dial.
	id, err := c.workerIDFor(ctx, m.ID())
	if err != nil {
		return nil, err
	}

	local, err := m.Forward(ctx, defaultRemotePort)
	if err != nil {
		return nil, fmt.Errorf("wings: tunnel to %s: %w", m.ID(), err)
	}

	backend, err := dialUntilReady(ctx, local, m.ID())
	if err != nil {
		return nil, fmt.Errorf("wings: worker on %s did not answer: %w", m.ID(), err)
	}

	w, err := c.connectBackend(id, backend)
	if err != nil {
		_ = backend.Close()
		return nil, err
	}
	w.machine = m
	w.lease = m.ID()
	return w, nil
}

// workerIDFor recovers the worker id that was running on a machine.
func (c *Cluster) workerIDFor(ctx context.Context, lease string) (string, error) {
	id, err := c.machines.workerFor(ctx, lease)
	if err != nil {
		return "", err
	}
	if id == "" {
		return "", fmt.Errorf("wings: machine %s has no recorded worker; it never finished starting one", lease)
	}
	return id, nil
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
		// Loopback only: the broker authenticates nobody, so the tunnel is what
		// keeps it off the internet.
		envListen: fmt.Sprintf("127.0.0.1:%d", defaultRemotePort),
		envDir:    path.Join(remoteWorkDir, "data"),

		envCompression: fmt.Sprint(int(streamCompression)),
	}
	if c.cfg.Concurrency > 0 {
		env[envConcurrency] = fmt.Sprint(c.cfg.Concurrency)
	}
	if c.cfg.JobTimeout > 0 {
		env[envJobTimeout] = c.cfg.JobTimeout.String()
	}
	if c.cfg.CommitInterval > 0 {
		env[envCommitInterval] = c.cfg.CommitInterval.String()
	}

	c.log.Info("wings: starting worker", "machine", m.ID())
	if err := m.Start(ctx, remoteBin, env); err != nil {
		return nil, fmt.Errorf("wings: start worker on %s: %w", m.ID(), err)
	}

	local, err := m.Forward(ctx, defaultRemotePort)
	if err != nil {
		return nil, fmt.Errorf("wings: tunnel to %s: %w", m.ID(), err)
	}

	// The broker is still booting, so the first dials fail; poll for it.
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
		backend, err := dialWorker(addr)
		if err == nil {
			// A dial can succeed before the broker serves, so ask it something.
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

// workerImage is the worker to deploy: bytes embedded in the coordinator, or a
// path to a cross-compiled binary.
type workerImage struct {
	blob []byte // embedded
	path string // cross-compiled
	size int64
}

// open returns a fresh reader over the image, safe to call concurrently for the
// parallel deploys.
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

// workerImage produces the worker to deploy: the embedded one from `wings build`,
// or a cross-compiled fallback that needs a Go toolchain and the module source.
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
	return filepath.Join(c.dir, "build")
}

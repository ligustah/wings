package wings

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

// fakeCloud is a Provisioner whose "machines" are child processes on this
// machine.
//
// Real enough to test what matters: a machine outlives the coordinator that
// made it, has to be found again by its lease, and costs something until it is
// destroyed. What it skips is the cloud API and the network, neither of which is
// what reattachment gets wrong.
type fakeCloud struct {
	t   *testing.T
	dir string

	mu    sync.Mutex
	live  map[string]*fakeMachine // lease -> machine, surviving across clusters
	calls struct {
		provision []string
		reattach  []string
	}

	// failProvisionAfter makes Provision create this many machines and then
	// fail, which is how a crash mid-creation is staged.
	failProvisionAfter int
	// noReattach makes this a plain Provisioner, to check what happens when a
	// provider cannot recover anything.
	noReattach bool
}

func newFakeCloud(t *testing.T) *fakeCloud {
	c := &fakeCloud{t: t, dir: t.TempDir(), live: map[string]*fakeMachine{}}
	t.Cleanup(func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, m := range c.live {
			m.kill()
		}
	})
	return c
}

func (c *fakeCloud) Provision(ctx context.Context, leases []string) ([]Machine, error) {
	c.mu.Lock()
	c.calls.provision = append(c.calls.provision, leases...)
	limit := len(leases)
	if c.failProvisionAfter > 0 && c.failProvisionAfter < limit {
		limit = c.failProvisionAfter
	}
	c.mu.Unlock()

	var out []Machine
	for i, lease := range leases {
		if i >= limit {
			// The machines already made are deliberately LEFT RUNNING and
			// untracked by us, exactly as a cloud would leave them. Only the
			// coordinator's write-ahead record can find them now.
			return nil, errors.New("fake cloud: out of capacity")
		}
		m := &fakeMachine{lease: lease, cloud: c, dir: filepath.Join(c.dir, lease)}
		c.mu.Lock()
		c.live[lease] = m
		c.mu.Unlock()
		out = append(out, m)
	}
	return out, nil
}

func (c *fakeCloud) reattachImpl(ctx context.Context, leases []string) ([]Machine, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls.reattach = append(c.calls.reattach, leases...)

	var out []Machine
	for _, lease := range leases {
		if m, ok := c.live[lease]; ok && m.exists() {
			out = append(out, m)
		}
	}
	return out, nil
}

// leases reports which machines still exist, which is the question the whole
// write-ahead record exists to answer.
func (c *fakeCloud) leases() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for lease, m := range c.live {
		if m.exists() {
			out = append(out, lease)
		}
	}
	return out
}

// reattachingCloud is fakeCloud with the optional capability.
type reattachingCloud struct{ *fakeCloud }

func (c reattachingCloud) Reattach(ctx context.Context, leases []string) ([]Machine, error) {
	return c.reattachImpl(ctx, leases)
}

func (c *fakeCloud) provisioner() Provisioner {
	if c.noReattach {
		return c
	}
	return reattachingCloud{c}
}

// fakeMachine is a worker process standing in for a VM.
type fakeMachine struct {
	lease string
	cloud *fakeCloud
	dir   string

	mu     sync.Mutex
	proc   *os.Process
	port   string
	closed bool
}

func (m *fakeMachine) ID() string { return m.lease }

func (m *fakeMachine) Upload(ctx context.Context, src io.Reader, size int64, remotePath string) error {
	if err := os.MkdirAll(m.dir, 0o755); err != nil {
		return err
	}
	// Discarded: the "machine" runs this test binary, not an uploaded one. The
	// bytes still have to be consumed, since Upload promises to read size of
	// them.
	n, err := io.CopyN(io.Discard, src, size)
	if err != nil {
		return fmt.Errorf("fake upload: after %d of %d bytes: %w", n, size, err)
	}
	return nil
}

func (m *fakeMachine) Start(ctx context.Context, cmd string, env map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.proc != nil {
		return errors.New("fake machine: already started")
	}

	exe, err := os.Executable()
	if err != nil {
		return err
	}
	// A port of its own. The coordinator names one fixed port because on a real
	// machine the worker has that port to itself; here every "machine" shares
	// this host's, so they are handed an ephemeral one and Forward reports back
	// where it actually landed — which is what a tunnel does anyway.
	const listen = "127.0.0.1:0"

	c := exec.Command(exe)
	c.Env = append(os.Environ(), envMode+"="+modeWorker)
	for k, v := range env {
		if k == envMode {
			continue
		}
		if k == envListen {
			v = listen
		}
		if k == envDir {
			v = m.dir
		}
		c.Env = append(c.Env, k+"="+v)
	}
	c.Stderr = os.Stderr

	stdout, err := c.StdoutPipe()
	if err != nil {
		return err
	}
	if err := c.Start(); err != nil {
		return err
	}
	addr, err := awaitReady(ctx, stdout, m.lease)
	if err != nil {
		_ = c.Process.Kill()
		return err
	}
	m.proc, m.port = c.Process, addr
	return nil
}

// Forward hands back the address the worker is really on. A tunnel to a machine
// that is already local has nothing to do.
func (m *fakeMachine) Forward(ctx context.Context, remotePort int) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.port == "" {
		return "", errors.New("fake machine: not started")
	}
	return m.port, nil
}

func (m *fakeMachine) Close(ctx context.Context) error {
	m.kill()
	m.cloud.mu.Lock()
	delete(m.cloud.live, m.lease)
	m.cloud.mu.Unlock()
	return nil
}

func (m *fakeMachine) kill() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.proc != nil && !m.closed {
		_ = m.proc.Kill()
		_, _ = m.proc.Wait()
	}
	m.closed = true
}

// exists reports whether the machine is still out there — which is what the
// record is trying to keep track of, and what a cloud would still be billing
// for. A machine nobody ever started a worker on exists just as expensively as
// one that is busy.
func (m *fakeMachine) exists() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.closed
}

// startRemote brings a cluster up on the fake cloud with a persistent Dir, so
// the record survives the cluster.
func startRemote(t *testing.T, dir string, cloud *fakeCloud, workers int) *Cluster {
	t.Helper()
	c, err := Start(t.Context(), Config{
		Target:  Remote(cloud.provisioner()),
		Dir:     dir,
		Workers: workers,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	return c
}

// abandon simulates the coordinator dying: everything it held goes away without
// any machine being destroyed, which is exactly what a crash looks like from
// the cloud's side.
func abandon(t *testing.T, c *Cluster) {
	t.Helper()
	c.cancel()
	c.wg.Wait()
	c.journal.close()
	if err := c.closeShared(); err != nil {
		t.Fatalf("closing the coordinator's streams: %v", err)
	}
}

// THE POINT: a coordinator that dies must not lose its machines. They are still
// running and still billing, and only its own record can find them again.
func TestARestartedCoordinatorRecoversItsMachines(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	cloud := newFakeCloud(t)
	dir := t.TempDir()

	first := startRemote(t, dir, cloud, 2)
	if got, err := Map(first.Bind(t.Context()), double, []int{1, 2, 3}); err != nil {
		t.Fatalf("Map: %v", err)
	} else if len(got) != 3 || got[0] != 2 {
		t.Fatalf("got %v", got)
	}

	before := cloud.leases()
	if len(before) != 2 {
		t.Fatalf("the cloud holds %d machines, want 2", len(before))
	}
	abandon(t, first)

	// The machines are still up. A second coordinator over the same directory
	// must find them rather than provision more.
	second := startRemote(t, dir, cloud, 2)
	defer func() {
		if err := second.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	if got := second.Workers(); got != 2 {
		t.Fatalf("the restarted coordinator has %d workers, want 2", got)
	}
	if after := cloud.leases(); len(after) != 2 {
		t.Fatalf("the cloud holds %d machines after a restart, want 2 — "+
			"a recovered machine must not be provisioned again", len(after))
	}

	cloud.mu.Lock()
	provisioned := len(cloud.calls.provision)
	cloud.mu.Unlock()
	if provisioned != 2 {
		t.Errorf("the cloud was asked for %d machines in total, want 2", provisioned)
	}

	// And it must actually work afterwards.
	got, err := Map(second.Bind(t.Context()), double, []int{4, 5})
	if err != nil {
		t.Fatalf("Map after restart: %v", err)
	}
	if len(got) != 2 || got[0] != 8 || got[1] != 10 {
		t.Fatalf("got %v, want [8 10]", got)
	}
}

// The case the write-ahead record exists for: the coordinator dies while the
// cloud is mid-creation. The machines it made are running, and nothing but the
// intent written BEFORE the call knows they are there.
func TestMachinesCreatedBeforeAFailedProvisionAreNotLost(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	cloud := newFakeCloud(t)
	cloud.failProvisionAfter = 1 // make one, then fail
	dir := t.TempDir()

	_, err := Start(t.Context(), Config{
		Target:  Remote(cloud.provisioner()),
		Dir:     dir,
		Workers: 2,
	})
	if err == nil {
		t.Fatal("want Start to fail when provisioning does")
	}

	// One machine is out there, untracked by anything except the record.
	if got := cloud.leases(); len(got) != 1 {
		t.Fatalf("the cloud holds %d machines, want the 1 that was created before the failure", len(got))
	}

	// A later start must find it. It cannot be resumed — no worker was ever
	// started on it — so it must be destroyed rather than left billing.
	cloud.failProvisionAfter = 0
	second := startRemote(t, dir, cloud, 1)
	defer func() {
		if err := second.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	cloud.mu.Lock()
	asked := len(cloud.calls.reattach)
	cloud.mu.Unlock()
	if asked == 0 {
		t.Fatal("the orphaned machine was never looked for; the intent was written for nothing")
	}
	if got := second.Workers(); got != 1 {
		t.Fatalf("the second coordinator has %d workers, want 1", got)
	}
	// The orphan is gone and only the new machine remains.
	if got := cloud.leases(); len(got) != 1 {
		t.Fatalf("the cloud holds %d machines; the orphan should have been destroyed", len(got))
	}
}

// A clean Stop closes every lease, so the next start has nothing to look for.
func TestAfterACleanStopThereIsNothingToReattach(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	cloud := newFakeCloud(t)
	dir := t.TempDir()

	c := startRemote(t, dir, cloud, 1)
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := cloud.leases(); len(got) != 0 {
		t.Fatalf("the cloud still holds %d machines after a clean stop", len(got))
	}

	second := startRemote(t, dir, cloud, 1)
	defer func() {
		if err := second.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	cloud.mu.Lock()
	asked := len(cloud.calls.reattach)
	cloud.mu.Unlock()
	if asked != 0 {
		t.Errorf("reattachment looked for %d leases after a clean stop; want none", asked)
	}
}

// A provider that cannot reattach must say so loudly and close the leases,
// rather than hunting for them on every start forever.
func TestAProviderThatCannotReattachClosesTheLeases(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	cloud := newFakeCloud(t)
	cloud.noReattach = true
	dir := t.TempDir()

	first := startRemote(t, dir, cloud, 1)
	abandon(t, first)

	second := startRemote(t, dir, cloud, 1)
	defer func() {
		if err := second.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	// It provisioned a fresh one rather than recovering, which is the only
	// thing it can do.
	if got := second.Workers(); got != 1 {
		t.Fatalf("the second coordinator has %d workers, want 1", got)
	}

	// And the lease must be closed, or every future start repeats the hunt.
	live, err := second.machines.outstanding(t.Context())
	if err != nil {
		t.Fatalf("outstanding: %v", err)
	}
	if len(live) != 1 {
		t.Fatalf("%d leases are still open, want only the live one", len(live))
	}
}

func TestTheIntentIsWrittenBeforeTheMachineIsCreated(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}

	cloud := newFakeCloud(t)
	dir := t.TempDir()

	c := startRemote(t, dir, cloud, 1)
	defer func() {
		if err := c.Stop(context.Background()); err != nil {
			t.Errorf("Stop: %v", err)
		}
	}()

	stream, err := c.shared.OpenStream[machineRecord](machineStream)
	if err != nil {
		t.Fatalf("open %s: %v", machineStream, err)
	}
	recs, err := stream.Read(t.Context(), 0, 64)
	if err != nil {
		t.Fatalf("read %s: %v", machineStream, err)
	}
	if len(recs) < 2 {
		t.Fatalf("the record holds %d entries, want at least an intent and a ready", len(recs))
	}
	if got := recs[0].Record.Kind; got != machineIntent {
		t.Fatalf("the first record is %q; the intent must be written before anything is created", got)
	}

	cloud.mu.Lock()
	asked := cloud.calls.provision
	cloud.mu.Unlock()
	if len(asked) != 1 || asked[0] != recs[0].Record.Lease {
		t.Fatalf("the cloud was asked for %v but the intent named %q; "+
			"the recorded lease must be the one that was created", asked, recs[0].Record.Lease)
	}
}

func TestLeasesAreUsableAsCloudNames(t *testing.T) {
	seen := map[string]bool{}
	for range 200 {
		l := newLease()
		if len(l) != 10 {
			t.Fatalf("lease %q is %d characters", l, len(l))
		}
		if l[0] < 'a' || l[0] > 'z' {
			t.Fatalf("lease %q does not start with a letter; several clouds refuse that", l)
		}
		for _, r := range l {
			if (r < 'a' || r > 'z') && (r < '0' || r > '9') {
				t.Fatalf("lease %q contains %q, which is not lowercase alphanumeric", l, r)
			}
		}
		if seen[l] {
			t.Fatalf("lease %q was minted twice", l)
		}
		seen[l] = true
	}
}

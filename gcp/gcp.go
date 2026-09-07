package gcp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/ligustah/wings"
	"github.com/ligustah/wings/internal/sshx"
	"golang.org/x/crypto/ssh"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// Config describes the machines to provision on Google Compute Engine.
type Config struct {
	// Project is the GCP project id. Required.
	Project string
	// Zone is where instances are created, e.g. "europe-west1-b". Required.
	Zone string

	// MachineType defaults to "e2-standard-4".
	MachineType string
	// SourceImage defaults to the latest Debian 12. Any image works as long as
	// it runs a static linux/amd64 binary and has sshd.
	SourceImage string
	// DiskSizeGB defaults to 20.
	DiskSizeGB int64
	// Network defaults to "global/networks/default".
	Network string
	// Preemptible requests Spot instances — much cheaper, reclaimable mid-job.
	// wings redispatches a lost worker's work, so this is safe for idempotent
	// work. A preempted instance deletes itself rather than stopping.
	Preemptible bool

	// NamePrefix prefixes generated instance names. Defaults to "wings".
	NamePrefix string
	// Labels are applied to each instance. Useful for finding strays.
	Labels map[string]string

	// User is the Linux account wings creates and connects as. Defaults to
	// "wings".
	User string
	// HostKey verifies the machine's SSH host key. Nil accepts any key — see
	// [sshx.InsecureIgnoreHostKey] for exactly what that gives up.
	HostKey ssh.HostKeyCallback

	// BootTimeout bounds waiting for an instance to accept SSH. Defaults to
	// 5 minutes.
	BootTimeout time.Duration

	// ClientOptions are passed to the Compute API client. Empty uses
	// Application Default Credentials.
	ClientOptions []option.ClientOption

	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

// New returns a [wings.Provisioner] that creates Compute Engine instances.
// Instances are deleted by [wings.Cluster.Stop], and billed until then.
//
//	wings.Start(ctx, wings.Config{
//		Target:  wings.Remote(gcp.New(gcp.Config{Project: "p", Zone: "europe-west1-b"})),
//		Workers: 4,
//	})
func New(cfg Config) wings.Provisioner { return &gcpProvisioner{cfg: cfg.withDefaults()} }

func (c Config) withDefaults() Config {
	if c.MachineType == "" {
		c.MachineType = "e2-standard-4"
	}
	if c.SourceImage == "" {
		c.SourceImage = "projects/debian-cloud/global/images/family/debian-12"
	}
	if c.DiskSizeGB == 0 {
		c.DiskSizeGB = 20
	}
	if c.Network == "" {
		c.Network = "global/networks/default"
	}
	if c.NamePrefix == "" {
		c.NamePrefix = "wings"
	}
	if c.User == "" {
		c.User = "wings"
	}
	if c.BootTimeout == 0 {
		c.BootTimeout = 5 * time.Minute
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

type gcpProvisioner struct {
	cfg Config

	// One Compute API client for the life of the provisioner, made on first
	// use. Provision and Reattach each used to make their own and hand it to
	// the machines they returned, and nothing closed it once they had: a
	// provisioner that scaled up ten times held ten clients' worth of
	// connections. A provisioner lives as long as its cluster, and so does
	// this.
	// One Compute client for the provisioner's life, made on first use.
	once      sync.Once
	client    *compute.InstancesClient
	clientErr error
}

// instances returns the Compute API client, made once.
func (p *gcpProvisioner) instances(ctx context.Context) (*compute.InstancesClient, error) {
	p.once.Do(func() {
		// Detached from the caller's context: the client outlives the call.
		p.client, p.clientErr = compute.NewInstancesRESTClient(context.WithoutCancel(ctx), p.cfg.ClientOptions...)
		if p.clientErr != nil {
			p.clientErr = fmt.Errorf("wings: compute client: %w", p.clientErr)
		}
	})
	return p.client, p.clientErr
}

// Provision creates one instance per lease in parallel and returns once each
// accepts SSH.
func (p *gcpProvisioner) Provision(ctx context.Context, leases []string) ([]wings.Machine, error) {
	if p.cfg.Project == "" || p.cfg.Zone == "" {
		return nil, fmt.Errorf("gcp: Config needs both Project and Zone")
	}

	signer, authorizedKey, err := ephemeralKey()
	if err != nil {
		return nil, err
	}

	client, err := p.instances(ctx)
	if err != nil {
		return nil, err
	}

	machines := make([]wings.Machine, len(leases))
	errs := make([]error, len(leases))

	var wg sync.WaitGroup
	for i, lease := range leases {
		wg.Go(func() {
			m, err := p.createOne(ctx, client, lease, signer, authorizedKey)
			machines[i], errs[i] = m, err
		})
	}
	wg.Wait()

	// Any failure means none of them: a half-provisioned batch would leave
	// instances running and billing untracked.
	var failed error
	for _, err := range errs {
		if err != nil {
			failed = err
			break
		}
	}
	if failed != nil {
		for _, m := range machines {
			if m != nil {
				_ = m.Close(context.WithoutCancel(ctx))
			}
		}
		return nil, failed
	}
	return machines, nil
}

// instanceName maps a lease to its GCE instance name, deterministically so
// reattachment can find it again.
func (p *gcpProvisioner) instanceName(lease string) string {
	return p.cfg.NamePrefix + "-" + lease
}

// Reattach finds instances a previous coordinator created and lets this one back
// in. The per-run SSH key died with that coordinator, so it installs a fresh
// public key on each instance through the metadata API and connects with that.
func (p *gcpProvisioner) Reattach(ctx context.Context, leases []string) ([]wings.Machine, error) {
	if p.cfg.Project == "" || p.cfg.Zone == "" {
		return nil, fmt.Errorf("gcp: Config needs both Project and Zone")
	}

	signer, authorizedKey, err := ephemeralKey()
	if err != nil {
		return nil, err
	}

	client, err := p.instances(ctx)
	if err != nil {
		return nil, err
	}

	found := make([]wings.Machine, len(leases))
	var wg sync.WaitGroup
	for i, lease := range leases {
		wg.Go(func() {
			m, err := p.reattachOne(ctx, client, lease, signer, authorizedKey)
			if err != nil {
				// Expected: a lease whose machine was preempted or never created.
				p.cfg.Logger.Info("wings: machine not recovered", "lease", lease, "err", err)
				return
			}
			found[i] = m
		})
	}
	wg.Wait()

	var out []wings.Machine
	for _, m := range found {
		if m != nil {
			out = append(out, m)
		}
	}
	return out, nil
}

func (p *gcpProvisioner) reattachOne(
	ctx context.Context,
	client *compute.InstancesClient,
	lease string,
	signer ssh.Signer,
	authorizedKey string,
) (wings.Machine, error) {
	name := p.instanceName(lease)
	log := p.cfg.Logger.With("instance", name, "lease", lease)

	got, err := client.Get(ctx, &computepb.GetInstanceRequest{
		Project:  p.cfg.Project,
		Zone:     p.cfg.Zone,
		Instance: name,
	})
	if err != nil {
		return nil, fmt.Errorf("not found: %w", err)
	}
	if status := got.GetStatus(); status != "RUNNING" {
		// Not serving and never resumed; delete it so a stopped instance does
		// not bill for its disk after the lease closes.
		log.Info("wings: deleting an instance that is not running", "status", status)
		if op, err := client.Delete(ctx, &computepb.DeleteInstanceRequest{
			Project: p.cfg.Project, Zone: p.cfg.Zone, Instance: name,
		}); err != nil {
			log.Warn("wings: could not delete it", "err", err)
		} else if err := op.Wait(ctx); err != nil {
			log.Warn("wings: could not delete it", "err", err)
		}
		return nil, fmt.Errorf("instance is %s, not RUNNING", status)
	}
	ip := externalIP(got)
	if ip == "" {
		return nil, fmt.Errorf("instance has no external IP")
	}

	// created: true so Close deletes this recovered instance like any other.
	m := &gcpMachine{name: name, lease: lease, ip: ip, cfg: p.cfg, client: client, log: log, created: true}

	// The fingerprint is GCE's optimistic-concurrency token for the metadata write.
	meta := got.GetMetadata()
	items := []*computepb.Items{{
		Key:   new("ssh-keys"),
		Value: new(fmt.Sprintf("%s:%s", p.cfg.User, authorizedKey)),
	}}
	for _, it := range meta.GetItems() {
		if it.GetKey() != "ssh-keys" {
			items = append(items, it)
		}
	}

	log.Info("wings: installing a fresh key on a recovered instance")
	op, err := client.SetMetadata(ctx, &computepb.SetMetadataInstanceRequest{
		Project:          p.cfg.Project,
		Zone:             p.cfg.Zone,
		Instance:         name,
		MetadataResource: &computepb.Metadata{Fingerprint: meta.Fingerprint, Items: items},
	})
	if err != nil {
		return nil, fmt.Errorf("install key: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return nil, fmt.Errorf("install key: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, p.cfg.BootTimeout)
	defer cancel()

	conn, err := sshx.Dial(dialCtx, sshx.Config{
		Addr:    ip,
		User:    p.cfg.User,
		Signer:  signer,
		HostKey: p.cfg.HostKey,
	})
	if err != nil {
		return nil, fmt.Errorf("never accepted the new key: %w", err)
	}
	m.ssh = conn

	log.Info("wings: recovered instance", "ip", ip)
	return m, nil
}

func (p *gcpProvisioner) createOne(ctx context.Context, client *compute.InstancesClient, lease string, signer ssh.Signer, authorizedKey string) (wings.Machine, error) {
	name := p.instanceName(lease)
	log := p.cfg.Logger.With("instance", name, "lease", lease)

	inst := &computepb.Instance{
		Name:        new(name),
		MachineType: new(fmt.Sprintf("zones/%s/machineTypes/%s", p.cfg.Zone, p.cfg.MachineType)),
		Disks: []*computepb.AttachedDisk{{
			Boot:       new(true),
			AutoDelete: new(true),
			InitializeParams: &computepb.AttachedDiskInitializeParams{
				SourceImage: new(p.cfg.SourceImage),
				DiskSizeGb:  new(p.cfg.DiskSizeGB),
			},
		}},
		NetworkInterfaces: []*computepb.NetworkInterface{{
			Network: new(p.cfg.Network),
			AccessConfigs: []*computepb.AccessConfig{{
				Name: new("External NAT"),
				Type: new("ONE_TO_ONE_NAT"),
			}},
		}},
		Metadata: &computepb.Metadata{
			Items: []*computepb.Items{
				{
					Key:   new("ssh-keys"),
					Value: new(fmt.Sprintf("%s:%s", p.cfg.User, authorizedKey)),
				},
				// OS Login ignores metadata SSH keys; disable it so the key
				// above is honored regardless of the project default.
				{
					Key:   new("enable-oslogin"),
					Value: new("FALSE"),
				},
			},
		},
		Labels: p.cfg.Labels,
	}
	if p.cfg.Preemptible {
		inst.Scheduling = &computepb.Scheduling{
			ProvisioningModel: new("SPOT"),
			Preemptible:       new(true),
			// Delete on preemption, not stop: wings never restarts one, and a
			// stopped instance still bills for its disk.
			InstanceTerminationAction: new("DELETE"),
		}
	}

	log.Info("wings: creating instance", "type", p.cfg.MachineType, "zone", p.cfg.Zone)
	op, err := client.Insert(ctx, &computepb.InsertInstanceRequest{
		Project:          p.cfg.Project,
		Zone:             p.cfg.Zone,
		InstanceResource: inst,
	})
	if err != nil {
		return nil, fmt.Errorf("wings: create instance %s: %w", name, err)
	}

	// The instance may now exist even if we fail, so every exit deletes it.
	m := &gcpMachine{
		name:    name,
		lease:   lease,
		cfg:     p.cfg,
		client:  client,
		log:     log,
		created: true,
	}
	fail := func(err error) (wings.Machine, error) {
		_ = m.Close(context.WithoutCancel(ctx))
		return nil, err
	}

	if err := op.Wait(ctx); err != nil {
		return fail(fmt.Errorf("wings: wait for instance %s: %w", name, err))
	}

	got, err := client.Get(ctx, &computepb.GetInstanceRequest{
		Project:  p.cfg.Project,
		Zone:     p.cfg.Zone,
		Instance: name,
	})
	if err != nil {
		return fail(fmt.Errorf("wings: describe instance %s: %w", name, err))
	}
	ip := externalIP(got)
	if ip == "" {
		return fail(fmt.Errorf("wings: instance %s has no external IP; wings reaches workers over SSH and needs one", name))
	}
	m.ip = ip

	dialCtx, cancel := context.WithTimeout(ctx, p.cfg.BootTimeout)
	defer cancel()

	log.Info("wings: waiting for ssh", "ip", ip)
	conn, err := sshx.Dial(dialCtx, sshx.Config{
		Addr:    ip,
		User:    p.cfg.User,
		Signer:  signer,
		HostKey: p.cfg.HostKey,
	})
	if err != nil {
		return fail(fmt.Errorf("wings: instance %s never accepted ssh: %w", name, err))
	}
	m.ssh = conn

	log.Info("wings: instance ready")
	return m, nil
}

func externalIP(inst *computepb.Instance) string {
	for _, ni := range inst.GetNetworkInterfaces() {
		for _, ac := range ni.GetAccessConfigs() {
			if ip := ac.GetNatIP(); ip != "" {
				return ip
			}
		}
	}
	return ""
}

// gcpMachine is one Compute Engine instance.
type gcpMachine struct {
	name    string
	lease   string
	ip      string
	cfg     Config
	client  *compute.InstancesClient
	ssh     *sshx.Client
	log     *slog.Logger
	created bool

	once sync.Once
	err  error
}

// ID is the lease the coordinator recorded before the machine existed, which is
// what makes recovery possible.
func (m *gcpMachine) ID() string { return m.lease }

func (m *gcpMachine) Upload(ctx context.Context, src io.Reader, size int64, remotePath string) error {
	return m.ssh.Upload(ctx, src, size, remotePath)
}

func (m *gcpMachine) Start(ctx context.Context, cmd string, env map[string]string) error {
	if m.cfg.Preemptible {
		// The worker polls this URL for the ~30s preemption notice and hands
		// its work over in that window.
		withNotice := make(map[string]string, len(env)+1)
		maps.Copy(withNotice, env)
		withNotice[wings.PreemptionURLEnv] = preemptionURL
		env = withNotice
	}
	return m.ssh.Start(ctx, cmd, env, "/tmp/wings-worker.log")
}

// preemptionURL blocks until the instance is flagged for preemption.
const preemptionURL = "http://metadata.google.internal/computeMetadata/v1/instance/preempted?wait_for_change=true"

func (m *gcpMachine) Forward(ctx context.Context, remotePort int) (string, error) {
	return m.ssh.Forward(ctx, remotePort)
}

// Alive asks the API whether the instance still exists and is running, so a
// preempted worker is given up on in seconds rather than at the reconnect timeout.
func (m *gcpMachine) Alive(ctx context.Context) (bool, error) {
	got, err := m.client.Get(ctx, &computepb.GetInstanceRequest{
		Project:  m.cfg.Project,
		Zone:     m.cfg.Zone,
		Instance: m.name,
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	switch got.GetStatus() {
	case "RUNNING", "STAGING", "PROVISIONING", "REPAIRING":
		return true, nil
	}
	return false, nil
}

// isNotFound reports whether the API said the instance does not exist.
func isNotFound(err error) bool {
	gerr, ok := errors.AsType[*googleapi.Error](err)
	return ok && gerr.Code == http.StatusNotFound
}

// Close deletes the instance. Idempotent, because both a failed bring-up and a
// normal Stop reach it.
func (m *gcpMachine) Close(ctx context.Context) error {
	m.once.Do(func() {
		if m.ssh != nil {
			_ = m.ssh.Close()
		}
		if !m.created {
			return
		}
		m.log.Info("wings: deleting instance")
		op, err := m.client.Delete(ctx, &computepb.DeleteInstanceRequest{
			Project:  m.cfg.Project,
			Zone:     m.cfg.Zone,
			Instance: m.name,
		})
		if err != nil {
			m.err = fmt.Errorf("wings: delete instance %s: %w", m.name, err)
			return
		}
		if err := op.Wait(ctx); err != nil {
			m.err = fmt.Errorf("wings: wait for deletion of %s: %w", m.name, err)
		}
	})
	return m.err
}

// ephemeralKey mints a keypair for this run only, so wings never needs a
// long-lived private key and its credential dies with the cluster.
func ephemeralKey() (ssh.Signer, string, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, "", fmt.Errorf("wings: generate ssh key: %w", err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		return nil, "", fmt.Errorf("wings: ssh signer: %w", err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return nil, "", fmt.Errorf("wings: ssh public key: %w", err)
	}
	line := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub))) + " wings"
	return signer, line, nil
}

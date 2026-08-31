package gcp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"golang.org/x/crypto/ssh"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"

	"github.com/ligustah/wings"
	"github.com/ligustah/wings/internal/sshx"
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
	// Preemptible requests Spot instances — much cheaper, and they can be
	// reclaimed mid-job. wings redispatches what a lost worker owed, so this is
	// a real option rather than a trap, but only if your work is idempotent.
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

// GCP returns a [Provisioner] that creates Compute Engine instances.
//
//	wings.Start(ctx, wings.Config{
//		Target:  wings.Remote(gcp.New(gcp.Config{Project: "p", Zone: "europe-west1-b"})),
//		Workers: 4,
//	})
//
// Instances are deleted by [Cluster.Stop]. They are billed until then.
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
}

// Provision creates n instances in parallel and returns once each accepts SSH.
//
// In parallel because these are minutes, not milliseconds: eight machines
// created in sequence is eight boot times, and the whole point of asking for
// eight is not to wait for them one after another.
func (p *gcpProvisioner) Provision(ctx context.Context, n int) ([]wings.Machine, error) {
	if p.cfg.Project == "" || p.cfg.Zone == "" {
		return nil, fmt.Errorf("gcp: Config needs both Project and Zone")
	}

	signer, authorizedKey, err := ephemeralKey()
	if err != nil {
		return nil, err
	}

	client, err := compute.NewInstancesRESTClient(ctx, p.cfg.ClientOptions...)
	if err != nil {
		return nil, fmt.Errorf("wings: compute client: %w", err)
	}

	run := strings.ToLower(randomToken(6))
	machines := make([]wings.Machine, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("%s-%s-%d", p.cfg.NamePrefix, run, i)
			m, err := p.createOne(ctx, client, name, signer, authorizedKey)
			machines[i], errs[i] = m, err
		}()
	}
	wg.Wait()

	// Any failure means none of them: a half-provisioned cluster that still
	// returned would leave the rest running and billing with nothing tracking
	// them.
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
		_ = client.Close()
		return nil, failed
	}
	return machines, nil
}

func (p *gcpProvisioner) createOne(ctx context.Context, client *compute.InstancesClient, name string, signer ssh.Signer, authorizedKey string) (wings.Machine, error) {
	log := p.cfg.Logger.With("instance", name)

	inst := &computepb.Instance{
		Name:        proto.String(name),
		MachineType: proto.String(fmt.Sprintf("zones/%s/machineTypes/%s", p.cfg.Zone, p.cfg.MachineType)),
		Disks: []*computepb.AttachedDisk{{
			Boot:       proto.Bool(true),
			AutoDelete: proto.Bool(true),
			InitializeParams: &computepb.AttachedDiskInitializeParams{
				SourceImage: proto.String(p.cfg.SourceImage),
				DiskSizeGb:  proto.Int64(p.cfg.DiskSizeGB),
			},
		}},
		NetworkInterfaces: []*computepb.NetworkInterface{{
			Network: proto.String(p.cfg.Network),
			AccessConfigs: []*computepb.AccessConfig{{
				Name: proto.String("External NAT"),
				Type: proto.String("ONE_TO_ONE_NAT"),
			}},
		}},
		Metadata: &computepb.Metadata{
			Items: []*computepb.Items{{
				Key:   proto.String("ssh-keys"),
				Value: proto.String(fmt.Sprintf("%s:%s", p.cfg.User, authorizedKey)),
			}},
		},
		Labels: p.cfg.Labels,
	}
	if p.cfg.Preemptible {
		inst.Scheduling = &computepb.Scheduling{
			ProvisioningModel: proto.String("SPOT"),
			Preemptible:       proto.Bool(true),
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

	// From here on the instance may exist even if we fail, so every exit deletes
	// it rather than leaking a billed VM.
	m := &gcpMachine{
		name:    name,
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
	ip      string
	cfg     Config
	client  *compute.InstancesClient
	ssh     *sshx.Client
	log     *slog.Logger
	created bool

	once sync.Once
	err  error
}

func (m *gcpMachine) ID() string { return m.name }

func (m *gcpMachine) Upload(ctx context.Context, src io.Reader, size int64, remotePath string) error {
	return m.ssh.Upload(ctx, src, size, remotePath)
}

func (m *gcpMachine) Start(ctx context.Context, cmd string, env map[string]string) error {
	return m.ssh.Start(ctx, cmd, env, "/tmp/wings-worker.log")
}

func (m *gcpMachine) Forward(ctx context.Context, remotePort int) (string, error) {
	return m.ssh.Forward(ctx, remotePort)
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

// ephemeralKey mints a keypair for this run only.
//
// Per-run rather than reusing the operator's key: nothing wings creates outlives
// the cluster, so its credential should not either, and it means running wings
// never requires handing it a private key you use for anything else.
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

func randomToken(n int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand does not fail in practice; a time-based fallback would be
		// worse than saying so.
		panic(fmt.Sprintf("wings: read randomness: %v", err))
	}
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

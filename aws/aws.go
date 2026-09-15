// Package aws provisions wings workers as EC2 instances. It mirrors the gcp
// package: instances are reached over SSH through the shared [sshx] transport,
// so only the compute lifecycle here is AWS-specific.
package aws

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2instanceconnect"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/ligustah/wings"
	"github.com/ligustah/wings/internal/sshx"
	"golang.org/x/crypto/ssh"
)

// leaseTag names the tag that records a machine's lease, so [Reattach] can find
// an instance a previous coordinator created.
const leaseTag = "wings-lease"

// al2023Param is the SSM public parameter naming the latest Amazon Linux 2023
// x86_64 AMI in each region, so a default image need not be hard-coded per region.
const al2023Param = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"

// Config describes the machines to provision on EC2.
type Config struct {
	// Region is where instances are created, e.g. "eu-west-1". Required.
	Region string

	// InstanceType defaults to "m5.xlarge" (4 vCPU, 16 GiB).
	InstanceType string
	// AMI is the image to boot. Empty resolves the latest Amazon Linux 2023
	// x86_64 image for the region; any image works as long as it runs a static
	// linux/amd64 binary and has sshd and scp.
	AMI string
	// DiskSizeGB is the root volume size. Defaults to 500.
	DiskSizeGB int32
	// SubnetID places instances in a specific subnet. Empty uses a subnet of the
	// region's default VPC.
	SubnetID string
	// SecurityGroupIDs attach to each instance and must allow inbound SSH from
	// the coordinator. Empty makes wings create one per instance that allows TCP
	// 22 from anywhere and deletes it with the instance.
	SecurityGroupIDs []string
	// Spot requests Spot instances — much cheaper, reclaimable mid-job. wings
	// redispatches a lost worker's work, so this is safe for idempotent work; a
	// reclaimed instance is terminated, not stopped.
	Spot bool

	// NamePrefix prefixes the Name tag of each instance. Defaults to "wings".
	NamePrefix string
	// Tags are applied to each instance on top of the Name and lease tags.
	Tags map[string]string

	// User is the Linux account wings creates and connects as. Defaults to
	// "wings".
	User string
	// HostKey verifies the machine's SSH host key. Nil accepts any key — see
	// [sshx.InsecureIgnoreHostKey] for exactly what that gives up.
	HostKey ssh.HostKeyCallback

	// BootTimeout bounds waiting for an instance to accept SSH. Defaults to
	// 5 minutes.
	BootTimeout time.Duration

	// ConfigOptions are passed to config.LoadDefaultConfig; Region is applied on
	// top. Empty uses the default credential chain and shared config.
	ConfigOptions []func(*config.LoadOptions) error

	// Logger defaults to slog.Default().
	Logger *slog.Logger
}

var (
	_ wings.Reattacher = (*awsProvisioner)(nil)
	_ wings.Prober     = (*awsMachine)(nil)
)

// New returns a [wings.Provisioner] that creates EC2 instances. Instances are
// terminated by [wings.Cluster.Stop], and billed until then.
//
//	wings.Start(ctx, wings.Config{
//		Target:  wings.Remote(aws.New(aws.Config{Region: "eu-west-1"})),
//		Workers: 4,
//	})
func New(cfg Config) wings.Provisioner { return &awsProvisioner{cfg: cfg.withDefaults()} }

func (c Config) withDefaults() Config {
	if c.InstanceType == "" {
		c.InstanceType = "m5.xlarge"
	}
	if c.DiskSizeGB == 0 {
		c.DiskSizeGB = 500
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

type awsProvisioner struct {
	cfg Config

	// Clients for the provisioner's life, made once on first use — a provisioner
	// lives as long as its cluster.
	once      sync.Once
	ec2       *ec2.Client
	connect   *ec2instanceconnect.Client
	ssm       *ssm.Client
	clientErr error
}

// clients builds the AWS clients once.
func (p *awsProvisioner) clients(ctx context.Context) (*ec2.Client, *ec2instanceconnect.Client, *ssm.Client, error) {
	p.once.Do(func() {
		opts := append([]func(*config.LoadOptions) error{config.WithRegion(p.cfg.Region)}, p.cfg.ConfigOptions...)
		cfg, err := config.LoadDefaultConfig(context.WithoutCancel(ctx), opts...)
		if err != nil {
			p.clientErr = fmt.Errorf("wings: aws config: %w", err)
			return
		}
		p.ec2 = ec2.NewFromConfig(cfg)
		p.connect = ec2instanceconnect.NewFromConfig(cfg)
		p.ssm = ssm.NewFromConfig(cfg)
	})
	return p.ec2, p.connect, p.ssm, p.clientErr
}

// Provision creates one instance per lease in parallel and returns once each
// accepts SSH.
func (p *awsProvisioner) Provision(ctx context.Context, leases []string) ([]wings.Machine, error) {
	if p.cfg.Region == "" {
		return nil, errors.New("aws: Config needs a Region")
	}
	if _, _, _, err := p.clients(ctx); err != nil {
		return nil, err
	}
	signer, authorizedKey, err := ephemeralKey()
	if err != nil {
		return nil, err
	}
	ami, err := p.resolveAMI(ctx)
	if err != nil {
		return nil, err
	}

	machines := make([]wings.Machine, len(leases))
	errs := make([]error, len(leases))

	var wg sync.WaitGroup
	for i, lease := range leases {
		wg.Go(func() {
			machines[i], errs[i] = p.createOne(ctx, lease, ami, signer, authorizedKey)
		})
	}
	wg.Wait()

	// Any failure means none of them: a half-provisioned batch would leave
	// instances running and billing untracked.
	for _, err := range errs {
		if err != nil {
			for _, m := range machines {
				if m != nil {
					_ = m.Close(context.WithoutCancel(ctx))
				}
			}
			return nil, err
		}
	}
	return machines, nil
}

// resolveAMI returns the configured AMI, or the region's latest Amazon Linux
// 2023 image when none is set.
func (p *awsProvisioner) resolveAMI(ctx context.Context) (string, error) {
	if p.cfg.AMI != "" {
		return p.cfg.AMI, nil
	}
	out, err := p.ssm.GetParameter(ctx, &ssm.GetParameterInput{Name: awssdk.String(al2023Param)})
	if err != nil {
		return "", fmt.Errorf("wings: resolve default AMI: %w", err)
	}
	return awssdk.ToString(out.Parameter.Value), nil
}

func (p *awsProvisioner) createOne(ctx context.Context, lease, ami string, signer ssh.Signer, authorizedKey string) (wings.Machine, error) {
	name := p.cfg.NamePrefix + "-" + lease
	log := p.cfg.Logger.With("instance", name, "lease", lease)

	subnet := p.cfg.SubnetID
	if subnet == "" {
		var err error
		if subnet, err = p.defaultSubnet(ctx); err != nil {
			return nil, err
		}
	}

	m := &awsMachine{lease: lease, cfg: p.cfg, ec2: p.ec2, connect: p.connect, log: log}
	fail := func(err error) (wings.Machine, error) {
		_ = m.Close(context.WithoutCancel(ctx))
		return nil, err
	}

	// A per-instance security group, deleted with the instance, when the caller
	// gave none: the default VPC's group does not allow inbound SSH.
	sgs := p.cfg.SecurityGroupIDs
	if len(sgs) == 0 {
		sg, err := p.createSecurityGroup(ctx, name, subnet)
		if err != nil {
			return fail(err)
		}
		m.ownedSG = sg
		sgs = []string{sg}
	}

	input := &ec2.RunInstancesInput{
		ImageId:      awssdk.String(ami),
		InstanceType: ec2types.InstanceType(p.cfg.InstanceType),
		MinCount:     awssdk.Int32(1),
		MaxCount:     awssdk.Int32(1),
		UserData:     awssdk.String(userData(p.cfg.User, authorizedKey)),
		NetworkInterfaces: []ec2types.InstanceNetworkInterfaceSpecification{{
			DeviceIndex:              awssdk.Int32(0),
			SubnetId:                 awssdk.String(subnet),
			Groups:                   sgs,
			AssociatePublicIpAddress: awssdk.Bool(true),
			DeleteOnTermination:      awssdk.Bool(true),
		}},
		BlockDeviceMappings: []ec2types.BlockDeviceMapping{{
			DeviceName: awssdk.String("/dev/xvda"),
			Ebs: &ec2types.EbsBlockDevice{
				VolumeSize:          awssdk.Int32(p.cfg.DiskSizeGB),
				DeleteOnTermination: awssdk.Bool(true),
			},
		}},
		TagSpecifications: []ec2types.TagSpecification{{
			ResourceType: ec2types.ResourceTypeInstance,
			Tags:         p.tags(name, lease),
		}},
	}
	if p.cfg.Spot {
		input.InstanceMarketOptions = &ec2types.InstanceMarketOptionsRequest{
			MarketType: ec2types.MarketTypeSpot,
			SpotOptions: &ec2types.SpotMarketOptions{
				// Terminate on reclaim, not stop: wings never restarts one, and a
				// stopped instance still bills for its volume.
				InstanceInterruptionBehavior: ec2types.InstanceInterruptionBehaviorTerminate,
			},
		}
	}

	log.Info("wings: creating instance", "type", p.cfg.InstanceType, "region", p.cfg.Region)
	out, err := p.ec2.RunInstances(ctx, input)
	if err != nil {
		return fail(fmt.Errorf("wings: run instance %s: %w", name, err))
	}
	if len(out.Instances) == 0 {
		return fail(fmt.Errorf("wings: run instance %s returned none", name))
	}
	m.id = awssdk.ToString(out.Instances[0].InstanceId)

	ip, err := p.awaitPublicIP(ctx, m.id)
	if err != nil {
		return fail(err)
	}
	m.ip = ip

	dialCtx, cancel := context.WithTimeout(ctx, p.cfg.BootTimeout)
	defer cancel()

	log.Info("wings: waiting for ssh", "ip", ip)
	conn, err := sshx.Dial(dialCtx, sshx.Config{Addr: ip, User: p.cfg.User, Signer: signer, HostKey: p.cfg.HostKey})
	if err != nil {
		return fail(fmt.Errorf("wings: instance %s never accepted ssh: %w", name, err))
	}
	m.ssh = conn

	log.Info("wings: instance ready")
	return m, nil
}

// Reattach finds instances a previous coordinator created and lets this one back
// in. The per-run SSH key died with that coordinator, so it pushes a fresh key
// with EC2 Instance Connect and dials within its short validity window.
func (p *awsProvisioner) Reattach(ctx context.Context, leases []string) ([]wings.Machine, error) {
	if p.cfg.Region == "" {
		return nil, errors.New("aws: Config needs a Region")
	}
	if _, _, _, err := p.clients(ctx); err != nil {
		return nil, err
	}
	signer, authorizedKey, err := ephemeralKey()
	if err != nil {
		return nil, err
	}

	found := make([]wings.Machine, len(leases))
	var wg sync.WaitGroup
	for i, lease := range leases {
		wg.Go(func() {
			m, err := p.reattachOne(ctx, lease, signer, authorizedKey)
			if err != nil {
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

func (p *awsProvisioner) reattachOne(ctx context.Context, lease string, signer ssh.Signer, authorizedKey string) (wings.Machine, error) {
	name := p.cfg.NamePrefix + "-" + lease
	log := p.cfg.Logger.With("instance", name, "lease", lease)

	inst, err := p.findByLease(ctx, lease)
	if err != nil {
		return nil, err
	}
	state := ec2types.InstanceStateNameRunning
	if inst.State == nil || inst.State.Name != state {
		got := ""
		if inst.State != nil {
			got = string(inst.State.Name)
		}
		// Not serving and never resumed; terminate it so it stops billing.
		id := awssdk.ToString(inst.InstanceId)
		log.Info("wings: terminating an instance that is not running", "state", got)
		if _, err := p.ec2.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{id}}); err != nil {
			log.Warn("wings: could not terminate it", "err", err)
		}
		return nil, fmt.Errorf("instance is %q, not running", got)
	}
	ip := awssdk.ToString(inst.PublicIpAddress)
	if ip == "" {
		return nil, errors.New("instance has no public IP")
	}

	// created true so Close terminates this recovered instance like any other.
	m := &awsMachine{
		id:      awssdk.ToString(inst.InstanceId),
		lease:   lease,
		ip:      ip,
		zone:    awssdk.ToString(inst.Placement.AvailabilityZone),
		cfg:     p.cfg,
		ec2:     p.ec2,
		connect: p.connect,
		log:     log,
	}

	log.Info("wings: pushing a fresh key to a recovered instance")
	if err := m.pushKey(ctx, authorizedKey); err != nil {
		return nil, err
	}

	dialCtx, cancel := context.WithTimeout(ctx, p.cfg.BootTimeout)
	defer cancel()
	conn, err := sshx.Dial(dialCtx, sshx.Config{Addr: ip, User: p.cfg.User, Signer: signer, HostKey: p.cfg.HostKey})
	if err != nil {
		return nil, fmt.Errorf("never accepted the new key: %w", err)
	}
	m.ssh = conn

	log.Info("wings: recovered instance", "ip", ip)
	return m, nil
}

// findByLease returns the running-or-pending instance tagged with lease.
func (p *awsProvisioner) findByLease(ctx context.Context, lease string) (ec2types.Instance, error) {
	out, err := p.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		Filters: []ec2types.Filter{
			{Name: awssdk.String("tag:" + leaseTag), Values: []string{lease}},
			{Name: awssdk.String("instance-state-name"), Values: []string{"pending", "running"}},
		},
	})
	if err != nil {
		return ec2types.Instance{}, fmt.Errorf("not found: %w", err)
	}
	for _, r := range out.Reservations {
		if len(r.Instances) > 0 {
			return r.Instances[0], nil
		}
	}
	return ec2types.Instance{}, errors.New("no instance for this lease")
}

// awaitPublicIP polls until the instance is running and has a public IP.
func (p *awsProvisioner) awaitPublicIP(ctx context.Context, id string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.BootTimeout)
	defer cancel()
	for {
		out, err := p.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{id}})
		if err == nil {
			for _, r := range out.Reservations {
				for _, inst := range r.Instances {
					if inst.State != nil && inst.State.Name == ec2types.InstanceStateNameRunning {
						if ip := awssdk.ToString(inst.PublicIpAddress); ip != "" {
							return ip, nil
						}
					}
				}
			}
		}
		select {
		case <-ctx.Done():
			return "", fmt.Errorf("wings: instance %s never got a public IP: %w", id, ctx.Err())
		case <-time.After(3 * time.Second):
		}
	}
}

// defaultSubnet returns a subnet of the region's default VPC.
func (p *awsProvisioner) defaultSubnet(ctx context.Context) (string, error) {
	vpcs, err := p.ec2.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
		Filters: []ec2types.Filter{{Name: awssdk.String("isDefault"), Values: []string{"true"}}},
	})
	if err != nil {
		return "", fmt.Errorf("wings: find default VPC: %w", err)
	}
	if len(vpcs.Vpcs) == 0 {
		return "", errors.New("wings: no default VPC; set Config.SubnetID")
	}
	vpc := awssdk.ToString(vpcs.Vpcs[0].VpcId)
	subnets, err := p.ec2.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		Filters: []ec2types.Filter{{Name: awssdk.String("vpc-id"), Values: []string{vpc}}},
	})
	if err != nil {
		return "", fmt.Errorf("wings: find default subnet: %w", err)
	}
	if len(subnets.Subnets) == 0 {
		return "", fmt.Errorf("wings: default VPC %s has no subnet; set Config.SubnetID", vpc)
	}
	return awssdk.ToString(subnets.Subnets[0].SubnetId), nil
}

// createSecurityGroup makes a group allowing inbound SSH in the subnet's VPC.
func (p *awsProvisioner) createSecurityGroup(ctx context.Context, name, subnet string) (string, error) {
	subs, err := p.ec2.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: []string{subnet}})
	if err != nil || len(subs.Subnets) == 0 {
		return "", fmt.Errorf("wings: look up subnet %s: %w", subnet, err)
	}
	vpc := awssdk.ToString(subs.Subnets[0].VpcId)

	sg, err := p.ec2.CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
		GroupName:   awssdk.String(name + "-ssh"),
		Description: awssdk.String("wings worker SSH access"),
		VpcId:       awssdk.String(vpc),
	})
	if err != nil {
		return "", fmt.Errorf("wings: create security group: %w", err)
	}
	id := awssdk.ToString(sg.GroupId)
	if _, err := p.ec2.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{
		GroupId: awssdk.String(id),
		IpPermissions: []ec2types.IpPermission{{
			IpProtocol: awssdk.String("tcp"),
			FromPort:   awssdk.Int32(22),
			ToPort:     awssdk.Int32(22),
			IpRanges:   []ec2types.IpRange{{CidrIp: awssdk.String("0.0.0.0/0"), Description: awssdk.String("wings SSH")}},
		}},
	}); err != nil {
		_, _ = p.ec2.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{GroupId: awssdk.String(id)})
		return "", fmt.Errorf("wings: open SSH on security group: %w", err)
	}
	return id, nil
}

func (p *awsProvisioner) tags(name, lease string) []ec2types.Tag {
	tags := []ec2types.Tag{
		{Key: awssdk.String("Name"), Value: awssdk.String(name)},
		{Key: awssdk.String(leaseTag), Value: awssdk.String(lease)},
	}
	for k, v := range p.cfg.Tags {
		tags = append(tags, ec2types.Tag{Key: awssdk.String(k), Value: awssdk.String(v)})
	}
	return tags
}

// userData is a cloud-init shell script that creates the login user and installs
// the run's public key, base64-encoded as EC2 requires.
func userData(user, authorizedKey string) string {
	script := fmt.Sprintf(`#!/bin/bash
set -e
id -u %[1]s >/dev/null 2>&1 || useradd -m -s /bin/bash %[1]s
install -d -m 700 -o %[1]s -g %[1]s /home/%[1]s/.ssh
printf '%%s\n' %[2]s > /home/%[1]s/.ssh/authorized_keys
chown %[1]s:%[1]s /home/%[1]s/.ssh/authorized_keys
chmod 600 /home/%[1]s/.ssh/authorized_keys
`, user, shellQuote(authorizedKey))
	return base64.StdEncoding.EncodeToString([]byte(script))
}

// awsMachine is one EC2 instance.
type awsMachine struct {
	id      string
	lease   string
	ip      string
	zone    string
	cfg     Config
	ec2     *ec2.Client
	connect *ec2instanceconnect.Client
	ssh     *sshx.Client
	log     *slog.Logger
	// ownedSG is a security group wings created for this instance and must delete.
	ownedSG string

	once sync.Once
	err  error
}

// ID is the lease the coordinator recorded before the machine existed, which is
// what makes recovery possible.
func (m *awsMachine) ID() string { return m.lease }

func (m *awsMachine) Upload(ctx context.Context, src io.Reader, size int64, remotePath string) error {
	return m.ssh.Upload(ctx, src, size, remotePath)
}

func (m *awsMachine) Start(ctx context.Context, cmd string, env map[string]string) error {
	return m.ssh.Start(ctx, cmd, env, "/tmp/wings-worker.log")
}

func (m *awsMachine) Forward(ctx context.Context, remotePort int) (string, error) {
	return m.ssh.Forward(ctx, remotePort)
}

// Alive asks EC2 whether the instance still exists and is running, so a reclaimed
// worker is given up on in seconds rather than at the reconnect timeout.
func (m *awsMachine) Alive(ctx context.Context) (bool, error) {
	out, err := m.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{m.id}})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, err
	}
	for _, r := range out.Reservations {
		if len(r.Instances) == 0 {
			continue
		}
		inst := r.Instances[0]
		if inst.State == nil {
			return false, nil
		}
		switch inst.State.Name {
		case ec2types.InstanceStateNameRunning, ec2types.InstanceStateNamePending:
			return true, nil
		}
		return false, nil
	}
	return false, nil
}

// pushKey sends authorizedKey to the instance with EC2 Instance Connect, which
// makes it usable for SSH for a short window — long enough for one dial.
func (m *awsMachine) pushKey(ctx context.Context, authorizedKey string) error {
	_, err := m.connect.SendSSHPublicKey(ctx, &ec2instanceconnect.SendSSHPublicKeyInput{
		InstanceId:     awssdk.String(m.id),
		InstanceOSUser: awssdk.String(m.cfg.User),
		SSHPublicKey:   awssdk.String(authorizedKey),
	})
	if err != nil {
		return fmt.Errorf("wings: push key with instance connect: %w", err)
	}
	return nil
}

// Close terminates the instance and deletes any security group wings made for
// it. Idempotent, because both a failed bring-up and a normal Stop reach it.
func (m *awsMachine) Close(ctx context.Context) error {
	m.once.Do(func() {
		if m.ssh != nil {
			_ = m.ssh.Close()
		}
		if m.id != "" {
			m.log.Info("wings: terminating instance")
			if _, err := m.ec2.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{m.id}}); err != nil {
				m.err = fmt.Errorf("wings: terminate instance %s: %w", m.id, err)
			}
		}
		if m.ownedSG != "" {
			// The group cannot be deleted while the instance still holds it, so wait
			// for the instance to go before removing it.
			m.deleteOwnedSG(ctx)
		}
	})
	return m.err
}

// deleteOwnedSG waits for the instance to terminate, then deletes the security
// group wings created for it. Best-effort: a leaked group is cheap and named.
func (m *awsMachine) deleteOwnedSG(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.BootTimeout)
	defer cancel()
	for {
		out, err := m.ec2.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: []string{m.id}})
		gone := err == nil
		if err == nil {
			for _, r := range out.Reservations {
				for _, inst := range r.Instances {
					if inst.State != nil && inst.State.Name != ec2types.InstanceStateNameTerminated {
						gone = false
					}
				}
			}
		}
		if gone {
			if _, err := m.ec2.DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{GroupId: awssdk.String(m.ownedSG)}); err != nil {
				m.log.Warn("wings: could not delete security group", "group", m.ownedSG, "err", err)
			}
			return
		}
		select {
		case <-ctx.Done():
			m.log.Warn("wings: gave up deleting security group", "group", m.ownedSG)
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// isNotFound reports whether EC2 said the instance does not exist.
func isNotFound(err error) bool {
	var api interface{ ErrorCode() string }
	if errors.As(err, &api) {
		code := api.ErrorCode()
		return strings.Contains(code, "NotFound") || code == "InvalidInstanceID.NotFound"
	}
	return false
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
	return signer, strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub))) + " wings", nil
}

// shellQuote wraps s for a POSIX shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

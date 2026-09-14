package wings

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	hscontrol "github.com/juanfont/headscale/hscontrol"
	"github.com/juanfont/headscale/hscontrol/types"
)

const (
	modeHeadscale = "headscale"
	// envHSDir and envHSListen are what a coordinator hands its headscale child:
	// where to keep the control plane's state, and the address to serve it on.
	envHSDir    = "WINGS_HS_DIR"
	envHSListen = "WINGS_HS_LISTEN"
	// headscaleUser is the single tailnet user every wings node enrols under.
	headscaleUser = "wings"
	// headscaleReadyWait bounds how long the coordinator waits for its child to
	// bring the control plane up and print its credentials.
	headscaleReadyWait = 90 * time.Second
)

// isHeadscaleProcess reports whether wings started this process to host the
// embedded Tailscale control plane.
func isHeadscaleProcess() bool { return os.Getenv(envMode) == modeHeadscale }

// maybeRunHeadscaleChild takes over the process when wings started it as the
// headscale child, serving the control plane until killed and never returning.
// Called first from [WorkerMain] and [CoordinatorMain], since the child is this
// same binary re-executed.
func maybeRunHeadscaleChild() {
	if !isHeadscaleProcess() {
		return
	}
	if err := runHeadscaleProcess(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "wings: headscale child: %v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}

// overlayCreds is what the headscale child prints on its ready line: the control
// URL a tsnet node dials and the pre-auth key it enrols with.
type overlayCreds struct {
	Control string `json:"control"`
	AuthKey string `json:"auth_key"`
}

// runHeadscaleProcess is the whole headscale child: it writes a config for the
// address and directory its parent chose, brings the control plane up on it,
// mints a reusable pre-auth key, prints it on the ready line, and serves until
// the process is killed. It owns this process, so Headscale's process-global
// signal handling — the reason it cannot be embedded in-process — is correct
// here. It does not return in the normal case.
func runHeadscaleProcess(_ context.Context) error {
	dir, addr := os.Getenv(envHSDir), os.Getenv(envHSListen)
	if dir == "" || addr == "" {
		return fmt.Errorf("wings: headscale child needs %s and %s", envHSDir, envHSListen)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("wings: headscale dir: %w", err)
	}
	// Serve from the state directory so every path in the config is a bare
	// basename: headscale resolves a relative path Unix-style (leading-slash test)
	// and would misread a Windows drive-letter path as relative, doubling it.
	if err := os.Chdir(dir); err != nil {
		return fmt.Errorf("wings: enter headscale dir: %w", err)
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("wings: overlay address %q: %w", addr, err)
	}
	if err := writeSelfSignedCert(host); err != nil {
		return err
	}
	if err := writeHeadscaleConfig(addr, host); err != nil {
		return err
	}
	if err := types.LoadConfig("config.yaml", true); err != nil {
		return fmt.Errorf("wings: load headscale config: %w", err)
	}
	cfg, err := types.LoadServerConfig()
	if err != nil {
		return fmt.Errorf("wings: read headscale config: %w", err)
	}
	app, err := hscontrol.NewHeadscale(cfg)
	if err != nil {
		return fmt.Errorf("wings: new headscale: %w", err)
	}
	key, err := mintOverlayKey(app)
	if err != nil {
		return err
	}
	creds, err := json.Marshal(overlayCreds{Control: "https://" + addr, AuthKey: key})
	if err != nil {
		return fmt.Errorf("wings: encode overlay creds: %w", err)
	}
	fmt.Printf("%s%s\n", readyPrefix, creds)
	_ = os.Stdout.Sync()
	return app.Serve()
}

// mintOverlayKey creates the tailnet user and a reusable pre-auth key every wings
// node enrols with, so bringing a node up needs only the control URL and this key.
func mintOverlayKey(app *hscontrol.Headscale) (string, error) {
	st := app.GetState()
	u, _, err := st.CreateUser(types.User{Name: headscaleUser})
	if err != nil {
		return "", fmt.Errorf("wings: headscale create user: %w", err)
	}
	uid := types.UserID(u.ID)
	pak, err := st.CreatePreAuthKey(&uid, true, false, nil, nil)
	if err != nil {
		return "", fmt.Errorf("wings: headscale pre-auth key: %w", err)
	}
	return pak.Key, nil
}

// writeHeadscaleConfig writes the child's config and DERP map into the current
// directory, all as bare basenames. The control plane and embedded DERP relay
// serve HTTPS with the self-signed cert: nodes trust it for the control-key fetch
// (see envSSLCert), Noise secures the control connection regardless, and the DERP
// node skips cert verification, so the overlay is fully self-hosted — no public
// CA, no third-party relay.
func writeHeadscaleConfig(addr, host string) error {
	stun, err := reserveUDPPort(host)
	if err != nil {
		return err
	}
	// grpc and metrics default to fixed ports (:50443, :9090); pin them to free
	// loopback ports so a second instance, or an orphan of a crashed one, does not
	// wedge on a port we never use (keys are minted in-process over the socket).
	grpcAddr, err := freeLoopbackAddr()
	if err != nil {
		return err
	}
	metricsAddr, err := freeLoopbackAddr()
	if err != nil {
		return err
	}
	_, port, _ := net.SplitHostPort(addr)
	derp := fmt.Sprintf("regions:\n"+
		"  900:\n"+
		"    regionid: 900\n"+
		"    regioncode: wings\n"+
		"    regionname: Wings\n"+
		"    nodes:\n"+
		"      - name: wings0\n"+
		"        regionid: 900\n"+
		"        hostname: %s\n"+
		"        ipv4: %s\n"+
		"        derpport: %s\n"+
		"        stunport: %d\n"+
		"        insecurefortests: true\n", host, host, port, stun)
	if err := os.WriteFile("derp.yaml", []byte(derp), 0o600); err != nil {
		return fmt.Errorf("wings: write derp map: %w", err)
	}
	cfg := fmt.Sprintf(`server_url: https://%s
listen_addr: %s
grpc_listen_addr: %s
metrics_listen_addr: %s
tls_cert_path: cert.pem
tls_key_path: key.pem
noise:
  private_key_path: noise.key
prefixes:
  v4: 100.64.0.0/10
  v6: fd7a:115c:a1e0::/48
  allocation: sequential
derp:
  server:
    enabled: true
    region_id: 900
    region_code: wings
    region_name: Wings
    stun_listen_addr: %s:%d
    private_key_path: derp.key
    automatically_add_embedded_derp_region: false
  urls: []
  paths:
    - derp.yaml
  auto_update_enabled: false
database:
  type: sqlite
  sqlite:
    path: db.sqlite
dns:
  magic_dns: false
  override_local_dns: false
  base_domain: wings.internal
policy:
  mode: database
unix_socket: hs.sock
log:
  level: warn
`, addr, addr, grpcAddr, metricsAddr, host, stun)
	if err := os.WriteFile("config.yaml", []byte(cfg), 0o600); err != nil {
		return fmt.Errorf("wings: write headscale config: %w", err)
	}
	return nil
}

// reserveUDPPort picks a UDP port on host the OS reports free, for the embedded
// DERP server's STUN listener. It races a rebind like [freeLoopbackAddr], which
// is fine: the DERP server binds it a moment later.
func reserveUDPPort(host string) (int, error) {
	c, err := net.ListenPacket("udp", net.JoinHostPort(host, "0"))
	if err != nil {
		return 0, fmt.Errorf("wings: reserve STUN port: %w", err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port, nil
}

// writeSelfSignedCert writes a self-signed TLS cert and key (cert.pem, key.pem)
// for host into the current directory, so the control plane and DERP relay serve
// HTTPS without a public CA. The tailnet's Noise layer secures the control
// connection regardless, and the DERP node skips verification (the pre-auth key
// and Noise already authenticate a peer), so the cert need not chain to any
// authority.
func writeSelfSignedCert(host string) error {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("wings: overlay cert key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return fmt.Errorf("wings: overlay cert serial: %w", err)
	}
	tmpl := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: host},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	if ip := net.ParseIP(host); ip != nil {
		tmpl.IPAddresses = []net.IP{ip}
	} else {
		tmpl.DNSNames = []string{host}
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return fmt.Errorf("wings: create overlay cert: %w", err)
	}
	if err := os.WriteFile("cert.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		return fmt.Errorf("wings: write overlay cert: %w", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return fmt.Errorf("wings: marshal overlay key: %w", err)
	}
	if err := os.WriteFile("key.pem", pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return fmt.Errorf("wings: write overlay key: %w", err)
	}
	return nil
}

// hostedHeadscale is a running headscale child: the control URL and pre-auth key
// its nodes enrol with, the path to the self-signed cert they must trust (the
// control key fetch verifies it), and a stop that kills it and waits.
type hostedHeadscale struct {
	control  string
	authKey  string
	certPath string
	stop     func()
}

// startHostedHeadscale spawns this same binary as a headscale child serving the
// control plane at addr with its state under dir, and returns once the child has
// printed the control URL and pre-auth key its nodes enrol with. The child owns
// the process it runs in, so Headscale's signal handling stays correct.
func startHostedHeadscale(ctx context.Context, dir, addr string) (*hostedHeadscale, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wings: headscale dir: %w", err)
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("wings: locate self for headscale child: %w", err)
	}
	proc, cancel := context.WithCancel(context.WithoutCancel(ctx))
	cmd := exec.CommandContext(proc, exe)
	cmd.Env = append(os.Environ(),
		envMode+"="+modeHeadscale,
		envHSDir+"="+dir,
		envHSListen+"="+addr,
	)
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("wings: headscale child stdout: %w", err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("wings: start headscale child: %w", err)
	}
	stop := func() { cancel(); _ = cmd.Wait() }

	creds, err := readOverlayCreds(ctx, out)
	if err != nil {
		stop()
		return nil, err
	}
	return &hostedHeadscale{control: creds.Control, authKey: creds.AuthKey, certPath: filepath.Join(dir, "cert.pem"), stop: stop}, nil
}

// readOverlayCreds reads the child's stdout until its ready line, parses the
// credentials, and gives up after headscaleReadyWait or when ctx is done.
func readOverlayCreds(ctx context.Context, out io.Reader) (overlayCreds, error) {
	type result struct {
		creds overlayCreds
		err   error
	}
	done := make(chan result, 1)
	go func() {
		sc := bufio.NewScanner(out)
		sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
		for sc.Scan() {
			line := sc.Text()
			rest, ok := strings.CutPrefix(line, readyPrefix)
			if !ok {
				continue
			}
			var creds overlayCreds
			if err := json.Unmarshal([]byte(rest), &creds); err != nil {
				done <- result{err: fmt.Errorf("wings: parse headscale ready line: %w", err)}
				return
			}
			done <- result{creds: creds}
			return
		}
		err := sc.Err()
		if err == nil {
			err = fmt.Errorf("wings: headscale child exited before it was ready")
		}
		done <- result{err: err}
	}()

	select {
	case <-ctx.Done():
		return overlayCreds{}, ctx.Err()
	case <-time.After(headscaleReadyWait):
		return overlayCreds{}, fmt.Errorf("wings: headscale child not ready after %s", headscaleReadyWait)
	case r := <-done:
		return r.creds, r.err
	}
}

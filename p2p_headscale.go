package wings

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	if err := writeHeadscaleConfig(addr); err != nil {
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
	creds, err := json.Marshal(overlayCreds{Control: "http://" + addr, AuthKey: key})
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

// writeHeadscaleConfig writes the child's config and a placeholder DERP map into
// the current directory, all as bare basenames. The DERP map lets the control
// plane build node maps without fetching Tailscale's public one; nodes on one
// host or LAN connect directly and never relay through it. Self-hosted DERP over
// TLS is a later step.
func writeHeadscaleConfig(addr string) error {
	derp := "regions:\n" +
		"  900:\n" +
		"    regionid: 900\n" +
		"    regioncode: wings\n" +
		"    regionname: Wings\n" +
		"    nodes:\n" +
		"      - name: wings0\n" +
		"        regionid: 900\n" +
		"        hostname: 127.0.0.1\n" +
		"        ipv4: 127.0.0.1\n" +
		"        derpport: -1\n"
	if err := os.WriteFile("derp.yaml", []byte(derp), 0o600); err != nil {
		return fmt.Errorf("wings: write derp map: %w", err)
	}
	cfg := fmt.Sprintf(`server_url: http://%s
listen_addr: %s
noise:
  private_key_path: noise.key
prefixes:
  v4: 100.64.0.0/10
  v6: fd7a:115c:a1e0::/48
  allocation: sequential
derp:
  server:
    enabled: false
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
`, addr, addr)
	if err := os.WriteFile("config.yaml", []byte(cfg), 0o600); err != nil {
		return fmt.Errorf("wings: write headscale config: %w", err)
	}
	return nil
}

// hostedHeadscale is a running headscale child: the control URL and pre-auth key
// its nodes enrol with, and a stop that kills it and waits.
type hostedHeadscale struct {
	control string
	authKey string
	stop    func()
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
	return &hostedHeadscale{control: creds.Control, authKey: creds.AuthKey, stop: stop}, nil
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

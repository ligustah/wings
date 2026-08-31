// Package sshx is the SSH machinery wings uses to deploy a worker onto a
// machine: copy a file, start a process, and open a tunnel back.
//
// It is cloud-agnostic on purpose. A [wings.Provisioner] for a new cloud has to
// create a VM and hand back an address and a credential; everything after that
// is the same everywhere, and lives here.
package sshx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Config describes one SSH destination.
type Config struct {
	// Addr is host:port. Port defaults to 22 when absent.
	Addr string
	User string
	// Signer authenticates us. wings generates an ephemeral key per run and
	// installs its public half on the machine it creates.
	Signer ssh.Signer
	// HostKey verifies the machine. Nil means accept any key — see
	// [InsecureIgnoreHostKey] for why that is not simply fine.
	HostKey ssh.HostKeyCallback
	Timeout time.Duration
}

// Client is a live SSH connection.
type Client struct {
	conn *ssh.Client

	mu        sync.Mutex
	listeners []net.Listener
	closed    bool
}

// InsecureIgnoreHostKey accepts whatever host key is offered.
//
// It is the default for a freshly provisioned VM because there is nothing to
// compare against: the machine did not exist a minute ago and no trusted record
// of its key exists yet. The exposure is a machine-in-the-middle on the path to
// the cloud provider, who would see the job payloads and could return forged
// results. Where that matters, read the host key from the provider's guest
// attributes and supply a real callback in Config.HostKey.
func InsecureIgnoreHostKey() ssh.HostKeyCallback { return ssh.InsecureIgnoreHostKey() }

// Dial connects, retrying until ctx expires.
//
// Retrying is not a nicety here: a VM reports RUNNING well before sshd accepts
// a connection, so the first several attempts failing is the normal path rather
// than a problem.
func Dial(ctx context.Context, cfg Config) (*Client, error) {
	addr := cfg.Addr
	if !strings.Contains(addr, ":") {
		addr = net.JoinHostPort(addr, "22")
	}
	hostKey := cfg.HostKey
	if hostKey == nil {
		hostKey = InsecureIgnoreHostKey()
	}
	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}

	clientCfg := &ssh.ClientConfig{
		User:            cfg.User,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(cfg.Signer)},
		HostKeyCallback: hostKey,
		Timeout:         timeout,
	}

	var lastErr error
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return nil, fmt.Errorf("ssh %s: gave up after %d attempts: %w", addr, attempt, lastErr)
			}
			return nil, err
		}
		conn, err := ssh.Dial("tcp", addr, clientCfg)
		if err == nil {
			return &Client{conn: conn}, nil
		}
		lastErr = err

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("ssh %s: gave up after %d attempts: %w", addr, attempt+1, lastErr)
		case <-time.After(3 * time.Second):
		}
	}
}

// Upload copies localPath to remotePath, creating parent directories and
// marking the result executable.
func (c *Client) Upload(ctx context.Context, localPath, remotePath string) error {
	src, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("ssh upload: open %s: %w", localPath, err)
	}
	defer src.Close()

	sc, err := sftp.NewClient(c.conn)
	if err != nil {
		return fmt.Errorf("ssh upload: sftp: %w", err)
	}
	defer sc.Close()

	if dir := path.Dir(remotePath); dir != "." && dir != "/" {
		if err := sc.MkdirAll(dir); err != nil {
			return fmt.Errorf("ssh upload: mkdir %s: %w", dir, err)
		}
	}

	dst, err := sc.Create(remotePath)
	if err != nil {
		return fmt.Errorf("ssh upload: create %s: %w", remotePath, err)
	}
	if _, err := io.Copy(dst, src); err != nil {
		dst.Close()
		return fmt.Errorf("ssh upload: write %s: %w", remotePath, err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("ssh upload: close %s: %w", remotePath, err)
	}
	if err := sc.Chmod(remotePath, 0o755); err != nil {
		return fmt.Errorf("ssh upload: chmod %s: %w", remotePath, err)
	}
	return nil
}

// Run executes cmd and returns its combined output, waiting for it to finish.
func (c *Client) Run(ctx context.Context, cmd string) (string, error) {
	sess, err := c.conn.NewSession()
	if err != nil {
		return "", fmt.Errorf("ssh run: session: %w", err)
	}
	defer sess.Close()

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = sess.Signal(ssh.SIGKILL)
			_ = sess.Close()
		case <-done:
		}
	}()
	out, err := sess.CombinedOutput(cmd)
	close(done)
	if err != nil {
		return string(out), fmt.Errorf("ssh run %q: %w: %s", cmd, err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// Start launches cmd detached and returns once it is running.
//
// setsid plus a redirect to a log file is what makes it survive this session
// closing; without it the worker dies with the connection that started it, and
// the failure looks like a worker that was never reachable.
func (c *Client) Start(ctx context.Context, cmd string, env map[string]string, logPath string) error {
	var b strings.Builder
	for k, v := range env {
		fmt.Fprintf(&b, "%s=%s ", k, shellQuote(v))
	}
	full := fmt.Sprintf("setsid env %s %s > %s 2>&1 < /dev/null &", b.String(), cmd, shellQuote(logPath))
	if _, err := c.Run(ctx, full); err != nil {
		return err
	}
	return nil
}

// Forward opens a loopback listener here that proxies to remotePort on the
// machine, and returns the local address to dial.
//
// This is how the coordinator reaches a worker's broker. The broker binds
// loopback on the VM and is reachable no other way, so the tunnel is not only
// convenient — it is the access control, since the broker itself speaks no
// authentication.
func (c *Client) Forward(ctx context.Context, remotePort int) (string, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("ssh forward: local listen: %w", err)
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		lis.Close()
		return "", errors.New("ssh forward: client is closed")
	}
	c.listeners = append(c.listeners, lis)
	c.mu.Unlock()

	remote := fmt.Sprintf("127.0.0.1:%d", remotePort)
	go func() {
		for {
			local, err := lis.Accept()
			if err != nil {
				return // listener closed
			}
			go c.pipe(local, remote)
		}
	}()
	return lis.Addr().String(), nil
}

func (c *Client) pipe(local net.Conn, remote string) {
	defer local.Close()
	rc, err := c.conn.Dial("tcp", remote)
	if err != nil {
		return
	}
	defer rc.Close()

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(rc, local); done <- struct{}{} }()
	go func() { _, _ = io.Copy(local, rc); done <- struct{}{} }()
	<-done
}

// Close tears down every tunnel and the connection.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	listeners := c.listeners
	c.listeners = nil
	c.mu.Unlock()

	for _, l := range listeners {
		_ = l.Close()
	}
	return c.conn.Close()
}

// shellQuote wraps s for a POSIX shell.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

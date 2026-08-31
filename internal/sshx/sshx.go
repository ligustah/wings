// Package sshx is the SSH machinery wings uses to deploy a worker onto a
// machine: copy a file, start a process, and open a tunnel back.
//
// It is cloud-agnostic on purpose. A [wings.Provisioner] for a new cloud has to
// create a VM and hand back an address and a credential; everything after that
// is the same everywhere, and lives here.
package sshx

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path"
	"strings"
	"sync"
	"time"

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

// Upload streams size bytes from src to remotePath, creating parent
// directories and marking the result executable.
//
// It speaks the scp source protocol over an ordinary exec channel, which is
// what scp itself has always done on the wire: run `scp -t <path>` on the far
// side and hand it a header and the bytes. That is deliberate rather than
// nostalgic — SFTP is a SUBSYSTEM, and an SSH server is under no obligation to
// offer it. Plenty do not: a hardened image with `Subsystem sftp` commented
// out, a container running dropbear, anything minimal. Every one of those still
// runs commands, which is a capability wings depends on anyway to start the
// worker at all. So this asks for nothing the rest of the deployment does not
// already need.
//
// The size is required because the protocol sends it in the header, and that
// is a feature: the far end knows exactly how many bytes to expect, so a
// connection that dies mid-copy is an error here instead of a truncated
// executable that fails confusingly later.
func (c *Client) Upload(ctx context.Context, src io.Reader, size int64, remotePath string) error {
	// scp -t does not create directories. Cheap, and it needs only the shell.
	if dir := path.Dir(remotePath); dir != "." && dir != "/" {
		if _, err := c.Run(ctx, "mkdir -p "+shellQuote(dir)); err != nil {
			return fmt.Errorf("ssh upload: mkdir %s: %w", dir, err)
		}
	}

	sess, err := c.conn.NewSession()
	if err != nil {
		return fmt.Errorf("ssh upload: session: %w", err)
	}
	defer sess.Close()

	stdin, err := sess.StdinPipe()
	if err != nil {
		return fmt.Errorf("ssh upload: stdin: %w", err)
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		return fmt.Errorf("ssh upload: stdout: %w", err)
	}
	// The sink reports trouble on stdout as a protocol ack; stderr is where the
	// remote scp puts anything it could not say that way, such as not existing.
	var stderr strings.Builder
	sess.Stderr = &stderr

	if err := sess.Start("scp -t " + shellQuote(remotePath)); err != nil {
		return fmt.Errorf("ssh upload: start remote scp: %w", err)
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = sess.Signal(ssh.SIGKILL)
			_ = sess.Close()
		case <-done:
		}
	}()
	defer close(done)

	fail := func(err error) error {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return fmt.Errorf("ssh upload %s: %w: %s", remotePath, err, msg)
		}
		return fmt.Errorf("ssh upload %s: %w", remotePath, err)
	}

	if err := scpSend(stdin, stdout, src, size, path.Base(remotePath)); err != nil {
		return fail(err)
	}

	// Closing stdin ends the transfer; the remote scp then exits.
	if err := stdin.Close(); err != nil {
		return fail(fmt.Errorf("close: %w", err))
	}
	if err := sess.Wait(); err != nil {
		return fail(fmt.Errorf("remote scp: %w", err))
	}
	return nil
}

// scpSend is the protocol itself, with the session plumbing left outside.
//
// Separated so it can be tested against a fake sink. A wire protocol that only
// runs when a real VM is on the other end is a protocol that is never tested,
// and this one has an ordering that is easy to get subtly wrong.
func scpSend(w io.Writer, acks io.Reader, src io.Reader, size int64, name string) error {
	ack := bufio.NewReader(acks)

	// The sink acks first, to say it is ready for a header.
	if err := readAck(ack); err != nil {
		return err
	}

	// C<mode> <size> <name>. The name matters only when the remote path turns
	// out to be a directory; otherwise the sink writes to the path it was given.
	if _, err := fmt.Fprintf(w, "C0755 %d %s\n", size, name); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	if err := readAck(ack); err != nil {
		return err
	}

	// CopyN, not Copy: sending fewer bytes than the header promised would leave
	// the sink waiting, and sending more would desynchronise the protocol.
	if n, err := io.CopyN(w, src, size); err != nil {
		return fmt.Errorf("write body after %d of %d bytes: %w", n, size, err)
	}
	// A zero byte terminates the file and asks for the final ack.
	if _, err := w.Write([]byte{0}); err != nil {
		return fmt.Errorf("terminate transfer: %w", err)
	}
	return readAck(ack)
}

// readAck reads one scp protocol acknowledgement.
//
// 0 is success. 1 is a warning and 2 is fatal, each followed by a message
// terminated by a newline — and we treat both as failures, because the only
// thing being uploaded is the binary the whole machine exists to run.
func readAck(r *bufio.Reader) error {
	code, err := r.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("remote scp closed the connection; is scp installed on the machine?")
		}
		return fmt.Errorf("read ack: %w", err)
	}
	if code == 0 {
		return nil
	}
	msg, err := r.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("remote scp reported status %d, and its message could not be read: %w", code, err)
	}
	return fmt.Errorf("remote scp: %s", strings.TrimSpace(msg))
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

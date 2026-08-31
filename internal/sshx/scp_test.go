package sshx

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

// sink is a fake `scp -t`: it reads what scpSend writes and acks the way a real
// one would. Driving the protocol against something that answers is the only
// way to catch an ordering mistake without a machine on the other end.
type sink struct {
	acks bytes.Buffer // what the sink says back
	got  bytes.Buffer // what the source sent

	// status is the code the sink returns for each ack it is asked for, in
	// order. Zero values mean success, so the common case needs no setup.
	status []byte
	msgs   []string
	n      int
}

// ack writes the next acknowledgement the source will read.
func (s *sink) ack() {
	code := byte(0)
	msg := ""
	if s.n < len(s.status) {
		code = s.status[s.n]
	}
	if s.n < len(s.msgs) {
		msg = s.msgs[s.n]
	}
	s.n++

	s.acks.WriteByte(code)
	if code != 0 {
		s.acks.WriteString(msg + "\n")
	}
}

// run pre-loads every ack the source will ask for, then lets it send. A real
// sink interleaves; buffering is fine here because the source never reads ahead
// of a write it has already made.
func (s *sink) run(t *testing.T, body io.Reader, size int64, name string) error {
	t.Helper()
	for range 3 { // ready, post-header, post-body
		s.ack()
	}
	return scpSend(&s.got, &s.acks, body, size, name)
}

func TestSCPSendsHeaderThenBodyThenTerminator(t *testing.T) {
	var s sink
	body := "#!/bin/sh\necho hello\n"

	if err := s.run(t, strings.NewReader(body), int64(len(body)), "worker"); err != nil {
		t.Fatalf("scpSend: %v", err)
	}

	want := "C0755 " + itoa(len(body)) + " worker\n" + body + "\x00"
	if got := s.got.String(); got != want {
		t.Errorf("wire bytes wrong:\n got %q\nwant %q", got, want)
	}
}

// The mode must be executable. A worker uploaded without the bit set produces a
// permission error at exec time, on the machine, which is the most expensive
// place to discover it.
func TestSCPUploadsExecutable(t *testing.T) {
	var s sink
	if err := s.run(t, strings.NewReader("x"), 1, "worker"); err != nil {
		t.Fatalf("scpSend: %v", err)
	}
	if header, _, _ := strings.Cut(s.got.String(), "\n"); !strings.HasPrefix(header, "C0755 ") {
		t.Errorf("header is %q; the mode must be 0755 or the worker cannot be run", header)
	}
}

// THE POINT: exactly size bytes go on the wire whatever the reader holds.
// Sending more would desynchronise the protocol — the extra bytes would be read
// as the next command.
func TestSCPSendsExactlySizeBytes(t *testing.T) {
	var s sink
	if err := s.run(t, strings.NewReader("abcdefghij"), 4, "worker"); err != nil {
		t.Fatalf("scpSend: %v", err)
	}
	if got, want := s.got.String(), "C0755 4 worker\nabcd\x00"; got != want {
		t.Errorf("sent %q, want %q", got, want)
	}
}

// A reader that ends early must fail here rather than produce a truncated
// binary on the machine — which is the whole reason the protocol carries a size.
func TestSCPFailsOnAShortReader(t *testing.T) {
	var s sink
	err := s.run(t, strings.NewReader("abc"), 100, "worker")
	if err == nil {
		t.Fatal("want an error when the source runs out before the promised size")
	}
	if !strings.Contains(err.Error(), "after 3 of 100 bytes") {
		t.Errorf("the error should say how far it got; got %v", err)
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("want the underlying EOF to survive wrapping; got %v", err)
	}
}

func TestSCPSurfacesTheSinksComplaint(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status []byte
		msgs   []string
		want   string
	}{
		{
			name:   "refused before the header",
			status: []byte{2},
			msgs:   []string{"scp: /opt/wings: Permission denied"},
			want:   "Permission denied",
		},
		{
			name:   "refused after the header",
			status: []byte{0, 1},
			msgs:   []string{"", "scp: no space left on device"},
			want:   "no space left on device",
		},
		{
			name:   "refused after the body",
			status: []byte{0, 0, 2},
			msgs:   []string{"", "", "scp: write failed"},
			want:   "write failed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := sink{status: tc.status, msgs: tc.msgs}
			err := s.run(t, strings.NewReader("hello"), 5, "worker")
			if err == nil {
				t.Fatal("want an error when the sink refuses")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("the sink's own message must survive; got %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// An scp that is not installed shows up as the channel closing with nothing on
// it, and that is worth naming: "EOF" alone sends you looking in the wrong place.
func TestSCPExplainsAnEmptyResponse(t *testing.T) {
	var got bytes.Buffer
	err := scpSend(&got, strings.NewReader(""), strings.NewReader("x"), 1, "worker")
	if err == nil {
		t.Fatal("want an error when the far side says nothing at all")
	}
	if !strings.Contains(err.Error(), "scp") {
		t.Errorf("the error should point at scp on the machine; got %v", err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

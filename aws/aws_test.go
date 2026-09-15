package aws

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"
)

// TestUserDataInstallsKey checks the cloud-init script creates the login user and
// writes exactly the run's key, base64-encoded as EC2 requires.
func TestUserDataInstallsKey(t *testing.T) {
	_, key, err := ephemeralKey()
	if err != nil {
		t.Fatalf("ephemeralKey: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(userData("wings", key))
	if err != nil {
		t.Fatalf("user data is not base64: %v", err)
	}
	script := string(raw)
	for _, want := range []string{"#!/bin/bash", "useradd -m -s /bin/bash wings", "/home/wings/.ssh/authorized_keys", key} {
		if !strings.Contains(script, want) {
			t.Errorf("user data missing %q:\n%s", want, script)
		}
	}
}

// TestEphemeralKeySignsWhatItAuthorizes checks the returned signer's public key
// matches the authorized_keys line, so the private key can log in with it.
func TestEphemeralKeySignsWhatItAuthorizes(t *testing.T) {
	signer, line, err := ephemeralKey()
	if err != nil {
		t.Fatalf("ephemeralKey: %v", err)
	}
	if !strings.HasSuffix(line, " wings") {
		t.Errorf("authorized line lacks the wings comment: %q", line)
	}
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(line))
	if err != nil {
		t.Fatalf("parse authorized key: %v", err)
	}
	if !strings.EqualFold(string(ssh.MarshalAuthorizedKey(pub)), string(ssh.MarshalAuthorizedKey(signer.PublicKey()))) {
		t.Error("signer public key does not match the authorized line")
	}
}

// TestTagsCarryNameAndLease checks every instance is tagged so Reattach can find
// it and a human can spot strays.
func TestTagsCarryNameAndLease(t *testing.T) {
	p := &awsProvisioner{cfg: Config{Tags: map[string]string{"team": "wings"}}.withDefaults()}
	got := map[string]string{}
	for _, tag := range p.tags("wings-abc", "abc") {
		got[*tag.Key] = *tag.Value
	}
	if got["Name"] != "wings-abc" {
		t.Errorf("Name tag = %q, want wings-abc", got["Name"])
	}
	if got[leaseTag] != "abc" {
		t.Errorf("%s tag = %q, want abc", leaseTag, got[leaseTag])
	}
	if got["team"] != "wings" {
		t.Errorf("extra tag not carried: %v", got)
	}
}

// TestWithDefaults fills the fields a caller may leave blank.
func TestWithDefaults(t *testing.T) {
	c := Config{Region: "eu-west-1"}.withDefaults()
	if c.InstanceType == "" || c.DiskSizeGB == 0 || c.NamePrefix == "" || c.User == "" || c.BootTimeout == 0 || c.Logger == nil {
		t.Errorf("withDefaults left a field zero: %+v", c)
	}
}

type codeErr struct{ code string }

func (e codeErr) Error() string     { return e.code }
func (e codeErr) ErrorCode() string { return e.code }

// TestIsNotFound recognizes EC2's missing-instance error and only that.
func TestIsNotFound(t *testing.T) {
	if !isNotFound(codeErr{"InvalidInstanceID.NotFound"}) {
		t.Error("did not recognize InvalidInstanceID.NotFound")
	}
	if isNotFound(errors.New("some other failure")) {
		t.Error("treated an unrelated error as not-found")
	}
	if isNotFound(codeErr{"RequestLimitExceeded"}) {
		t.Error("treated a throttle as not-found")
	}
}

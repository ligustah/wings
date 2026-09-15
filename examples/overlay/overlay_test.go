package overlay

import "testing"

func TestProvisionerFromEnv(t *testing.T) {
	t.Run("neither set is nil", func(t *testing.T) {
		t.Setenv("WINGS_OVERLAY_GCP_PROJECT", "")
		t.Setenv("WINGS_OVERLAY_AWS_REGION", "")
		if p := Provisioner(); p != nil {
			t.Errorf("got %T, want nil", p)
		}
	})
	t.Run("a configured cloud yields a provisioner", func(t *testing.T) {
		t.Setenv("WINGS_OVERLAY_GCP_PROJECT", "proj")
		t.Setenv("WINGS_OVERLAY_AWS_REGION", "eu-west-1")
		if Provisioner() == nil {
			t.Error("both clouds configured but got nil")
		}
	})
}

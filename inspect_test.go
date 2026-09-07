package wings

import (
	"encoding/json"
	"net/http"
	"testing"
)

func getJSON(t *testing.T, url string, into any) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", url, resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

// THE POINT: the inspection API reports the cluster's live state — the summary
// and the running workers — over HTTP.
func TestInspectionAPIReportsLiveState(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, UI: "127.0.0.1:0"})

	var status statusView
	getJSON(t, "http://"+c.uiAddr+"/api/status", &status)
	if status.Target != "inprocess" {
		t.Fatalf("target %q, want inprocess", status.Target)
	}
	if status.Workers != 2 {
		t.Fatalf("status reports %d workers, want 2", status.Workers)
	}

	var workers []workerView
	getJSON(t, "http://"+c.uiAddr+"/api/workers", &workers)
	if len(workers) != 2 {
		t.Fatalf("workers endpoint returned %d, want 2", len(workers))
	}
	for _, wk := range workers {
		if wk.Kind != "inprocess" {
			t.Fatalf("worker %s kind %q, want inprocess", wk.ID, wk.Kind)
		}
	}

	// The pending endpoint is valid and empty with nothing running.
	var pending []pendingView
	getJSON(t, "http://"+c.uiAddr+"/api/pending", &pending)
	if len(pending) != 0 {
		t.Fatalf("pending returned %d with no work submitted, want 0", len(pending))
	}
}

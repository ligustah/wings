package wings

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/ligustah/wings/flow"
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

// THE POINT: the coordinator serves the embedded web UI at the root.
func TestUIServesTheWebApp(t *testing.T) {
	c := start(t, Config{Target: InProcess(), UI: "127.0.0.1:0"})
	resp, err := http.Get("http://" + c.uiAddr + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /: status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "<title>wings</title>") {
		t.Fatalf("root did not serve the UI page; got %d bytes", len(body))
	}
}

// THE POINT: after a run completes, the API lists it and serves its decoded
// history.
func TestInspectionAPIServesRunHistory(t *testing.T) {
	c := start(t, Config{Target: InProcess(), UI: "127.0.0.1:0"})
	name := "test-inspect-" + strconv.FormatUint(runSeq.Add(1), 36)

	if err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		return ctx.Sleep(0)
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var runs []runView
	getJSON(t, "http://"+c.uiAddr+"/api/runs", &runs)
	var found *runView
	for i := range runs {
		if runs[i].Run == name {
			found = &runs[i]
		}
	}
	if found == nil {
		t.Fatalf("run %q not listed in %+v", name, runs)
	}
	if found.Status != "completed" {
		t.Fatalf("run status %q, want completed", found.Status)
	}

	var snap flow.Snapshot
	getJSON(t, "http://"+c.uiAddr+"/api/runs/"+name, &snap)
	if snap.Run != name {
		t.Fatalf("snapshot run %q, want %q", snap.Run, name)
	}
	main := false
	for _, th := range snap.Threads {
		if th.ID == "main" && th.Status == "completed" {
			main = true
		}
	}
	if !main {
		t.Fatalf("no completed main thread in %+v", snap.Threads)
	}
}

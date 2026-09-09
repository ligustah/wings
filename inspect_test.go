package wings

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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

// THE POINT: the coordinator serves pprof on the inspection endpoint, so a live
// coordinator can be profiled for goroutine growth and churn.
func TestInspectionServesPprof(t *testing.T) {
	c := start(t, Config{Target: InProcess(), UI: "127.0.0.1:0"})
	resp, err := http.Get("http://" + c.uiAddr + "/debug/pprof/goroutine?debug=1")
	if err != nil {
		t.Fatalf("GET goroutine profile: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET goroutine profile: status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "goroutine profile") {
		t.Fatalf("did not serve a goroutine profile; got %d bytes", len(body))
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

	var detail runDetail
	getJSON(t, "http://"+c.uiAddr+"/api/runs/"+name, &detail)
	if detail.Run != name {
		t.Fatalf("detail run %q, want %q", detail.Run, name)
	}
	main := false
	for _, th := range detail.Threads {
		if th.ID == "main" && th.Status == "completed" {
			main = true
		}
	}
	if !main {
		t.Fatalf("no completed main thread in %+v", detail.Threads)
	}

	// The events come from the paged endpoint, not the run detail.
	var page flow.EventPage
	getJSON(t, "http://"+c.uiAddr+"/api/runs/"+name+"/threads/main?from=0&limit=100", &page)
	if !page.Done {
		t.Fatalf("a small run's first page should be the last: %+v", page)
	}
	if !hasEventKind(page.Events, "sleep") {
		t.Fatalf("main's events missing the recorded sleep: %+v", page.Events)
	}
}

// THE POINT: the paged events endpoint walks a long thread to the end over
// several pages — each page advancing the cursor — and terminates. The UI's
// "Load more" depends on this.
func TestRunEventsPaginateToTheEnd(t *testing.T) {
	mem := flow.NewMemStore()
	const n = 1200
	err := flow.Run(t.Context(), "big", func(c flow.Context) error {
		for i := 0; i < n; i++ {
			if _, err := c.Effect(func() ([]byte, error) { return []byte{byte(i)}, nil }); err != nil {
				return err
			}
		}
		return nil
	}, flow.WithStore(mem))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	srv := httptest.NewServer(inspectHandler("", mem))
	defer srv.Close()

	got, pages, from := 0, 0, int64(0)
	for {
		var page flow.EventPage
		getJSON(t, fmt.Sprintf("%s/api/runs/big/threads/main?from=%d&limit=500", srv.URL, from), &page)
		pages++
		for _, ev := range page.Events {
			if ev.Kind == "effect" {
				got++
			}
		}
		if page.Done {
			break
		}
		if page.Next <= from {
			t.Fatalf("cursor stuck at %d (page %d)", from, pages)
		}
		from = page.Next
		if pages > 50 {
			t.Fatalf("pagination did not terminate after %d pages", pages)
		}
	}
	if got != n {
		t.Fatalf("paged %d effect events across %d pages, want %d", got, pages, n)
	}
	if pages < 3 {
		t.Fatalf("expected several pages for %d events, got %d", n, pages)
	}
}

func hasEventKind(events []flow.EventView, kind string) bool {
	for _, e := range events {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

// THE POINT: -inspect serves a stopped run's recorded history from its data
// directory, with no cluster orchestrating.
func TestInspectModeServesStoppedRun(t *testing.T) {
	dir := t.TempDir()
	name := "test-postmortem-" + strconv.FormatUint(runSeq.Add(1), 36)

	c, err := Start(t.Context(), Config{Target: InProcess(), Dir: dir})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := c.Run(t.Context(), name, func(ctx flow.Context) error {
		return ctx.Sleep(0)
	}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := c.Stop(context.Background()); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	srv, err := serveInspector(dir, "127.0.0.1:0", slog.Default())
	if err != nil {
		t.Fatalf("serveInspector: %v", err)
	}
	defer srv.close()

	var st statusView
	getJSON(t, "http://"+srv.addr+"/api/status", &st)
	if st.Target != "inspect" {
		t.Fatalf("status target %q, want inspect", st.Target)
	}

	var detail runDetail
	getJSON(t, "http://"+srv.addr+"/api/runs/"+name, &detail)
	completed := false
	for _, th := range detail.Threads {
		if th.ID == "main" && th.Status == "completed" {
			completed = true
		}
	}
	if !completed {
		t.Fatalf("post-mortem did not recover a completed main thread: %+v", detail.Threads)
	}
}

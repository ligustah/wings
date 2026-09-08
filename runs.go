package wings

import (
	"net/http"
	"strconv"

	"github.com/ligustah/wings/flow"
)

// The run-inspection half of the UI API: the recorded histories in an engine,
// decoded by flow. Read-only, and shared by the live coordinator and the
// -inspect post-mortem mode — both supply a store, differing only in where its
// engine comes from. Histories are complete in the engine for the in-process
// and shared-broker targets; for targets where a worker keeps its own data, use
// -inspect against that data directory.

// runAPI serves the recorded-run endpoints off whatever store it is given.
type runAPI struct {
	store func() (flow.Store, error)
}

func (a runAPI) register(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/runs", a.handleRuns)
	mux.HandleFunc("GET /api/runs/{run}", a.handleRun)
	mux.HandleFunc("GET /api/runs/{run}/threads/{thread}", a.handleThreadEvents)
}

type runView struct {
	Run    string `json:"run"`
	Status string `json:"status"`
}

type runDetail struct {
	Run     string            `json:"run"`
	Threads []flow.ThreadInfo `json:"threads"`
}

func (a runAPI) handleRuns(w http.ResponseWriter, r *http.Request) {
	store, err := a.store()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	names, err := flow.ListRuns(r.Context(), store)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]runView, 0, len(names))
	for _, name := range names {
		v := runView{Run: name, Status: "running"}
		if status, err := flow.Status(r.Context(), store, name, "main"); err == nil {
			v.Status = status
		}
		out = append(out, v)
	}
	writeJSON(w, out)
}

func (a runAPI) handleRun(w http.ResponseWriter, r *http.Request) {
	run := r.PathValue("run")
	if run == "" {
		http.Error(w, "missing run", http.StatusBadRequest)
		return
	}
	store, err := a.store()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	threads, err := flow.InspectRunHeaders(r.Context(), store, run)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(threads) == 0 {
		http.Error(w, "no such run", http.StatusNotFound)
		return
	}
	writeJSON(w, runDetail{Run: run, Threads: threads})
}

// handleThreadEvents pages one thread's events, so the UI reads a run's timeline
// a window at a time rather than holding a whole thread's history at once.
func (a runAPI) handleThreadEvents(w http.ResponseWriter, r *http.Request) {
	run, thread := r.PathValue("run"), r.PathValue("thread")
	if run == "" || thread == "" {
		http.Error(w, "missing run or thread", http.StatusBadRequest)
		return
	}
	var from int64
	if s := r.URL.Query().Get("from"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil && v >= 0 {
			from = v
		}
	}
	limit := 500
	if s := r.URL.Query().Get("limit"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			limit = v
		}
	}
	store, err := a.store()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	page, err := flow.ReadEvents(r.Context(), store, run, thread, from, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, page)
}

// inspectStore returns a read-only view of the engine's recorded histories,
// including the threads that ran as jobs on workers (inspect_store.go).
func (c *Cluster) inspectStore() (flow.Store, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	return newInspectionStore(client), nil
}

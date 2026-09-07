package wings

import (
	"net/http"

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
}

type runView struct {
	Run    string `json:"run"`
	Status string `json:"status"`
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
		if snap, err := flow.Inspect(r.Context(), store, name, []string{"main"}); err == nil && len(snap.Threads) > 0 {
			v.Status = snap.Threads[0].Status
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
	snap, err := flow.InspectRun(r.Context(), store, run)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(snap.Threads) == 0 {
		http.Error(w, "no such run", http.StatusNotFound)
		return
	}
	writeJSON(w, snap)
}

// inspectStore returns a read-only view of the engine's recorded histories.
func (c *Cluster) inspectStore() (flow.Store, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	return flow.NewStore(client), nil
}

package wings

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"
)

// The inspection API: a read-only view of the cluster served over HTTP when
// Config.UI is set. It reports the live runtime state — workers, the pending
// queue, a cluster summary — that lives only in the coordinator's memory, plus
// the recorded runs; see runs.go for run detail. The handlers only read, under
// the cluster's lock.

// startUI serves the inspection API (and the embedded web UI) on addr.
func (c *Cluster) startUI(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("wings: serve the UI on %s: %w", addr, err)
	}
	srv := &http.Server{Handler: c.uiHandler()}
	c.uiSrv = srv
	c.uiAddr = lis.Addr().String()
	c.wg.Go(func() {
		if err := srv.Serve(lis); err != nil && err != http.ErrServerClosed {
			c.log.Error("wings: UI server stopped", "err", err)
		}
	})
	c.log.Info("wings: serving the inspection UI", "addr", c.uiAddr)
	return nil
}

func (c *Cluster) uiHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", c.handleStatus)
	mux.HandleFunc("GET /api/workers", c.handleWorkers)
	mux.HandleFunc("GET /api/pending", c.handlePending)
	mux.HandleFunc("GET /api/runs", c.handleRuns)
	mux.HandleFunc("GET /api/runs/{run}", c.handleRun)
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

type statusView struct {
	Target      string `json:"target"`
	Dir         string `json:"dir"`
	Workers     int    `json:"workers"`
	Outstanding int    `json:"outstanding"`
	MinWorkers  int    `json:"minWorkers"`
	MaxWorkers  int    `json:"maxWorkers"`
}

func (c *Cluster) handleStatus(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	available := 0
	for _, wk := range c.workers {
		if wk.available() {
			available++
		}
	}
	out := statusView{
		Target:      targetName(c.cfg.Target.kind),
		Dir:         c.dir,
		Workers:     available,
		Outstanding: len(c.pending),
		MinWorkers:  c.cfg.Scaling.Min,
		MaxWorkers:  c.cfg.Scaling.Max,
	}
	c.mu.Unlock()
	writeJSON(w, out)
}

type workerView struct {
	ID        string     `json:"id"`
	Kind      string     `json:"kind"`
	Inflight  int        `json:"inflight"`
	Blocked   int        `json:"blocked"`
	Load      int        `json:"load"`
	Draining  bool       `json:"draining"`
	Dead      bool       `json:"dead"`
	IdleSince *time.Time `json:"idleSince,omitempty"`
}

func (c *Cluster) handleWorkers(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	out := make([]workerView, 0, len(c.workers))
	for _, wk := range c.workers {
		v := workerView{
			ID:       wk.id,
			Kind:     workerKind(wk),
			Inflight: wk.inflight,
			Blocked:  wk.blocked,
			Load:     wk.load(),
			Draining: wk.draining,
			Dead:     wk.dead.Load(),
		}
		if !wk.idleSince.IsZero() {
			t := wk.idleSince
			v.IdleSince = &t
		}
		out = append(out, v)
	}
	c.mu.Unlock()
	writeJSON(w, out)
}

type pendingView struct {
	ID      string    `json:"id"`
	Func    string    `json:"fn,omitempty"`
	Run     string    `json:"run,omitempty"`
	Thread  string    `json:"thread,omitempty"`
	Attempt int       `json:"attempt"`
	Worker  string    `json:"worker,omitempty"`
	Blocked bool      `json:"blocked"`
	Placed  bool      `json:"placed"`
	Yielded bool      `json:"yielded"`
	Since   time.Time `json:"since"`
}

func (c *Cluster) handlePending(w http.ResponseWriter, _ *http.Request) {
	c.mu.Lock()
	out := make([]pendingView, 0, len(c.pending))
	for _, p := range c.pending {
		v := pendingView{
			ID:      p.job.ID,
			Func:    p.job.Func,
			Run:     p.job.Run,
			Thread:  p.job.Thread,
			Attempt: p.job.Attempt,
			Blocked: p.blocked,
			Placed:  p.placed,
			Yielded: p.yield != nil,
			Since:   p.since,
		}
		if p.worker != nil {
			v.Worker = p.worker.id
		}
		out = append(out, v)
	}
	c.mu.Unlock()
	writeJSON(w, out)
}

// workerKind names how a worker runs, for the UI.
func workerKind(w *workerConn) string {
	switch {
	case w.node != nil:
		return "inprocess"
	case w.machine != nil:
		return "remote"
	case w.proc != nil:
		return "local"
	default:
		return "unknown"
	}
}

// targetName is a target kind as a word, for the UI.
func targetName(k targetKind) string {
	switch k {
	case targetInProcess:
		return "inprocess"
	case targetLocalProcess:
		return "local"
	case targetRemote:
		return "remote"
	default:
		return "unknown"
	}
}

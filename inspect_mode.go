package wings

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"

	"github.com/ligustah/durable_streams/broker/embed"
	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/flow"
)

// Inspect serves the read-only UI and API over the recorded histories in a data
// directory, without orchestrating anything — a post-mortem on a stopped run.
// dir is the -dir a coordinator ran with; addr is where to listen (e.g.
// 127.0.0.1:8080). It blocks until ctx is cancelled, then shuts down.
func Inspect(ctx context.Context, dir, addr string, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	srv, err := serveInspector(dir, addr, log)
	if err != nil {
		return err
	}
	log.Info("wings: inspecting", "dir", srv.dir, "addr", srv.addr)
	<-ctx.Done()
	return srv.close()
}

type inspectServer struct {
	dir   string
	addr  string
	http  *http.Server
	close func() error
}

// serveInspector opens the engine under dir and serves the inspection UI and
// API on addr. It returns once listening; the caller closes it.
func serveInspector(dir, addr string, log *slog.Logger) (*inspectServer, error) {
	if dir == "" {
		dir = defaultDataDir
	}
	engineDir := filepath.Join(dir, "engine")
	b, err := embed.StartInProcess(embed.InProcessConfig{Dir: engineDir, Logger: streamLogger(log)})
	if err != nil {
		return nil, fmt.Errorf("wings: open engine in %s: %w", engineDir, err)
	}
	store := flow.NewStore(dsclient.Wrap(b.Client()))

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		_ = b.Close()
		return nil, fmt.Errorf("wings: serve inspection on %s: %w", addr, err)
	}
	httpSrv := &http.Server{Handler: inspectHandler(dir, store)}
	go func() {
		if err := httpSrv.Serve(lis); err != nil && err != http.ErrServerClosed {
			log.Error("wings: inspection server stopped", "err", err)
		}
	}()
	return &inspectServer{
		dir:  dir,
		addr: lis.Addr().String(),
		http: httpSrv,
		close: func() error {
			err := httpSrv.Shutdown(context.Background())
			if cerr := b.Close(); err == nil {
				err = cerr
			}
			return err
		},
	}, nil
}

// inspectHandler serves the same UI and run endpoints as the live coordinator,
// minus the live-only views (no workers or pending queue in a post-mortem).
func inspectHandler(dir string, store flow.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, statusView{Target: "inspect", Dir: dir})
	})
	mux.HandleFunc("GET /api/workers", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []workerView{})
	})
	mux.HandleFunc("GET /api/pending", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, []pendingView{})
	})
	runAPI{store: func() (flow.Store, error) { return store, nil }}.register(mux)
	mux.Handle("GET /", uiStatic())
	return mux
}

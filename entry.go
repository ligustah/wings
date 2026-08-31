package wings

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"

	"github.com/ligustah/wings/internal/payload"
)

// CoordinatorOptions is what a generated coordinator main hands to
// [CoordinatorMain]. You do not construct one; `wings build` writes the code
// that does.
type CoordinatorOptions struct {
	// Worker is the gzipped worker binary, embedded by the generated main.
	Worker []byte
	// WorkerOS and WorkerArch are the platform Worker was compiled for.
	WorkerOS, WorkerArch string

	// Provisioner supplies machines for the remote target. Nil means this
	// program was built without one, and -target=remote is refused rather than
	// failing later with something less obvious.
	Provisioner Provisioner

	// Coordinate is your code. It is called once, with a cluster that is
	// already up, and the cluster is torn down when it returns.
	Coordinate func(context.Context, *Cluster) error
}

// WorkerMain is the entire worker binary.
//
// The generated worker main is this call and an import of your package, whose
// [Define] calls register the work. A worker needs nothing else: it never
// provisions, never dispatches, and never runs your Coordinate.
//
// It does not return.
func WorkerMain() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	log := slog.Default()

	if !isWorkerProcess() {
		fmt.Fprintf(os.Stderr,
			"This is the wings WORKER binary. It is uploaded and started by a coordinator,\n"+
				"which sets %s=%s. Run the coordinator binary instead.\n", envMode, modeWorker)
		os.Exit(2)
	}

	if err := runWorkerProcess(ctx, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("wings: worker exiting", "err", err)
		os.Exit(1)
	}
}

// CoordinatorMain is the entire coordinator binary: it parses the standard
// flags, brings a cluster up, runs your Coordinate, and takes the cluster down
// again.
//
// It does not return.
func CoordinatorMain(opts CoordinatorOptions) {
	if opts.Coordinate == nil {
		fmt.Fprintln(os.Stderr, "wings: no Coordinate function was supplied")
		os.Exit(2)
	}
	if len(opts.Worker) > 0 {
		payload.Set(opts.Worker, opts.WorkerOS, opts.WorkerArch)
	}

	var (
		target      = flag.String("target", "inprocess", "where workers run: inprocess | local | remote")
		workers     = flag.Int("workers", 0, "number of workers; 0 uses the default for the target")
		concurrency = flag.Int("concurrency", 0, "jobs in flight per worker; 0 lets each worker decide")
		dir         = flag.String("dir", "", "data directory; empty uses a temporary one that is removed on exit")
		jobTimeout  = flag.Duration("job-timeout", 0, "bound on a single work function call; 0 means no bound")
		verbose     = flag.Bool("v", false, "log at debug level")
	)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	cfg := Config{
		Workers:     *workers,
		Concurrency: *concurrency,
		Dir:         *dir,
		JobTimeout:  *jobTimeout,
		Logger:      log,
	}

	switch strings.ToLower(*target) {
	case "inprocess", "inproc", "":
		cfg.Target = InProcess()
	case "local", "localprocess":
		cfg.Target = LocalProcess()
	case "remote", "cloud":
		if opts.Provisioner == nil {
			fmt.Fprintln(os.Stderr,
				"wings: -target=remote needs a provisioner, and this binary has none.\n"+
					"Export `func Provisioner() wings.Provisioner` from the package you built,\n"+
					"returning e.g. gcp.New(gcp.Config{Project: ..., Zone: ...}).")
			os.Exit(2)
		}
		cfg.Target = Remote(opts.Provisioner)
	default:
		fmt.Fprintf(os.Stderr, "wings: unknown -target %q; want inprocess, local or remote\n", *target)
		os.Exit(2)
	}

	// Interrupt cancels the work, but teardown gets a fresh context: machines
	// still have to be deleted, and doing it with a cancelled context would
	// leave them running and billing.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, err := Start(ctx, cfg)
	if err != nil {
		log.Error("wings: start", "err", err)
		os.Exit(1)
	}

	runErr := opts.Coordinate(ctx, c)
	stopErr := c.Stop(context.Background())

	if runErr != nil {
		log.Error("wings: coordinate", "err", runErr)
		if stopErr != nil {
			log.Error("wings: stop", "err", stopErr)
		}
		os.Exit(1)
	}
	if stopErr != nil {
		log.Error("wings: stop", "err", stopErr)
		os.Exit(1)
	}
}

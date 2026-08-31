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
		provider    = flag.String("provider", "", providerUsage())
		workers     = flag.Int("workers", 0, "number of workers; 0 uses the default for the target")
		concurrency = flag.Int("concurrency", 0, "jobs in flight per worker; 0 lets each worker decide")
		dir         = flag.String("dir", "", "data directory; empty uses a temporary one that is removed on exit")
		jobTimeout  = flag.Duration("job-timeout", 0, "bound on a single work function call; 0 means no bound")
		verbose     = flag.Bool("v", false, "log at debug level")

		// Autoscaling. Off unless -max-workers is set, and expressed only in
		// jobs and durations — nothing here names a target, so the same numbers
		// mean the same thing whether a worker is a goroutine or a VM.
		maxWorkers    = flag.Int("max-workers", 0, "autoscale up to this many workers; 0 keeps the count fixed")
		minWorkers    = flag.Int("min-workers", 0, "when autoscaling, never drop below this many workers")
		jobsPerWorker = flag.Int("jobs-per-worker", 0, "when autoscaling, how much backlog one worker should carry")
		idleTimeout   = flag.Duration("idle-timeout", 0, "when autoscaling, how long a worker must be idle before it is retired")
		scaleInterval = flag.Duration("scale-interval", 0, "when autoscaling, how often the policy is evaluated")
		maxScaleStep  = flag.Int("max-scale-step", 0, "when autoscaling, the most workers one decision may add")
	)
	// Every linked-in provider's flags, before parsing — which provider is
	// selected is itself a parsed flag, so they all have to be declared first.
	registerProviderFlags(flag.CommandLine)
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
		Scaling: Scaling{
			Min:           *minWorkers,
			Max:           *maxWorkers,
			JobsPerWorker: *jobsPerWorker,
			IdleTimeout:   *idleTimeout,
			Interval:      *scaleInterval,
			MaxStep:       *maxScaleStep,
		},
	}

	switch strings.ToLower(*target) {
	case "inprocess", "inproc", "":
		cfg.Target = InProcess()
	case "local", "localprocess":
		cfg.Target = LocalProcess()
	case "remote", "cloud":
		// A provisioner supplied in code wins: it is the escape hatch for a
		// cloud wings does not ship, and someone who wrote one meant it.
		// Otherwise the choice comes off the command line, which is what keeps
		// the program itself free of any mention of where it runs.
		prov := opts.Provisioner
		if prov == nil {
			var err error
			if prov, err = resolveProvider(*provider); err != nil {
				fmt.Fprintf(os.Stderr, "wings: %v\n", err)
				os.Exit(2)
			}
		}
		cfg.Target = Remote(prov)
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

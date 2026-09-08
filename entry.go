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

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/internal/payload"
)

// CoordinatorOptions is what a generated coordinator main hands to
// [CoordinatorMain]; `wings build` writes the code that constructs it.
type CoordinatorOptions struct {
	// Worker is the gzipped worker binary, embedded by the generated main.
	Worker []byte
	// WorkerOS and WorkerArch are the platform Worker was compiled for.
	WorkerOS, WorkerArch string

	// Provisioner supplies machines for the remote target. Nil refuses
	// -target=remote unless a provider is linked in.
	Provisioner Provisioner
}

// WorkerMain is the entire worker binary: a call to this plus an import of the
// package whose [flow.Define] calls register the work. It does not return.
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
// flags, brings a cluster up, runs the chosen workflow, and takes it down again.
// It does not return.
func CoordinatorMain(opts CoordinatorOptions) {
	if len(opts.Worker) > 0 {
		payload.Set(opts.Worker, opts.WorkerOS, opts.WorkerArch)
	}

	var (
		target      = flag.String("target", "inprocess", "where workers run: inprocess | local | remote")
		provider    = flag.String("provider", "", providerUsage())
		workers     = flag.Int("workers", 0, "number of workers; 0 uses the default for the target")
		concurrency = flag.Int("concurrency", 0, "jobs in flight per worker; 0 lets each worker decide")
		dir         = flag.String("dir", "", "data directory; empty uses ./wings-data")
		localShared = flag.Bool("local-shared-broker", false, "for -target local: workers share the coordinator's broker instead of each keeping its own data")
		ui          = flag.String("ui", "", "serve a read-only inspection UI and API at this address; a bare port or :PORT or 0.0.0.0:PORT binds every interface (reachable over e.g. Tailscale), 127.0.0.1:PORT stays local; empty is off")
		keepHistory = flag.Bool("keep-history", false, "keep forked threads' histories after they are joined, so a finished run's whole thread tree stays inspectable (coordinator-process threads only)")
		inspect     = flag.String("inspect", "", "serve the inspection UI over an existing data directory and exit; does not run a workflow")
		jobTimeout  = flag.Duration("job-timeout", 0, "bound on a single work function call; 0 means no bound")
		verbose     = flag.Bool("v", false, "log at debug level")
		workflow    = flag.String("workflow", "", "which defined workflow to run; unneeded when the program defines only one")
		input       = flag.String("input", "", "the workflow's input as JSON, or @file to read it from a file; leave off to resume a run already in -dir")

		// Autoscaling, off unless -max-workers is set.
		maxWorkers    = flag.Int("max-workers", 0, "autoscale up to this many workers; 0 keeps the count fixed")
		minWorkers    = flag.Int("min-workers", 0, "when autoscaling, never drop below this many workers")
		jobsPerWorker = flag.Int("jobs-per-worker", 0, "when autoscaling, how much backlog one worker should carry; 0 means -concurrency, or 1 if that is unset")
		idleTimeout   = flag.Duration("idle-timeout", 0, "when autoscaling, how long a worker must be idle before it is retired")
		scaleInterval = flag.Duration("scale-interval", 0, "when autoscaling, how often the policy is evaluated")
		maxScaleStep  = flag.Int("max-scale-step", 0, "when autoscaling, the most workers one decision may add")
	)
	registerProviderFlags(flag.CommandLine)
	flag.Parse()

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	// -inspect serves the UI over a stopped run's data directory and runs no
	// workflow, so it short-circuits the rest.
	if *inspect != "" {
		addr := listenAddr(*ui)
		if addr == "" {
			addr = "127.0.0.1:8080"
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		defer stop()
		if err := Inspect(ctx, *inspect, addr, log); err != nil {
			log.Error("wings: inspect", "err", err)
			os.Exit(1)
		}
		return
	}

	// A worker is this same binary with no workflow to run, so it must not be
	// asked to choose one.
	var (
		w       flow.WorkflowInfo
		payload []byte
	)
	if !isWorkerProcess() {
		var err error
		if w, err = chooseWorkflow(*workflow, flow.Workflows()); err != nil {
			fmt.Fprintf(os.Stderr, "wings: %v\n", err)
			os.Exit(2)
		}
		if payload, err = readInput(*input, w); err != nil {
			fmt.Fprintf(os.Stderr, "wings: %v\n", err)
			os.Exit(2)
		}
	}

	cfg := Config{
		Workers:           *workers,
		Concurrency:       *concurrency,
		Dir:               *dir,
		LocalSharedBroker: *localShared,
		UI:                listenAddr(*ui),
		RetainHistory:     *keepHistory,
		JobTimeout:        *jobTimeout,
		Logger:            log,
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
		// A provisioner supplied in code wins; otherwise -provider chooses.
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

	// Teardown gets a fresh context so an interrupt still deletes machines.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	c, err := Start(ctx, cfg)
	if err != nil {
		log.Error("wings: start", "err", err)
		os.Exit(1)
	}

	runErr := c.runWorkflow(ctx, w.Name, payload)
	stopErr := c.Stop(context.Background())

	if runErr != nil {
		log.Error("wings: workflow", "name", w.Name, "err", runErr)
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

// listenAddr normalizes a UI/inspect listen address so it is easy to expose: a
// bare port ("8080") becomes ":8080", which binds every interface — reachable
// over Tailscale or a LAN — as does ":8080" or "0.0.0.0:8080". A host-qualified
// address like "127.0.0.1:8080" is left alone and stays local. Empty is empty.
func listenAddr(s string) string {
	if s == "" || strings.Contains(s, ":") {
		return s
	}
	return ":" + s
}

// chooseWorkflow picks which defined workflow to run: the only one, or the one
// -workflow names when there are several.
func chooseWorkflow(name string, defined []flow.WorkflowInfo) (flow.WorkflowInfo, error) {
	if len(defined) == 0 {
		return flow.WorkflowInfo{}, errors.New("no root is declared in this program; " +
			"declare one at package scope with flow.Main")
	}
	if name == "" {
		if len(defined) == 1 {
			return defined[0], nil
		}
		return flow.WorkflowInfo{}, fmt.Errorf("this program defines %d workflows; pick one with -workflow: %s",
			len(defined), workflowNames(defined))
	}
	for _, w := range defined {
		if w.Name == name {
			return w, nil
		}
	}
	return flow.WorkflowInfo{}, fmt.Errorf("no workflow %q is defined in this program; it defines: %s",
		name, workflowNames(defined))
}

func workflowNames(ws []flow.WorkflowInfo) string {
	names := make([]string, len(ws))
	for i, w := range ws {
		names[i] = w.Name
	}
	return strings.Join(names, ", ")
}

// readInput turns the -input flag into the workflow's input JSON: the text
// itself, or the contents of a file named with a leading @. Empty is nil.
func readInput(flagValue string, w flow.WorkflowInfo) ([]byte, error) {
	if flagValue == "" {
		return nil, nil
	}
	if w.Input == nil {
		return nil, fmt.Errorf("workflow %s takes no input, and -input was given", w.Name)
	}
	if path, ok := strings.CutPrefix(flagValue, "@"); ok {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read -input file: %w", err)
		}
		return b, nil
	}
	return []byte(flagValue), nil
}

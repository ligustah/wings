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
}

// Your code is not in CoordinatorOptions: it is whatever the linked packages
// declared as roots with [flow.Main]. A program that declares one root
// runs it; one that declares several is told which by -workflow. Either way it
// runs as a durable run on a cluster that is already up — see
// [Cluster.RunWorkflow] — under its own name in Dir, so a second start over
// the same directory is that run resuming, and the cluster is torn down when
// it returns.

// WorkerMain is the entire worker binary.
//
// The generated worker main is this call and an import of your package, whose
// flow.Define calls register the work. A worker needs nothing else: it never
// provisions, never dispatches, and never runs a workflow.
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
// flags, brings a cluster up, runs the chosen workflow, and takes the cluster
// down again.
//
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
		dir         = flag.String("dir", "", "data directory; empty uses a temporary one that is removed on exit")
		jobTimeout  = flag.Duration("job-timeout", 0, "bound on a single work function call; 0 means no bound")
		verbose     = flag.Bool("v", false, "log at debug level")
		workflow    = flag.String("workflow", "", "which defined workflow to run; unneeded when the program defines only one")
		input       = flag.String("input", "", "the workflow's input as JSON, or @file to read it from a file; leave off to resume a run already in -dir")

		// Autoscaling. Off unless -max-workers is set, and expressed only in
		// jobs and durations — nothing here names a target, so the same numbers
		// mean the same thing whether a worker is a goroutine or a VM.
		maxWorkers    = flag.Int("max-workers", 0, "autoscale up to this many workers; 0 keeps the count fixed")
		minWorkers    = flag.Int("min-workers", 0, "when autoscaling, never drop below this many workers")
		jobsPerWorker = flag.Int("jobs-per-worker", 0, "when autoscaling, how much backlog one worker should carry; 0 means -concurrency, or 1 if that is unset")
		idleTimeout   = flag.Duration("idle-timeout", 0, "when autoscaling, how long a worker must be idle before it is retired")
		scaleInterval = flag.Duration("scale-interval", 0, "when autoscaling, how often the policy is evaluated")
		maxScaleStep  = flag.Int("max-scale-step", 0, "when autoscaling, the most workers one decision may add")
	)
	// Every linked-in provider's flags, before parsing — which provider is
	// selected is itself a parsed flag, so they all have to be declared first.
	registerProviderFlags(flag.CommandLine)
	flag.Parse()

	// Which workflow, and with what: a coordinator's question, settled
	// before any machine is paid for. A worker is this same binary with the
	// same flags and no workflow to run, so it must not be asked — with two
	// defined and no -workflow it would refuse to start, and the coordinator
	// would wait for a worker that had exited.
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

	// As a run, not a call: every function called inside the workflow goes to
	// this cluster's workers and into a history under Dir, so a coordinator
	// started again in the same directory carries on from where the last one
	// stopped. That is the whole reason a function is callable rather than
	// something you pass to a method: the context already knows where work
	// goes, and what has already been done.
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

// chooseWorkflow picks which of the defined workflows this coordinator runs.
//
// With one defined, that one — a program that is about one thing should not
// have to say so. With several, the name is required and a missing or unknown
// one is answered with the list, since the list is the only thing the caller
// needs to fix the command line.
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

// readInput turns the -input flag into the workflow's input as JSON: the
// text itself, or the contents of a file named with a leading @. Empty is nil,
// which flow reads as "none given" — fine for a workflow that takes none, and
// for resuming a run whose input is already recorded; what a fresh run of a
// workflow that takes input makes of it is flow's error to give, with the
// shape it wants.
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

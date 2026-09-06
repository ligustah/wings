// Command wings builds a self-contained coordinator with its worker inside it.
//
//	go run github.com/ligustah/wings/cmd/wings build -pkg ./job -worker linux/amd64 -o myapp
//
// # What you write
//
// A LIBRARY package — not a main — holding your work functions and the
// coordinator body:
//
//	package job
//
//	var Render = flow.Define(func(ctx flow.Context, f Frame) (Image, error) { … }, flow.WithName("render"))
//
//	// A root is run as a flow once the cluster is up.
//	var Frames = flow.Define(func(ctx flow.Context, job Job) (flow.None, error) {
//		imgs, err := ctx.Map(Render, job.Frames)
//		…
//	})
//	var _ = flow.Main(Frames)
//
// That is all. WHERE the work runs is not in it: -providers links clouds into
// the coordinator, and -target and -provider choose between them at run time.
//
// For a cloud wings does not ship you may instead export
// `func Provisioner() wings.Provisioner`, which takes precedence over -provider
// — the one thing that puts a cloud back into your own source.
//
// # What this builds
//
// TWO programs, from two mains generated into a scratch directory under your
// module root (.wings-build/) which is removed afterwards. It has to be inside
// the module so the generated code can import your package by its normal path;
// no file of yours is touched, and nothing is left behind by a build that is
// not killed outright.
//
//	worker       imports your package for its Define calls, and calls
//	             wings.WorkerMain. It carries no provisioner, no coordinator
//	             body, and no embedded binary of its own. Cross-compiled for the
//	             machines it will run on.
//
//	coordinator  imports your package, embeds the worker with //go:embed, and
//	             calls wings.CoordinatorMain. Built for the machine YOU will run
//	             it on — natively, so nothing about the coordinator is
//	             cross-compiled.
//
// Splitting them is why the coordinator's platform and the worker's are
// independent, and why the worker does not have to contain the cloud SDK it is
// deployed by.
//
// # Keeping workers small
//
// Providers are linked into the coordinator only, so a worker never carries the
// cloud SDK that deployed it — dropping the example's worker from 28 MB to 15 MB
// when its provisioner moved to a flag.
//
// Both mains do import the package named by -pkg, so anything IT pulls in lands
// in the worker too. Pass -coordinator-pkg to split them: -pkg holds the work
// functions both halves need, -coordinator-pkg holds the workflows and is
// linked only into the coordinator.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
)

const blobName = "wings_worker.bin.gz"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "build":
		if err := build(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "wings build: %v\n", err)
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "wings: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `wings builds a self-contained coordinator with its worker embedded.

Usage:
  wings build -pkg <package> -o <output> [flags]

Flags:
  -pkg              package holding your work functions; linked into BOTH halves (default ".")
  -coordinator-pkg  package holding the workflows; defaults to -pkg. Split it out to keep
                    anything only the coordinator needs out of the worker.
  -coordinator      os/arch to run the coordinator on (default: this machine)
  -worker           os/arch to run workers on (default "linux/amd64")
  -o                output path for the coordinator binary (required)
  -worker-out       also write the bare worker binary here (for inspection)
  -providers        clouds to link into the coordinator, comma-separated (default "gcp").
                    A bare name means github.com/ligustah/wings/<name>; empty links none.
  -tags             build tags, passed to go build
  -ldflags          extra linker flags, appended after -s -w
  -keep-debug       keep debug info; binaries are much larger
  -v                print the go build commands

Your package must declare, at package scope:
  var X = flow.Define(func(ctx flow.Context, in Input) (Out, error) { … })
  var _ = flow.Main(X)                    (at least one; with several, the binary takes -workflow;
                                          the input comes from -input as JSON, or is flow.None)
  func Provisioner() wings.Provisioner    (optional; overrides -provider)

Example:
  wings build -pkg ./job -coordinator windows/amd64 -worker linux/amd64 -o myapp.exe

`)
}

type platform struct{ os, arch string }

func (p platform) String() string { return p.os + "/" + p.arch }

func parsePlatform(s, what string) (platform, error) {
	os_, arch, ok := strings.Cut(s, "/")
	if !ok || os_ == "" || arch == "" {
		return platform{}, fmt.Errorf("%s platform %q must look like os/arch, e.g. linux/amd64", what, s)
	}
	return platform{os_, arch}, nil
}

func build(args []string) error {
	fs := flag.NewFlagSet("build", flag.ExitOnError)
	pkg := fs.String("pkg", ".", "package holding your work functions")
	coordPkg := fs.String("coordinator-pkg", "", "package holding the workflows and Provisioner; defaults to -pkg")
	coordFlag := fs.String("coordinator", runtime.GOOS+"/"+runtime.GOARCH, "os/arch to run the coordinator on")
	workerFlag := fs.String("worker", "linux/amd64", "os/arch to run workers on")
	out := fs.String("o", "", "output path for the coordinator binary (required)")
	workerOut := fs.String("worker-out", "", "also write the bare worker binary here")
	providers := fs.String("providers", "gcp",
		"comma-separated cloud providers to link into the coordinator; a bare name means "+
			"github.com/ligustah/wings/<name>, a slash means an import path. Empty links none.")
	tags := fs.String("tags", "", "build tags")
	ldflags := fs.String("ldflags", "", "extra linker flags")
	keepDebug := fs.Bool("keep-debug", false, "keep debug info")
	verbose := fs.Bool("v", false, "print the go build commands")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *out == "" {
		return fmt.Errorf("-o is required")
	}
	if *coordPkg == "" {
		*coordPkg = *pkg
	}

	coord, err := parsePlatform(*coordFlag, "coordinator")
	if err != nil {
		return err
	}
	worker, err := parsePlatform(*workerFlag, "worker")
	if err != nil {
		return err
	}

	work, err := describe(*pkg)
	if err != nil {
		return err
	}
	coordinate := work
	if *coordPkg != *pkg {
		if coordinate, err = describe(*coordPkg); err != nil {
			return err
		}
	}
	if work.Name == "main" {
		return fmt.Errorf("package %s is a main package; wings generates main for you, so -pkg must be a library package", *pkg)
	}

	api, err := inspectAPI(coordinate.Dir)
	if err != nil {
		return err
	}
	if !api.definesWorkflow && coordinate.Dir != work.Dir {
		// A split build may keep the workflows with the work functions.
		workAPI, err := inspectAPI(work.Dir)
		if err != nil {
			return err
		}
		api.definesWorkflow = workAPI.definesWorkflow
	}
	if !api.definesWorkflow {
		return fmt.Errorf("package %s declares no root.\n"+
			"Add, at package scope:\n\n\tvar Main = flow.Define(func(ctx flow.Context, in Input) (Out, error) { … })\n\tvar _ = flow.Main(Main)\n",
			coordinate.ImportPath)
	}

	absOut, err := filepath.Abs(*out)
	if err != nil {
		return err
	}

	// The scratch tree lives INSIDE the module so the generated mains can import
	// the user's package by its normal path, with the module graph already
	// correct. A directory elsewhere would need a synthesised go.mod and a
	// replace, which is a second, worse copy of information the module already
	// has.
	scratch, cleanup, err := scratchDir(work.Module.Dir)
	if err != nil {
		return err
	}
	defer cleanup()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	defer signal.Stop(sigs)
	go func() {
		if _, ok := <-sigs; ok {
			cleanup()
			os.Exit(130)
		}
	}()

	opts := buildOpts{tags: *tags, ldflags: *ldflags, keepDebug: *keepDebug, verbose: *verbose}

	// ---- worker ----
	workerDir := filepath.Join(scratch, "worker")
	if err := os.MkdirAll(workerDir, 0o755); err != nil {
		return err
	}
	workerSrc, err := workerMain(work.ImportPath)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(workerDir, "main.go"), workerSrc, 0o644); err != nil {
		return err
	}

	workerBin := filepath.Join(scratch, "worker.bin")
	fmt.Fprintf(os.Stderr, "worker       %-14s ", worker)
	if err := goBuild(workerBin, workerDir, worker, opts); err != nil {
		fmt.Fprintln(os.Stderr)
		return err
	}
	fmt.Fprintln(os.Stderr, sizeOf(workerBin))

	if *workerOut != "" {
		if err := copyFile(workerBin, *workerOut); err != nil {
			return err
		}
	}

	// ---- coordinator ----
	coordDir := filepath.Join(scratch, "coordinator")
	if err := os.MkdirAll(coordDir, 0o755); err != nil {
		return err
	}
	blobPath := filepath.Join(coordDir, blobName)
	if err := gzipTo(blobPath, workerBin); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "embedded     %-14s %s (gzipped)\n", worker, sizeOf(blobPath))

	coordSrc, err := coordinatorMain(work.ImportPath, coordinate.ImportPath, worker,
		api.hasProvisioner, providerImports(*providers))
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(coordDir, "main.go"), coordSrc, 0o644); err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "coordinator  %-14s ", coord)
	if err := goBuild(absOut, coordDir, coord, opts); err != nil {
		fmt.Fprintln(os.Stderr)
		return err
	}
	fmt.Fprintln(os.Stderr, sizeOf(absOut))

	fmt.Fprintf(os.Stderr, "\n%s carries a %s worker.\n", absOut, worker)
	switch links := providerImports(*providers); {
	case api.hasProvisioner:
		fmt.Fprintf(os.Stderr, "-target remote uses the Provisioner your package exports.\n")
	case len(links) > 0:
		fmt.Fprintf(os.Stderr, "-target remote is available via -provider (%s).\n", *providers)
	default:
		fmt.Fprintf(os.Stderr, "No provider linked in and no Provisioner exported, so -target remote "+
			"is unavailable; inprocess and local work.\n")
	}
	return nil
}

// pkgInfo is the part of `go list -json` we need.
// providerImports turns the -providers list into import paths. A bare name is
// one of the providers wings ships; anything with a slash is somebody else's.
func providerImports(list string) []string {
	var out []string
	for name := range strings.SplitSeq(list, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if !strings.Contains(name, "/") {
			name = wingsPkg + "/" + name
		}
		out = append(out, name)
	}
	return out
}

// pkgInfo is the part of `go list -json` we need.
type pkgInfo struct {
	Dir        string
	Name       string
	ImportPath string
	Module     struct{ Dir string }
}

func describe(pkg string) (pkgInfo, error) {
	var info pkgInfo
	cmd := exec.Command("go", "list", "-json", pkg)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return info, fmt.Errorf("go list %s: %w\n%s", pkg, err, strings.TrimSpace(stderr.String()))
	}
	if err := json.Unmarshal(out, &info); err != nil {
		return info, fmt.Errorf("parse go list output for %s: %w", pkg, err)
	}
	if info.Module.Dir == "" {
		return info, fmt.Errorf("package %s is not in a module; wings needs one to generate against", pkg)
	}
	return info, nil
}

type pkgAPI struct {
	definesWorkflow bool
	hasProvisioner  bool
}

// inspectAPI looks for the function the generated main may call, and for the
// flow.Main call that gives the coordinator a root to run.
//
// Parsed rather than probed by compiling: a missing root should be one clear
// sentence naming the call to add, not a compile error inside generated code
// the user never wrote and cannot see — or worse, a binary that builds and then
// refuses to start.
func inspectAPI(dir string) (pkgAPI, error) {
	var api pkgAPI
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		return api, fmt.Errorf("parse %s: %w", dir, err)
	}
	for _, p := range pkgs {
		for _, f := range p.Files {
			for _, decl := range f.Decls {
				switch d := decl.(type) {
				case *ast.FuncDecl:
					if d.Recv == nil && d.Name.Name == "Provisioner" {
						api.hasProvisioner = true
					}
				case *ast.GenDecl:
					// Package-scope vars only: a workflow defined inside a
					// function is not in the registry when the coordinator
					// looks, and it is better to say so here.
					ast.Inspect(d, func(n ast.Node) bool {
						call, ok := n.(*ast.CallExpr)
						if !ok {
							return true
						}
						switch fn := call.Fun.(type) {
						case *ast.SelectorExpr:
							api.definesWorkflow = api.definesWorkflow || fn.Sel.Name == "Main"
						case *ast.Ident:
							api.definesWorkflow = api.definesWorkflow || fn.Name == "Main"
						}
						return true
					})
				}
			}
		}
	}
	return api, nil
}

func scratchDir(moduleDir string) (string, func(), error) {
	base := filepath.Join(moduleDir, ".wings-build")
	if err := os.MkdirAll(base, 0o755); err != nil {
		return "", nil, fmt.Errorf("create scratch dir: %w", err)
	}
	dir, err := os.MkdirTemp(base, "b-")
	if err != nil {
		return "", nil, fmt.Errorf("create scratch dir: %w", err)
	}
	return dir, func() {
		_ = os.RemoveAll(dir)
		// Remove the parent too when nothing else is using it, so a clean build
		// leaves no trace.
		_ = os.Remove(base)
	}, nil
}

type buildOpts struct {
	tags      string
	ldflags   string
	keepDebug bool
	verbose   bool
}

func goBuild(out, dir string, p platform, o buildOpts) error {
	// Stripped and trimmed: these are shipped, not debugged in place, and the
	// worker is uploaded once per machine.
	ld := "-s -w"
	if o.keepDebug {
		ld = ""
	}
	if o.ldflags != "" {
		ld = strings.TrimSpace(ld + " " + o.ldflags)
	}

	args := []string{"build", "-trimpath"}
	if ld != "" {
		args = append(args, "-ldflags", ld)
	}
	if o.tags != "" {
		args = append(args, "-tags", o.tags)
	}
	args = append(args, "-o", out, ".")

	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	// CGO off so a cross build needs no target C toolchain, and so the worker is
	// static — the VM image it lands on need not have the same libc as the
	// machine that built it.
	cmd.Env = append(os.Environ(), "GOOS="+p.os, "GOARCH="+p.arch, "CGO_ENABLED=0")

	if o.verbose {
		fmt.Fprintf(os.Stderr, "\n  cd %s && GOOS=%s GOARCH=%s go %s\n", dir, p.os, p.arch, strings.Join(args, " "))
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("go build for %s: %w\n%s", p, err, stderr.String())
	}
	return nil
}

func gzipTo(dst, src string) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	f, err := os.Create(dst)
	if err != nil {
		return fmt.Errorf("create %s: %w", dst, err)
	}
	defer f.Close()

	zw, err := gzip.NewWriterLevel(f, gzip.BestCompression)
	if err != nil {
		return err
	}
	if _, err := zw.Write(raw); err != nil {
		return fmt.Errorf("compress worker: %w", err)
	}
	if err := zw.Close(); err != nil {
		return fmt.Errorf("compress worker: %w", err)
	}
	return f.Close()
}

func copyFile(src, dst string) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, raw, 0o755)
}

func sizeOf(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return "?"
	}
	return fmt.Sprintf("%6.1f MB", float64(info.Size())/(1<<20))
}

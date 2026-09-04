package wings

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ligustah/durable_streams/dsclient"

	"github.com/ligustah/wings/internal/invoke"
)

// Getting a job's streams off a worker and onto the coordinator, and back again
// when the job moves.
//
// A worker owns its storage, and a cloud worker's storage is destroyed the
// moment its work is done. Anything a job writes that has to outlive the machine
// must be copied while the machine is still up. That copy is a mirror —
// [dsclient.MirrorSet], which discovers streams on another deployment, forwards
// each one verbatim, and commits how far it has got in the same transaction as
// the records themselves.
//
// wings therefore keeps no bookkeeping about any of it. Where a copy has reached
// lives at the destination beside the data, so the two cannot disagree; which
// streams to copy is re-derived from their names on every pass, so it is never
// stale. Both directions are the same machinery.
//
// Nothing here knows what a recording or an artifact is. They are separate
// features that both need a stream to survive its worker, the way two programs
// both need a filesystem.

const (
	// outputSet names the mirror. It becomes the consumer group each copy's
	// position is stored under, so it is a constant rather than anything
	// per-run: a coordinator that restarts resumes its copies where they had got
	// to instead of making them again.
	outputSet = "wings.outputs"

	// outputDiscover is how often the fleet is re-read and re-listed without
	// being asked.
	//
	// Long, deliberately. The set FOLLOWS each worker's catalog, so a job
	// creating a stream wakes a pass at the moment it happens rather than at the
	// next tick — the window this used to bound is closed by the create itself.
	// What remains for the ticker is what no per-worker watch can see: a machine
	// arriving or leaving, a watch that was lost, a worker too old to serve a
	// catalog at all. wings pokes on the first of those, so this is the net
	// under the other two.
	outputDiscover = 5 * time.Minute

	// outputDrain bounds how long taking work off a worker waits for the copy of
	// what it wrote to catch up.
	//
	// Generous, because it is paid per redispatch and per retirement rather than
	// per anything frequent, and it ends early the moment the copy is level.
	outputDrain = 15 * time.Second

	// outputPoll is how often that wait looks again.
	outputPoll = 50 * time.Millisecond

	// outputWait bounds how long a reader waits for output still on its way —
	// the handle travels in the result, and what it names travels behind it.
	outputWait = 2 * time.Minute
)

// The three families of stream a job's output lives in. All three are built and
// taken apart by outputName below, and nothing else may assume their shape.
const (
	// recordingPrefix is a job's event log. See recording.go.
	recordingPrefix = "wings.replay."
	// artifactPrefix is a job's file. See artifact.go.
	artifactPrefix = "wings.artifact."
	// priorPrefix is where a PREVIOUS attempt's log is put on the worker that is
	// about to run the next one.
	//
	// A namespace of its own, and this is the only reason it has one: a worker's
	// output is mirrored to the coordinator, and a copy travelling the other way
	// must not be caught by that mirror and forwarded straight back. Two mirrors
	// writing one destination interleave their records and fence each other's
	// producer, so the two directions are kept where they cannot meet.
	priorPrefix = "wings.prior."
)

// outputName is one output's stream name, taken apart.
//
// The name carries everything the coordinator needs to know about a stream it
// finds on a worker — whose it is, which attempt wrote it, what the job called
// it. That is what lets there be no register of outputs anywhere: a register
// would be a second answer to a question the name already answers, and a second
// answer can be wrong.
type outputName struct {
	Prefix  string
	Job     string
	Attempt int
	Name    string
}

// String builds the stream name.
func (o outputName) String() string {
	return o.Prefix + streamPart(o.Job) + "." + strconv.Itoa(o.Attempt) + "." + streamPart(o.Name)
}

// in returns the same output in another family — the coordinator's copy of a
// recording, and the copy of it put on a worker for a retry, are the same thing
// under two prefixes.
func (o outputName) in(prefix string) outputName { o.Prefix = prefix; return o }

// parseOutput takes a stream name apart, and reports whether it is one of ours.
//
// A plain split is enough because every part goes through streamPart, which
// leaves a dot in none of them.
func parseOutput(stream string) (outputName, bool) {
	for _, prefix := range []string{recordingPrefix, artifactPrefix, priorPrefix} {
		rest, ok := strings.CutPrefix(stream, prefix)
		if !ok {
			continue
		}
		parts := strings.Split(rest, ".")
		if len(parts) != 3 {
			return outputName{}, false
		}
		attempt, err := strconv.Atoi(parts[1])
		if err != nil {
			return outputName{}, false
		}
		return outputName{Prefix: prefix, Job: parts[0], Attempt: attempt, Name: parts[2]}, true
	}
	return outputName{}, false
}

// streamPart makes one part of a stream name safe to join with dots.
//
// A job id is minted here and is already safe, but the output's name is the
// caller's word and a work function may call its output whatever it likes.
// Rather than reject those, map them: everything outside a small safe set
// becomes an underscore — dots especially, since a dot is what separates the
// parts.
func streamPart(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// batchFor is how many records of one kind of output move per transaction.
func batchFor(prefix string) int {
	if prefix == artifactPrefix {
		return artifactBatch
	}
	return recordBatch
}

// outputSink is a worker, from the point of view of something writing bulk
// output. Implemented by workerNode.
type outputSink interface {
	// declareOutput stands a stream up on this worker and returns the client to
	// write it through.
	declareOutput(ctx context.Context, stream, name string) (*dsclient.Client, error)
}

// jobOutput resolves the job an output belongs to and stands its stream up.
//
// The one thing [Record] and [Create] both call, because both need a stream on
// the worker that the coordinator will keep. What they put in it is their own
// business, and they share no code past this point.
func jobOutput(ctx context.Context, prefix, name string) (*dsclient.Client, string, error) {
	if name == "" {
		return nil, "", errors.New("wings: this needs a name")
	}
	st := beatFrom(ctx)
	if st == nil {
		return nil, "", errors.New("wings: this was called outside a work function; " +
			"it belongs to a job, and there is no job here")
	}
	sink, ok := st.sink.(outputSink)
	if !ok || sink == nil {
		return nil, "", errors.New("wings: this worker cannot store bulk output")
	}
	stream := outputName{Prefix: prefix, Job: st.job, Attempt: st.attempt, Name: name}.String()
	client, err := sink.declareOutput(ctx, stream, name)
	if err != nil {
		return nil, "", err
	}
	return client, stream, nil
}

// ensureStream creates a stream if it is not already there.
func ensureStream(ctx context.Context, client *dsclient.Client, name string) error {
	ok, err := client.StreamExists(ctx, name)
	if err != nil {
		return fmt.Errorf("wings: check %s: %w", name, err)
	}
	if ok {
		return nil
	}
	if err := client.CreateStream(ctx, name, nil); err != nil {
		return fmt.Errorf("wings: create %s: %w", name, err)
	}
	return nil
}

// awaitStream waits for a stream to be here, when something is still expected to
// arrive in it.
//
// A handle travels in the result, and the result is a different stream from the
// output it names — so on the coordinator a handle can arrive before the mirror
// has finished, or even started, copying what it points at. A reader that took
// absence for an answer would report a file that never arrived when it was
// merely early, which is the worst way to lose data.
//
// expect says whether anything is still coming. A handle from a writer that
// never finished — a dead attempt's — names whatever did arrive, and waiting on
// that would be waiting for a machine that is gone.
func awaitStream(ctx context.Context, client *dsclient.Client, name string, expect bool) error {
	deadline := ctx
	if expect {
		var cancel context.CancelFunc
		deadline, cancel = context.WithTimeout(ctx, outputWait)
		defer cancel()
	}
	for {
		ok, err := client.StreamExists(deadline, name)
		if err == nil && ok {
			return nil
		}
		if err != nil && ctx.Err() != nil {
			return fmt.Errorf("wings: check %s: %w", name, err)
		}
		if !expect {
			return fmt.Errorf("wings: %s is not here; it was discarded, or it never arrived", name)
		}
		select {
		case <-deadline.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("wings: %s has not arrived after %s; the worker that wrote "+
				"it may have gone before its output was copied", name, outputWait)
		case <-time.After(outputPoll):
		}
	}
}

// startOutputMirror begins keeping a copy of everything jobs write, anywhere in
// the fleet, for as long as the cluster runs.
//
// ONE set over every worker, not one per worker. wings' machines come and go —
// autoscaling adds and retires them, and a crash retires one without asking —
// so the fleet is handed over as [dsclient.MirrorSet.LiveSources], read again on
// every pass. A machine that appears has its streams picked up; one that goes
// has its copies stopped and its name released. Nothing else in the fleet
// notices either.
func (c *Cluster) startOutputMirror() error {
	client, err := c.sharedClient()
	if err != nil {
		return err
	}
	h, err := client.NewMirrorSet(outputSet, dsclient.MirrorSet{
		LiveSources: func(context.Context) ([]dsclient.MirrorSource, error) {
			return c.mirrorSources(client), nil
		},
		Create:   true,
		Discover: outputDiscover,
		Select: func(cand dsclient.MirrorCandidate) (dsclient.MirrorTarget, error) {
			o, ok := parseOutput(cand.Stream)
			// Everything else on a worker is wings' own plumbing — its jobs, its
			// results, its heartbeats — which the coordinator talks to directly
			// and has no use for a copy of. A prior is declined too: it was put
			// there BY a mirror, and forwarding it back would be two mirrors
			// writing one destination.
			if !ok || o.Prefix == priorPrefix {
				return dsclient.MirrorTarget{}, dsclient.ErrSkipStream
			}
			// A stream this job has finished with. Declining it is what makes
			// dropping it stick: forgetting a copy stops it, and only a decision
			// keeps the next pass from starting it again.
			if c.wasDropped(cand.Stream) {
				return dsclient.MirrorTarget{}, dsclient.ErrSkipStream
			}
			// The same name at the destination, which is what lets one handle
			// mean the same thing on the worker that wrote it and on the
			// coordinator that kept it.
			//
			// The batch is per stream because the two kinds are nothing alike. A
			// recording is one event per record, so a log is as many records as
			// it has events and a small batch is thousands of round trips; a file
			// is a quarter of a megabyte per record, and the same number would be
			// a message nothing should try to carry.
			return dsclient.MirrorTarget{Name: cand.Stream, Batch: batchFor(o.Prefix)}, nil
		},
		OnStreamError: func(cand dsclient.MirrorCandidate, err error) error {
			c.log.Warn("wings: keeping worker output", "worker", cand.Source,
				"stream", cand.Stream, "err", err)
			return nil // one worker having trouble is not the fleet stopping
		},
		OnDiscoveryDegraded: func(source string, err error) {
			// A worker that cannot be followed is discovered by listing on the
			// interval instead — which still works, and is exactly why it is
			// worth saying out loud. It would otherwise present as a healthy
			// cluster that quietly took minutes to keep anything a job wrote,
			// and on a fleet that retires idle machines those minutes are the
			// whole risk.
			//
			// Reported unfiltered: a worker that is merely shutting down no
			// longer arrives here, and it takes a persistent failure rather
			// than one, so anything that reaches this is worth the line.
			c.log.Warn("wings: cannot follow a worker's new streams; falling back to polling",
				"worker", source, "every", outputDiscover, "err", err)
		},
	})
	if err != nil {
		return fmt.Errorf("wings: set up output storage: %w", err)
	}
	c.outputs = h

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		if err := h.Run(c.ctx); err != nil && c.ctx.Err() == nil {
			c.log.Error("wings: stopped keeping output", "err", err)
		}
	}()
	return nil
}

// mirrorSources is the fleet as the mirror should see it right now.
//
// An in-process worker is left out, and has to be: it shares the coordinator's
// engine, so what a job wrote there already IS the coordinator's copy, and
// mirroring it would forward a stream onto itself.
func (c *Cluster) mirrorSources(shared *dsclient.Client) []dsclient.MirrorSource {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []dsclient.MirrorSource
	for _, w := range c.workers {
		if w.client == shared {
			continue
		}
		out = append(out, dsclient.MirrorSource{Name: w.id, Client: w.client})
	}
	return out
}

// pokeOutputs tells the mirror the FLEET has changed.
//
// Not that a stream has: the set follows each worker's catalog and learns that
// by itself, at the moment it happens. What it cannot see is a machine — the
// list of workers is wings' own, read on a discovery pass, and on an autoscaling
// cluster a machine that has just arrived would otherwise write for a whole
// interval before anything was keeping what it wrote. So the pokes are exactly
// the fleet changes: a worker adopted, and workers taken away.
//
// Pokes coalesce, so scaling up by twenty costs one pass rather than twenty.
func (c *Cluster) pokeOutputs() {
	if c.outputs != nil {
		c.outputs.Poke()
	}
}

// hydrate puts the coordinator's copy of a job's earlier recordings onto the
// worker that is about to run it again.
//
// The same mirror, run the other way and in bulk: the coordinator is the source,
// the worker is the destination, and StopWhenCaughtUp makes it one pass that
// copies what it finds and returns rather than a mirror left running.
//
// This is what a moved job needs, and there is no way around it. A retry reads
// what its predecessor wrote through its OWN storage, because a worker on a
// machine somewhere cannot reach the coordinator's — so the log has to be there
// before the job is. Running it twice costs nothing: the position is committed
// at the destination together with the records, so a pass over a stream already
// copied copies nothing, and one over a stream copied halfway resumes.
func (c *Cluster) hydrate(ctx context.Context, w *workerConn, priors []Recording) error {
	if len(priors) == 0 {
		return nil
	}
	client, err := c.sharedClient()
	if err != nil {
		return err
	}
	if w.client == client {
		return nil // one engine; it is already where it needs to be
	}

	// What to copy, keyed by the name it has on the coordinator. A prior's
	// handle names where it will land, and the two differ only by prefix.
	want := make(map[string]string, len(priors))
	for _, rec := range priors {
		o, ok := parseOutput(rec.ID)
		if !ok {
			continue
		}
		want[o.in(recordingPrefix).String()] = rec.ID
	}
	if len(want) == 0 {
		return nil
	}

	return w.client.RunMirrorSet(ctx, outputSet, dsclient.MirrorSet{
		Sources:          []dsclient.MirrorSource{{Name: "coordinator", Client: client}},
		Create:           true,
		StopWhenCaughtUp: true,
		Batch:            recordBatch,
		Select: func(cand dsclient.MirrorCandidate) (dsclient.MirrorTarget, error) {
			dest, ok := want[cand.Stream]
			if !ok {
				return dsclient.MirrorTarget{}, dsclient.ErrSkipStream
			}
			return dsclient.MirrorTarget{Name: dest}, nil
		},
	})
}

// drainOutputs waits for the coordinator's copies of what a worker holds to be
// as complete as the worker's own.
//
// Called at the two moments a worker is about to stop being readable: a job
// being taken off it, and the machine itself being retired. The mirror runs in
// the background, and between a job writing its last record and the copy
// catching up there is a window where those records exist only on a machine that
// is going away.
//
// Caught up is a MOMENT, not a promise. It is an answer about a machine that has
// been told to stop, which is why both callers have already stopped sending it
// work before asking.
//
// Best effort, and bounded. A worker is often being abandoned BECAUSE it stopped
// answering, in which case nothing here can succeed and what survives is
// whatever the coordinator already had. That is still a prefix, which is still
// valid.
func (c *Cluster) drainOutputs(ctx context.Context, from *workerConn, job string) {
	client, err := c.sharedClient()
	if err != nil || from == nil || from.client == client || c.outputs == nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, outputDrain)
	defer cancel()

	// Ordinarily its streams are already being copied — the set followed the
	// creates. The exception is a worker adopted so recently that the fleet
	// change has not been reconciled, and a worker the set has not listed reports
	// NOT drained rather than "no copies, nothing outstanding" — which is the
	// answer that matters here, since the two look identical from outside.
	c.pokeOutputs()

	for {
		done, err := c.outputs.CaughtUp(ctx, from.id)
		if err != nil {
			return // not a source of this set; there is nothing to wait for
		}
		if done {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(outputPoll):
		}
	}
}

// priorsOf finds the event logs a job's earlier attempts left on the
// coordinator, oldest attempt first, each named as it will be found on the
// worker that is about to be given it.
//
// Derived by looking, not remembered. The coordinator's copies are the fact —
// they are what a retry can actually be handed — and a list kept beside them
// would be a second answer that can disagree. A job has few attempts and this is
// asked only when one is being moved.
func (c *Cluster) priorsOf(ctx context.Context, w *workerConn, job string) ([]Recording, error) {
	client, err := c.sharedClient()
	if err != nil {
		return nil, err
	}
	// Where the retry will look. A worker with storage of its own is given a
	// copy under the prior namespace, out of reach of the mirror carrying its
	// own output the other way; in process there is one engine and one copy, and
	// the retry reads the original where it already is.
	lands := priorPrefix
	if w.client == client {
		lands = recordingPrefix
	}
	names, err := client.ListStreams(ctx)
	if err != nil {
		return nil, fmt.Errorf("wings: look for what job %s recorded: %w", job, err)
	}
	var out []Recording
	for _, name := range names {
		o, ok := parseOutput(name)
		if !ok || o.Prefix != recordingPrefix || o.Job != streamPart(job) {
			continue
		}
		out = append(out, Recording{
			Name:    o.Name,
			ID:      o.in(lands).String(),
			Attempt: o.Attempt,
		})
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Attempt < out[j-1].Attempt; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, nil
}

// dropOutputsOf deletes what a job's ABANDONED attempts wrote, on both sides.
//
// The attempt that produced the result is the one whose handles the caller is
// holding, so its output stays until the caller discards it. Every earlier
// attempt's is unreachable — a handle only ever leaves in a result, and an
// attempt that was moved produced none — so it goes, from the worker that wrote
// it as well as from the coordinator that kept it.
//
// The order is the whole of it. A copy still running would put back a
// destination deleted under it the moment its source produced another record,
// and it would put it back holding only those new records — the position lives
// at the destination, and deleting a stream does not roll it back. So: decline
// it, so no pass starts it again; forget the copy, so none is running; then
// delete, source first.
func (c *Cluster) dropOutputsOf(job string, keep int) {
	client, err := c.sharedClient()
	if err != nil {
		return
	}
	ctx := context.WithoutCancel(c.ctx)
	names, err := client.ListStreams(ctx)
	if err != nil {
		c.log.Warn("wings: could not look for abandoned output", "job", job, "err", err)
		return
	}
	var stale []string
	for _, name := range names {
		o, ok := parseOutput(name)
		if !ok || o.Job != streamPart(job) || o.Attempt == keep {
			continue
		}
		stale = append(stale, name)
	}
	if len(stale) == 0 {
		return
	}

	c.markDropped(stale)
	// Only until they are gone: after the source is deleted there is nothing
	// left to offer, so the decision has nothing to decide and holding it would
	// grow a map for the life of the cluster.
	defer c.unmarkDropped(stale)

	fleet := c.fleet()
	for _, name := range stale {
		for _, w := range fleet {
			if w.client == client {
				continue // one engine: the worker's copy IS the coordinator's
			}
			if c.outputs != nil {
				c.outputs.Forget(w.id, name)
			}
			// Whichever worker wrote it. Deleting one that was never here is a
			// cheap no-op, and cheaper than asking every worker which it was.
			if err := dropStream(ctx, w.client, name); err != nil {
				c.log.Warn("wings: could not discard abandoned output on a worker",
					"worker", w.id, "stream", name, "err", err)
			}
		}
		if err := dropStream(ctx, client, name); err != nil {
			c.log.Warn("wings: could not discard abandoned output", "stream", name, "err", err)
		}
	}
}

// fleet is a snapshot of the workers, for use without the lock.
func (c *Cluster) fleet() []*workerConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*workerConn(nil), c.workers...)
}

// markDropped and wasDropped are how a stream being deleted stays deleted.
//
// Forgetting a copy stops it; it does not decline the stream, so the next
// discovery pass would find it and start again. The decision is what closes
// that, and it is only needed for as long as the stream still exists to be
// offered.
func (c *Cluster) markDropped(names []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dropped == nil {
		c.dropped = map[string]bool{}
	}
	for _, n := range names {
		c.dropped[n] = true
	}
}

func (c *Cluster) unmarkDropped(names []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range names {
		delete(c.dropped, n)
	}
}

func (c *Cluster) wasDropped(name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropped[name]
}

// dropStream deletes a stream. Deleting one already gone is not an error.
func dropStream(ctx context.Context, client *dsclient.Client, name string) error {
	ok, err := client.StreamExists(ctx, name)
	if err != nil {
		return fmt.Errorf("wings: check %s: %w", name, err)
	}
	if !ok {
		return nil
	}
	if err := client.DeleteStream(ctx, name); err != nil {
		return fmt.Errorf("wings: discard %s: %w", name, err)
	}
	return nil
}

// streamsFor is somewhere to read and write, from a bound context.
//
// Two places supply it. On the coordinator it is the cluster's own instance,
// through the host a bound context carries. On a worker it is that worker's own
// broker, bound directly — a worker is not a place work dispatches to and has no
// host, but it does have storage, and a job that wants to read what a previous
// attempt of it wrote needs exactly that.
func streamsFor(ctx context.Context) (*dsclient.Client, error) {
	if c := invoke.StreamsFrom(ctx); c != nil {
		return c, nil
	}
	h := invoke.From(ctx)
	if h == nil {
		return nil, errors.New("wings: this context is not bound to a cluster; " +
			"use the context Coordinate was given, or Cluster.Bind")
	}
	client := h.Streams()
	if client == nil {
		return nil, errors.New("wings: this cluster has no durable streams")
	}
	return client, nil
}

// Attempt reports how many times this job has been dispatched before, starting
// at zero.
//
// A work function that resumes rather than restarting wants to know, and until
// now nothing exposed it: a retry looked exactly like a first run. Zero outside
// a work function.
func Attempt(ctx context.Context) int {
	st := beatFrom(ctx)
	if st == nil {
		return 0
	}
	return st.attempt
}

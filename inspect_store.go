package wings

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/ligustah/durable_streams/dsclient"
	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow"
	"github.com/ligustah/wings/flow/protos"
)

// A thread forked onto a worker keeps its history on a wings.history.* job
// stream, not a flow.thread.* one, so the coordinator's own store shows only the
// threads that ran in its process — a run that fans out to workers looks like a
// single main thread with nothing under its forks. inspectionStore fills the
// gap: it reads each job history's opening RunStart to learn the run and thread
// it belongs to (RunStartEvent carries both; see attempt.go) and serves those
// threads' events alongside the coordinator's, so a post-mortem sees the whole
// fork tree. Read-only.

// jobHeadWindow is how many of a job history's first events to scan for the
// RunStart that names its run and thread; that RunStart is the stream's first
// event, so a small window finds it.
const jobHeadWindow = 4

type inspectionStore struct {
	base   flow.Store
	client *dsclient.Client

	mu sync.Mutex
	// builtFor is the job-history stream count the index reflects; a change means
	// a fork or a retry added a stream and the index is rebuilt. -1 is unbuilt.
	builtFor int
	runs     []string
	// place[run][thread] is where a thread's events live: "" for the
	// coordinator's own flow.thread.* stream, or a wings.history.* stream name for
	// a thread that ran as a job.
	place map[string]map[string]string
}

// newInspectionStore returns a read-only store that surfaces both the
// coordinator's own thread histories and the worker-job histories in client's
// engine as one run tree.
func newInspectionStore(client *dsclient.Client) *inspectionStore {
	return &inspectionStore{base: flow.NewStore(client), client: client, builtFor: -1}
}

func (s *inspectionStore) openEvents(name string) (*dsclient.Stream[*protos.Event], error) {
	st, err := s.client.OpenStream[*protos.Event](name, dsclient.WithCodec[*protos.Event](
		dswire.ReflectCodec[*protos.Event]{New: func() *protos.Event { return &protos.Event{} }},
	))
	if err != nil {
		return nil, fmt.Errorf("wings: open history %s: %w", name, err)
	}
	return st, nil
}

// originOf reads a job history's opening RunStart to learn the run and thread it
// records. ok is false for a history with no RunStart in its head.
func (s *inspectionStore) originOf(ctx context.Context, name string) (run, thread string, ok bool, err error) {
	st, err := s.openEvents(name)
	if err != nil {
		return "", "", false, err
	}
	recs, err := st.Read(ctx, 0, jobHeadWindow)
	if err != nil {
		return "", "", false, fmt.Errorf("wings: read history head %s: %w", name, err)
	}
	for _, r := range recs {
		if start := r.Record.GetRunStart(); start != nil {
			return start.GetWorkflowName(), start.GetInstanceId(), true, nil
		}
	}
	return "", "", false, nil
}

// index returns the run→thread→location map, building it the first time and
// whenever the number of job-history streams has changed.
func (s *inspectionStore) index(ctx context.Context) (map[string]map[string]string, []string, error) {
	names, err := s.client.ListStreams(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("wings: list streams: %w", err)
	}
	var histories []string
	for _, n := range names {
		if o, ok := parseOutput(n); ok && o.Prefix == historyPrefix {
			histories = append(histories, n)
		}
	}

	s.mu.Lock()
	if s.builtFor == len(histories) && s.place != nil {
		place, runs := s.place, s.runs
		s.mu.Unlock()
		return place, runs, nil
	}
	s.mu.Unlock()

	place := map[string]map[string]string{}
	runSet := map[string]bool{}
	set := func(run, thread, where string) {
		if place[run] == nil {
			place[run] = map[string]string{}
		}
		place[run][thread] = where
		runSet[run] = true
	}

	// The coordinator's own threads win over a job history of the same name: they
	// are the live record a resume replays from.
	for _, n := range names {
		if run, thread, ok := flow.ParseThreadStream(n); ok {
			set(run, thread, "")
		}
	}

	// A moved or retried thread has a history per attempt under one job; the
	// latest attempt is the one to show.
	type cand struct {
		name    string
		attempt int
	}
	best := map[string]map[string]cand{}
	for _, h := range histories {
		o, _ := parseOutput(h)
		run, thread, ok, err := s.originOf(ctx, h)
		if err != nil {
			return nil, nil, err
		}
		if !ok || run == "" {
			continue
		}
		if best[run] == nil {
			best[run] = map[string]cand{}
		}
		if c, seen := best[run][thread]; !seen || o.Attempt > c.attempt {
			best[run][thread] = cand{name: h, attempt: o.Attempt}
		}
	}
	for run, threads := range best {
		for thread, c := range threads {
			if _, coordinator := place[run][thread]; coordinator {
				continue
			}
			set(run, thread, c.name)
		}
	}

	runs := make([]string, 0, len(runSet))
	for r := range runSet {
		runs = append(runs, r)
	}
	slices.Sort(runs)

	s.mu.Lock()
	s.place, s.runs, s.builtFor = place, runs, len(histories)
	s.mu.Unlock()
	return place, runs, nil
}

// locate reports where a thread's events live, and whether it is known at all.
func (s *inspectionStore) locate(ctx context.Context, run, thread string) (where string, known bool, err error) {
	place, _, err := s.index(ctx)
	if err != nil {
		return "", false, err
	}
	if place[run] == nil {
		return "", false, nil
	}
	where, known = place[run][thread]
	return where, known, nil
}

func (s *inspectionStore) Sink(ctx context.Context, run, thread string) (flow.Sink, error) {
	return s.base.Sink(ctx, run, thread)
}

func (s *inspectionStore) Read(ctx context.Context, run, thread string, offset int64, n int) ([]flow.EventAt, error) {
	where, known, err := s.locate(ctx, run, thread)
	if err != nil {
		return nil, err
	}
	if known && where != "" {
		return s.readJobThread(ctx, where, thread, offset, n)
	}
	return s.base.Read(ctx, run, thread, offset, n)
}

func (s *inspectionStore) Events(ctx context.Context, run, thread string) ([]*protos.Event, error) {
	where, known, err := s.locate(ctx, run, thread)
	if err != nil {
		return nil, err
	}
	if known && where != "" {
		return s.eventsJobThread(ctx, where, thread)
	}
	return s.base.Events(ctx, run, thread)
}

// Tail returns the last n events of a thread. A job history interleaves its
// threads, so a thread's tail is the tail of its events filtered out of the
// combined stream; a coordinator thread tails cheaply through the base store.
func (s *inspectionStore) Tail(ctx context.Context, run, thread string, n int) ([]flow.EventAt, error) {
	where, known, err := s.locate(ctx, run, thread)
	if err != nil {
		return nil, err
	}
	if known && where != "" {
		all, err := s.eventsAtJobThread(ctx, where, thread)
		if err != nil {
			return nil, err
		}
		if n > 0 && len(all) > n {
			all = all[len(all)-n:]
		}
		return all, nil
	}
	if t, ok := s.base.(flow.Tailer); ok {
		return t.Tail(ctx, run, thread, n)
	}
	all, err := s.base.Read(ctx, run, thread, 0, 1<<30)
	if err != nil {
		return nil, err
	}
	if n > 0 && len(all) > n {
		all = all[len(all)-n:]
	}
	return all, nil
}

func (s *inspectionStore) Drop(ctx context.Context, run, thread string) error {
	where, known, err := s.locate(ctx, run, thread)
	if err != nil {
		return err
	}
	if known && where != "" {
		return nil // a job history is not the inspector's to drop
	}
	return s.base.Drop(ctx, run, thread)
}

// ListRuns implements [flow.Lister].
func (s *inspectionStore) ListRuns(ctx context.Context) ([]string, error) {
	_, runs, err := s.index(ctx)
	if err != nil {
		return nil, err
	}
	return slices.Clone(runs), nil
}

// ListThreads implements [flow.Lister].
func (s *inspectionStore) ListThreads(ctx context.Context, run string) ([]string, error) {
	place, _, err := s.index(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(place[run]))
	for thread := range place[run] {
		out = append(out, thread)
	}
	slices.Sort(out)
	return out, nil
}

// readJobThread returns up to n of one thread's events at or after offset in a
// job's combined history stream, each with its offset there. Offsets are sparse
// per thread; paging by the last offset plus one carries on scanning. Mirrors
// historyStore.Read.
func (s *inspectionStore) readJobThread(ctx context.Context, name, thread string, offset int64, n int) ([]flow.EventAt, error) {
	ok, err := s.client.StreamExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("wings: look for history %s: %w", name, err)
	}
	if !ok {
		return nil, nil
	}
	st, err := s.openEvents(name)
	if err != nil {
		return nil, err
	}
	from := offset
	if from < 0 {
		from = 0
	}
	var out []flow.EventAt
	for len(out) < n {
		recs, err := st.Read(ctx, from, recordBatch)
		if err != nil {
			return nil, fmt.Errorf("wings: read history %s: %w", name, err)
		}
		if len(recs) == 0 {
			return out, nil
		}
		for _, r := range recs {
			from = r.Offset + 1
			if r.Record.GetThreadId() == thread {
				out = append(out, flow.EventAt{Event: r.Record, Offset: r.Offset})
				if len(out) >= n {
					break
				}
			}
		}
	}
	return out, nil
}

func (s *inspectionStore) eventsAtJobThread(ctx context.Context, name, thread string) ([]flow.EventAt, error) {
	ok, err := s.client.StreamExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("wings: look for history %s: %w", name, err)
	}
	if !ok {
		return nil, nil
	}
	st, err := s.openEvents(name)
	if err != nil {
		return nil, err
	}
	var out []flow.EventAt
	var from int64
	for {
		recs, err := st.Read(ctx, from, recordBatch)
		if err != nil {
			return nil, fmt.Errorf("wings: read history %s: %w", name, err)
		}
		if len(recs) == 0 {
			return out, nil
		}
		for _, r := range recs {
			from = r.Offset + 1
			if r.Record.GetThreadId() == thread {
				out = append(out, flow.EventAt{Event: r.Record, Offset: r.Offset})
			}
		}
	}
}

func (s *inspectionStore) eventsJobThread(ctx context.Context, name, thread string) ([]*protos.Event, error) {
	at, err := s.eventsAtJobThread(ctx, name, thread)
	if err != nil {
		return nil, err
	}
	out := make([]*protos.Event, len(at))
	for i, e := range at {
		out[i] = e.Event
	}
	return out, nil
}

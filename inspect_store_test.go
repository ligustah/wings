package wings

import (
	"context"
	"slices"
	"testing"

	"github.com/ligustah/wings/flow"
)

// THE POINT: a thread forked onto a worker keeps its history on a job stream,
// not a coordinator flow.thread stream, so the plain store shows only main. The
// inspection store reads the job histories back into the run's fork tree, so a
// finished run's forked threads — what they ran and the events they recorded —
// are all inspectable.
func TestInspectionStoreSurfacesForkedThreads(t *testing.T) {
	c := start(t, Config{Target: InProcess(), Workers: 2, Concurrency: 2})

	run := flow.NewName()
	err := c.Run(t.Context(), run, func(ctx flow.Context) error {
		ch := ctx.NewChannel[int]()
		producer := ctx.Go(counts, feed{Values: ch, Count: 4})
		consumer := ctx.Go(sums, feed{Values: ch})
		if _, err := producer.Await(ctx); err != nil {
			return err
		}
		_, err := consumer.Await(ctx)
		return err
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	client, err := c.sharedClient()
	if err != nil {
		t.Fatalf("shared client: %v", err)
	}
	store := newInspectionStore(client)
	ctx := context.Background()

	threads, err := store.ListThreads(ctx, run)
	if err != nil {
		t.Fatalf("ListThreads: %v", err)
	}
	// main runs in the coordinator; the two forks ran as jobs on workers.
	for _, want := range []string{"main", "main.0", "main.1"} {
		if !slices.Contains(threads, want) {
			t.Fatalf("thread %q not surfaced; got %v", want, threads)
		}
	}

	headers, err := flow.InspectRunHeaders(ctx, store, run)
	if err != nil {
		t.Fatalf("InspectRunHeaders: %v", err)
	}
	fn := map[string]string{}
	for _, h := range headers {
		fn[h.ID] = h.Fn
		if h.Parent == "" && h.ID != "main" {
			t.Fatalf("forked thread %q has no parent", h.ID)
		}
	}
	// The forked threads carry the function they ran, read from the job history's
	// RunStart — which is exactly what makes the fork tree meaningful.
	forks := []string{fn["main.0"], fn["main.1"]}
	if !slices.Contains(forks, "test.counts") || !slices.Contains(forks, "test.sums") {
		t.Fatalf("forked threads ran %v, want test.counts and test.sums", forks)
	}

	// The producer's events are readable from its job history, not just its
	// header: four sends and a close.
	producer := "main.0"
	if fn["main.0"] != "test.counts" {
		producer = "main.1"
	}
	page, err := flow.ReadEvents(ctx, store, run, producer, 0, 100)
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	sends := 0
	for _, e := range page.Events {
		if e.Kind == "send" {
			sends++
		}
	}
	if sends == 0 {
		t.Fatalf("producer thread %q surfaced no send events; got %d events", producer, len(page.Events))
	}
}

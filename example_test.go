package wings_test

import (
	"context"
	"fmt"
	"log"

	"github.com/ligustah/wings"
	"github.com/ligustah/wings/flow"
)

// Work functions and the root are declared at package scope with flow.Define, so
// a worker process — which never runs the root — still has them registered.
var exRender = flow.Define(func(ctx flow.Context, frame int) (int, error) {
	return frame * frame, nil
})

var exFrames = flow.Define(func(ctx flow.Context, frames []int) ([]int, error) {
	return ctx.Map(exRender, frames)
})

var _ = flow.Main(exFrames)

// Bind dispatches calls to the cluster's workers, outside any durable run.
func ExampleCluster_Bind() {
	c, err := wings.Start(context.Background(), wings.Config{
		Target:  wings.InProcess(),
		Workers: 2,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Stop(context.Background())

	ctx := c.Bind(context.Background())
	out, err := exRender(ctx, 9)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(out)
}

// RunWorkflow runs a root durably: a coordinator restarted over the same Dir
// resumes it rather than starting over.
func ExampleCluster_RunWorkflow() {
	c, err := wings.Start(context.Background(), wings.Config{
		Target: wings.LocalProcess(),
		Dir:    "/var/lib/myapp",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Stop(context.Background())

	if err := c.RunWorkflow(context.Background(), exFrames, []int{1, 2, 3}); err != nil {
		log.Fatal(err)
	}
}

// Record keeps a per-job event log that a moved attempt can replay to catch up;
// only a small handle travels in the result.
func ExampleRecord() {
	var Simulate = flow.Define(func(ctx flow.Context, steps int) (wings.Recording, error) {
		rec, err := wings.Record[int](ctx, "ticks")
		if err != nil {
			return wings.Recording{}, err
		}
		for i := range steps {
			if err := rec.Record(i); err != nil {
				return wings.Recording{}, err
			}
		}
		return rec.Recording(), rec.Close()
	})
	_ = Simulate
}

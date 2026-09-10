package flow_test

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"

	"github.com/ligustah/wings/flow"
)

// exDouble is a work function: a plain typed func declared with flow.Define.
var exDouble = flow.Define(func(ctx flow.Context, n int) (int, error) {
	return n * 2, nil
})

func ExampleDefine() {
	var sum int
	err := flow.Run(context.Background(), flow.NewName(), func(ctx flow.Context) error {
		// Map runs exDouble over every input, concurrently, and returns the
		// results in input order.
		out, err := ctx.Map(exDouble, []int{1, 2, 3})
		if err != nil {
			return err
		}
		sum = out[0] + out[1] + out[2]
		return nil
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sum)
	// Output: 12
}

func ExampleContext_Go() {
	var sum int
	err := flow.Run(context.Background(), flow.NewName(), func(ctx flow.Context) error {
		// Go forks a thread and returns immediately; both run concurrently.
		a := ctx.Go(exDouble, 10)
		b := ctx.Go(exDouble, 20)
		ra, err := a.Await(ctx)
		if err != nil {
			return err
		}
		rb, err := b.Await(ctx)
		if err != nil {
			return err
		}
		sum = ra + rb
		return nil
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(sum)
	// Output: 60
}

func ExampleChannel() {
	var got []int
	err := flow.Run(context.Background(), flow.NewName(), func(ctx flow.Context) error {
		r, w := ctx.NewChannel[int](flow.WithCapacity(3))
		producer := ctx.Spawn(func(ctx flow.Context) (flow.None, error) {
			for _, n := range []int{1, 2, 3} {
				if err := w.Send(ctx, n); err != nil {
					return flow.None{}, err
				}
			}
			return flow.None{}, w.Close(ctx)
		})
		for {
			v, ok, err := r.Recv(ctx)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			got = append(got, v)
		}
		_, err := producer.Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		log.Fatal(err)
	}
	sort.Ints(got)
	fmt.Println(got)
	// Output: [1 2 3]
}

func ExampleContext_Select() {
	var got int
	err := flow.Run(context.Background(), flow.NewName(), func(ctx flow.Context) error {
		a := ctx.Go(exDouble, 5)
		b := ctx.Go(exDouble, 6)
		// Take whichever finishes first; the loser keeps running.
		_, v, err := flow.AwaitAny(ctx, a, b)
		got = v
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		log.Fatal(err)
	}
	// One of the two doublings won the race.
	fmt.Println(got == 10 || got == 12)
	// Output: true
}

func ExampleContext_Signal() {
	host := flow.NewMemChannelHost()
	name := flow.NewName()
	// Delivered before the run asks; the host holds it until it does.
	if err := flow.Deliver(context.Background(), host, name, "approval", "shipped"); err != nil {
		log.Fatal(err)
	}
	err := flow.Run(context.Background(), name, func(ctx flow.Context) error {
		v, err := ctx.Signal[string]("approval")
		if err != nil {
			return err
		}
		fmt.Println(v)
		return nil
	}, flow.WithStore(flow.NewMemStore()), flow.WithChannelHost(host))
	if err != nil {
		log.Fatal(err)
	}
	// Output: shipped
}

func ExampleByteWriter() {
	var n int
	err := flow.Run(context.Background(), flow.NewName(), func(ctx flow.Context) error {
		cr, cw := ctx.NewChannel[flow.Bytes](flow.WithCapacity(4))
		writer := ctx.Spawn(func(ctx flow.Context) (flow.None, error) {
			w := flow.NewByteWriter(ctx, cw)
			if _, err := io.WriteString(w, "hello, world"); err != nil {
				return flow.None{}, err
			}
			return flow.None{}, w.Close()
		})
		read, err := io.ReadAll(flow.NewByteReader(ctx, cr))
		if err != nil {
			return err
		}
		n = len(read)
		_, err = writer.Await(ctx)
		return err
	}, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(n)
	// Output: 12
}

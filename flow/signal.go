package flow

import (
	"context"
	"errors"
	"fmt"
	"uuid"

	"github.com/ligustah/durable_streams/dswire"

	"github.com/ligustah/wings/flow/protos"
)

// A signal is an external event delivered into a running workflow by name. It
// rides the shared-channel machinery: a signal travels on a channel whose id is
// derived from the run and the signal's name, so a deliverer outside the run and
// the running workflow meet on it without passing a handle. The receive is
// recorded like any other, so a replay observes the signal once.

// signalSender names the deliverer to a ChannelHost; it owns no run.
const signalSender = "flow.signal"

// SignalSender is the run name [Deliver] links a [ChannelHost] under. It owns no
// run, so a host must not treat its send as part of a run's transaction.
const SignalSender = signalSender

// signalID names the shared channel a signal travels on.
func signalID(run, name string) string { return run + "/signal." + name }

// Signal waits for the next signal named name delivered to this run, decodes it
// as T, and records it so a replay observes the same one. Successive calls take
// successive signals of that name in the order the host received them. The run
// needs a [ChannelHost]; deliver with [Deliver].
func (c Context) Signal[T any](name string) (T, error) {
	var zero T
	t := threadFrom(c)
	if t == nil {
		return zero, errors.New("flow: Signal called outside a Run")
	}
	if name == "" {
		return zero, errors.New("flow: Signal requires a name")
	}
	ch := &Channel[T]{
		id:    signalID(t.run.name, name),
		codec: dswire.ReflectCodec[T]{New: allocator[T]()},
	}
	v, ok, err := ch.Recv(c)
	if err != nil {
		return zero, err
	}
	if !ok {
		return zero, fmt.Errorf("flow: signal %q is closed", name)
	}
	return v, nil
}

// Deliver sends a signal to a running workflow: the run named run receives it
// through [Context.Signal] under name. Sent from outside any run, through the
// same [Store] the run uses. A signal delivered before the run asks for it is
// held on the channel's value stream until it does.
func Deliver[T any](ctx context.Context, store Store, run, name string, v T) error {
	if store == nil {
		return errors.New("flow: Deliver requires a Store")
	}
	if run == "" || name == "" {
		return errors.New("flow: Deliver requires a run and a name")
	}
	codec := dswire.ReflectCodec[T]{New: allocator[T]()}
	data, err := dswire.EncodeRecord(codec, v)
	if err != nil {
		return fmt.Errorf("flow: encode signal %q: %w", name, err)
	}
	// A signal owns no thread; open a transaction for a synthetic sender and write
	// the value straight onto the channel's value stream, committed at once. A fresh
	// id per delivery, so two signals are two items on the stream.
	tx, err := store.Begin(ctx, signalSender, name)
	if err != nil {
		return fmt.Errorf("flow: deliver signal %q to run %s: %w", name, run, err)
	}
	from := "signal/" + uuid.New().String()
	ev := &protos.Event{Payload: protos.PackEventPayload(&protos.ChannelItem{From: from, Data: data})}
	if err := tx.AppendTo(ctx, ChannelValueStream(signalID(run, name)), ev); err != nil {
		return fmt.Errorf("flow: deliver signal %q to run %s: %w", name, run, err)
	}
	return tx.Flush(ctx)
}

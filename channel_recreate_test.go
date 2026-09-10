package wings

import (
	"testing"
	"time"

	"github.com/ligustah/wings/flow"
)

// THE POINT: the relay copies a shared channel's outboxes into the one
// canonical stream. An outbox on the coordinator is made by the output mirror,
// which CREATES the stream — and a moved attempt's outbox is discovered and
// tailed the instant it appears, empty, before the mirror has fed it. A read
// handle opened on that empty stream is orphaned when the mirror's copy
// re-creates it, and a stale handle never sees what is written after: the
// values sit unread and whoever waits on them hangs forever. The relay reads
// each outbox as it is now, not as it was when first seen, so a re-created
// outbox's values still reach the channel.
func TestAnOutboxRecreatedUnderTheRelayStillReachesTheChannel(t *testing.T) {
	c := start(t, Config{Target: InProcess()})
	client, err := c.sharedClient()
	if err != nil {
		t.Fatalf("sharedClient: %v", err)
	}
	ctx := c.ctx

	id := "recreate/ch0"
	sender := outboxFor("sndjob", 0, id)
	receiver := outboxFor("rcvjob", 0, id)
	for _, n := range []string{sender, receiver} {
		if err := ensureStream(ctx, client, n); err != nil {
			t.Fatalf("ensureStream %s: %v", n, err)
		}
	}

	// Let the relay discover and tail both outboxes while they are EMPTY, so
	// each tail has opened its read handle on the empty stream — the window a
	// moved attempt's outbox is tailed in.
	c.pokeRelay()
	time.Sleep(600 * time.Millisecond)

	// The mirror re-creates the sender's outbox as it copies a moved attempt's
	// home, then feeds it: the exact shape that orphans the tail's handle.
	if err := dropStream(ctx, client, sender); err != nil {
		t.Fatalf("drop sender: %v", err)
	}
	if err := ensureStream(ctx, client, sender); err != nil {
		t.Fatalf("recreate sender: %v", err)
	}
	so, err := eventStream[flow.ChannelItem](client, sender)
	if err != nil {
		t.Fatalf("open sender: %v", err)
	}
	if _, err := so.Append(ctx, []flow.ChannelItem{{From: "recreate/main.0", Seq: 0, Data: []byte("v")}}); err != nil {
		t.Fatalf("send value: %v", err)
	}

	// The value must reach the canonical stream, though the sender's outbox was
	// re-created under the tail. Without the relay re-opening its handle, the
	// value stays stranded and never arrives.
	canonical := chanStreamFor(id)
	deadline := time.Now().Add(10 * time.Second)
	for {
		if st, err := eventStream[flow.ChannelItem](client, canonical); err == nil {
			recs, _ := st.Read(ctx, 0, 200)
			for _, r := range recs {
				if it := r.Record; it.From == "recreate/main.0" && it.Seq == 0 && !it.Consumed && !it.Closed {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the re-created outbox's value never reached the canonical stream; the relay's read handle went stale")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

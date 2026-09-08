package flow

import "testing"

// THE POINT: the arbiter keeps only the identity of a value it is holding for a
// later want, not its bytes. The value's bytes travel on in the record the host
// appends (Offer's return); keeping them too held a wave's worth of relayed
// values on the coordinator (the memory write-up's finding 4).
func TestArbiterDoesNotRetainValueBytes(t *testing.T) {
	a := NewArbiter()
	payload := []byte("a sizeable decision payload")

	recs := a.Offer(ChannelItem{From: "run/main", Seq: 0, Data: payload})
	if len(recs) != 1 || string(recs[0].Data) != string(payload) {
		t.Fatalf("Offer must return the value with its bytes for the record; got %+v", recs)
	}
	if len(a.values) != 1 {
		t.Fatalf("want the value backlogged (no want yet), got %d", len(a.values))
	}
	if a.values[0].Data != nil {
		t.Fatalf("arbiter retained %d value bytes; it should keep only identity", len(a.values[0].Data))
	}
	if a.values[0].From != "run/main" || a.values[0].Seq != 0 {
		t.Fatalf("arbiter lost the value identity: %+v", a.values[0])
	}

	// The identity is enough to match a want and grant it.
	grants := a.Offer(ChannelItem{Want: true, From: "run/other", Seq: 0})
	var granted *ChannelItem
	for i := range grants {
		if grants[i].To != "" {
			granted = &grants[i]
		}
	}
	if granted == nil {
		t.Fatalf("a want after a backlogged value should be granted; got %+v", grants)
	}
	if granted.From != "run/main" || granted.Seq != 0 || granted.To != "run/other" || granted.ToSeq != 0 {
		t.Fatalf("grant named the wrong pair: %+v", *granted)
	}
}

// THE POINT: the dedupe is a per-sender run, not a key per value, and it admits a
// seq that arrives out of order — a lost announcement whose gap the replay fills
// — rather than mistaking the replay for a resend.
func TestArbiterAdmitsAGapFilledOutOfOrder(t *testing.T) {
	a := NewArbiter()

	// seq 1's announcement was lost, so seq 2 reaches the record before it.
	if recs := a.Offer(ChannelItem{From: "s", Seq: 0, Data: []byte("a")}); len(recs) != 1 {
		t.Fatalf("seq 0 should be admitted: %v", recs)
	}
	if recs := a.Offer(ChannelItem{From: "s", Seq: 2, Data: []byte("c")}); len(recs) != 1 {
		t.Fatalf("seq 2 should be admitted out of order: %v", recs)
	}
	// The replay re-sends the gap; it must be admitted, not taken for a resend.
	if recs := a.Offer(ChannelItem{From: "s", Seq: 1, Data: []byte("b")}); len(recs) != 1 {
		t.Fatalf("the missing seq 1 should be admitted on replay: %v", recs)
	}
	// Everything is a resend now.
	for _, seq := range []uint64{0, 1, 2} {
		if recs := a.Offer(ChannelItem{From: "s", Seq: seq}); len(recs) != 0 {
			t.Fatalf("seq %d is a resend and must be dropped: %v", seq, recs)
		}
	}
	// One sender, one entry — not one per value — and the gap closed.
	if len(a.seen) != 1 {
		t.Fatalf("arbiter kept %d sender entries, want 1", len(a.seen))
	}
	if r := a.seen["s"]; r == nil || r.next != 3 || len(r.ahead) != 0 {
		t.Fatalf("sender run did not close the gap: %+v", r)
	}
}

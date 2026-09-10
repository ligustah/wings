package flow

import "testing"

// THE POINT: the ledger passes a value's bytes through in the record it returns
// for the host to append, but keeps none of them itself — only counts and the
// identity needed to dedupe a resend. It tracks values arrived, consumes reported
// and the close, which is all a holder needs to tell whether a parked receive or
// send can proceed.
func TestLedgerCountsWithoutRetainingBytes(t *testing.T) {
	a := NewArbiter()
	payload := []byte("a sizeable decision payload")

	recs := a.Offer(ChannelItem{From: "run/main", Seq: 0, Data: payload})
	if len(recs) != 1 || string(recs[0].Data) != string(payload) {
		t.Fatalf("Offer must return the value with its bytes for the record; got %+v", recs)
	}
	if a.Values() != 1 || a.Consumed() != 0 || a.Closed() {
		t.Fatalf("after one value: values=%d consumed=%d closed=%v, want 1/0/false", a.Values(), a.Consumed(), a.Closed())
	}

	if recs := a.Offer(ChannelItem{Consumed: true, From: "run/main", Seq: 0}); len(recs) != 1 {
		t.Fatalf("a consume report should be admitted once: %v", recs)
	}
	if recs := a.Offer(ChannelItem{Consumed: true, From: "run/main", Seq: 0}); len(recs) != 0 {
		t.Fatalf("a repeated consume report must be dropped: %v", recs)
	}
	if a.Consumed() != 1 {
		t.Fatalf("consumed=%d, want 1", a.Consumed())
	}

	if recs := a.Offer(ChannelItem{Closed: true}); len(recs) != 1 {
		t.Fatalf("a close should be admitted once: %v", recs)
	}
	if recs := a.Offer(ChannelItem{Closed: true}); len(recs) != 0 {
		t.Fatalf("a repeated close must be dropped: %v", recs)
	}
	if !a.Closed() {
		t.Fatalf("the channel should read closed")
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

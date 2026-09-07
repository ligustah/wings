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

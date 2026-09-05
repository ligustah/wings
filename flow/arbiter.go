package flow

import "sync"

// A shared channel gives each value to ONE receiver, as a channel between
// threads does, and the receivers are on machines that cannot see each other.
// So a receive across runs is a request — a WANT, the receiver's nth on the
// channel — and the host answers it with a GRANT naming the value that
// receiver gets. The host decides; nobody else can. The rule it decides by is
// this file, so that every host decides the same way: values in the order
// they reached the host, wants in the order they reached the host, each
// value to the earliest want still open.
//
// A want is idempotent. A receiver that asks again — its attempt ended while
// it waited, and the replay has reached the same receive — is asking for the
// grant it may already have, and gets nothing new; the grant is on the
// channel's record where the replay reads it. That is what lets a receive be
// abandoned and resumed anywhere without a value being given twice.

// Arbiter applies the rule a host follows to hand out a shared channel's
// values. A host keeps one per channel and appends to the channel's record
// exactly what Offer returns.
//
// Not safe for concurrent use from more than one goroutine without a lock of
// the host's own; a host's record is written by one writer, and that writer
// holds the arbiter.
type Arbiter struct {
	mu     sync.Mutex
	seen   map[string]bool // values on the record, by sender#seq
	asked  map[string]bool // wants on the record, by receiver#seq
	values []ChannelItem   // on the record and not yet granted, in order
	wants  []ChannelItem   // on the record and not yet granted, in order
	closed bool
}

// NewArbiter returns the arbiter of an empty channel. For a channel with a
// record already, Restore each of its records first.
func NewArbiter() *Arbiter {
	return &Arbiter{seen: map[string]bool{}, asked: map[string]bool{}}
}

// Offer takes a record a run sent — a value, a want, a close — and returns
// what the host must append to the channel's record, in order: the record
// itself if it is new, then every grant it makes possible. Nothing for a
// copy of something already on the record, and never a grant a run sent, as
// grants are the host's to make.
func (a *Arbiter) Offer(it ChannelItem) []ChannelItem {
	a.mu.Lock()
	defer a.mu.Unlock()
	if it.To != "" {
		return nil
	}
	if !a.admit(it) {
		return nil
	}
	return append([]ChannelItem{it}, a.match()...)
}

// Restore replays one record already on the channel's record, in order, for
// a host that starts with a record its predecessor wrote. Grants restored
// this way settle the wants and values they name. Call Grants afterwards.
func (a *Arbiter) Restore(it ChannelItem) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if it.To == "" {
		a.admit(it)
		return
	}
	a.values = withoutItem(a.values, it.From, it.Seq)
	a.wants = withoutItem(a.wants, it.To, it.ToSeq)
}

// Grants returns the grants owed now: after a Restore, whatever a predecessor
// admitted and did not live to grant.
func (a *Arbiter) Grants() []ChannelItem {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.match()
}

// Settled reports whether a wait on the channel is over as far as the record
// goes: a want (Want set, From and Seq the receiver's) has been granted, or
// the channel is closed; a value (From and Seq the sender's) has been
// granted to someone. For whoever holds a thread that stopped waiting
// before it could see.
func (a *Arbiter) Settled(it ChannelItem) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if it.Want {
		if a.closed {
			return true
		}
		return a.asked[itemKey(it.From, it.Seq)] && !has(a.wants, it.From, it.Seq)
	}
	return a.seen[itemKey(it.From, it.Seq)] && !has(a.values, it.From, it.Seq)
}

func has(items []ChannelItem, from string, seq uint64) bool {
	for _, it := range items {
		if it.From == from && it.Seq == seq {
			return true
		}
	}
	return false
}

// admit takes a value, want or close into the arbiter's state and reports
// whether it was new. Call with mu held.
func (a *Arbiter) admit(it ChannelItem) bool {
	switch {
	case it.Closed:
		if a.closed {
			return false
		}
		a.closed = true
	case it.Want:
		key := itemKey(it.From, it.Seq)
		if a.asked[key] {
			return false
		}
		a.asked[key] = true
		a.wants = append(a.wants, it)
	default:
		key := itemKey(it.From, it.Seq)
		if a.seen[key] {
			return false
		}
		a.seen[key] = true
		a.values = append(a.values, it)
	}
	return true
}

// match pairs values with wants, earliest with earliest, and returns the
// grants. Call with mu held.
func (a *Arbiter) match() []ChannelItem {
	var grants []ChannelItem
	for len(a.values) > 0 && len(a.wants) > 0 {
		v, w := a.values[0], a.wants[0]
		a.values, a.wants = a.values[1:], a.wants[1:]
		grants = append(grants, ChannelItem{From: v.From, Seq: v.Seq, To: w.From, ToSeq: w.Seq})
	}
	return grants
}

func withoutItem(items []ChannelItem, from string, seq uint64) []ChannelItem {
	for i, it := range items {
		if it.From == from && it.Seq == seq {
			return append(items[:i:i], items[i+1:]...)
		}
	}
	return items
}

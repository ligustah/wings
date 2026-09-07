package flow

import "sync"

// A shared channel gives each value to one receiver. A cross-run receive is a
// want; the host answers with a grant naming the value that receiver gets. The
// rule, applied by every host alike: each value to the earliest still-open want,
// both in the order they reached the host. A want is idempotent, so a receive
// can be abandoned and resumed without granting a value twice.

// Arbiter applies that rule. A host keeps one per channel and appends to the
// channel's record exactly what Offer returns. Not safe for concurrent use
// without the host's own lock.
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

// admit takes a value, want or close into state and reports whether it was new.
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

// match pairs values with wants, earliest with earliest, and returns the grants.
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

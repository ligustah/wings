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
	mu sync.Mutex
	// seen and asked track, per sender and per receiver, which seqs are on the
	// record, so a resend is dropped. A party's seqs mostly arrive in order, but a
	// lost announcement (a worker that died between a send's record and its copy
	// home) lets a later one arrive first, with the gap filled by the replay — so
	// a plain high-water is not enough; see seqRun. One small entry per party
	// rather than one per record ever.
	seen   map[string]*seqRun
	asked  map[string]*seqRun
	values []ChannelItem // on the record and not yet granted, in order
	wants  []ChannelItem // on the record and not yet granted, in order
	closed bool
}

// NewArbiter returns the arbiter of an empty channel. For a channel with a
// record already, Restore each of its records first.
func NewArbiter() *Arbiter {
	return &Arbiter{seen: map[string]*seqRun{}, asked: map[string]*seqRun{}}
}

// seqRun is the set of seqs admitted from one party: a contiguous run [0, next)
// plus any admitted above it out of order (ahead), which the run absorbs as the
// gaps fill. Compact because in-order arrivals keep ahead empty and a lost
// announcement opens only a brief gap.
type seqRun struct {
	next  uint64
	ahead map[uint64]bool
}

func (r *seqRun) has(seq uint64) bool { return seq < r.next || r.ahead[seq] }

func (r *seqRun) add(seq uint64) {
	if r.has(seq) {
		return
	}
	if seq > r.next {
		if r.ahead == nil {
			r.ahead = map[uint64]bool{}
		}
		r.ahead[seq] = true
		return
	}
	for r.next = seq + 1; r.ahead[r.next]; r.next++ {
		delete(r.ahead, r.next)
	}
}

func admittedIn(marks map[string]*seqRun, from string, seq uint64) bool {
	r := marks[from]
	return r != nil && r.has(seq)
}

func markSeq(marks map[string]*seqRun, from string, seq uint64) {
	r := marks[from]
	if r == nil {
		r = &seqRun{}
		marks[from] = r
	}
	r.add(seq)
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
		return admittedIn(a.asked, it.From, it.Seq) && !has(a.wants, it.From, it.Seq)
	}
	return admittedIn(a.seen, it.From, it.Seq) && !has(a.values, it.From, it.Seq)
}

func has(items []ChannelItem, from string, seq uint64) bool {
	for _, it := range items {
		if it.From == from && it.Seq == seq {
			return true
		}
	}
	return false
}

// admit takes a value, want, close or retraction into state and reports whether
// it was new.
func (a *Arbiter) admit(it ChannelItem) bool {
	switch {
	case it.Closed:
		if a.closed {
			return false
		}
		a.closed = true
	case it.Unwant:
		// Too late once granted: the want has left a.wants, the grant stands, and
		// the value it named is the next receive's. Only a still-pending want is
		// retracted, and only that is worth recording.
		if !has(a.wants, it.From, it.Seq) {
			return false
		}
		a.wants = withoutItem(a.wants, it.From, it.Seq)
	case it.Want:
		if admittedIn(a.asked, it.From, it.Seq) {
			return false
		}
		markSeq(a.asked, it.From, it.Seq)
		a.wants = append(a.wants, ChannelItem{From: it.From, Seq: it.Seq, Want: true})
	default:
		if admittedIn(a.seen, it.From, it.Seq) {
			return false
		}
		markSeq(a.seen, it.From, it.Seq)
		// Only the identity is kept: match, has, withoutItem and Settled read no
		// more, and the value's bytes travel on in Offer's returned record. Storing
		// the whole item held a wave's worth of relayed values on the coordinator.
		a.values = append(a.values, ChannelItem{From: it.From, Seq: it.Seq})
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

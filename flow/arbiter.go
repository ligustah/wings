package flow

import "sync"

// A shared channel has one reader. The reader consumes the host's ordered record
// in arrival order, so the host keeps no matching state — only enough to dedupe a
// record a replaying party resends, and to tell a holder whether a parked receive
// or send can now proceed: how many values have arrived, how many the reader has
// reported consuming, and whether the channel is closed.

// Arbiter is a host's per-channel bookkeeping. A host keeps one per channel,
// offering each record a run sends and appending exactly what Offer returns. Not
// safe for concurrent use without the host's own lock.
type Arbiter struct {
	mu sync.Mutex
	// seen and consumed track, per party, which seqs are on the record, so a
	// resend is dropped. A party's seqs mostly arrive in order, but a lost
	// announcement (a worker that died between a send's record and its copy home)
	// lets a later one arrive first, with the gap filled by the replay — so a plain
	// high-water is not enough; see seqRun. One small entry per party.
	seen     map[string]*seqRun
	consumed map[string]*seqRun
	nvalues  uint64 // distinct values admitted
	nconsume uint64 // distinct consume reports admitted
	closed   bool
}

// NewArbiter returns the bookkeeping of an empty channel. For a channel with a
// record already, Restore each of its records first.
func NewArbiter() *Arbiter {
	return &Arbiter{seen: map[string]*seqRun{}, consumed: map[string]*seqRun{}}
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

// Offer takes a record a run sent — a value, a consume report, or a close — and
// returns what the host must append to the channel's record: the record itself if
// it is new, nothing for a copy of something already on the record.
func (a *Arbiter) Offer(it ChannelItem) []ChannelItem {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.admit(it) {
		return nil
	}
	return []ChannelItem{it}
}

// Restore folds one record already on the channel's record into state, for a host
// that starts with a record its predecessor wrote. Call Grants afterwards.
func (a *Arbiter) Restore(it ChannelItem) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.admit(it)
}

// Grants returns the records owed now after a Restore. A single-reader channel
// owes nothing — the host makes no matches — so this is always empty; kept so a
// host restoring a record need not special-case it.
func (a *Arbiter) Grants() []ChannelItem { return nil }

// Values reports how many distinct values have arrived, so a holder can tell
// whether a parked receive at a given sequence has a value to take.
func (a *Arbiter) Values() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.nvalues
}

// Consumed reports how many distinct values the reader has reported consuming, so
// a holder can tell whether a parked send has had room freed since it parked.
func (a *Arbiter) Consumed() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.nconsume
}

// Closed reports whether the channel has been closed.
func (a *Arbiter) Closed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}

// admit folds a record into state and reports whether it was new.
func (a *Arbiter) admit(it ChannelItem) bool {
	switch {
	case it.Closed:
		if a.closed {
			return false
		}
		a.closed = true
	case it.Consumed:
		if admittedIn(a.consumed, it.From, it.Seq) {
			return false
		}
		markSeq(a.consumed, it.From, it.Seq)
		a.nconsume++
	default:
		if admittedIn(a.seen, it.From, it.Seq) {
			return false
		}
		markSeq(a.seen, it.From, it.Seq)
		a.nvalues++
	}
	return true
}

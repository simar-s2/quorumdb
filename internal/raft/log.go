package raft

// memLog is the in-memory copy of the log. ents[0] is a sentinel that holds
// the index and term of the last entry covered by the snapshot (0/0 when
// there is none), so the entry at index i lives at ents[i-snapIndex].
type memLog struct {
	ents []Entry
}

func newMemLog(snapIndex, snapTerm uint64, entries []Entry) *memLog {
	ents := make([]Entry, 1, 1+len(entries))
	ents[0] = Entry{Index: snapIndex, Term: snapTerm}
	return &memLog{ents: append(ents, entries...)}
}

func (l *memLog) snapIndex() uint64 { return l.ents[0].Index }
func (l *memLog) snapTerm() uint64  { return l.ents[0].Term }
func (l *memLog) lastIndex() uint64 { return l.ents[len(l.ents)-1].Index }
func (l *memLog) lastTerm() uint64  { return l.ents[len(l.ents)-1].Term }

// term returns the term of entry i. It works for the snapshot's last index
// too; ok is false if i is compacted away or beyond the end of the log.
func (l *memLog) term(i uint64) (uint64, bool) {
	if i < l.snapIndex() || i > l.lastIndex() {
		return 0, false
	}
	return l.ents[i-l.snapIndex()].Term, true
}

// slice returns a copy of entries in [lo, hi). The copy matters: the caller
// uses it after releasing the lock, while the log may be truncated and
// overwritten in place.
func (l *memLog) slice(lo, hi uint64) []Entry {
	if lo <= l.snapIndex() || hi > l.lastIndex()+1 || lo >= hi {
		return nil
	}
	s := l.snapIndex()
	return append([]Entry(nil), l.ents[lo-s:hi-s]...)
}

// truncateFrom removes entries with index >= i.
func (l *memLog) truncateFrom(i uint64) {
	if i <= l.snapIndex() || i > l.lastIndex() {
		return
	}
	l.ents = l.ents[:i-l.snapIndex()]
}

func (l *memLog) append(es ...Entry) { l.ents = append(l.ents, es...) }

// compact discards entries up to and including index i, which becomes the new
// sentinel. If i is past the end of the log the whole log is discarded.
func (l *memLog) compact(i, term uint64) {
	var rest []Entry
	if i < l.lastIndex() {
		rest = l.ents[i-l.snapIndex()+1:]
	}
	nl := newMemLog(i, term, rest)
	l.ents = nl.ents
}

// lastIndexOfTerm returns the highest index holding an entry of the given
// term, or 0 if the log has none.
func (l *memLog) lastIndexOfTerm(term uint64) uint64 {
	for k := len(l.ents) - 1; k > 0; k-- {
		switch t := l.ents[k].Term; {
		case t == term:
			return l.ents[k].Index
		case t < term:
			return 0 // terms only decrease going backwards
		}
	}
	return 0
}

// firstIndexOfTerm returns the first index of the run of entries with the
// same term as entry i, scanning back no further than the snapshot.
func (l *memLog) firstIndexOfTerm(i uint64) uint64 {
	t, ok := l.term(i)
	if !ok {
		return i
	}
	for i > l.snapIndex()+1 {
		if pt, _ := l.term(i - 1); pt != t {
			break
		}
		i--
	}
	return i
}

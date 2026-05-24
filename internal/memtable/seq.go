package memtable

import "sync/atomic"

// seqTracker is a concurrent min/max tracker for sequence numbers observed
// by a memtable. Each call to observe atomically updates the tracker.
//
// Implementation: a single uint64 holds the running min in the low half if
// initialized; we keep a separate uint64 for max to avoid packing/unpacking
// on the hot path.
type seqTracker struct {
	v atomic.Uint64
}

// Load returns the current min (the lower bound; 0 if never written).
func (t *seqTracker) Load() uint64 { return t.v.Load() }

// MaxLoad returns the current max (the upper bound; 0 if never written).
func (t *seqTracker) MaxLoad() uint64 { return t.v.Load() }

// observe updates the tracker to reflect a write at seq. The Memtable type
// embeds two seqTrackers — one for min and one for max — but they share
// type because the underlying primitive (atomic compare-and-swap-down or
// compare-and-swap-up) is identical apart from the comparison direction.
//
// Memtable wires them with the right direction via observeMin / observeMax.
func (t *seqTracker) observeMin(seq uint64) {
	for {
		cur := t.v.Load()
		if cur != 0 && cur <= seq {
			return
		}
		if t.v.CompareAndSwap(cur, seq) {
			return
		}
	}
}

func (t *seqTracker) observeMax(seq uint64) {
	for {
		cur := t.v.Load()
		if cur >= seq {
			return
		}
		if t.v.CompareAndSwap(cur, seq) {
			return
		}
	}
}

func (m *Memtable) observe(seq uint64) {
	m.minSeq.observeMin(seq)
	m.maxSeq.observeMax(seq)
}

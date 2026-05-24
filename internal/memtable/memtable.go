// Package memtable implements slate's in-memory MVCC store.
//
// A memtable holds recent writes in a lock-free concurrent skiplist (package
// skl) keyed by slate's internal-key encoding (package keys). It separately
// tracks range tombstones in a parallel skiplist so that range-deletion
// lookups do not blow through the point-entry index.
//
// Lifecycle. A memtable starts mutable (active). When its arena reaches the
// configured byte budget, the engine rotates: the current memtable is
// sealed (marking the arena read-only), pushed onto the immutable queue,
// and a fresh active memtable is allocated. A flush worker then writes
// the immutable memtable's contents to an L0 SSTable. Once the manifest
// edit committing the L0 file is durable, the immutable memtable's arena
// is returned to a pool and discarded.
package memtable

import (
	"bytes"

	"github.com/harimalladi/slate/internal/arena"
	"github.com/harimalladi/slate/internal/keys"
	"github.com/harimalladi/slate/internal/skl"
)

// Memtable is one in-memory level of the LSM. It is safe for concurrent
// readers and writers; multiple writers may insert concurrently and readers
// observe a consistent view at any sequence number.
type Memtable struct {
	pointArena *arena.Arena
	point      *skl.Skl
	rangeArena *arena.Arena
	rangeTomb  *skl.Skl

	// minSeq / maxSeq bound the sequence numbers of entries written to this
	// memtable; tracked so the flush worker can record them on the produced
	// SSTable. Both start at sentinel values: minSeq = math.MaxUint64 sentinel
	// and maxSeq = 0.
	minSeq, maxSeq seqTracker
}

// New constructs an empty memtable backed by an arena of `capBytes` bytes.
// The arena holds both point entries and range tombstones interleaved; a
// secondary arena of capBytes/8 is allocated for range-tombstone bookkeeping.
func New(capBytes int) *Memtable {
	if capBytes < 1024 {
		capBytes = 1024
	}
	a := arena.New(capBytes)
	r := arena.New(max(1024, capBytes/8))
	return &Memtable{
		pointArena: a,
		point:      skl.NewWithComparator(a, keys.Compare),
		rangeArena: r,
		rangeTomb:  skl.NewWithComparator(r, keys.Compare),
	}
}

// Used reports how many arena bytes the memtable has consumed (including
// both point and range-tombstone arenas).
func (m *Memtable) Used() int {
	return m.pointArena.Used() + m.rangeArena.Used()
}

// Cap reports total arena capacity (point + range).
func (m *Memtable) Cap() int {
	return m.pointArena.Cap() + m.rangeArena.Cap()
}

// Sealed reports whether the memtable's arenas have been sealed against
// further writes. A sealed memtable accepts no new entries; reads still work.
func (m *Memtable) Sealed() bool { return m.pointArena.Sealed() }

// Seal marks the memtable immutable.
func (m *Memtable) Seal() {
	m.pointArena.Seal()
	m.rangeArena.Seal()
}

// MinSeq returns the smallest sequence number written to this memtable,
// or 0 if the memtable is empty.
func (m *Memtable) MinSeq() uint64 { return m.minSeq.Load() }

// MaxSeq returns the largest sequence number written, or 0 if empty.
func (m *Memtable) MaxSeq() uint64 { return m.maxSeq.MaxLoad() }

// Set writes a point entry with kind = KindInlineValue or KindVlogPointer.
// `value` for an inline entry is the raw value bytes; for a vlog pointer
// it is the 16-byte serialized pointer. Returns false if the arenas are
// full (caller should rotate).
func (m *Memtable) Set(userKey []byte, seq uint64, kind keys.Kind, value []byte) bool {
	if kind != keys.KindInlineValue && kind != keys.KindVlogPointer {
		panic("memtable: Set requires KindInlineValue or KindVlogPointer")
	}
	ikey := keys.Encode(nil, userKey, seq, kind)
	if !m.point.Insert(ikey, value) {
		return false
	}
	m.observe(seq)
	return true
}

// Delete writes a tombstone for userKey at seq.
func (m *Memtable) Delete(userKey []byte, seq uint64) bool {
	ikey := keys.Encode(nil, userKey, seq, keys.KindDeletion)
	if !m.point.Insert(ikey, nil) {
		return false
	}
	m.observe(seq)
	return true
}

// DeleteRange writes a range tombstone covering [start, end). It is stored
// in the parallel range-tombstone skiplist, not the point skiplist.
func (m *Memtable) DeleteRange(start, end []byte, seq uint64) bool {
	if bytes.Compare(start, end) >= 0 {
		// Empty or inverted range; reject without consuming arena.
		return true
	}
	ikey := keys.Encode(nil, start, seq, keys.KindRangeDeletion)
	if !m.rangeTomb.Insert(ikey, end) {
		return false
	}
	m.observe(seq)
	return true
}

// Get returns the value visible at snapshotSeq for userKey. The boolean is
// false if the key is absent OR if its latest visible version is a tombstone
// (deletion or range deletion).
//
// The returned (kind, value) describe the located entry; callers that need
// to distinguish "not present" from "deleted at snapshot" can inspect kind.
func (m *Memtable) Get(userKey []byte, snapshotSeq uint64) (value []byte, kind keys.Kind, ok bool) {
	// Look up the latest visible point entry for userKey.
	lookup := keys.LookupKey(nil, userKey, snapshotSeq)
	k, v, found := m.point.Get(lookup)
	if !found {
		// No entry >= lookup; userKey may still be range-deleted, but if
		// no point entry exists the answer is simply "not present" anyway.
		return nil, keys.KindInvalid, false
	}
	gotUser, gotSeq, gotKind, parseOK := keys.Parse(k)
	if !parseOK || !bytes.Equal(gotUser, userKey) {
		// Located key belongs to a different user key — userKey not present.
		// Still check range tombstones in case of a future write convention.
		if m.coveredByRangeTomb(userKey, snapshotSeq, 0) {
			return nil, keys.KindDeletion, false
		}
		return nil, keys.KindInvalid, false
	}
	if gotSeq > snapshotSeq {
		// Entry is in the future relative to the snapshot.
		return nil, keys.KindInvalid, false
	}
	// MEM-RD-005: check range tombstones with seq > point seq.
	if m.coveredByRangeTomb(userKey, snapshotSeq, gotSeq) {
		return nil, keys.KindDeletion, false
	}
	switch gotKind {
	case keys.KindInlineValue, keys.KindVlogPointer:
		return v, gotKind, true
	case keys.KindDeletion:
		return nil, keys.KindDeletion, false
	default:
		return nil, keys.KindInvalid, false
	}
}

// coveredByRangeTomb returns true if userKey is covered by any range
// tombstone with seq in (pointSeq, snapshotSeq]. Memtables typically hold
// only a handful of range tombstones, so a linear scan from the start of
// the range-tomb skiplist is acceptable.
func (m *Memtable) coveredByRangeTomb(userKey []byte, snapshotSeq, pointSeq uint64) bool {
	it := m.rangeTomb.NewIterator()
	for it.First(); it.Valid(); it.Next() {
		startUser, seq, _, ok := keys.Parse(it.Key())
		if !ok {
			continue
		}
		if bytes.Compare(startUser, userKey) > 0 {
			// range starts past userKey; subsequent tombstones start
			// even later, so the scan can stop.
			break
		}
		end := it.Value()
		if bytes.Compare(end, userKey) <= 0 {
			continue
		}
		if seq > snapshotSeq || seq <= pointSeq {
			continue
		}
		return true
	}
	return false
}

// NewRawIterator returns a forward iterator visiting every point entry in
// internal-key order, without MVCC or range-tombstone filtering. This is
// the iterator used by flush: every version of every key becomes an SSTable
// entry so that snapshot reads at a later point can still see older versions.
func (m *Memtable) NewRawIterator() *RawIterator {
	return &RawIterator{raw: m.point.NewIterator()}
}

// RangeTombstone is one [start, end) deletion recorded in the memtable.
type RangeTombstone struct {
	Start []byte
	End   []byte
	Seq   uint64
}

// RangeTombstones returns every range tombstone in this memtable in
// ascending (start, descending-seq) order. Used by the flush path to
// preserve range deletions across SSTable writes.
//
// Returned byte slices reference the memtable's arena; callers that need
// to retain them beyond the next memtable rotation must copy.
func (m *Memtable) RangeTombstones() []RangeTombstone {
	var out []RangeTombstone
	it := m.rangeTomb.NewIterator()
	for it.First(); it.Valid(); it.Next() {
		start, seq, _, ok := keys.Parse(it.Key())
		if !ok {
			continue
		}
		out = append(out, RangeTombstone{
			Start: start,
			End:   it.Value(),
			Seq:   seq,
		})
	}
	return out
}

// RawIterator yields raw (internalKey, value) pairs from a memtable.
type RawIterator struct {
	raw interface {
		First()
		Next()
		Valid() bool
		Key() []byte
		Value() []byte
	}
}

func (it *RawIterator) First()        { it.raw.First() }
func (it *RawIterator) Next()         { it.raw.Next() }
func (it *RawIterator) Valid() bool   { return it.raw.Valid() }
func (it *RawIterator) Key() []byte   { return it.raw.Key() }
func (it *RawIterator) Value() []byte { return it.raw.Value() }

// NewIterator returns a forward iterator over point entries at snapshotSeq.
// Iteration applies MVCC and range-tombstone filtering.
//
// The iterator's lifetime is bounded by the memtable's arena lifetime;
// callers must release iterators before the memtable is discarded.
func (m *Memtable) NewIterator(snapshotSeq uint64) *Iterator {
	return &Iterator{m: m, snap: snapshotSeq, raw: m.point.NewIterator()}
}

// Iterator walks point entries in ascending user-key order, exposing the
// largest visible version of each user key.
type Iterator struct {
	m    *Memtable
	snap uint64
	raw  *skl.Iterator
	// lastUser tracks the user key of the most recently emitted entry so
	// duplicate-user-key versions are skipped automatically.
	lastUser []byte
	curUser  []byte
	curSeq   uint64
	curKind  keys.Kind
	curVal   []byte
	valid    bool
}

// First positions the iterator at the smallest visible user key.
func (it *Iterator) First() {
	it.raw.First()
	it.lastUser = nil
	it.advance()
}

// SeekGE positions at the smallest visible user key >= target.
func (it *Iterator) SeekGE(target []byte) {
	lookup := keys.LookupKey(nil, target, keys.MaxSeq)
	it.raw.SeekGE(lookup)
	it.lastUser = nil
	it.advance()
}

// Next advances to the next distinct visible user key.
func (it *Iterator) Next() { it.advance() }

// Valid reports whether the iterator is positioned on an entry.
func (it *Iterator) Valid() bool { return it.valid }

// Key returns the current user key.
func (it *Iterator) Key() []byte { return it.curUser }

// Value returns the current value bytes (caller must consult Kind to
// interpret them).
func (it *Iterator) Value() []byte { return it.curVal }

// Kind returns the current entry's kind tag.
func (it *Iterator) Kind() keys.Kind { return it.curKind }

// Seq returns the current entry's sequence number.
func (it *Iterator) Seq() uint64 { return it.curSeq }

func (it *Iterator) advance() {
	for ; it.raw.Valid(); it.raw.Next() {
		k := it.raw.Key()
		user, seq, kind, ok := keys.Parse(k)
		if !ok {
			continue
		}
		if seq > it.snap {
			continue
		}
		// Skip remaining versions of an already-emitted user key.
		if it.lastUser != nil && bytes.Equal(it.lastUser, user) {
			continue
		}
		if it.m.coveredByRangeTomb(user, it.snap, seq) {
			it.lastUser = append(it.lastUser[:0], user...)
			continue
		}
		if kind == keys.KindDeletion {
			it.lastUser = append(it.lastUser[:0], user...)
			continue
		}
		it.curUser = user
		it.curSeq = seq
		it.curKind = kind
		it.curVal = it.raw.Value()
		it.valid = true
		// Stash and advance the raw iterator so the next call moves past
		// the current key.
		it.lastUser = append(it.lastUser[:0], user...)
		it.raw.Next()
		return
	}
	it.valid = false
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

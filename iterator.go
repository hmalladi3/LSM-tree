package slate

import (
	"bytes"
	"sort"

	"github.com/harimalladi/slate/internal/keys"
	manifest "github.com/harimalladi/slate/internal/manifest"
	"github.com/harimalladi/slate/internal/memtable"
	"github.com/harimalladi/slate/internal/sstable"
	"github.com/harimalladi/slate/internal/vlog"
)

// IterOptions configures a new iterator.
type IterOptions struct {
	// Lower is the inclusive lower bound on user keys. Nil means unbounded.
	Lower []byte
	// Upper is the exclusive upper bound on user keys. Nil means unbounded.
	Upper []byte
}

// NewIterator returns an iterator over the database's current snapshot.
// Iteration walks the merged LSM (active memtable, every immutable memtable,
// and every L0 SSTable) in ascending user-key order, applying MVCC
// filtering against the snapshot's sequence number.
//
// The iterator pins the underlying manifest Version; callers MUST Close it
// to release the pin.
func (db *DB) NewIterator(opts *IterOptions) *Iterator {
	return db.newIteratorAt(opts, db.visibleTS.Load())
}

// newIteratorAt opens an iterator at an explicit snapshot ts. Used by
// db.NewIterator (at db.visibleTS) and Txn.NewIterator (at the txn's
// read_ts) to share the same source-construction logic.
func (db *DB) newIteratorAt(opts *IterOptions, snap uint64) *Iterator {
	if db.closed.Load() {
		return &Iterator{err: ErrClosed}
	}
	it := &Iterator{
		db:   db,
		opts: opts,
		snap: snap,
	}
	if opts != nil {
		it.lower = append([]byte(nil), opts.Lower...)
		it.upper = append([]byte(nil), opts.Upper...)
	}

	// Pin the current Version so L0 files don't get cleaned up underneath
	// us. The version is released in Close.
	it.ver = db.manifest.Current()

	// Capture the immutable queue snapshot for stable iteration.
	db.rotMu.Lock()
	imms := append([]*memtable.Memtable(nil), db.immutable...)
	db.rotMu.Unlock()

	// Source 0: active memtable.
	it.sources = append(it.sources, newMemSource(db.memtable))
	// Sources 1..k: immutable memtables, newest last (so heap-min-key
	// tiebreaks naturally pick newer-seq entries first).
	for _, m := range imms {
		it.sources = append(it.sources, newMemSource(m))
	}
	// Sources k+1..k+L0: L0 SSTables, newest first.
	for i := len(it.ver.Tables[0]) - 1; i >= 0; i-- {
		t := it.ver.Tables[0][i]
		db.sstMu.RLock()
		r := db.readers[t.FileNum]
		db.sstMu.RUnlock()
		if r == nil {
			it.err = errInternal
			return it
		}
		it.sources = append(it.sources, newSSTSource(r))
	}

	// Sources for L1..L6: one per level, walking files in ascending order.
	open := func(num uint32) *sstable.Reader {
		db.sstMu.RLock()
		defer db.sstMu.RUnlock()
		return db.readers[num]
	}
	for lvl := 1; lvl < manifest.NumLevels; lvl++ {
		if len(it.ver.Tables[lvl]) == 0 {
			continue
		}
		it.sources = append(it.sources, &levelSource{newLevelIter(it.ver.Tables[lvl], open)})
	}

	return it
}

// Iterator walks the database in ascending user-key order.
//
// Iterators are not safe for concurrent use; create one per goroutine.
// Returned slices from Key and Value are valid only until the next
// iterator method call or until Close.
type Iterator struct {
	db    *DB
	ver   *manifest.Version
	opts  *IterOptions
	lower []byte
	upper []byte
	snap  uint64

	sources []iterSource

	// Materialized state for the current user key.
	curUser  []byte
	curValue []byte
	valid    bool
	err      error

	// Synthetic mode: when isSynthetic is true, the iterator walks a
	// pre-materialized list of (key, value) pairs rather than the source
	// arms. Used by Txn.NewIterator after merging the LSM iterator output
	// with the transaction's buffered writes.
	isSynthetic  bool
	synthetic    []syntheticEntry
	syntheticIdx int

	// closed becomes true after Close runs. All public methods then
	// return their zero behavior (Valid → false, Key/Value → nil,
	// First/SeekGE/Next → false).
	closed bool
}

type syntheticEntry struct {
	key   []byte
	value []byte
}

// wrapWithTxnBuffer materializes the merged view of `inner` and the txn
// buffer into a synthetic iterator. The inner LSM iterator is fully
// consumed and closed; the returned iterator walks the merged result.
//
// Memory cost is O(visible keys + buffer size). For typical txns this is
// modest; very-large-scan transactions should use db.NewIterator instead
// and handle their own staging.
func wrapWithTxnBuffer(inner *Iterator, writes map[string]*pendingOp) *Iterator {
	if inner.err != nil {
		return inner
	}

	// Build buffer entries in sorted-by-key order.
	bufKeys := make([]string, 0, len(writes))
	for k := range writes {
		bufKeys = append(bufKeys, k)
	}
	sort.Strings(bufKeys)

	merged := make([]syntheticEntry, 0, len(writes)+16)

	bi := 0
	bufLen := len(bufKeys)

	// inner is already positioned at First() implicitly? No — caller does
	// not call First. We advance it here.
	inner.First()
	for inner.Valid() && bi < bufLen {
		lsmKey := inner.Key()
		bufKey := []byte(bufKeys[bi])
		c := bytes.Compare(bufKey, lsmKey)
		switch {
		case c < 0:
			// Emit buffer entry (if not a deletion).
			if op := writes[bufKeys[bi]]; op.kind == keys.KindInlineValue {
				merged = append(merged, syntheticEntry{
					key:   append([]byte(nil), bufKey...),
					value: append([]byte(nil), op.value...),
				})
			}
			bi++
		case c > 0:
			// Emit LSM entry.
			merged = append(merged, syntheticEntry{
				key:   append([]byte(nil), lsmKey...),
				value: append([]byte(nil), inner.Value()...),
			})
			inner.Next()
		default:
			// Buffer wins for same user key.
			if op := writes[bufKeys[bi]]; op.kind == keys.KindInlineValue {
				merged = append(merged, syntheticEntry{
					key:   append([]byte(nil), bufKey...),
					value: append([]byte(nil), op.value...),
				})
			}
			bi++
			inner.Next()
		}
	}
	// Drain whichever side has remaining entries.
	for inner.Valid() {
		merged = append(merged, syntheticEntry{
			key:   append([]byte(nil), inner.Key()...),
			value: append([]byte(nil), inner.Value()...),
		})
		inner.Next()
	}
	for ; bi < bufLen; bi++ {
		if op := writes[bufKeys[bi]]; op.kind == keys.KindInlineValue {
			merged = append(merged, syntheticEntry{
				key:   append([]byte(nil), bufKeys[bi]...),
				value: append([]byte(nil), op.value...),
			})
		}
	}

	innerErr := inner.Error()
	_ = inner.Close()

	out := &Iterator{
		db:           inner.db,
		opts:         inner.opts,
		lower:        inner.lower,
		upper:        inner.upper,
		snap:         inner.snap,
		isSynthetic:  true,
		synthetic:    merged,
		syntheticIdx: -1,
		err:          innerErr,
	}
	return out
}

// First positions the iterator at the first visible user key.
func (it *Iterator) First() bool {
	if it.closed || it.err != nil {
		return false
	}
	if it.isSynthetic {
		it.syntheticIdx = 0
		return it.advanceSynthetic()
	}
	target := it.lower
	for _, s := range it.sources {
		if target == nil {
			s.first()
		} else {
			s.seekGE(keys.LookupKey(nil, target, keys.MaxSeq))
		}
	}
	return it.advance(nil)
}

// SeekGE positions at the first visible user key >= target.
func (it *Iterator) SeekGE(target []byte) bool {
	if it.closed || it.err != nil {
		return false
	}
	if it.lower != nil && bytes.Compare(target, it.lower) < 0 {
		target = it.lower
	}
	if it.isSynthetic {
		it.syntheticIdx = sort.Search(len(it.synthetic), func(i int) bool {
			return bytes.Compare(it.synthetic[i].key, target) >= 0
		})
		return it.advanceSynthetic()
	}
	for _, s := range it.sources {
		s.seekGE(keys.LookupKey(nil, target, keys.MaxSeq))
	}
	return it.advance(nil)
}

// Next advances to the next visible user key.
func (it *Iterator) Next() bool {
	if it.closed || !it.valid || it.err != nil {
		return false
	}
	if it.isSynthetic {
		it.syntheticIdx++
		return it.advanceSynthetic()
	}
	prev := append(it.curUser[:0:0], it.curUser...)
	return it.advance(prev)
}

// advanceSynthetic positions onto the next synthetic entry that falls
// within bounds, or marks the iterator invalid.
func (it *Iterator) advanceSynthetic() bool {
	for it.syntheticIdx < len(it.synthetic) {
		e := it.synthetic[it.syntheticIdx]
		if it.upper != nil && bytes.Compare(e.key, it.upper) >= 0 {
			it.valid = false
			return false
		}
		it.curUser = append(it.curUser[:0], e.key...)
		it.curValue = append(it.curValue[:0], e.value...)
		it.valid = true
		return true
	}
	it.valid = false
	return false
}

// Valid reports whether the iterator is positioned on a visible key.
func (it *Iterator) Valid() bool { return !it.closed && it.valid && it.err == nil }

// Key returns the current user key. The slice is valid until the next call.
func (it *Iterator) Key() []byte {
	if it.closed || !it.valid {
		return nil
	}
	return it.curUser
}

// Value returns the current value. The slice is valid until the next call.
func (it *Iterator) Value() []byte {
	if it.closed || !it.valid {
		return nil
	}
	return it.curValue
}

// Error returns the sticky error, if any.
func (it *Iterator) Error() error { return it.err }

// Close releases all resources held by the iterator. After Close, all
// methods are no-ops: Valid returns false, Key/Value return nil, and
// First/SeekGE/Next return false. Close itself is idempotent.
func (it *Iterator) Close() error {
	if it.closed {
		return it.err
	}
	for _, s := range it.sources {
		_ = s.close()
		if e := s.error(); e != nil && it.err == nil {
			it.err = e
		}
	}
	it.sources = nil
	if it.ver != nil {
		it.ver.Unref()
		it.ver = nil
	}
	it.synthetic = nil
	it.valid = false
	it.curUser = it.curUser[:0]
	it.curValue = it.curValue[:0]
	it.closed = true
	return it.err
}

// advance moves to the next visible user key strictly greater than prev (or
// the smallest if prev is nil). Returns true iff Valid() afterwards.
func (it *Iterator) advance(prev []byte) bool {
	for {
		idx := -1
		var minKey []byte
		for i, s := range it.sources {
			if e := s.error(); e != nil {
				it.err = e
				it.valid = false
				return false
			}
			if !s.valid() {
				continue
			}
			k := s.key()
			if idx < 0 || keys.Compare(k, minKey) < 0 {
				idx = i
				minKey = k
			}
		}
		if idx < 0 {
			it.valid = false
			return false
		}
		user, seq, kind, ok := keys.Parse(minKey)
		if !ok {
			it.err = ErrCorrupted
			it.valid = false
			return false
		}

		// Drop entries that:
		// - are above the snapshot
		// - belong to the just-emitted user key (we already emitted the
		//   latest visible version for prev)
		// - fall outside the configured bounds
		drop := false
		if seq > it.snap {
			drop = true
		} else if prev != nil && bytes.Equal(user, prev) {
			drop = true
		} else if it.upper != nil && bytes.Compare(user, it.upper) >= 0 {
			it.valid = false
			return false
		}
		if drop {
			it.sources[idx].next()
			continue
		}

		// First visible entry for this user key. If it is a deletion, skip
		// the user key entirely; otherwise emit.
		if kind == keys.KindDeletion {
			it.sources[idx].next()
			prev = append(prev[:0], user...)
			continue
		}
		// Read the value at the current position BEFORE advancing.
		raw := it.sources[idx].value()
		if kind == keys.KindVlogPointer {
			// Dereference the 16-byte pointer into the actual value bytes.
			// Without this, Value() would surface the raw pointer to the
			// caller — a real correctness bug for any iterator scan over
			// data spilled to the vlog.
			ptr, perr := vlog.DecodePointer(raw)
			if perr != nil {
				it.err = perr
				it.valid = false
				return false
			}
			derefed, derr := it.db.vlogReader.Dereference(ptr)
			if derr != nil {
				it.err = derr
				it.valid = false
				return false
			}
			it.curUser = append(it.curUser[:0], user...)
			it.curValue = append(it.curValue[:0], derefed...)
		} else {
			it.curUser = append(it.curUser[:0], user...)
			it.curValue = append(it.curValue[:0], raw...)
		}
		it.sources[idx].next()
		it.valid = true
		return true
	}
}

// ----- per-source adapters -----

// iterSource is the internal abstraction over per-source iterators. Each
// source produces internal keys in ascending order. `value()` returns the
// value at the current position; the merge driver must read the value
// before calling `next()`.
type iterSource interface {
	first()
	seekGE(internalKey []byte)
	next()
	valid() bool
	key() []byte   // current internal key
	value() []byte // current value
	close() error
	error() error
}

type memSource struct {
	it *memtable.RawIterator
}

func newMemSource(m *memtable.Memtable) *memSource {
	return &memSource{it: m.NewRawIterator()}
}

func (s *memSource) first()        { s.it.First() }
func (s *memSource) next()         { s.it.Next() }
func (s *memSource) valid() bool   { return s.it.Valid() }
func (s *memSource) key() []byte   { return s.it.Key() }
func (s *memSource) value() []byte { return s.it.Value() }
func (s *memSource) close() error  { return nil }
func (s *memSource) error() error  { return nil }

// memSource doesn't currently support efficient SeekGE on the skiplist
// directly (NewRawIterator exposes only First/Next). For now we fall back
// to a linear scan from the head.
func (s *memSource) seekGE(target []byte) {
	s.it.First()
	for s.it.Valid() && keys.Compare(s.it.Key(), target) < 0 {
		s.it.Next()
	}
}

type sstSource struct {
	r  *sstable.Reader
	it *sstable.Iterator
}

func newSSTSource(r *sstable.Reader) *sstSource {
	return &sstSource{r: r, it: r.NewIterator()}
}

func (s *sstSource) first()          { s.it.First() }
func (s *sstSource) seekGE(k []byte) { s.it.SeekGE(k) }
func (s *sstSource) next()           { s.it.Next() }
func (s *sstSource) valid() bool     { return s.it.Valid() }
func (s *sstSource) key() []byte     { return s.it.Key() }
func (s *sstSource) value() []byte   { return s.it.Value() }
func (s *sstSource) close() error    { return s.it.Close() }
func (s *sstSource) error() error    { return s.it.Error() }

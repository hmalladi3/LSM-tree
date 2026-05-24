// Package skl implements a lock-free concurrent skiplist backed by an arena.
//
// The skiplist stores raw byte-comparable keys (slate's "internal key" format
// — see package keys). Keys are sorted by bytes.Compare. The data structure
// supports lock-free concurrent inserts via CAS on per-level next pointers
// and entirely-lock-free reads (no atomic on the read path beyond loading
// next pointers).
//
// References:
//   - W. Pugh. "Skip Lists: A Probabilistic Alternative to Balanced Trees."
//     CACM 1990.
//   - Herlihy & Shavit, "The Art of Multiprocessor Programming," Ch. 14.
//   - cockroachdb/pebble internal/arenaskl: the Go reference implementation
//     for this exact pattern; slate's layout follows the same shape.
//
// Memory layout. Every node lives inside the supplied arena.Arena:
//
//	+---------------------+
//	| keyOff   u32        |
//	| keyLen   u32        |
//	| valueOff u32        |  0 if no value bytes (e.g., tombstone)
//	| valueLen u32        |
//	| meta     u32        |  low byte = tower height, rest reserved
//	| next[0]  u32        |  arena offset of the next node at level 0
//	| next[1]  u32        |  ... (only `height` slots allocated)
//	+---------------------+
//
// next slots are accessed via sync/atomic on a *uint32 obtained from
// unsafe.Pointer over the arena's backing buffer. The buffer is allocated
// once and never reallocated, so the pointers stay valid for the arena's
// lifetime.
package skl

import (
	"bytes"
	"sync/atomic"
	"unsafe"

	"github.com/harimalladi/slate/internal/arena"
)

const (
	// MaxHeight is the largest tower height a node may have. At p = 1/4
	// and a memtable cap measured in hundreds of MiB, 17 levels handle
	// any realistic number of entries (~ 4^17 ≈ 1.7×10^10).
	MaxHeight = 17

	// pBranching is 1/4 expressed in bits: a node ascends one level
	// whenever the low two bits of its random source are zero.
	pBranchingMask uint32 = 0x3

	headerSize  uint32 = 20 // bytes before the next[] array
	nextSlotLen uint32 = 4  // each next slot is a u32
)

// Comparator orders keys. nil means bytes.Compare.
type Comparator func(a, b []byte) int

// Skl is a concurrent skiplist.
type Skl struct {
	arena   *arena.Arena
	head    uint32       // offset of the sentinel head node
	height  atomic.Int32 // current observed maximum tower height
	compare Comparator
}

// New constructs a skiplist on top of a freshly-created arena, using
// bytes.Compare as the key comparator. Use NewWithComparator for custom
// orderings (slate's internal-key comparator, for example).
func New(a *arena.Arena) *Skl {
	return NewWithComparator(a, bytes.Compare)
}

// NewWithComparator constructs a skiplist with a user-supplied comparator.
func NewWithComparator(a *arena.Arena, cmp Comparator) *Skl {
	if cmp == nil {
		cmp = bytes.Compare
	}
	s := &Skl{arena: a, compare: cmp}
	headSize := headerSize + MaxHeight*nextSlotLen
	off := a.Alloc(headSize, 4)
	if off == arena.InvalidOff {
		panic("skl: arena too small for head node")
	}
	s.writeMeta(off, MaxHeight)
	s.head = off
	s.height.Store(1)
	return s
}

// Insert adds (key, value) to the skiplist. Returns true on success, false
// if the arena is full or has been sealed.
//
// Key bytes are NOT copied — the caller must hand over a stable slice that
// lives at least until Insert returns. The slice contents are then copied
// into the arena, so subsequent caller-side mutations are safe.
//
// Insert assumes keys are unique. Two concurrent Inserts of the same key
// produce undefined ordering and (in debug builds) panic — slate ensures
// uniqueness at the oracle layer by assigning distinct sequence numbers.
func (s *Skl) Insert(key, value []byte) bool {
	if s.arena.Sealed() {
		return false
	}
	height := randomHeight(key)
	nodeOff, ok := s.allocNode(key, value, height)
	if !ok {
		return false
	}

	// Top-down descent to find predecessors / successors at every level.
	var prev, next [MaxHeight]uint32
	prev[MaxHeight-1] = s.head
	next[MaxHeight-1] = 0
	listHeight := int(s.height.Load())
	for lvl := MaxHeight - 1; lvl >= 0; lvl-- {
		if lvl >= listHeight {
			prev[lvl] = s.head
			next[lvl] = 0
			continue
		}
		p, n := s.findSpliceAt(lvl, key, prev[min(lvl+1, MaxHeight-1)])
		prev[lvl] = p
		next[lvl] = n
	}

	// Link bottom-up. CAS retries on contention at the affected level.
	for lvl := 0; lvl < height; lvl++ {
		for {
			s.setNext(nodeOff, lvl, next[lvl])
			if s.casNext(prev[lvl], lvl, next[lvl], nodeOff) {
				break
			}
			// Someone inserted between prev[lvl] and next[lvl]; re-find.
			prev[lvl], next[lvl] = s.findSpliceAt(lvl, key, prev[lvl])
		}
	}

	// Bump observed height. Retry CAS if another inserter beat us.
	for {
		h := s.height.Load()
		if int(h) >= height {
			break
		}
		if s.height.CompareAndSwap(h, int32(height)) {
			break
		}
	}
	return true
}

// Get returns the value, kind tag, and presence for the FIRST node whose key
// is >= the supplied lookup key.
//
// Callers must already have encoded the lookup key with a max kind tag (see
// package keys, LookupKey) so the located node is the largest seq ≤ snapshot.
func (s *Skl) Get(lookup []byte) (key, value []byte, ok bool) {
	curr := s.head
	for lvl := int(s.height.Load()) - 1; lvl >= 0; lvl-- {
		for {
			nxt := s.nextOff(curr, lvl)
			if nxt == 0 {
				break
			}
			cmp := s.compare(s.nodeKey(nxt), lookup)
			if cmp >= 0 {
				break
			}
			curr = nxt
		}
	}
	// curr.next[0] is now the first node with key >= lookup.
	nxt := s.nextOff(curr, 0)
	if nxt == 0 {
		return nil, nil, false
	}
	return s.nodeKey(nxt), s.nodeValue(nxt), true
}

// findSpliceAt walks forward at the given level starting from `from` until
// the next node's key is >= `key`. Returns (predecessor, successor) at that
// level.
func (s *Skl) findSpliceAt(lvl int, key []byte, from uint32) (prev, next uint32) {
	if from == 0 {
		from = s.head
	}
	prev = from
	for {
		nxt := s.nextOff(prev, lvl)
		if nxt == 0 {
			return prev, 0
		}
		if s.compare(s.nodeKey(nxt), key) >= 0 {
			return prev, nxt
		}
		prev = nxt
	}
}

// allocNode reserves space in the arena for a node of the given tower
// height and copies key + value bytes into the arena.
func (s *Skl) allocNode(key, value []byte, height int) (uint32, bool) {
	keyOff := s.arena.Alloc(uint32(len(key)), 1)
	if keyOff == arena.InvalidOff && len(key) != 0 {
		return 0, false
	}
	if len(key) > 0 {
		copy(s.arena.Buf()[keyOff:], key)
	}

	var valOff uint32
	if len(value) > 0 {
		valOff = s.arena.Alloc(uint32(len(value)), 1)
		if valOff == arena.InvalidOff {
			return 0, false
		}
		copy(s.arena.Buf()[valOff:], value)
	}

	nodeSize := headerSize + uint32(height)*nextSlotLen
	nodeOff := s.arena.Alloc(nodeSize, 4)
	if nodeOff == arena.InvalidOff {
		return 0, false
	}
	base := s.arena.Buf()[nodeOff:]
	*(*uint32)(unsafe.Pointer(&base[0])) = keyOff
	*(*uint32)(unsafe.Pointer(&base[4])) = uint32(len(key))
	*(*uint32)(unsafe.Pointer(&base[8])) = valOff
	*(*uint32)(unsafe.Pointer(&base[12])) = uint32(len(value))
	*(*uint32)(unsafe.Pointer(&base[16])) = uint32(height) & 0xff
	// next[] slots are zero already (Go zeroes the arena buffer on make).
	return nodeOff, true
}

func (s *Skl) writeMeta(off uint32, height int) {
	base := s.arena.Buf()[off:]
	*(*uint32)(unsafe.Pointer(&base[16])) = uint32(height) & 0xff
}

func (s *Skl) nodeKey(off uint32) []byte {
	base := s.arena.Buf()[off:]
	keyOff := *(*uint32)(unsafe.Pointer(&base[0]))
	keyLen := *(*uint32)(unsafe.Pointer(&base[4]))
	if keyLen == 0 {
		return nil
	}
	return s.arena.Buf()[keyOff : keyOff+keyLen]
}

func (s *Skl) nodeValue(off uint32) []byte {
	base := s.arena.Buf()[off:]
	valOff := *(*uint32)(unsafe.Pointer(&base[8]))
	valLen := *(*uint32)(unsafe.Pointer(&base[12]))
	if valOff == 0 {
		return nil
	}
	return s.arena.Buf()[valOff : valOff+valLen]
}

func (s *Skl) nodeHeight(off uint32) int {
	base := s.arena.Buf()[off:]
	return int(*(*uint32)(unsafe.Pointer(&base[16])) & 0xff)
}

// nextOff atomically loads the offset stored at next[lvl] of node `off`.
func (s *Skl) nextOff(off uint32, lvl int) uint32 {
	ptr := (*uint32)(unsafe.Pointer(&s.arena.Buf()[off+headerSize+uint32(lvl)*nextSlotLen]))
	return atomic.LoadUint32(ptr)
}

// setNext stores a value into next[lvl] without atomicity. Use only on nodes
// that have not yet been linked into the list.
func (s *Skl) setNext(off uint32, lvl int, value uint32) {
	ptr := (*uint32)(unsafe.Pointer(&s.arena.Buf()[off+headerSize+uint32(lvl)*nextSlotLen]))
	atomic.StoreUint32(ptr, value)
}

// casNext atomically updates next[lvl] from old to new.
func (s *Skl) casNext(off uint32, lvl int, old, new uint32) bool {
	ptr := (*uint32)(unsafe.Pointer(&s.arena.Buf()[off+headerSize+uint32(lvl)*nextSlotLen]))
	return atomic.CompareAndSwapUint32(ptr, old, new)
}

// randomHeight chooses a tower height for a new node. We derive a 32-bit
// pseudo-random value from the key via FNV-1a — this is fully deterministic
// (the same key always picks the same height) and avoids any shared RNG
// state, which would otherwise be a hot-path contention point.
//
// p(height = h) = (1/4)^(h-1) * 3/4 for h < MaxHeight.
func randomHeight(key []byte) int {
	const (
		offset uint32 = 2166136261
		prime  uint32 = 16777619
	)
	h := offset
	for _, b := range key {
		h ^= uint32(b)
		h *= prime
	}
	height := 1
	for height < MaxHeight && (h&pBranchingMask) == 0 {
		height++
		h >>= 2
	}
	return height
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ---- Iterator ----

// Iterator walks the skiplist in ascending key order. Iterators are not safe
// for concurrent use by multiple goroutines; create one per goroutine.
type Iterator struct {
	s    *Skl
	curr uint32
}

// NewIterator returns an iterator positioned before the first node.
func (s *Skl) NewIterator() *Iterator {
	return &Iterator{s: s, curr: 0}
}

// SeekGE positions the iterator at the first node whose key >= target.
func (it *Iterator) SeekGE(target []byte) {
	curr := it.s.head
	for lvl := int(it.s.height.Load()) - 1; lvl >= 0; lvl-- {
		for {
			nxt := it.s.nextOff(curr, lvl)
			if nxt == 0 || it.s.compare(it.s.nodeKey(nxt), target) >= 0 {
				break
			}
			curr = nxt
		}
	}
	it.curr = it.s.nextOff(curr, 0)
}

// First positions the iterator at the smallest key.
func (it *Iterator) First() {
	it.curr = it.s.nextOff(it.s.head, 0)
}

// Next advances to the next key. Becomes invalid past the last key.
func (it *Iterator) Next() {
	if it.curr == 0 {
		return
	}
	it.curr = it.s.nextOff(it.curr, 0)
}

// Valid reports whether the iterator is positioned on a key.
func (it *Iterator) Valid() bool {
	return it.curr != 0
}

// Key returns the key at the current position. The slice references arena
// memory and is valid until the iterator's owning arena is discarded.
func (it *Iterator) Key() []byte {
	if it.curr == 0 {
		return nil
	}
	return it.s.nodeKey(it.curr)
}

// Value returns the value at the current position.
func (it *Iterator) Value() []byte {
	if it.curr == 0 {
		return nil
	}
	return it.s.nodeValue(it.curr)
}

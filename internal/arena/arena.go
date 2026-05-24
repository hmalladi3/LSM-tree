// Package arena implements a bump allocator backed by a fixed-size byte
// buffer. Memtable nodes and entry bytes live in an arena so that:
//
//  1. Allocations are a single atomic add — no malloc, no GC pressure.
//  2. The whole memtable is freed at once when it is flushed to L0; no
//     per-entry deallocation, no compaction, no fragmentation.
//  3. Skiplist next-pointers can be stored as 32-bit offsets into the
//     arena rather than 64-bit Go pointers, halving pointer size and
//     keeping the lock-free CAS slots on cache lines.
//
// Arenas are not safe to grow after construction — the backing buffer is
// allocated once. When the arena fills, the caller switches to a fresh arena
// (this is what memtable rotation does in slate).
package arena

import (
	"sync/atomic"
)

// ErrFull is returned (as a sentinel offset) when an allocation would exceed
// the arena's capacity. Callers compare returned offsets against InvalidOff.
const InvalidOff uint32 = 0

// Arena is a fixed-size bump allocator with optional alignment.
//
// Offset 0 is reserved as the InvalidOff sentinel so allocators can use a
// zero-valued offset to mean "no node." The first byte of the buffer is
// therefore never returned by Alloc.
type Arena struct {
	used atomic.Uint32
	buf  []byte
	// sealed is set true when the memtable transitions to immutable.
	// Allocations after seal return InvalidOff; readers and writers that
	// already hold pointers into the arena continue to work because the
	// buffer is not freed until the arena is discarded.
	sealed atomic.Bool
}

// New returns an Arena of exactly capacity bytes. Capacity must be at least 1.
func New(capacity int) *Arena {
	if capacity < 1 {
		panic("arena: capacity must be >= 1")
	}
	a := &Arena{buf: make([]byte, capacity)}
	// Reserve offset 0 as the InvalidOff sentinel.
	a.used.Store(1)
	return a
}

// Cap returns the arena's total capacity in bytes.
func (a *Arena) Cap() int { return len(a.buf) }

// Used returns the number of bytes allocated so far, including the reserved
// sentinel byte.
func (a *Arena) Used() int { return int(a.used.Load()) }

// Sealed reports whether the arena has been sealed against further
// allocations.
func (a *Arena) Sealed() bool { return a.sealed.Load() }

// Seal marks the arena immutable. Subsequent Alloc calls return InvalidOff.
// Safe to call multiple times.
func (a *Arena) Seal() { a.sealed.Store(true) }

// Alloc returns the offset of a freshly-allocated region of size bytes,
// aligned to align (must be a power of two, 1..8). Returns InvalidOff if
// the arena is sealed or would overflow.
//
// The returned offset is stable for the lifetime of the arena; callers
// read/write via Buf()[off:off+size].
func (a *Arena) Alloc(size, align uint32) uint32 {
	if size == 0 {
		return InvalidOff
	}
	if align&(align-1) != 0 || align > 8 {
		panic("arena: alignment must be 1, 2, 4, or 8")
	}
	if a.sealed.Load() {
		return InvalidOff
	}
	mask := align - 1
	for {
		curr := a.used.Load()
		padding := (^curr + 1) & mask
		want := curr + padding + size
		if want > uint32(len(a.buf)) {
			return InvalidOff
		}
		if a.used.CompareAndSwap(curr, want) {
			return curr + padding
		}
	}
}

// Buf returns the arena's underlying byte slice. Callers must respect
// allocation offsets — writes outside an allocated region race with other
// users.
func (a *Arena) Buf() []byte { return a.buf }

// Slice returns the slice of length n starting at offset off. It does NOT
// validate that (off, n) was a single Alloc result; the caller is responsible.
func (a *Arena) Slice(off uint32, n uint32) []byte {
	return a.buf[off : off+n]
}

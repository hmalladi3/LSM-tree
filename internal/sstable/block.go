package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/harimalladi/slate/internal/keys"
)

// blockBuilder serializes sorted entries with LevelDB-style prefix
// compression. Every restartInterval entries, the prefix-compression run
// resets so binary search inside the block can land on a full key.
type blockBuilder struct {
	buf             []byte
	restartInterval int
	restarts        []uint32
	counter         int // entries since last restart
	lastKey         []byte
	entries         int
}

func newBlockBuilder(restartInterval int) *blockBuilder {
	if restartInterval < 1 {
		restartInterval = 16
	}
	return &blockBuilder{
		restartInterval: restartInterval,
		restarts:        []uint32{0},
	}
}

// add appends (key, value) to the block. Keys must arrive in ascending
// order (verified in debug builds).
func (b *blockBuilder) add(key, value []byte) {
	if b.entries > 0 && keys.Compare(key, b.lastKey) <= 0 {
		panic(fmt.Sprintf("sstable: out-of-order add: prev=%q new=%q", b.lastKey, key))
	}
	shared := 0
	if b.counter < b.restartInterval {
		// Compute common prefix with previous key.
		max := len(key)
		if len(b.lastKey) < max {
			max = len(b.lastKey)
		}
		for shared < max && key[shared] == b.lastKey[shared] {
			shared++
		}
	} else {
		// New restart point; record offset BEFORE appending the entry.
		b.restarts = append(b.restarts, uint32(len(b.buf)))
		b.counter = 0
	}
	unshared := len(key) - shared
	b.buf = appendVarint(b.buf, uint64(shared))
	b.buf = appendVarint(b.buf, uint64(unshared))
	b.buf = appendVarint(b.buf, uint64(len(value)))
	b.buf = append(b.buf, key[shared:]...)
	b.buf = append(b.buf, value...)
	b.lastKey = append(b.lastKey[:0], key...)
	b.counter++
	b.entries++
}

// finish appends the restart array and returns the serialized block.
// Subsequent calls to add panic.
func (b *blockBuilder) finish() []byte {
	for _, r := range b.restarts {
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], r)
		b.buf = append(b.buf, buf[:]...)
	}
	var nbuf [4]byte
	binary.LittleEndian.PutUint32(nbuf[:], uint32(len(b.restarts)))
	b.buf = append(b.buf, nbuf[:]...)
	out := b.buf
	b.buf = nil
	return out
}

func (b *blockBuilder) sizeEstimate() int {
	// Current bytes + 4-byte per restart + 4-byte count.
	return len(b.buf) + 4*len(b.restarts) + 4
}

func (b *blockBuilder) empty() bool     { return b.entries == 0 }
func (b *blockBuilder) numEntries() int { return b.entries }

// ----- decoder / iterator -----

// blockIter iterates entries within one block.
type blockIter struct {
	data        []byte
	restarts    []uint32
	numRestarts int

	pos       int // byte offset into data
	currKey   []byte
	currValue []byte
	valid     bool
}

func newBlockIter(data []byte) (*blockIter, error) {
	if len(data) < 4 {
		return nil, errors.New("sstable: block too short")
	}
	numRestarts := int(binary.LittleEndian.Uint32(data[len(data)-4:]))
	restartArrEnd := len(data) - 4
	restartArrStart := restartArrEnd - numRestarts*4
	if restartArrStart < 0 {
		return nil, errors.New("sstable: malformed restart array")
	}
	restarts := make([]uint32, numRestarts)
	for i := 0; i < numRestarts; i++ {
		restarts[i] = binary.LittleEndian.Uint32(data[restartArrStart+i*4 : restartArrStart+(i+1)*4])
	}
	return &blockIter{
		data:        data[:restartArrStart],
		restarts:    restarts,
		numRestarts: numRestarts,
	}, nil
}

// first positions the iterator on the first entry.
func (it *blockIter) first() {
	it.pos = 0
	it.currKey = it.currKey[:0]
	it.parseAt(0)
}

// seekGE positions the iterator at the first key >= target.
func (it *blockIter) seekGE(target []byte) {
	// Binary search across restart points to find the largest restart whose
	// key is <= target. Then linear-scan forward.
	lo, hi := 0, it.numRestarts-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		offset := int(it.restarts[mid])
		if !it.parseAt(offset) {
			hi = mid - 1
			continue
		}
		if keys.Compare(it.currKey, target) <= 0 {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	// Walk forward from restart `lo` until we find key >= target.
	startOff := int(it.restarts[lo])
	it.currKey = it.currKey[:0]
	if !it.parseAt(startOff) {
		it.valid = false
		return
	}
	for it.valid && keys.Compare(it.currKey, target) < 0 {
		it.advance()
	}
}

// parseAt reads one entry starting at byte offset off. The 'shared'
// component is taken from it.currKey, so the caller is responsible for
// either (a) resetting currKey before calling at a restart, or (b)
// reaching here via advance().
func (it *blockIter) parseAt(off int) bool {
	if off >= len(it.data) {
		it.valid = false
		return false
	}
	it.pos = off
	return it.advance()
}

// advance reads the entry at pos, updates pos to the next entry, sets
// currKey/currValue. Returns false (and clears valid) if no more entries.
func (it *blockIter) advance() bool {
	if it.pos >= len(it.data) {
		it.valid = false
		return false
	}
	shared, n := binary.Uvarint(it.data[it.pos:])
	if n <= 0 {
		it.valid = false
		return false
	}
	it.pos += n
	unshared, n := binary.Uvarint(it.data[it.pos:])
	if n <= 0 {
		it.valid = false
		return false
	}
	it.pos += n
	vlen, n := binary.Uvarint(it.data[it.pos:])
	if n <= 0 {
		it.valid = false
		return false
	}
	it.pos += n
	if it.pos+int(unshared)+int(vlen) > len(it.data) {
		it.valid = false
		return false
	}
	// Reconstruct key by appending unshared portion onto the shared prefix
	// of the previous key.
	if int(shared) > len(it.currKey) {
		it.valid = false
		return false
	}
	it.currKey = append(it.currKey[:int(shared)], it.data[it.pos:it.pos+int(unshared)]...)
	it.pos += int(unshared)
	it.currValue = it.data[it.pos : it.pos+int(vlen)]
	it.pos += int(vlen)
	it.valid = true
	return true
}

func (it *blockIter) next()         { it.advance() }
func (it *blockIter) valid_() bool  { return it.valid }
func (it *blockIter) key() []byte   { return it.currKey }
func (it *blockIter) value() []byte { return it.currValue }

// ----- helpers -----

func appendVarint(dst []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

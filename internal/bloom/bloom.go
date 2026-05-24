// Package bloom implements a fixed-bits-per-key Bloom filter using the
// double-hashing trick of Kirsch & Mitzenmacher.
//
// Each SSTable carries one Bloom filter built over its distinct user keys
// (including keys whose only entry is a tombstone). Probing the filter on a
// point Get short-circuits the index/block walk for keys not in the file.
//
// Defaults match LevelDB / RocksDB: bits per key = 10 (about 1% false-positive
// rate), hashes derived from a single 64-bit FNV-1a via double hashing. The
// false-positive math is the standard
//
//	fpRate ≈ (1 - exp(-k*n/m))^k
//
// where k = numHashes, n = numKeys, m = numBits. For k = 7 and m/n = 10, the
// rate is approximately 0.82%.
package bloom

import "math"

// Filter is an immutable, read-only Bloom filter that has been built and
// frozen. Build via Builder.
type Filter struct {
	bits      []byte
	numBits   uint32 // total bit count; equals len(bits) * 8 minus any unused trailing bits
	numHashes uint8
}

// NewFilter constructs a Filter from a serialized representation produced by
// Builder.Bytes. Returns nil if the input is malformed.
func NewFilter(buf []byte) *Filter {
	// Layout: [bits...][numHashes:u8][bitsPerKey:u8]
	if len(buf) < 2 {
		return nil
	}
	numHashes := buf[len(buf)-2]
	// bitsPerKey is stored for diagnostics only; not used at probe time.
	bits := buf[:len(buf)-2]
	if numHashes == 0 || len(bits) == 0 {
		return nil
	}
	return &Filter{
		bits:      bits,
		numBits:   uint32(len(bits)) * 8,
		numHashes: numHashes,
	}
}

// Contains reports whether key may be in the filter. False is definitive;
// true means "possibly present, fall back to authoritative lookup."
func (f *Filter) Contains(key []byte) bool {
	if f == nil || f.numBits == 0 {
		return true // null filter = no information; conservative pass-through
	}
	h1, h2 := hashes(key)
	for i := uint8(0); i < f.numHashes; i++ {
		bit := (h1 + uint32(i)*h2) % f.numBits
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// SizeBytes returns the on-disk size of the filter payload, including trailer.
func (f *Filter) SizeBytes() int {
	return len(f.bits) + 2
}

// Builder accumulates keys and emits a serialized Filter via Bytes.
type Builder struct {
	hashes     []uint32 // h1 values; h2 stored interleaved below
	bitsPerKey int
	numHashes  uint8
}

// NewBuilder returns a Builder configured for bitsPerKey bits per key. The
// caller must call Add for every distinct user key before calling Bytes.
//
// Picks the number of hash functions that minimizes the false-positive rate
// for the chosen bits-per-key (the standard 0.69 * (m/n) rule, clamped).
func NewBuilder(bitsPerKey int) *Builder {
	if bitsPerKey < 1 {
		bitsPerKey = 1
	}
	if bitsPerKey > 30 {
		bitsPerKey = 30
	}
	// Optimal number of hashes ≈ (m/n) * ln(2) = bitsPerKey * 0.69.
	k := int(math.Round(float64(bitsPerKey) * 0.69))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return &Builder{
		bitsPerKey: bitsPerKey,
		numHashes:  uint8(k),
	}
}

// Add records a key. Duplicate keys (same byte slice contents) are stored
// twice; callers should deduplicate before calling Add to size the filter
// correctly.
func (b *Builder) Add(key []byte) {
	h1, h2 := hashes(key)
	// Stash both hashes; computing here once amortizes the hash work.
	b.hashes = append(b.hashes, h1, h2)
}

// NumKeys returns how many keys have been added (counts duplicates).
func (b *Builder) NumKeys() int {
	return len(b.hashes) / 2
}

// Bytes finalizes the builder and returns the serialized filter payload.
// The builder is reusable after Reset.
func (b *Builder) Bytes() []byte {
	n := b.NumKeys()
	if n == 0 {
		// Empty filter: still emit a trailer so the reader can distinguish
		// "no filter" from "malformed". An empty bits slice with numHashes=1
		// causes Contains to deterministically return false for any key,
		// which is correct (an empty filter has no members).
		return []byte{0x00, b.numHashes, byte(b.bitsPerKey)}
	}
	numBits := uint32(n * b.bitsPerKey)
	// Round up to a byte boundary; require at least 64 bits to bound the
	// false-positive rate for very small filters.
	if numBits < 64 {
		numBits = 64
	}
	numBytes := (numBits + 7) / 8
	numBits = numBytes * 8

	out := make([]byte, numBytes+2)
	bits := out[:numBytes]

	for i := 0; i < n; i++ {
		h1 := b.hashes[i*2]
		h2 := b.hashes[i*2+1]
		for j := uint8(0); j < b.numHashes; j++ {
			bit := (h1 + uint32(j)*h2) % numBits
			bits[bit/8] |= 1 << (bit % 8)
		}
	}
	out[numBytes] = b.numHashes
	out[numBytes+1] = byte(b.bitsPerKey)
	return out
}

// Reset returns the builder to a freshly-constructed state, retaining its
// allocated buffers for reuse.
func (b *Builder) Reset() {
	b.hashes = b.hashes[:0]
}

// hashes computes a pair (h1, h2) of 32-bit hashes of key suitable for the
// Kirsch-Mitzenmacher double-hashing scheme.
//
// We use FNV-1a 64-bit and split the result into the high and low halves;
// FNV-1a is simple, branch-light, and adequate for set membership where the
// independence requirement is weak.
func hashes(key []byte) (h1, h2 uint32) {
	const (
		offset64 uint64 = 14695981039346656037
		prime64  uint64 = 1099511628211
	)
	h := offset64
	for _, b := range key {
		h ^= uint64(b)
		h *= prime64
	}
	h1 = uint32(h)
	h2 = uint32(h >> 32)
	if h2 == 0 {
		// Avoid degenerate case where every probe collides on h1.
		h2 = 1
	}
	return h1, h2
}

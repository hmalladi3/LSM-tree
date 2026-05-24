// Package keys defines slate's internal key format and entry types.
//
// Every record in the engine is keyed by an "internal key": the user-supplied
// bytes concatenated with an 8-byte sequence number and a 1-byte type tag.
// The sequence number is stored bit-inverted so that bytes.Compare alone
// produces the right ordering: user keys ascending, and within a user key
// the highest sequence (latest version) sorts first.
//
// This is the LevelDB convention. Encoding once here lets every consumer
// (memtable, SSTable, iterator) compare keys with a single bytes.Compare
// call and no MVCC-aware comparator.
package keys

import (
	"encoding/binary"
	"fmt"
)

// Kind classifies an internal key's payload.
type Kind uint8

const (
	// KindInvalid (0x00) marks a kind that did not parse from a valid
	// internal key. It is also the wire value used for lookup keys
	// (see KindLookup) — lookup keys never appear in real records so
	// the overload is safe.
	KindInvalid       Kind = 0x00
	KindInlineValue   Kind = 0x01
	KindVlogPointer   Kind = 0x02
	KindDeletion      Kind = 0x03
	KindRangeDeletion Kind = 0x04
)

// KindLookup is the kind tag carried by lookup keys constructed via
// LookupKey. Because the trailing kind byte sorts last after the
// bit-inverted sequence number, a lookup key with kind=0x00 sorts strictly
// before every real entry at the same (user, seq); SeekGE then lands on
// the first real entry whose seq is at most the snapshot — the latest
// visible version. Same wire value as KindInvalid.
const KindLookup = KindInvalid

func (k Kind) String() string {
	switch k {
	case KindLookup:
		return "lookup"
	case KindInlineValue:
		return "inline"
	case KindVlogPointer:
		return "vlogptr"
	case KindDeletion:
		return "delete"
	case KindRangeDeletion:
		return "rangedel"
	default:
		return fmt.Sprintf("kind(%d)", k)
	}
}

// MaxSeq is the largest valid sequence number. Reserved values above it (the
// inverted form, which collides with byte 0xff..0xff) are not assigned to user
// transactions.
const MaxSeq uint64 = (1 << 56) - 1

// SuffixLen is the fixed-size suffix every internal key carries:
// 7 bytes of inverted sequence + 1 byte of kind = 8 bytes.
//
// We pack seq into 7 bytes (giving 56 bits of sequence number — enough for
// 10^14 transactions, ~100 years at 1M txns/s) and reserve the high byte for
// the kind tag, so the 8-byte trailer is one machine word on 64-bit CPUs.
const SuffixLen = 8

// Encode appends the internal-key encoding of (user, seq, kind) to dst and
// returns the extended slice.
//
// Layout: dst || user || invSeq[7] || kind[1]
func Encode(dst, user []byte, seq uint64, kind Kind) []byte {
	if seq > MaxSeq {
		panic(fmt.Sprintf("keys: seq %d exceeds MaxSeq %d", seq, MaxSeq))
	}
	dst = append(dst, user...)
	var trailer [SuffixLen]byte
	inv := ^seq & MaxSeq
	binary.BigEndian.PutUint64(trailer[:], inv<<8|uint64(kind))
	dst = append(dst, trailer[:]...)
	return dst
}

// Parse splits an internal key into (user, seq, kind, ok). ok is false if the
// key is shorter than SuffixLen.
func Parse(ikey []byte) (user []byte, seq uint64, kind Kind, ok bool) {
	if len(ikey) < SuffixLen {
		return nil, 0, KindInvalid, false
	}
	user = ikey[:len(ikey)-SuffixLen]
	trailer := binary.BigEndian.Uint64(ikey[len(ikey)-SuffixLen:])
	kind = Kind(trailer & 0xff)
	inv := (trailer >> 8) & MaxSeq
	seq = ^inv & MaxSeq
	return user, seq, kind, true
}

// UserKey returns the user-key prefix of an internal key without allocating.
// Panics if ikey is shorter than SuffixLen.
func UserKey(ikey []byte) []byte {
	if len(ikey) < SuffixLen {
		panic("keys: short internal key")
	}
	return ikey[:len(ikey)-SuffixLen]
}

// Seq returns the sequence number embedded in ikey.
// Panics if ikey is shorter than SuffixLen.
func Seq(ikey []byte) uint64 {
	if len(ikey) < SuffixLen {
		panic("keys: short internal key")
	}
	trailer := binary.BigEndian.Uint64(ikey[len(ikey)-SuffixLen:])
	inv := (trailer >> 8) & MaxSeq
	return ^inv & MaxSeq
}

// KindOf returns the kind byte of ikey.
// Panics if ikey is shorter than SuffixLen.
func KindOf(ikey []byte) Kind {
	if len(ikey) < SuffixLen {
		panic("keys: short internal key")
	}
	return Kind(ikey[len(ikey)-1])
}

// Compare orders internal keys: user key ascending, then trailer ascending
// (which, because the seq is bit-inverted, means descending seq within a
// user key — the latest visible version sorts first).
//
// This MUST be used instead of bytes.Compare on raw internal keys: a plain
// byte-compare gets the wrong answer when one user key is a prefix of
// another, because the comparator would treat the trailing-seq byte of the
// shorter key as if it were part of its user-key prefix.
//
// Both arguments must be at least SuffixLen bytes long.
func Compare(a, b []byte) int {
	if len(a) < SuffixLen || len(b) < SuffixLen {
		panic("keys: Compare called on short key")
	}
	userA := a[:len(a)-SuffixLen]
	userB := b[:len(b)-SuffixLen]
	if c := bytesCompare(userA, userB); c != 0 {
		return c
	}
	return bytesCompare(a[len(a)-SuffixLen:], b[len(b)-SuffixLen:])
}

// bytesCompare is a tiny re-implementation to avoid importing bytes from
// this very-low-level package.
func bytesCompare(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}

// LookupKey constructs an internal key for searching at (user, snapshotSeq).
// It uses KindLookup so that SeekGE on the returned key lands on the first
// real entry whose seq is at most snapshotSeq — the latest visible version
// of user at that snapshot. If no such entry exists, SeekGE returns the next
// user key (or nothing).
func LookupKey(dst, user []byte, snapshotSeq uint64) []byte {
	return Encode(dst, user, snapshotSeq, KindLookup)
}

// Package record defines slate's WAL record format.
//
// Each WAL record carries a single committed operation. The record format is
// stable across versions; format extensions add new op codes without
// changing the framing.
//
//	[seq: u64 LE][kind: u8][key_len: u32 LE][key: bytes]
//	  [value_len: u32 LE][value: bytes]    (for kind = inline)
//	  (no value bytes)                     (for kind = deletion)
//
// Range tombstones, vlog pointers, and multi-op batches will extend this
// format in later releases; this package owns the on-the-wire definition.
package record

import (
	"encoding/binary"
	"errors"

	"github.com/harimalladi/slate/internal/keys"
)

// ErrTruncated is returned when a record is shorter than its declared size.
var ErrTruncated = errors.New("record: truncated")

// HeaderSize is the byte cost of the per-record fixed prefix:
// 8 (seq) + 1 (kind) + 4 (key_len) = 13 bytes; the value_len field is
// added per-kind below.
const HeaderSize = 13

// Encode writes (seq, kind, key, value) to dst as a wire record and returns
// the extended slice. Value bytes are omitted when kind == KindDeletion.
//
// Pre-condition: key fits in u32; value fits in u32.
func Encode(dst []byte, seq uint64, kind keys.Kind, key, value []byte) []byte {
	var hdr [HeaderSize]byte
	binary.LittleEndian.PutUint64(hdr[0:8], seq)
	hdr[8] = byte(kind)
	binary.LittleEndian.PutUint32(hdr[9:13], uint32(len(key)))
	dst = append(dst, hdr[:]...)
	dst = append(dst, key...)
	if kind != keys.KindDeletion {
		var vlen [4]byte
		binary.LittleEndian.PutUint32(vlen[:], uint32(len(value)))
		dst = append(dst, vlen[:]...)
		dst = append(dst, value...)
	}
	return dst
}

// Op is a single operation inside a WriteBatch.
type Op struct {
	Kind  keys.Kind
	Key   []byte
	Value []byte // for KindDeletion the slice is empty
}

// BatchMagic is the first byte of a batched WAL record (one logical
// transaction containing N ops). Distinct from any Kind byte so legacy
// per-op records can still be decoded by Decode.
const BatchMagic byte = 0xFE

// EncodeBatch writes a multi-op batch as a single WAL record:
//
//	[BatchMagic: u8][seq: u64 LE][count: u32 LE]
//	for each op:
//	  [kind: u8][key_len: u32 LE][key]
//	  if kind != KindDeletion: [value_len: u32 LE][value]
//
// All ops share the transaction's commit_ts. Recovery either accepts the
// entire batch (CRC valid) or discards it (CRC invalid) — no partial
// in-the-middle replay is possible.
func EncodeBatch(dst []byte, seq uint64, ops []Op) []byte {
	var hdr [1 + 8 + 4]byte
	hdr[0] = BatchMagic
	binary.LittleEndian.PutUint64(hdr[1:9], seq)
	binary.LittleEndian.PutUint32(hdr[9:13], uint32(len(ops)))
	dst = append(dst, hdr[:]...)
	for _, op := range ops {
		var oh [5]byte
		oh[0] = byte(op.Kind)
		binary.LittleEndian.PutUint32(oh[1:5], uint32(len(op.Key)))
		dst = append(dst, oh[:]...)
		dst = append(dst, op.Key...)
		if op.Kind != keys.KindDeletion {
			var vlen [4]byte
			binary.LittleEndian.PutUint32(vlen[:], uint32(len(op.Value)))
			dst = append(dst, vlen[:]...)
			dst = append(dst, op.Value...)
		}
	}
	return dst
}

// DecodeBatch parses a batch record. If b is a legacy single-op record
// (does not begin with BatchMagic), DecodeBatch wraps it into a one-op
// batch using Decode for backward compatibility with WALs written by
// earlier slate versions.
func DecodeBatch(b []byte) (seq uint64, ops []Op, err error) {
	if len(b) == 0 {
		return 0, nil, ErrTruncated
	}
	if b[0] != BatchMagic {
		// Legacy single-op record — wrap.
		s, kind, key, value, _, derr := Decode(b)
		if derr != nil {
			return 0, nil, derr
		}
		return s, []Op{{Kind: kind, Key: key, Value: value}}, nil
	}
	if len(b) < 1+8+4 {
		return 0, nil, ErrTruncated
	}
	seq = binary.LittleEndian.Uint64(b[1:9])
	count := binary.LittleEndian.Uint32(b[9:13])
	pos := 13
	ops = make([]Op, 0, count)
	for i := uint32(0); i < count; i++ {
		if pos+5 > len(b) {
			return 0, nil, ErrTruncated
		}
		kind := keys.Kind(b[pos])
		keyLen := binary.LittleEndian.Uint32(b[pos+1 : pos+5])
		pos += 5
		if pos+int(keyLen) > len(b) {
			return 0, nil, ErrTruncated
		}
		key := b[pos : pos+int(keyLen)]
		pos += int(keyLen)
		var value []byte
		if kind != keys.KindDeletion {
			if pos+4 > len(b) {
				return 0, nil, ErrTruncated
			}
			valLen := binary.LittleEndian.Uint32(b[pos : pos+4])
			pos += 4
			if pos+int(valLen) > len(b) {
				return 0, nil, ErrTruncated
			}
			value = b[pos : pos+int(valLen)]
			pos += int(valLen)
		}
		ops = append(ops, Op{Kind: kind, Key: key, Value: value})
	}
	return seq, ops, nil
}

// Decode parses a single legacy (pre-batch) record from b, returning
// (seq, kind, key, value) and the number of bytes consumed.
func Decode(b []byte) (seq uint64, kind keys.Kind, key, value []byte, n int, err error) {
	if len(b) < HeaderSize {
		return 0, 0, nil, nil, 0, ErrTruncated
	}
	seq = binary.LittleEndian.Uint64(b[0:8])
	kind = keys.Kind(b[8])
	keyLen := binary.LittleEndian.Uint32(b[9:13])
	off := HeaderSize
	if off+int(keyLen) > len(b) {
		return 0, 0, nil, nil, 0, ErrTruncated
	}
	key = b[off : off+int(keyLen)]
	off += int(keyLen)
	if kind != keys.KindDeletion {
		if off+4 > len(b) {
			return 0, 0, nil, nil, 0, ErrTruncated
		}
		valLen := binary.LittleEndian.Uint32(b[off : off+4])
		off += 4
		if off+int(valLen) > len(b) {
			return 0, 0, nil, nil, 0, ErrTruncated
		}
		value = b[off : off+int(valLen)]
		off += int(valLen)
	}
	return seq, kind, key, value, off, nil
}

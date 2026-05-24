// Package sstable implements slate's on-disk sorted-string table format.
//
// An SSTable is an immutable, sorted, indexed file of (internal key, value)
// entries produced by a memtable flush or a compaction. It supports two
// operations: point Get by user key + snapshot sequence, and forward
// iteration in key order.
//
// File layout:
//
//	+-----------------------+
//	| data block 0          |
//	| data block 1          |
//	| ...                   |
//	+-----------------------+
//	| bloom filter block    |
//	+-----------------------+
//	| index block           |
//	+-----------------------+
//	| footer (40 bytes)     |
//	+-----------------------+
//
// Every block is followed by a u32 CRC-32C trailer over the block bytes.
//
// Data block: LevelDB-style prefix-compressed sorted entries followed by a
// restart array. Index block: one entry per data block, key = largest user
// key in that block, value = (offset, length) handle.
//
// Footer (40 bytes, read by seeking to file_size-40):
//
//	[index_offset:u64][index_length:u32]
//	[bloom_offset:u64][bloom_length:u32]
//	[format_version:u32][footer_crc:u32]
//	[magic:8 = "SLATESST"]
package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/harimalladi/slate/internal/blockcache"
	"github.com/harimalladi/slate/internal/bloom"
	"github.com/harimalladi/slate/internal/crc"
	"github.com/harimalladi/slate/internal/encryption"
	"github.com/harimalladi/slate/internal/keys"
)

const (
	// FormatV1 was the original on-disk format (no range tombstones).
	FormatV1 uint32 = 1

	// FormatV2 added the range_del block.
	FormatV2 uint32 = 2

	// CurrentFormat is the version this binary writes.
	CurrentFormat = FormatV2

	footerSize = 52

	blockTrailerSize = 4 // CRC-32C
)

var magic = []byte("SLATESST")

// ErrCorrupted is returned when on-disk data fails integrity checks.
var ErrCorrupted = errors.New("sstable: corrupted")

// ErrEmpty is returned by Writer.Finish when no entries were added.
var ErrEmpty = errors.New("sstable: empty")

// ErrUnsupportedVersion is returned by Open when the format version is
// newer than this build supports.
var ErrUnsupportedVersion = errors.New("sstable: unsupported format version")

// WriterOptions configures a Writer.
type WriterOptions struct {
	// BlockSize is the soft byte target per data block. The writer closes
	// the current block when adding the next entry would exceed this
	// threshold. Default 4 KiB.
	BlockSize int
	// RestartInterval controls prefix-compression resets within a block.
	// Default 16.
	RestartInterval int
	// BloomBitsPerKey controls Bloom filter sizing. Default 10.
	BloomBitsPerKey int

	// Codec, if non-nil, encrypts every on-disk block under AES-256-GCM
	// with a deterministic nonce derived from (FileNum, block_offset).
	// FileNum must then also be set so nonces are unique across files.
	Codec *encryption.Codec
	// FileNum is the file number used to construct AEAD nonces. Required
	// iff Codec is non-nil.
	FileNum uint32
	// Level is the destination level used in the AEAD's associated data.
	Level int
}

func (o *WriterOptions) defaulted() WriterOptions {
	d := WriterOptions{
		BlockSize:       4 * 1024,
		RestartInterval: 16,
		BloomBitsPerKey: 10,
	}
	if o == nil {
		return d
	}
	if o.BlockSize > 0 {
		d.BlockSize = o.BlockSize
	}
	if o.RestartInterval > 0 {
		d.RestartInterval = o.RestartInterval
	}
	if o.BloomBitsPerKey > 0 {
		d.BloomBitsPerKey = o.BloomBitsPerKey
	}
	// Encryption fields are carried through as-is.
	d.Codec = o.Codec
	d.FileNum = o.FileNum
	d.Level = o.Level
	return d
}

// Writer streams sorted entries to an io.Writer in SSTable format.
type Writer struct {
	w           io.Writer
	opts        WriterOptions
	codec       *encryption.Codec
	fileNum     uint32
	level       int
	data        *blockBuilder
	index       *blockBuilder
	bloom       *bloom.Builder
	offset      int64
	pendingIdx  bool
	lastKey     []byte
	lastUserKey []byte
	numEntries  int

	// lastBlockOffset / lastBlockLen describe the most recently finalized
	// data block; used to emit the index entry on the following Add.
	lastBlockOffset int64
	lastBlockLen    int64

	smallest    []byte
	largest     []byte
	smallestSeq uint64
	largestSeq  uint64

	// rangeTombs are buffered until Finish, then sorted and written into
	// a dedicated block whose offset is recorded in the footer.
	rangeTombs []RangeTombstone
}

// RangeTombstone is one [start, end) deletion to be written into the
// SSTable. Multiple tombstones may be added; they are sorted by start key
// on Finish and emitted as a single range_del block.
type RangeTombstone struct {
	Start []byte
	End   []byte
	Seq   uint64
}

// AddRangeTombstone records a range deletion to write at Finish time.
// May be called in any order with respect to point Add calls; tombstones
// are sorted and de-duplicated when the writer finalizes the file.
func (w *Writer) AddRangeTombstone(start, end []byte, seq uint64) {
	if len(start) == 0 || len(end) == 0 {
		return
	}
	if bytes.Compare(start, end) >= 0 {
		return
	}
	w.rangeTombs = append(w.rangeTombs, RangeTombstone{
		Start: append([]byte(nil), start...),
		End:   append([]byte(nil), end...),
		Seq:   seq,
	})
}

// NewWriter constructs a Writer that streams to w.
func NewWriter(w io.Writer, opts *WriterOptions) *Writer {
	o := opts.defaulted()
	return &Writer{
		w:       w,
		opts:    o,
		codec:   o.Codec,
		fileNum: o.FileNum,
		level:   o.Level,
		data:    newBlockBuilder(o.RestartInterval),
		index:   newBlockBuilder(1), // index blocks: every entry is a restart for fast Seek
		bloom:   bloom.NewBuilder(o.BloomBitsPerKey),
	}
}

// Add appends one (internalKey, value) pair. Keys must arrive in ascending
// internal-key order. The writer guarantees that all versions of one user
// key live in the same data block — when the block's size threshold is
// crossed but the next entry continues an in-progress user key run, the
// block stays open until the user key changes.
func (w *Writer) Add(internalKey, value []byte) error {
	user, seq, _, ok := keys.Parse(internalKey)
	if !ok {
		return fmt.Errorf("sstable: short internal key (%d bytes)", len(internalKey))
	}

	// If a prior block is pending an index entry AND the user key boundary
	// has changed, this is the moment to roll over: emit the deferred index
	// entry now (key = previous block's largest internal key).
	if w.pendingIdx {
		var handle [12]byte
		binary.LittleEndian.PutUint64(handle[0:8], uint64(w.lastBlockOffset))
		binary.LittleEndian.PutUint32(handle[8:12], uint32(w.lastBlockLen))
		w.index.add(w.lastKey, handle[:])
		w.pendingIdx = false
	}

	if w.numEntries == 0 {
		w.smallest = append([]byte(nil), internalKey...)
		w.smallestSeq = seq
		w.largestSeq = seq
	} else {
		// Check whether a block roll-over is overdue: the current block has
		// exceeded its size target AND the incoming user key differs from
		// the last-added one (so we are at a clean user-key boundary).
		if w.data.sizeEstimate() >= w.opts.BlockSize && !bytes.Equal(user, w.lastUserKey) {
			if err := w.finishBlock(); err != nil {
				return err
			}
			// Emit the just-deferred index entry immediately so the next
			// add doesn't re-trigger this path. Use the last entry's key.
			var handle [12]byte
			binary.LittleEndian.PutUint64(handle[0:8], uint64(w.lastBlockOffset))
			binary.LittleEndian.PutUint32(handle[8:12], uint32(w.lastBlockLen))
			w.index.add(w.lastKey, handle[:])
			w.pendingIdx = false
		}
		if seq < w.smallestSeq {
			w.smallestSeq = seq
		}
		if seq > w.largestSeq {
			w.largestSeq = seq
		}
	}

	// Add the bloom-filter entry for the user key, deduplicating against
	// the previous user key (multiple seqs of one user key produce one
	// bloom membership).
	if !bytes.Equal(user, w.lastUserKey) {
		w.bloom.Add(user)
		w.lastUserKey = append(w.lastUserKey[:0], user...)
	}

	w.data.add(internalKey, value)
	w.lastKey = append(w.lastKey[:0], internalKey...)
	w.largest = append(w.largest[:0], internalKey...)
	w.numEntries++
	return nil
}

// NumEntries returns how many entries have been Added so far.
func (w *Writer) NumEntries() int { return w.numEntries }

// Offset returns the number of bytes written to the underlying writer so
// far. Compaction uses this to decide when to roll over to a new output.
func (w *Writer) Offset() int64 { return w.offset }

// Finish writes the bloom filter, the index block, and the footer; then
// returns the metadata describing the resulting SSTable.
func (w *Writer) Finish() (Metadata, error) {
	if w.numEntries == 0 {
		return Metadata{}, ErrEmpty
	}
	// Flush the in-progress data block.
	if w.data.numEntries() > 0 {
		if err := w.finishBlock(); err != nil {
			return Metadata{}, err
		}
	}
	if w.pendingIdx {
		var handle [12]byte
		binary.LittleEndian.PutUint64(handle[0:8], uint64(w.lastBlockOffset))
		binary.LittleEndian.PutUint32(handle[8:12], uint32(w.lastBlockLen))
		w.index.add(w.lastKey, handle[:])
		w.pendingIdx = false
	}

	// Bloom block.
	bloomBuf := w.bloom.Bytes()
	bloomOffset := w.offset
	if err := w.writeBlock(bloomBuf); err != nil {
		return Metadata{}, err
	}
	bloomLen := w.offset - bloomOffset

	// Range-del block. Encoded as:
	//   [count:u32]
	//   for each: [start_len:u32][start_bytes][end_len:u32][end_bytes][seq:u64]
	// Tombstones are sorted by (start, seq desc).
	rangeDelOffset := w.offset
	var rangeDelLen int64
	if len(w.rangeTombs) > 0 {
		sortRangeTombs(w.rangeTombs)
		buf := encodeRangeDelBlock(w.rangeTombs)
		if err := w.writeBlock(buf); err != nil {
			return Metadata{}, err
		}
		rangeDelLen = w.offset - rangeDelOffset
	} else {
		// Mark "no range tombstones" with offset==length==0.
		rangeDelOffset = 0
	}

	// Index block.
	indexBuf := w.index.finish()
	indexOffset := w.offset
	if err := w.writeBlock(indexBuf); err != nil {
		return Metadata{}, err
	}
	indexLen := w.offset - indexOffset

	// Footer (52 bytes).
	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:8], uint64(indexOffset))
	binary.LittleEndian.PutUint32(footer[8:12], uint32(indexLen))
	binary.LittleEndian.PutUint64(footer[12:20], uint64(bloomOffset))
	binary.LittleEndian.PutUint32(footer[20:24], uint32(bloomLen))
	binary.LittleEndian.PutUint64(footer[24:32], uint64(rangeDelOffset))
	binary.LittleEndian.PutUint32(footer[32:36], uint32(rangeDelLen))
	binary.LittleEndian.PutUint32(footer[36:40], CurrentFormat)
	// CRC over bytes 0..40.
	binary.LittleEndian.PutUint32(footer[40:44], crc.Compute(footer[0:40]))
	copy(footer[44:52], magic)
	if _, err := w.w.Write(footer[:]); err != nil {
		return Metadata{}, err
	}
	w.offset += footerSize

	// Take user-key smallest / largest (strip trailer for the meta).
	userSmallest, _, _, _ := keys.Parse(w.smallest)
	userLargest, _, _, _ := keys.Parse(w.largest)
	return Metadata{
		Size:        w.offset,
		Smallest:    append([]byte(nil), userSmallest...),
		Largest:     append([]byte(nil), userLargest...),
		SmallestSeq: w.smallestSeq,
		LargestSeq:  w.largestSeq,
		NumEntries:  w.numEntries,
	}, nil
}

// finishBlock emits the current data block (with CRC trailer), records the
// index handle, and resets for the next block.
func (w *Writer) finishBlock() error {
	if w.data.empty() {
		return nil
	}
	blockBytes := w.data.finish()
	off := w.offset
	if err := w.writeBlock(blockBytes); err != nil {
		return err
	}
	w.lastBlockOffset = off
	w.lastBlockLen = w.offset - off
	w.pendingIdx = true
	w.data = newBlockBuilder(w.opts.RestartInterval)
	return nil
}

// writeBlock writes a block to disk. Without encryption, the layout is
// `payload || crc32c(payload)`. With encryption, the same plaintext frame
// is wrapped: `nonce || gcm_tag || ciphertext(payload || crc32c(payload))`.
//
// The CRC is computed over the plaintext before encryption; the GCM tag
// covers the ciphertext. Together they provide two layers: GCM catches
// adversarial tampering, CRC catches post-decrypt logic errors and
// non-malicious storage corruption.
func (w *Writer) writeBlock(payload []byte) error {
	// Build the plaintext frame: payload || crc32c trailer.
	plain := make([]byte, len(payload)+4)
	copy(plain, payload)
	binary.LittleEndian.PutUint32(plain[len(payload):], crc.Compute(payload))

	if w.codec == nil {
		if _, err := w.w.Write(plain); err != nil {
			return err
		}
		w.offset += int64(len(plain))
		return nil
	}

	// Encrypted path. Nonce is deterministic from (file_num, current offset).
	ct := w.codec.Seal(plain, w.fileNum, w.level, uint64(w.offset))
	if _, err := w.w.Write(ct); err != nil {
		return err
	}
	w.offset += int64(len(ct))
	return nil
}

// Metadata summarizes a finished SSTable.
type Metadata struct {
	Size        int64
	Smallest    []byte
	Largest     []byte
	SmallestSeq uint64
	LargestSeq  uint64
	NumEntries  int
}

// ----- Reader -----

// Reader is an open SSTable file. It is safe for concurrent use.
type Reader struct {
	f           *os.File
	fileNum     uint32
	level       int
	size        int64
	indexBytes  []byte
	bloomFilter *bloom.Filter
	cache       *blockcache.Cache
	codec       *encryption.Codec

	// rangeTombs are loaded eagerly at Open. Each entry is a [start, end)
	// deletion at the given seq. Sorted by start.
	rangeTombs []RangeTombstone
}

// RangeTombstones returns the range tombstones stored in this SST. The
// returned slice is owned by the reader; callers must not mutate it.
func (r *Reader) RangeTombstones() []RangeTombstone { return r.rangeTombs }

// CoveredByTombstone reports whether userKey is covered by any tombstone
// in this SST with seq in (entrySeq, snapshotSeq].
func (r *Reader) CoveredByTombstone(userKey []byte, entrySeq, snapshotSeq uint64) bool {
	for _, t := range r.rangeTombs {
		if bytes.Compare(t.Start, userKey) > 0 {
			break // sorted by start; no later tomb can cover
		}
		if bytes.Compare(userKey, t.End) >= 0 {
			continue
		}
		if t.Seq > entrySeq && t.Seq <= snapshotSeq {
			return true
		}
	}
	return false
}

// ReaderOptions configures Open. Codec, FileNum, and Level must be supplied
// iff the file was written with encryption enabled.
type ReaderOptions struct {
	Codec   *encryption.Codec
	FileNum uint32
	Level   int
}

// SetCache attaches a block cache to this reader. All subsequent data-block
// reads consult the cache first. Passing nil disables caching.
//
// Index and bloom blocks are loaded once at Open and held resident on the
// reader; they do NOT consume cache capacity.
func (r *Reader) SetCache(c *blockcache.Cache, fileNum uint32) {
	r.cache = c
	r.fileNum = fileNum
}

// Open opens the SSTable at path. For unencrypted files, opts may be nil.
// For encrypted files, opts.Codec, opts.FileNum, and opts.Level must match
// the values used at write time.
func Open(path string, opts *ReaderOptions) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if fi.Size() < footerSize {
		f.Close()
		return nil, fmt.Errorf("%w: file too short (%d bytes)", ErrCorrupted, fi.Size())
	}
	var footer [footerSize]byte
	if _, err := f.ReadAt(footer[:], fi.Size()-footerSize); err != nil {
		f.Close()
		return nil, err
	}
	if !bytes.Equal(footer[44:52], magic) {
		f.Close()
		return nil, fmt.Errorf("%w: bad magic", ErrCorrupted)
	}
	ver := binary.LittleEndian.Uint32(footer[36:40])
	if ver != FormatV2 {
		f.Close()
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedVersion, ver)
	}
	want := binary.LittleEndian.Uint32(footer[40:44])
	if crc.Compute(footer[0:40]) != want {
		f.Close()
		return nil, fmt.Errorf("%w: footer CRC mismatch", ErrCorrupted)
	}

	indexOff := int64(binary.LittleEndian.Uint64(footer[0:8]))
	indexLen := int64(binary.LittleEndian.Uint32(footer[8:12]))
	bloomOff := int64(binary.LittleEndian.Uint64(footer[12:20]))
	bloomLen := int64(binary.LittleEndian.Uint32(footer[20:24]))
	rangeDelOff := int64(binary.LittleEndian.Uint64(footer[24:32]))
	rangeDelLen := int64(binary.LittleEndian.Uint32(footer[32:36]))

	var codec *encryption.Codec
	var fileNum uint32
	var level int
	if opts != nil {
		codec = opts.Codec
		fileNum = opts.FileNum
		level = opts.Level
	}
	indexBytes, err := readEncodedBlock(f, indexOff, indexLen, codec, fileNum, level)
	if err != nil {
		f.Close()
		return nil, err
	}
	bloomBytes, err := readEncodedBlock(f, bloomOff, bloomLen, codec, fileNum, level)
	if err != nil {
		f.Close()
		return nil, err
	}
	var rangeTombs []RangeTombstone
	if rangeDelOff > 0 && rangeDelLen > 0 {
		rangeDelBytes, err := readEncodedBlock(f, rangeDelOff, rangeDelLen, codec, fileNum, level)
		if err != nil {
			f.Close()
			return nil, err
		}
		rangeTombs, err = decodeRangeDelBlock(rangeDelBytes)
		if err != nil {
			f.Close()
			return nil, err
		}
	}
	return &Reader{
		f:           f,
		fileNum:     fileNum,
		level:       level,
		size:        fi.Size(),
		indexBytes:  indexBytes,
		bloomFilter: bloom.NewFilter(bloomBytes),
		codec:       codec,
		rangeTombs:  rangeTombs,
	}, nil
}

// Close releases the underlying file handle.
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

// Get returns the value bytes and kind for the latest version of userKey at
// or below snapshotSeq. Returns (nil, 0, false, nil) if the key is absent.
//
// Range tombstones in this SST are consulted: if a tombstone with seq in
// (entry_seq, snapshot_seq] covers userKey, the entry is shadowed and the
// result is (nil, KindDeletion, false, nil) — same as a point tombstone.
func (r *Reader) Get(userKey []byte, snapshotSeq uint64) (value []byte, kind keys.Kind, ok bool, err error) {
	if r.bloomFilter != nil && !r.bloomFilter.Contains(userKey) {
		// Bloom filter says no point entry. But a range tombstone in this
		// SST may still apply (the bloom is over user keys with point
		// entries only).
		if r.CoveredByTombstone(userKey, 0, snapshotSeq) {
			return nil, keys.KindDeletion, false, nil
		}
		return nil, 0, false, nil
	}
	// Find the data block whose largest user key >= userKey.
	idxIter, err := newBlockIter(r.indexBytes)
	if err != nil {
		return nil, 0, false, err
	}
	idxIter.seekGE(keys.LookupKey(nil, userKey, keys.MaxSeq))
	for ; idxIter.valid_(); idxIter.next() {
		// Each index entry's key is the largest internal key of a block;
		// its value is the 12-byte block handle.
		handle := idxIter.value()
		if len(handle) != 12 {
			return nil, 0, false, fmt.Errorf("%w: index handle size %d", ErrCorrupted, len(handle))
		}
		off := int64(binary.LittleEndian.Uint64(handle[0:8]))
		length := int64(binary.LittleEndian.Uint32(handle[8:12]))
		blockBytes, berr := r.loadBlock(off, length)
		if berr != nil {
			return nil, 0, false, berr
		}
		it, berr := newBlockIter(blockBytes)
		if berr != nil {
			return nil, 0, false, berr
		}
		lookup := keys.LookupKey(nil, userKey, snapshotSeq)
		it.seekGE(lookup)
		for ; it.valid_(); it.next() {
			gotUser, gotSeq, gotKind, parseOK := keys.Parse(it.key())
			if !parseOK {
				return nil, 0, false, ErrCorrupted
			}
			if !bytes.Equal(gotUser, userKey) {
				// Walked past target user key — definitely absent in this block.
				return nil, 0, false, nil
			}
			if gotSeq > snapshotSeq {
				// Future-snapshot entry; the next entry should be older.
				continue
			}
			// Found the latest visible version. If a range tombstone with
			// a newer seq covers the user key, the entry is shadowed.
			if r.CoveredByTombstone(userKey, gotSeq, snapshotSeq) {
				return nil, keys.KindDeletion, false, nil
			}
			if gotKind == keys.KindDeletion {
				return nil, gotKind, false, nil
			}
			return append([]byte(nil), it.value()...), gotKind, true, nil
		}
		// Reached end of block without finding the key; subsequent blocks
		// have larger user keys (by index ordering), so absent here.
		// Range tomb may still cover it.
		if r.CoveredByTombstone(userKey, 0, snapshotSeq) {
			return nil, keys.KindDeletion, false, nil
		}
		return nil, 0, false, nil
	}
	// Past end of index.
	return nil, 0, false, nil
}

// NewIterator returns a forward iterator visiting every entry in the file
// in ascending internal-key order.
func (r *Reader) NewIterator() *Iterator {
	idxIter, err := newBlockIter(r.indexBytes)
	if err != nil {
		return &Iterator{err: err}
	}
	return &Iterator{r: r, idx: idxIter}
}

// Iterator walks an SSTable in ascending internal-key order.
type Iterator struct {
	r       *Reader
	idx     *blockIter
	curr    *blockIter
	err     error
	started bool
}

// First positions the iterator at the smallest internal key.
func (it *Iterator) First() {
	it.idx.first()
	it.curr = nil
	it.started = true
	it.advanceBlock()
}

// SeekGE positions at the smallest internal key >= target.
func (it *Iterator) SeekGE(target []byte) {
	it.idx.seekGE(target)
	it.curr = nil
	it.started = true
	if !it.idx.valid_() {
		return
	}
	if err := it.loadCurrentBlock(); err != nil {
		it.err = err
		return
	}
	it.curr.seekGE(target)
	if !it.curr.valid_() {
		// Block exhausted; advance to next block.
		it.idx.next()
		it.advanceBlock()
	}
}

// Next advances to the next entry.
func (it *Iterator) Next() {
	if it.curr == nil {
		return
	}
	it.curr.next()
	if !it.curr.valid_() {
		it.idx.next()
		it.advanceBlock()
	}
}

// Valid reports whether the iterator is positioned on a key.
func (it *Iterator) Valid() bool {
	return it.err == nil && it.curr != nil && it.curr.valid_()
}

// Key returns the current internal key.
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.curr.key()
}

// Value returns the current value bytes.
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.curr.value()
}

// Error returns any sticky error.
func (it *Iterator) Error() error { return it.err }

// Close releases iterator resources.
func (it *Iterator) Close() error { return nil }

func (it *Iterator) advanceBlock() {
	for it.idx.valid_() {
		if err := it.loadCurrentBlock(); err != nil {
			it.err = err
			it.curr = nil
			return
		}
		it.curr.first()
		if it.curr.valid_() {
			return
		}
		it.idx.next()
	}
	it.curr = nil
}

func (it *Iterator) loadCurrentBlock() error {
	handle := it.idx.value()
	if len(handle) != 12 {
		return fmt.Errorf("%w: index handle size %d", ErrCorrupted, len(handle))
	}
	off := int64(binary.LittleEndian.Uint64(handle[0:8]))
	length := int64(binary.LittleEndian.Uint32(handle[8:12]))
	blockBytes, err := it.r.loadBlock(off, length)
	if err != nil {
		return err
	}
	bi, err := newBlockIter(blockBytes)
	if err != nil {
		return err
	}
	it.curr = bi
	return nil
}

// loadBlock returns a data block's bytes, consulting the cache if one is
// attached. Cache misses fall through to the disk path; results are
// inserted into the cache (post-decryption / post-decompression) for next
// time.
func (r *Reader) loadBlock(offset, length int64) ([]byte, error) {
	if r.cache != nil {
		key := blockcache.Key{FileNum: r.fileNum, Offset: uint64(offset)}
		if cached, ok := r.cache.Get(key); ok {
			return cached, nil
		}
		plain, err := readEncodedBlock(r.f, offset, length, r.codec, r.fileNum, r.level)
		if err != nil {
			return nil, err
		}
		// Insert a fresh copy into the cache (the cache takes ownership).
		cp := make([]byte, len(plain))
		copy(cp, plain)
		r.cache.Put(key, cp)
		return plain, nil
	}
	return readEncodedBlock(r.f, offset, length, r.codec, r.fileNum, r.level)
}

// sortRangeTombs orders tombstones by start key ascending, breaking ties
// by seq descending (newest seq first — so we test the latest applicable
// tombstone earliest in CoveredByTombstone).
func sortRangeTombs(rts []RangeTombstone) {
	sort.Slice(rts, func(i, j int) bool {
		if c := bytes.Compare(rts[i].Start, rts[j].Start); c != 0 {
			return c < 0
		}
		return rts[i].Seq > rts[j].Seq
	})
}

func encodeRangeDelBlock(rts []RangeTombstone) []byte {
	buf := make([]byte, 4, 4+len(rts)*32)
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(rts)))
	for _, t := range rts {
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(t.Start)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, t.Start...)
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(t.End)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, t.End...)
		var seqBuf [8]byte
		binary.LittleEndian.PutUint64(seqBuf[:], t.Seq)
		buf = append(buf, seqBuf[:]...)
	}
	return buf
}

func decodeRangeDelBlock(b []byte) ([]RangeTombstone, error) {
	if len(b) < 4 {
		return nil, fmt.Errorf("%w: range-del block too short", ErrCorrupted)
	}
	n := binary.LittleEndian.Uint32(b[0:4])
	out := make([]RangeTombstone, 0, n)
	pos := 4
	for i := uint32(0); i < n; i++ {
		if pos+4 > len(b) {
			return nil, fmt.Errorf("%w: range-del truncated", ErrCorrupted)
		}
		startLen := binary.LittleEndian.Uint32(b[pos : pos+4])
		pos += 4
		if pos+int(startLen) > len(b) {
			return nil, fmt.Errorf("%w: range-del start truncated", ErrCorrupted)
		}
		start := append([]byte(nil), b[pos:pos+int(startLen)]...)
		pos += int(startLen)
		if pos+4 > len(b) {
			return nil, fmt.Errorf("%w: range-del truncated", ErrCorrupted)
		}
		endLen := binary.LittleEndian.Uint32(b[pos : pos+4])
		pos += 4
		if pos+int(endLen) > len(b) {
			return nil, fmt.Errorf("%w: range-del end truncated", ErrCorrupted)
		}
		end := append([]byte(nil), b[pos:pos+int(endLen)]...)
		pos += int(endLen)
		if pos+8 > len(b) {
			return nil, fmt.Errorf("%w: range-del seq truncated", ErrCorrupted)
		}
		seq := binary.LittleEndian.Uint64(b[pos : pos+8])
		pos += 8
		out = append(out, RangeTombstone{Start: start, End: end, Seq: seq})
	}
	return out, nil
}

// readBlock reads `length` bytes at offset and verifies the trailing CRC.
// Returns the payload (without the CRC). For unencrypted files only.
func readBlock(f *os.File, offset, length int64) ([]byte, error) {
	if length < blockTrailerSize {
		return nil, fmt.Errorf("%w: block length %d", ErrCorrupted, length)
	}
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	if int64(n) != length {
		return nil, fmt.Errorf("%w: short read %d/%d", ErrCorrupted, n, length)
	}
	payload := buf[:length-blockTrailerSize]
	expected := binary.LittleEndian.Uint32(buf[length-blockTrailerSize:])
	if crc.Compute(payload) != expected {
		return nil, fmt.Errorf("%w: block CRC mismatch at %d", ErrCorrupted, offset)
	}
	return payload, nil
}

// readEncodedBlock reads a block, decrypts if a codec is present, then
// verifies the trailing CRC. Returns the inner payload bytes (without the
// CRC trailer).
func readEncodedBlock(f *os.File, offset, length int64, codec *encryption.Codec, fileNum uint32, level int) ([]byte, error) {
	if codec == nil {
		return readBlock(f, offset, length)
	}
	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	if int64(n) != length {
		return nil, fmt.Errorf("%w: short read %d/%d", ErrCorrupted, n, length)
	}
	plain, err := codec.Open(buf, fileNum, level, uint64(offset))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupted, err)
	}
	if len(plain) < blockTrailerSize {
		return nil, fmt.Errorf("%w: post-decrypt block too short", ErrCorrupted)
	}
	payload := plain[:len(plain)-blockTrailerSize]
	expected := binary.LittleEndian.Uint32(plain[len(plain)-blockTrailerSize:])
	if crc.Compute(payload) != expected {
		return nil, fmt.Errorf("%w: block CRC mismatch at %d (post-decrypt)", ErrCorrupted, offset)
	}
	return payload, nil
}

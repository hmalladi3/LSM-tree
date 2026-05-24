// Package vlog implements slate's append-only value log. Large values are
// spilled here so that compaction (which rewrites only the keys and small
// inline values in the LSM) does not have to recopy them.
//
// On-disk layout per segment file (`<dir>/NNNNNN.vlog`):
//
//	[length: u32 LE][value bytes][crc32c: u32 LE]
//	[length: u32 LE][value bytes][crc32c: u32 LE]
//	...
//
// A Pointer (16 bytes) names a single entry: file_num + offset of the
// length prefix + length. Pointers are stored in the LSM in place of the
// raw value bytes.
//
// v0 keeps a single active segment at a time and never garbage-collects
// dead entries; the segment may grow unboundedly. A future revision will
// introduce rotation thresholds and a WiscKey-style GC pass.
package vlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/harimalladi/slate/internal/crc"
	"github.com/harimalladi/slate/internal/encryption"
)

const (
	// PointerSize is the on-disk size of a vlog pointer (file_num + offset
	// + length). Stored verbatim in the LSM and the WAL.
	PointerSize = 16

	// EntryOverhead is the byte cost of the length prefix + CRC trailer
	// around each plaintext value.
	EntryOverhead = 8

	// EncryptedOverhead is the byte cost when AEAD-wrapping the value:
	// length(4) + nonce(12) + gcm_tag(16) + ciphertext + crc(4). The
	// length field continues to record the plaintext byte count.
	EncryptedOverhead = 4 + 12 + 16 + 4
)

// ErrCorrupted is returned when a dereference finds data that fails its
// length or CRC checks.
var ErrCorrupted = errors.New("vlog: corrupted")

// Pointer identifies a single value in the vlog. The pointer is stable
// for the segment's lifetime; pointers are never rewritten without going
// through a fresh manifest edit.
type Pointer struct {
	FileNum uint32
	Offset  uint64
	Length  uint32
}

// EncodePointer serializes p as 16 bytes (file_num:4 || offset:8 ||
// length:4, little-endian).
func EncodePointer(p Pointer) [PointerSize]byte {
	var b [PointerSize]byte
	binary.LittleEndian.PutUint32(b[0:4], p.FileNum)
	binary.LittleEndian.PutUint64(b[4:12], p.Offset)
	binary.LittleEndian.PutUint32(b[12:16], p.Length)
	return b
}

// DecodePointer parses a 16-byte pointer.
func DecodePointer(b []byte) (Pointer, error) {
	if len(b) < PointerSize {
		return Pointer{}, fmt.Errorf("%w: pointer length %d", ErrCorrupted, len(b))
	}
	return Pointer{
		FileNum: binary.LittleEndian.Uint32(b[0:4]),
		Offset:  binary.LittleEndian.Uint64(b[4:12]),
		Length:  binary.LittleEndian.Uint32(b[12:16]),
	}, nil
}

// ----- Writer -----

// Writer is the append-only producer side of the vlog. Open one Writer per
// database; concurrent callers may invoke Append safely.
type Writer struct {
	dir         string
	segmentSize int64
	codec       *encryption.Codec

	mu      sync.Mutex
	file    *os.File
	fileNum uint32
	offset  int64

	// existingSegments is the set of vlog segment numbers persisted at Open;
	// useful for the engine to verify file presence on recovery.
	existingSegments []uint32
}

// NewWriter opens (or creates) the vlog directory and a fresh segment for
// appends. The next allocated file number is `nextFileNum` — supplied by
// the caller so it can be coordinated with the manifest's monotonic
// allocator (no two engine components may collide on a file number).
//
// segmentSize caps an active segment's bytes; once exceeded, the writer
// rotates to a fresh segment at the next Append.
func NewWriter(dir string, nextFileNum uint32, segmentSize int64) (*Writer, error) {
	return NewWriterWithCodec(dir, nextFileNum, segmentSize, nil)
}

// NewWriterWithCodec opens a writer that AEAD-wraps each entry's value
// bytes via the supplied codec. Pass nil for plaintext.
func NewWriterWithCodec(dir string, nextFileNum uint32, segmentSize int64, codec *encryption.Codec) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if segmentSize <= 0 {
		segmentSize = 1 << 30 // 1 GiB default
	}

	existing, err := listSegments(dir)
	if err != nil {
		return nil, err
	}

	w := &Writer{
		dir:              dir,
		segmentSize:      segmentSize,
		codec:            codec,
		fileNum:          nextFileNum,
		existingSegments: existing,
	}
	if err := w.openLocked(nextFileNum); err != nil {
		return nil, err
	}
	return w, nil
}

// ExistingSegments returns the set of segment numbers that were present in
// the directory when the writer opened. Used by the engine on recovery to
// reconcile against the manifest.
func (w *Writer) ExistingSegments() []uint32 { return w.existingSegments }

// Append writes value to the active segment and returns a pointer to it.
// Thread-safe.
//
// `allocateNext` is invoked to allocate a fresh file number whenever the
// active segment overflows segmentSize. The caller (the engine) wires this
// to the manifest's monotonic file-number allocator so the new segment's
// number is never reused.
//
// When the writer has a codec attached, the value bytes are AEAD-wrapped
// in place: the on-disk frame becomes
// `[length:u32][nonce:12B][gcm_tag:16B][ciphertext:length bytes][crc:u32]`.
// The pointer's Length field always holds the PLAINTEXT byte count, so
// callers and the LSM are unaware of the wrapper.
func (w *Writer) Append(value []byte, allocateNext func() uint32) (Pointer, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	frameLen := int64(EntryOverhead + len(value))
	if w.codec != nil {
		frameLen = int64(EncryptedOverhead + len(value))
	}
	if w.file != nil && w.offset+frameLen > w.segmentSize {
		if err := w.rotateLocked(allocateNext()); err != nil {
			return Pointer{}, err
		}
	}

	startOff := w.offset

	// Build the frame.
	frame := make([]byte, 4, frameLen)
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(value)))
	if w.codec == nil {
		frame = append(frame, value...)
	} else {
		sealed := w.codec.SealVlog(value, w.fileNum, uint64(startOff))
		frame = append(frame, sealed...)
	}
	var tail [4]byte
	binary.LittleEndian.PutUint32(tail[:], crc.Compute(value))
	frame = append(frame, tail[:]...)

	if _, err := w.file.Write(frame); err != nil {
		return Pointer{}, err
	}
	w.offset += int64(len(frame))
	return Pointer{
		FileNum: w.fileNum,
		Offset:  uint64(startOff),
		Length:  uint32(len(value)),
	}, nil
}

// Sync forces the active segment to durable storage.
func (w *Writer) Sync() error {
	w.mu.Lock()
	f := w.file
	w.mu.Unlock()
	if f == nil {
		return nil
	}
	return f.Sync()
}

// Close flushes and releases the active segment. Idempotent.
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// ActiveFileNum returns the file number of the currently-open segment.
func (w *Writer) ActiveFileNum() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.fileNum
}

func (w *Writer) openLocked(num uint32) error {
	path := segmentPath(w.dir, num)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	// Append to the end of an existing file.
	if _, err := f.Seek(fi.Size(), 0); err != nil {
		f.Close()
		return err
	}
	w.file = f
	w.fileNum = num
	w.offset = fi.Size()
	return nil
}

func (w *Writer) rotateLocked(nextNum uint32) error {
	if w.file != nil {
		if err := w.file.Sync(); err != nil {
			return err
		}
		if err := w.file.Close(); err != nil {
			return err
		}
		w.file = nil
	}
	return w.openLocked(nextNum)
}

// ----- Reader -----

// Reader services Dereference calls by opening segment files lazily and
// caching handles. Safe for concurrent use.
type Reader struct {
	dir   string
	codec *encryption.Codec

	mu       sync.RWMutex
	files    map[uint32]*os.File
	deadInfo map[uint32]*deadCounter // dead bytes per segment
}

// deadCounter tracks how many bytes in one vlog segment have been
// superseded by compaction. When dead/total approaches 1.0 the segment is
// a strong GC candidate (GC itself is a v0.2 milestone; the bookkeeping
// is wired now so we don't have to retrofit it later).
type deadCounter struct {
	bytes atomic.Int64
}

// NewReader returns a Reader rooted at dir. Files are opened lazily on
// first Dereference.
func NewReader(dir string) *Reader {
	return &Reader{
		dir:      dir,
		files:    make(map[uint32]*os.File),
		deadInfo: make(map[uint32]*deadCounter),
	}
}

// NewReaderWithCodec returns a Reader that decrypts value bytes via the
// supplied codec. Must match the codec the Writer used.
func NewReaderWithCodec(dir string, codec *encryption.Codec) *Reader {
	return &Reader{
		dir:      dir,
		codec:    codec,
		files:    make(map[uint32]*os.File),
		deadInfo: make(map[uint32]*deadCounter),
	}
}

// MarkDead records that the entry referenced by ptr is no longer reachable
// from the LSM (a newer version has shadowed it during compaction). The
// bytes consumed by the entry — payload + per-entry framing overhead —
// are added to the segment's dead-bytes counter. Safe for concurrent use.
//
// The Reader does not act on dead bytes in v0; that's the vlog GC pass,
// deferred to v0.2. The bookkeeping is what GC needs at minimum, so we
// pay it now to keep the format stable.
func (r *Reader) MarkDead(ptr Pointer) {
	overhead := int64(EntryOverhead)
	if r.codec != nil {
		overhead = int64(EncryptedOverhead)
	}
	r.mu.Lock()
	info, ok := r.deadInfo[ptr.FileNum]
	if !ok {
		info = &deadCounter{}
		r.deadInfo[ptr.FileNum] = info
	}
	r.mu.Unlock()
	info.bytes.Add(int64(ptr.Length) + overhead)
}

// DeadBytes returns the per-segment dead-byte counters as a fresh map.
// Used by db.Stats for inspection.
func (r *Reader) DeadBytes() map[uint32]int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[uint32]int64, len(r.deadInfo))
	for num, info := range r.deadInfo {
		v := info.bytes.Load()
		if v > 0 {
			out[num] = v
		}
	}
	return out
}

// Dereference reads the value bytes pointed to by ptr. If a codec is
// attached the AEAD wrapper is opened transparently.
func (r *Reader) Dereference(ptr Pointer) ([]byte, error) {
	f, err := r.fileFor(ptr.FileNum)
	if err != nil {
		return nil, err
	}

	var totalLen int64
	if r.codec == nil {
		totalLen = int64(EntryOverhead + ptr.Length)
	} else {
		totalLen = int64(EncryptedOverhead + ptr.Length)
	}
	buf := make([]byte, totalLen)
	n, err := f.ReadAt(buf, int64(ptr.Offset))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupted, err)
	}
	if int64(n) != totalLen {
		return nil, fmt.Errorf("%w: short read %d/%d", ErrCorrupted, n, totalLen)
	}
	readLen := binary.LittleEndian.Uint32(buf[0:4])
	if readLen != ptr.Length {
		return nil, fmt.Errorf("%w: length mismatch %d (ptr says %d)", ErrCorrupted, readLen, ptr.Length)
	}

	var value []byte
	if r.codec == nil {
		value = buf[4 : 4+ptr.Length]
	} else {
		// Ciphertext block sits between the length prefix and the
		// trailing CRC. Its bytes are [nonce(12)][gcm_tag(16)][ciphertext].
		cipherBlock := buf[4 : int64(len(buf))-4]
		plain, oerr := r.codec.OpenVlog(cipherBlock, ptr.FileNum, uint64(ptr.Offset))
		if oerr != nil {
			return nil, fmt.Errorf("%w: %v", ErrCorrupted, oerr)
		}
		if uint32(len(plain)) != ptr.Length {
			return nil, fmt.Errorf("%w: plaintext length %d (ptr says %d)", ErrCorrupted, len(plain), ptr.Length)
		}
		value = plain
	}

	readCRC := binary.LittleEndian.Uint32(buf[totalLen-4:])
	if crc.Compute(value) != readCRC {
		return nil, fmt.Errorf("%w: CRC mismatch", ErrCorrupted)
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out, nil
}

func (r *Reader) fileFor(num uint32) (*os.File, error) {
	r.mu.RLock()
	if f, ok := r.files[num]; ok {
		r.mu.RUnlock()
		return f, nil
	}
	r.mu.RUnlock()

	r.mu.Lock()
	defer r.mu.Unlock()
	if f, ok := r.files[num]; ok {
		return f, nil
	}
	path := segmentPath(r.dir, num)
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: opening segment %d: %v", ErrCorrupted, num, err)
	}
	r.files[num] = f
	return f, nil
}

// Close releases all cached file handles. Idempotent.
func (r *Reader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for _, f := range r.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	r.files = make(map[uint32]*os.File)
	return firstErr
}

// ----- helpers -----

func segmentPath(dir string, num uint32) string {
	return filepath.Join(dir, fmt.Sprintf("%06d.vlog", num))
}

func listSegments(dir string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var nums []uint32
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".vlog") {
			continue
		}
		var n uint32
		if _, err := fmt.Sscanf(e.Name(), "%06d.vlog", &n); err == nil {
			nums = append(nums, n)
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	return nums, nil
}

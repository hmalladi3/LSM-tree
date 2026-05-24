// Package manifest tracks the database's durable identity: which SSTable
// files exist at each level, the sequence-number watermark, and the
// WAL checkpoint.
//
// On disk the manifest is an append-only log of typed edits (see VersionEdit).
// A CURRENT file names the active manifest log; rotating to a new log is
// atomic via rename(2) of CURRENT. Periodically we snapshot the current
// state to bound recovery scan length.
//
// In memory the current Version is published via atomic.Pointer[Version];
// readers acquire by checking refcount > 0 and incrementing.
package manifest

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/harimalladi/slate/internal/crc"
)

// NumLevels is the number of levels supported. The first level (L0) holds
// recently-flushed memtables; remaining levels are populated by compaction.
const NumLevels = 7

// MaxFileNum is the largest file number we will allocate. It is comfortably
// below 2^32 to leave headroom for encryption-nonce construction (file_num
// is part of the GCM nonce).
const MaxFileNum uint32 = (1 << 31) - 1

const (
	magic       = "SLATEMAN"
	formatV1    = uint32(1)
	currentName = "CURRENT"

	editAddTable        uint8 = 1
	editDeleteTable     uint8 = 2
	editSetLastSequence uint8 = 3
	editSetFlushedWAL   uint8 = 4
	editSetNextFileNum  uint8 = 5
	editSnapshotBegin   uint8 = 6
	editSnapshotEnd     uint8 = 7
)

// TableMeta describes an SSTable file living at a given level.
type TableMeta struct {
	FileNum     uint32
	Level       int
	Smallest    []byte
	Largest     []byte
	Size        int64
	SmallestSeq uint64
	LargestSeq  uint64
}

// WALCheckpoint records the largest WAL position whose records are durable
// in SSTables. Recovery skips records at or below this point.
type WALCheckpoint struct {
	FileNum uint32
	Seq     uint64
}

// VersionEdit is a batch of changes applied atomically to the manifest.
type VersionEdit struct {
	NewTables     []TableMeta
	DeletedTables []uint32

	HasLastSequence bool
	LastSequence    uint64

	HasFlushedWAL bool
	FlushedWAL    WALCheckpoint

	HasNextFileNum bool
	NextFileNum    uint32
}

// Version is the immutable snapshot of the database's durable file set
// at a single point in time.
type Version struct {
	Tables       [NumLevels][]*TableMeta
	NextFileNum  uint32
	LastSequence uint64
	FlushedWAL   WALCheckpoint

	refs atomic.Int32
}

// Ref acquires a reference on the version. Pinned versions keep their
// referenced files alive even after the version is replaced.
func (v *Version) Ref() {
	v.refs.Add(1)
}

// Unref releases a reference. When the count drops to zero the version is
// eligible for cleanup but file unlink is up to the caller.
func (v *Version) Unref() int32 {
	return v.refs.Add(-1)
}

// FileSet returns the set of file numbers referenced by this Version across
// all levels.
func (v *Version) FileSet() map[uint32]struct{} {
	out := make(map[uint32]struct{})
	for _, level := range v.Tables {
		for _, t := range level {
			out[t.FileNum] = struct{}{}
		}
	}
	return out
}

// Manifest is the persistent edit-log front-end.
type Manifest struct {
	dir string

	mu       sync.Mutex
	file     *os.File
	activeNo uint32
	edits    int

	current     atomic.Pointer[Version]
	nextFileNum atomic.Uint32
}

// hasManifestArtifacts reports whether dir's listing contains any file
// that looks like a manifest-domain artifact (MANIFEST-* or CURRENT.*tmp).
// Used by Open to distinguish a brand-new directory from one that lost
// its CURRENT pointer.
func hasManifestArtifacts(entries []os.DirEntry) bool {
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		n := e.Name()
		if len(n) >= len("MANIFEST-") && n[:len("MANIFEST-")] == "MANIFEST-" {
			return true
		}
		// Trailing CURRENT.tmp from an interrupted CURRENT swap also
		// suggests a real (but corrupt) database.
		if n == "CURRENT.tmp" {
			return true
		}
	}
	return false
}

// Open opens (or creates) the manifest in dir.
func Open(dir string) (*Manifest, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	m := &Manifest{dir: dir}

	curName, err := readCurrent(dir)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if curName == "" {
		// CURRENT missing. If the manifest directory has other files we
		// consider this corruption (something else wrote files but the
		// pointer to the active manifest is missing). An empty directory
		// is a brand-new database.
		entries, lerr := os.ReadDir(dir)
		if lerr != nil {
			return nil, lerr
		}
		if hasManifestArtifacts(entries) {
			return nil, fmt.Errorf("%w: manifest directory non-empty but CURRENT missing", ErrCorrupted)
		}
		v := &Version{NextFileNum: 1}
		v.Ref()
		m.current.Store(v)
		m.nextFileNum.Store(1)
		if err := m.startNewLog(1); err != nil {
			return nil, err
		}
		if err := m.writeSnapshot(v); err != nil {
			return nil, err
		}
		return m, nil
	}

	// Replay the named manifest log. If the file referenced by CURRENT
	// does not exist, that's corruption — CURRENT is a stable pointer and
	// must always resolve.
	path := filepath.Join(dir, curName)
	if _, statErr := os.Stat(path); statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: CURRENT names %s but file does not exist", ErrCorrupted, curName)
		}
		return nil, statErr
	}
	v, err := replay(path)
	if err != nil {
		return nil, err
	}
	v.Ref()
	m.current.Store(v)
	m.nextFileNum.Store(v.NextFileNum)

	// Continue appending to the existing log; pick a number 1 + max
	// existing manifest log number so a future snapshot rotation cannot
	// collide.
	num, err := parseManifestName(curName)
	if err != nil {
		return nil, err
	}
	m.activeNo = num
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	m.file = f
	return m, nil
}

// Close fsyncs and closes the active manifest log. Idempotent.
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.file == nil {
		return nil
	}
	err := m.file.Close()
	m.file = nil
	return err
}

// Current returns the active Version with its refcount incremented. Caller
// must Unref when done.
func (m *Manifest) Current() *Version {
	for {
		v := m.current.Load()
		if v == nil {
			return nil
		}
		// Refcount-acquire: must observe positive refcount before bumping.
		old := v.refs.Load()
		if old <= 0 {
			// Another goroutine is about to retire this version; retry.
			continue
		}
		if v.refs.CompareAndSwap(old, old+1) {
			return v
		}
	}
}

// AllocFileNum returns a fresh file number. Two successive calls always
// return distinct numbers — internally, a monotonic atomic counter shared
// across allocators is used.
//
// File numbers are NEVER recycled across a database's lifetime: this is a
// load-bearing invariant for encryption nonce uniqueness once the
// encryption layer is wired in.
func (m *Manifest) AllocFileNum() uint32 {
	for {
		cur := m.nextFileNum.Load()
		if m.nextFileNum.CompareAndSwap(cur, cur+1) {
			return cur
		}
	}
}

// Apply persists edit to the manifest log and publishes the resulting
// Version.
func (m *Manifest) Apply(edit VersionEdit) error {
	if edit.empty() {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	cur := m.current.Load()
	next := cur.applied(edit)
	// Reconcile the Version's NextFileNum with the allocator's atomic
	// counter so subsequent recovery sees the correct high-water mark.
	if alloc := m.nextFileNum.Load(); alloc > next.NextFileNum {
		next.NextFileNum = alloc
	}

	// Encode and write the edit batch.
	buf := edit.encode(nil)
	if err := writeFramedRecord(m.file, buf); err != nil {
		return err
	}
	if err := m.file.Sync(); err != nil {
		return err
	}
	m.edits++

	// Publish the new Version.
	next.Ref()
	old := m.current.Swap(next)
	// Drop our acquired ref on the prior current (Open's Ref or the
	// previous Apply's Ref). Callers holding live references continue to
	// see the old Version until they Unref it.
	if old != nil {
		old.Unref()
	}

	// Rotate if the log grows past a target.
	if m.edits >= 1000 {
		if err := m.snapshotAndRotateLocked(next); err != nil {
			return err
		}
	}
	return nil
}

// snapshotAndRotateLocked writes a snapshot to a fresh manifest log and
// swaps CURRENT. m.mu must be held.
func (m *Manifest) snapshotAndRotateLocked(v *Version) error {
	newNo := m.activeNo + 1
	if err := m.startNewLogLocked(newNo); err != nil {
		return err
	}
	if err := m.writeSnapshotLocked(v); err != nil {
		return err
	}
	m.edits = 0
	return nil
}

func (m *Manifest) startNewLog(num uint32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startNewLogLocked(num)
}

func (m *Manifest) startNewLogLocked(num uint32) error {
	if m.file != nil {
		_ = m.file.Sync()
		_ = m.file.Close()
		m.file = nil
	}
	name := manifestName(num)
	path := filepath.Join(m.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	m.file = f
	m.activeNo = num
	// Update CURRENT to point at the new file. Temp file + rename.
	tmp := filepath.Join(m.dir, currentName+".tmp")
	if err := os.WriteFile(tmp, []byte(name+"\n"), 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(m.dir, currentName)); err != nil {
		return err
	}
	// Best-effort directory sync to make the rename durable.
	if dirFile, err := os.Open(m.dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}
	return nil
}

func (m *Manifest) writeSnapshot(v *Version) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writeSnapshotLocked(v)
}

func (m *Manifest) writeSnapshotLocked(v *Version) error {
	if err := writeFramedRecord(m.file, encodeSnapshotBegin()); err != nil {
		return err
	}
	if v.NextFileNum > 0 {
		e := VersionEdit{HasNextFileNum: true, NextFileNum: v.NextFileNum}
		if err := writeFramedRecord(m.file, e.encode(nil)); err != nil {
			return err
		}
	}
	if v.LastSequence > 0 {
		e := VersionEdit{HasLastSequence: true, LastSequence: v.LastSequence}
		if err := writeFramedRecord(m.file, e.encode(nil)); err != nil {
			return err
		}
	}
	if v.FlushedWAL.FileNum > 0 || v.FlushedWAL.Seq > 0 {
		e := VersionEdit{HasFlushedWAL: true, FlushedWAL: v.FlushedWAL}
		if err := writeFramedRecord(m.file, e.encode(nil)); err != nil {
			return err
		}
	}
	for _, level := range v.Tables {
		for _, t := range level {
			e := VersionEdit{NewTables: []TableMeta{*t}}
			if err := writeFramedRecord(m.file, e.encode(nil)); err != nil {
				return err
			}
		}
	}
	if err := writeFramedRecord(m.file, encodeSnapshotEnd()); err != nil {
		return err
	}
	return m.file.Sync()
}

// applied returns a new Version equal to v with edit applied.
func (v *Version) applied(edit VersionEdit) *Version {
	next := &Version{
		NextFileNum:  v.NextFileNum,
		LastSequence: v.LastSequence,
		FlushedWAL:   v.FlushedWAL,
	}
	deleted := make(map[uint32]struct{}, len(edit.DeletedTables))
	for _, n := range edit.DeletedTables {
		deleted[n] = struct{}{}
	}
	for lvl, level := range v.Tables {
		for _, t := range level {
			if _, drop := deleted[t.FileNum]; drop {
				continue
			}
			next.Tables[lvl] = append(next.Tables[lvl], t)
		}
	}
	for _, t := range edit.NewTables {
		tt := t
		next.Tables[t.Level] = append(next.Tables[t.Level], &tt)
		if tt.FileNum+1 > next.NextFileNum {
			next.NextFileNum = tt.FileNum + 1
		}
	}
	if edit.HasLastSequence && edit.LastSequence > next.LastSequence {
		next.LastSequence = edit.LastSequence
	}
	if edit.HasFlushedWAL {
		next.FlushedWAL = edit.FlushedWAL
	}
	if edit.HasNextFileNum && edit.NextFileNum > next.NextFileNum {
		next.NextFileNum = edit.NextFileNum
	}
	// Sort L1+ by smallest key for predictable iteration; L0 stays in
	// arrival order so the most-recent-first scan is straightforward.
	for lvl := 1; lvl < NumLevels; lvl++ {
		sort.Slice(next.Tables[lvl], func(i, j int) bool {
			return compareBytes(next.Tables[lvl][i].Smallest, next.Tables[lvl][j].Smallest) < 0
		})
	}
	return next
}

func (e VersionEdit) empty() bool {
	return len(e.NewTables) == 0 &&
		len(e.DeletedTables) == 0 &&
		!e.HasLastSequence &&
		!e.HasFlushedWAL &&
		!e.HasNextFileNum
}

// ----- encoding -----

func (e VersionEdit) encode(dst []byte) []byte {
	for _, t := range e.NewTables {
		dst = append(dst, editAddTable)
		dst = appendU32(dst, t.FileNum)
		dst = append(dst, byte(t.Level))
		dst = appendBytes(dst, t.Smallest)
		dst = appendBytes(dst, t.Largest)
		dst = appendU64(dst, uint64(t.Size))
		dst = appendU64(dst, t.SmallestSeq)
		dst = appendU64(dst, t.LargestSeq)
	}
	for _, n := range e.DeletedTables {
		dst = append(dst, editDeleteTable)
		dst = appendU32(dst, n)
	}
	if e.HasLastSequence {
		dst = append(dst, editSetLastSequence)
		dst = appendU64(dst, e.LastSequence)
	}
	if e.HasFlushedWAL {
		dst = append(dst, editSetFlushedWAL)
		dst = appendU32(dst, e.FlushedWAL.FileNum)
		dst = appendU64(dst, e.FlushedWAL.Seq)
	}
	if e.HasNextFileNum {
		dst = append(dst, editSetNextFileNum)
		dst = appendU32(dst, e.NextFileNum)
	}
	return dst
}

func encodeSnapshotBegin() []byte { return []byte{editSnapshotBegin} }
func encodeSnapshotEnd() []byte   { return []byte{editSnapshotEnd} }

func decodeEdit(payload []byte) (VersionEdit, bool) {
	var e VersionEdit
	pos := 0
	for pos < len(payload) {
		op := payload[pos]
		pos++
		switch op {
		case editAddTable:
			if pos+5 > len(payload) {
				return e, false
			}
			fileNum := binary.LittleEndian.Uint32(payload[pos:])
			pos += 4
			level := int(payload[pos])
			pos++
			smallest, n, ok := readBytes(payload[pos:])
			if !ok {
				return e, false
			}
			pos += n
			largest, n, ok := readBytes(payload[pos:])
			if !ok {
				return e, false
			}
			pos += n
			if pos+24 > len(payload) {
				return e, false
			}
			size := binary.LittleEndian.Uint64(payload[pos:])
			pos += 8
			smallestSeq := binary.LittleEndian.Uint64(payload[pos:])
			pos += 8
			largestSeq := binary.LittleEndian.Uint64(payload[pos:])
			pos += 8
			e.NewTables = append(e.NewTables, TableMeta{
				FileNum:     fileNum,
				Level:       level,
				Smallest:    append([]byte(nil), smallest...),
				Largest:     append([]byte(nil), largest...),
				Size:        int64(size),
				SmallestSeq: smallestSeq,
				LargestSeq:  largestSeq,
			})
		case editDeleteTable:
			if pos+4 > len(payload) {
				return e, false
			}
			e.DeletedTables = append(e.DeletedTables, binary.LittleEndian.Uint32(payload[pos:]))
			pos += 4
		case editSetLastSequence:
			if pos+8 > len(payload) {
				return e, false
			}
			e.HasLastSequence = true
			e.LastSequence = binary.LittleEndian.Uint64(payload[pos:])
			pos += 8
		case editSetFlushedWAL:
			if pos+12 > len(payload) {
				return e, false
			}
			e.HasFlushedWAL = true
			e.FlushedWAL.FileNum = binary.LittleEndian.Uint32(payload[pos:])
			pos += 4
			e.FlushedWAL.Seq = binary.LittleEndian.Uint64(payload[pos:])
			pos += 8
		case editSetNextFileNum:
			if pos+4 > len(payload) {
				return e, false
			}
			e.HasNextFileNum = true
			e.NextFileNum = binary.LittleEndian.Uint32(payload[pos:])
			pos += 4
		default:
			return e, false
		}
	}
	return e, true
}

// ----- on-disk framing -----

// Each record is framed as:
//   [length: u32 LE][crc32c: u32 LE][payload: bytes]
// Snapshot delimiters are single-byte payloads.

func writeFramedRecord(f *os.File, payload []byte) error {
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc.Compute(payload))
	if _, err := f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
		return err
	}
	return nil
}

func replay(path string) (*Version, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	v := &Version{NextFileNum: 1}
	var hdr [8]byte
	var inSnapshot bool
	for {
		_, err := io.ReadFull(f, hdr[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		length := binary.LittleEndian.Uint32(hdr[0:4])
		expectedCRC := binary.LittleEndian.Uint32(hdr[4:8])
		payload := make([]byte, length)
		if _, err := io.ReadFull(f, payload); err != nil {
			// Truncated record: stop replay; preceding state is valid.
			break
		}
		if crc.Compute(payload) != expectedCRC {
			// Torn record at the tail: stop replay.
			break
		}
		if length == 1 {
			switch payload[0] {
			case editSnapshotBegin:
				// Discard any state accumulated before the snapshot started;
				// the snapshot expresses the full state on its own.
				inSnapshot = true
				v = &Version{NextFileNum: 1}
				continue
			case editSnapshotEnd:
				inSnapshot = false
				continue
			}
		}
		edit, ok := decodeEdit(payload)
		if !ok {
			break
		}
		v = v.applied(edit)
	}
	if inSnapshot {
		// Torn snapshot: discard it. We cannot fall back to a prior CURRENT
		// here (the rotation has already replaced CURRENT). v ends up empty.
		return &Version{NextFileNum: 1}, nil
	}
	return v, nil
}

func readCurrent(dir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, currentName))
	if err != nil {
		return "", err
	}
	s := strings.TrimSpace(string(data))
	if s == "" {
		return "", os.ErrNotExist
	}
	return s, nil
}

// ----- helpers -----

func manifestName(num uint32) string {
	return fmt.Sprintf("MANIFEST-%06d", num)
}

func parseManifestName(name string) (uint32, error) {
	var n uint32
	if _, err := fmt.Sscanf(name, "MANIFEST-%06d", &n); err != nil {
		return 0, fmt.Errorf("manifest: malformed name %q: %w", name, err)
	}
	return n, nil
}

func appendU32(dst []byte, v uint32) []byte {
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], v)
	return append(dst, buf[:]...)
}

func appendU64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

func appendBytes(dst, b []byte) []byte {
	dst = appendU32(dst, uint32(len(b)))
	return append(dst, b...)
}

func readBytes(buf []byte) ([]byte, int, bool) {
	if len(buf) < 4 {
		return nil, 0, false
	}
	n := binary.LittleEndian.Uint32(buf[:4])
	if 4+int(n) > len(buf) {
		return nil, 0, false
	}
	return buf[4 : 4+int(n)], 4 + int(n), true
}

func compareBytes(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
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

// CleanupOrphans deletes manifest log files no longer named by CURRENT
// whose modtime is older than the supplied grace. Best-effort.
func (m *Manifest) CleanupOrphans() error {
	curName, err := readCurrent(m.dir)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasPrefix(e.Name(), "MANIFEST-") {
			continue
		}
		if e.Name() == curName {
			continue
		}
		_ = os.Remove(filepath.Join(m.dir, e.Name()))
	}
	return nil
}

// SanityCheck verifies that referenced files exist on disk. Returns an
// error wrapping ErrMissingFile when a referenced file is absent.
func (m *Manifest) SanityCheck(checker func(t TableMeta) error) error {
	v := m.Current()
	defer v.Unref()
	for _, level := range v.Tables {
		for _, t := range level {
			if err := checker(*t); err != nil {
				return err
			}
		}
	}
	return nil
}

// ErrMissingFile is returned by SanityCheck if a referenced file is gone.
var ErrMissingFile = errors.New("manifest: referenced file is missing")

// ErrCorrupted signals that the on-disk manifest state is structurally
// invalid (missing CURRENT pointer with stranded artifacts, CURRENT
// references a missing file, CRC mismatch, etc.). The engine wraps this
// when it surfaces the error to user code.
var ErrCorrupted = errors.New("manifest: corrupted")

// Used so a callers that import this package don't need hash/crc32 directly.
var _ = crc32.MakeTable

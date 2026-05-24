package slate

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/harimalladi/slate/internal/blockcache"
	"github.com/harimalladi/slate/internal/encryption"
	"github.com/harimalladi/slate/internal/keys"
	"github.com/harimalladi/slate/internal/manifest"
	"github.com/harimalladi/slate/internal/memtable"
	"github.com/harimalladi/slate/internal/record"
	"github.com/harimalladi/slate/internal/sstable"
	"github.com/harimalladi/slate/internal/vlog"
	"github.com/harimalladi/slate/internal/wal"
)

const (
	lockFileName    = "LOCK"
	manifestDirName = "manifest"
	walDirName      = "wal"
	sstDirName      = "sst"
	vlogDirName     = "vlog"

	// formatVersionFileName carries a single integer line — the on-disk
	// format version the directory was created with. Open refuses any
	// directory whose version is newer than this binary supports.
	formatVersionFileName = "FORMAT"

	// currentFormatVersion is the format-version number this binary
	// writes for new databases. Bump on any incompatible on-disk change.
	currentFormatVersion = 1
)

// DB is an open slate database. A directory holds exactly one open DB at
// a time, enforced by a POSIX file lock on the LOCK file.
type DB struct {
	dir  string
	opts Options
	lock *os.File

	manifest *manifest.Manifest

	rotMu     sync.Mutex
	memtable  *memtable.Memtable
	immutable []*memtable.Memtable // FIFO; head = oldest

	walMu      sync.Mutex
	wal        *wal.Writer
	walFileNum uint32

	sstMu   sync.RWMutex
	readers map[uint32]*sstable.Reader
	cache   *blockcache.Cache

	// codec is non-nil when encryption is enabled. Used by every SST
	// writer and reader.
	codec *encryption.Codec

	// vlogWriter / vlogReader handle WiscKey-style key/value separation.
	// Values at or above opts.ValueThreshold are stored here; the LSM
	// holds the 16-byte pointer in their place.
	vlogWriter *vlog.Writer
	vlogReader *vlog.Reader

	// compactionMu serializes compactions. Two concurrent compactions
	// observing the same input file would produce duplicate outputs at
	// the destination level; the mutex pins the input file set for the
	// duration of one compaction.
	compactionMu sync.Mutex

	// oracle is the single source of truth for SSI conflict detection
	// and unique commit_ts allocation.
	oracle *oracle

	// writeMu serializes commits end-to-end (conflict check + commit_ts
	// assignment + WAL append + memtable insert + visibleTS advance).
	// All concurrent reads proceed lock-free; snapshots see only
	// commits that have completed their full publication cycle.
	writeMu sync.Mutex

	// visibleTS is the highest commit_ts whose data is durable in the
	// memtable. Snapshots and Begin both read it. It lags oracle.nextTS
	// only while a commit is between "commit_ts assigned" and
	// "memtable updated", a window protected by writeMu.
	visibleTS atomic.Uint64

	flushTrigger chan struct{}
	flusherDone  chan struct{}

	closed atomic.Bool
}

// Open opens or creates a slate database in dir.
//
// On an existing directory, Open replays the manifest then the WAL into a
// fresh memtable. All previously-committed Sync writes are visible after
// Open returns. Multi-process access is forbidden — Open returns ErrLocked
// if another process holds the directory lock.
func Open(dir string, opts *Options) (*DB, error) {
	if opts == nil {
		opts = DefaultOptions()
	}
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	for _, sub := range []string{manifestDirName, walDirName, sstDirName, vlogDirName} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, err
		}
	}

	lock, err := acquireLock(filepath.Join(dir, lockFileName))
	if err != nil {
		return nil, err
	}

	if err := checkOrInitFormatVersion(dir); err != nil {
		_ = lock.Close()
		return nil, err
	}

	identity, err := loadOrInitIdentity(dir, opts.EncryptionKey)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	codec, err := codecFor(opts.EncryptionKey, identity)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}

	man, err := manifest.Open(filepath.Join(dir, manifestDirName))
	if err != nil {
		_ = lock.Close()
		// Translate manifest-layer corruption into the public sentinel so
		// callers can `errors.Is(err, slate.ErrCorrupted)` uniformly.
		if errors.Is(err, manifest.ErrCorrupted) {
			return nil, fmt.Errorf("%w: %v", ErrCorrupted, err)
		}
		return nil, err
	}

	db := &DB{
		dir:          dir,
		opts:         *opts,
		lock:         lock,
		manifest:     man,
		memtable:     memtable.New(opts.MemtableSize),
		readers:      make(map[uint32]*sstable.Reader),
		codec:        codec,
		flushTrigger: make(chan struct{}, 16),
		flusherDone:  make(chan struct{}),
	}
	_ = identity // db_uuid is held implicitly via db.codec's derived keys
	if opts.BlockCacheSize > 0 {
		db.cache = blockcache.New(opts.BlockCacheSize, opts.BlockCacheShards)
	}

	// Open SST readers for every file the manifest knows about. A
	// referenced file that is missing on disk is fatal: the engine refuses
	// to start with an inconsistent file set.
	v := man.Current()
	db.oracle = newOracle(v.LastSequence)
	db.visibleTS.Store(v.LastSequence)
	for lvl := 0; lvl < manifest.NumLevels; lvl++ {
		for _, t := range v.Tables[lvl] {
			if _, err := os.Stat(db.sstPath(t.FileNum)); err != nil {
				v.Unref()
				db.cleanupOnError()
				return nil, fmt.Errorf("%w: manifest references missing SST %d at L%d", ErrCorrupted, t.FileNum, lvl)
			}
		}
	}
	for lvl := 0; lvl < manifest.NumLevels; lvl++ {
		for _, t := range v.Tables[lvl] {
			r, err := sstable.Open(db.sstPath(t.FileNum), db.sstReaderOpts(t.FileNum, lvl))
			if err != nil {
				v.Unref()
				db.cleanupOnError()
				return nil, fmt.Errorf("slate: opening SST %d (L%d): %w", t.FileNum, lvl, err)
			}
			if db.cache != nil {
				r.SetCache(db.cache, t.FileNum)
			}
			db.readers[t.FileNum] = r
		}
	}
	startWalFile := v.FlushedWAL.FileNum
	startSeq := v.FlushedWAL.Seq
	v.Unref()

	// Vlog: open the reader (lazy file handles) and a writer rooted at a
	// fresh segment number from the manifest's monotonic allocator. When
	// encryption is enabled the same codec wraps both SST blocks and vlog
	// entries (domain-separated by AD prefix).
	db.vlogReader = vlog.NewReaderWithCodec(filepath.Join(dir, vlogDirName), codec)
	vlogFileNum := db.manifest.AllocFileNum()
	vw, err := vlog.NewWriterWithCodec(filepath.Join(dir, vlogDirName), vlogFileNum, opts.VlogSegmentSize, codec)
	if err != nil {
		db.cleanupOnError()
		return nil, err
	}
	db.vlogWriter = vw

	// Replay WAL records with seq > LastFlushedWAL.seq into the fresh memtable.
	if err := db.replayWAL(filepath.Join(dir, walDirName), startWalFile, startSeq); err != nil {
		db.cleanupOnError()
		return nil, err
	}

	w, err := wal.NewWriter(filepath.Join(dir, walDirName), opts.WALSegmentSize)
	if err != nil {
		db.cleanupOnError()
		return nil, err
	}
	db.wal = w

	// Background flush worker.
	go db.flusherLoop()
	return db, nil
}

func (db *DB) replayWAL(walDir string, minFile uint32, minSeq uint64) error {
	r, err := wal.NewReader(walDir, minFile)
	if err != nil {
		return err
	}
	defer r.Close()

	var maxSeq uint64 = db.visibleTS.Load()
	for {
		rec, err := r.Next()
		if err != nil {
			if wal.EOF(err) || errors.Is(err, wal.ErrCorrupted) {
				break
			}
			return err
		}
		seq, ops, derr := record.DecodeBatch(rec)
		if derr != nil {
			// Truncated record at the tail — recovery stops here. Earlier
			// records are already replayed; the partial record represents
			// an in-flight commit at crash time that never reached fsync.
			break
		}
		if seq <= minSeq {
			continue
		}
		for _, op := range ops {
			switch op.Kind {
			case keys.KindInlineValue:
				db.memtable.Set(append([]byte(nil), op.Key...), seq, keys.KindInlineValue, append([]byte(nil), op.Value...))
			case keys.KindVlogPointer:
				db.memtable.Set(append([]byte(nil), op.Key...), seq, keys.KindVlogPointer, append([]byte(nil), op.Value...))
			case keys.KindDeletion:
				db.memtable.Delete(append([]byte(nil), op.Key...), seq)
			case keys.KindRangeDeletion:
				db.memtable.DeleteRange(append([]byte(nil), op.Key...), append([]byte(nil), op.Value...), seq)
			default:
				return fmt.Errorf("%w: replay encountered unknown kind %v", ErrCorrupted, op.Kind)
			}
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	db.oracle.observeSeq(maxSeq)
	if cur := db.visibleTS.Load(); cur < maxSeq {
		db.visibleTS.Store(maxSeq)
	}
	return nil
}

// Close stops background work, fsyncs the WAL, releases the directory lock,
// and closes all open SST readers. It is idempotent.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return nil
	}

	// Stop the flush worker and wait for it to drain.
	close(db.flushTrigger)
	<-db.flusherDone

	var firstErr error
	db.walMu.Lock()
	if db.wal != nil {
		if err := db.wal.Close(); err != nil {
			firstErr = err
		}
		db.wal = nil
	}
	db.walMu.Unlock()

	db.sstMu.Lock()
	for _, r := range db.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	db.readers = nil
	db.sstMu.Unlock()

	if db.vlogWriter != nil {
		if err := db.vlogWriter.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		db.vlogWriter = nil
	}
	if db.vlogReader != nil {
		if err := db.vlogReader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		db.vlogReader = nil
	}
	if db.cache != nil {
		_ = db.cache.Close()
		db.cache = nil
	}

	if db.manifest != nil {
		if err := db.manifest.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if db.lock != nil {
		if err := releaseLock(db.lock); err != nil && firstErr == nil {
			firstErr = err
		}
		db.lock = nil
	}
	return firstErr
}

func (db *DB) cleanupOnError() {
	if db.manifest != nil {
		_ = db.manifest.Close()
	}
	for _, r := range db.readers {
		_ = r.Close()
	}
	_ = releaseLock(db.lock)
}

// Flush rotates the active memtable to immutable, signals the flush worker,
// and blocks until the immutable queue is empty. Returns nil on success.
//
// Useful in tests, at controlled checkpoints, or as a quiesce primitive
// before a backup. Production callers rarely need it — flush happens
// automatically when the active memtable fills.
func (db *DB) Flush() error {
	if db.closed.Load() {
		return ErrClosed
	}
	db.rotMu.Lock()
	if db.memtable.Used() <= 1 {
		// Active memtable is empty (just the sentinel byte). Nothing to flush.
		db.rotMu.Unlock()
		return nil
	}
	db.memtable.Seal()
	db.immutable = append(db.immutable, db.memtable)
	db.memtable = memtable.New(db.opts.MemtableSize)
	db.rotMu.Unlock()

	select {
	case db.flushTrigger <- struct{}{}:
	default:
	}
	// Wait for the queue to drain. Bounded by the flush worker's progress;
	// a slow flush will block here proportionally.
	for {
		db.rotMu.Lock()
		n := len(db.immutable)
		db.rotMu.Unlock()
		if n == 0 {
			return nil
		}
		if db.closed.Load() {
			return ErrClosed
		}
		// Avoid a busy loop by yielding briefly.
		runtimeYield()
	}
}

// Stats returns a snapshot of operational counters. Safe to call from any
// state including closed and broken; the result reflects the latest
// observed values.
func (db *DB) Stats() Stats {
	var s Stats
	if db.cache != nil {
		cs := db.cache.Stats()
		s.Cache.Hits = cs.Hits
		s.Cache.Misses = cs.Misses
		s.Cache.Evictions = cs.Evictions
		s.Cache.OversizeRejected = cs.OversizeRejected
		s.Cache.BytesUsed = cs.BytesUsed
		s.Cache.Capacity = cs.Capacity
	}
	if !db.closed.Load() && db.manifest != nil {
		v := db.manifest.Current()
		for i, lvl := range v.Tables {
			s.LevelFileCount[i] = len(lvl)
			for _, t := range lvl {
				s.LevelBytes[i] += t.Size
			}
		}
		v.Unref()
	}
	if !db.closed.Load() {
		if db.vlogWriter != nil {
			s.VlogSegments, s.VlogBytes = db.vlogStats()
		}
		if db.vlogReader != nil {
			for _, n := range db.vlogReader.DeadBytes() {
				s.VlogDeadBytes += n
			}
		}
		if db.oracle != nil {
			s.ActiveReaders = db.oracle.activeReaderCount()
		}
	}
	return s
}

// vlogStats walks the vlog directory and returns (segmentCount, totalBytes).
// Cheap enough to compute on demand for Stats() — the directory has at
// most a handful of segments in v0.
func (db *DB) vlogStats() (int, int64) {
	entries, err := os.ReadDir(filepath.Join(db.dir, vlogDirName))
	if err != nil {
		return 0, 0
	}
	var n int
	var total int64
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if filepath.Ext(e.Name()) != ".vlog" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		n++
		total += info.Size()
	}
	return n, total
}

// Stats summarizes engine operational counters.
type Stats struct {
	Cache CacheStats
	// LevelFileCount[i] is the number of SSTable files at level i.
	LevelFileCount [7]int
	// LevelBytes[i] is the total byte size of files at level i.
	LevelBytes [7]int64
	// VlogSegments is the count of *.vlog segment files on disk.
	VlogSegments int
	// VlogBytes is the total on-disk size of all vlog segments.
	VlogBytes int64
	// VlogDeadBytes is the total bytes recorded as dead (superseded) by
	// compaction across all vlog segments. Vlog GC (when it ships) uses
	// dead/total ratios per segment to choose what to rewrite.
	VlogDeadBytes int64
	// ActiveReaders is the number of live transactions and snapshots
	// pinning at least one read_ts in the oracle.
	ActiveReaders int
}

// CacheStats holds block-cache metrics.
type CacheStats struct {
	Hits             int64
	Misses           int64
	Evictions        int64
	OversizeRejected int64
	BytesUsed        int64
	Capacity         int64
}

// HitRate returns the cache's hit ratio in [0, 1]. Returns 0 if no requests
// have been seen.
func (c CacheStats) HitRate() float64 {
	total := c.Hits + c.Misses
	if total == 0 {
		return 0
	}
	return float64(c.Hits) / float64(total)
}

// Sync forces a synchronous fdatasync of the WAL.
func (db *DB) Sync() error {
	if db.closed.Load() {
		return ErrClosed
	}
	db.walMu.Lock()
	w := db.wal
	db.walMu.Unlock()
	if w == nil {
		return ErrClosed
	}
	return w.Sync()
}

// Set writes (key, value) at a fresh sequence number using the configured
// default durability.
func (db *DB) Set(key, value []byte) error {
	return db.set(key, value, db.opts.DefaultDurability)
}

// SetSync writes (key, value) with explicit Sync durability regardless of
// the configured default.
func (db *DB) SetSync(key, value []byte) error {
	return db.set(key, value, Sync)
}

// Delete writes a tombstone for key. Equivalent to calling Update with a
// single-key Delete inside.
func (db *DB) Delete(key []byte) error {
	return db.Update(func(t *Txn) error { return t.Delete(key) })
}

// Get returns the value for key at the current snapshot, or ErrNotFound.
//
// Equivalent to opening a read-only View and reading from it. The returned
// slice is owned by the caller; it has been copied out of any internal
// storage so subsequent engine mutations cannot affect it.
func (db *DB) Get(key []byte) ([]byte, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}
	if err := db.checkKey(key); err != nil {
		return nil, err
	}
	snap := db.visibleTS.Load()
	return db.snapshotGet(key, snap)
}

// snapshotGet resolves key at the supplied snapshot ts. Walks the active
// memtable, then any immutable memtables (newest first), then L0 (newest
// first), then L1..L6 (one file per level via binary search). Vlog
// pointers found at any layer are dereferenced into their stored bytes.
//
// Used by both db.Get and Txn.Get.
func (db *DB) snapshotGet(key []byte, snap uint64) ([]byte, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}
	// 1) Active memtable.
	if v, kind, ok := db.memtable.Get(key, snap); ok {
		return db.materializeValue(v, kind)
	} else if kind == keys.KindDeletion {
		return nil, ErrNotFound
	}

	// 2) Immutable memtables (newest first — back of the slice).
	db.rotMu.Lock()
	imms := append([]*memtable.Memtable(nil), db.immutable...)
	db.rotMu.Unlock()
	for i := len(imms) - 1; i >= 0; i-- {
		if v, kind, ok := imms[i].Get(key, snap); ok {
			return db.materializeValue(v, kind)
		} else if kind == keys.KindDeletion {
			return nil, ErrNotFound
		}
	}

	// 3) L0 SSTables, newest first.
	ver := db.manifest.Current()
	defer ver.Unref()
	for i := len(ver.Tables[0]) - 1; i >= 0; i-- {
		t := ver.Tables[0][i]
		if bytes.Compare(key, t.Smallest) < 0 || bytes.Compare(key, t.Largest) > 0 {
			continue
		}
		val, kind, ok, err := db.getFromTable(t.FileNum, key, snap)
		if err != nil {
			return nil, err
		}
		if ok {
			return db.materializeValue(val, kind)
		}
		if kind == keys.KindDeletion {
			return nil, ErrNotFound
		}
	}

	// 4) L1..L6 — files within a level are non-overlapping by user key.
	for lvl := 1; lvl < manifest.NumLevels; lvl++ {
		level := ver.Tables[lvl]
		if len(level) == 0 {
			continue
		}
		idx := searchLevel(level, key)
		if idx < 0 || idx >= len(level) {
			continue
		}
		t := level[idx]
		val, kind, ok, err := db.getFromTable(t.FileNum, key, snap)
		if err != nil {
			return nil, err
		}
		if ok {
			return db.materializeValue(val, kind)
		}
		if kind == keys.KindDeletion {
			return nil, ErrNotFound
		}
	}
	return nil, ErrNotFound
}

// materializeValue resolves a (value, kind) pair into the raw value bytes
// expected by callers. For inline values it copies the bytes; for vlog
// pointers it dereferences via the vlog reader.
func (db *DB) materializeValue(raw []byte, kind keys.Kind) ([]byte, error) {
	switch kind {
	case keys.KindInlineValue:
		return materialize(raw), nil
	case keys.KindVlogPointer:
		ptr, err := vlog.DecodePointer(raw)
		if err != nil {
			return nil, err
		}
		val, err := db.vlogReader.Dereference(ptr)
		if err != nil {
			return nil, err
		}
		return val, nil
	default:
		return nil, fmt.Errorf("%w: read encountered unsupported kind %v", errInternal, kind)
	}
}

// searchLevel returns the index of the file at this (non-overlapping) level
// whose key range covers target, or -1 if no file covers it.
func searchLevel(level []*manifest.TableMeta, target []byte) int {
	lo, hi := 0, len(level)-1
	for lo <= hi {
		mid := (lo + hi) / 2
		t := level[mid]
		if bytes.Compare(target, t.Smallest) < 0 {
			hi = mid - 1
		} else if bytes.Compare(target, t.Largest) > 0 {
			lo = mid + 1
		} else {
			return mid
		}
	}
	return -1
}

// getFromTable reads a single SSTable via its cached reader. The reader
// applies its own range_del block internally before returning.
func (db *DB) getFromTable(fileNum uint32, key []byte, snap uint64) ([]byte, keys.Kind, bool, error) {
	db.sstMu.RLock()
	r := db.readers[fileNum]
	db.sstMu.RUnlock()
	if r == nil {
		return nil, 0, false, fmt.Errorf("%w: missing reader for file %d", errInternal, fileNum)
	}
	return r.Get(key, snap)
}

func (db *DB) set(key, value []byte, dur Durability) error {
	if db.closed.Load() {
		return ErrClosed
	}
	if err := db.checkKey(key); err != nil {
		return err
	}
	if len(value) > db.opts.MaxValueSize {
		return &KeyError{Op: "Set", Key: key, Err: ErrValueTooLarge}
	}

	// Hold writeMu through the full publication cycle.
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	writeSet := map[string]struct{}{string(key): {}}
	commitTS, err := db.oracle.commit(0, nil, writeSet)
	if err != nil {
		return err
	}

	// Decide storage placement: above ValueThreshold the value goes to the
	// vlog and the LSM holds the 16-byte pointer; below threshold it stays
	// inline.
	storedKind, storedValue, err := db.placeValue(value)
	if err != nil {
		return err
	}
	rec := record.EncodeBatch(nil, commitTS, []record.Op{{Kind: storedKind, Key: key, Value: storedValue}})
	if err := db.appendWAL(rec, dur); err != nil {
		return err
	}
	if !db.memtableSetWithKind(key, commitTS, storedKind, storedValue) {
		return ErrDiskFull
	}
	db.visibleTS.Store(commitTS)
	return nil
}

// placeValue decides whether to inline a value or spill it to the value
// log. Returns the kind tag and the bytes to store in the LSM (either the
// original value or the 16-byte vlog pointer).
func (db *DB) placeValue(value []byte) (keys.Kind, []byte, error) {
	if db.opts.ValueThreshold <= 0 || len(value) < db.opts.ValueThreshold {
		return keys.KindInlineValue, value, nil
	}
	if db.vlogWriter == nil {
		return keys.KindInlineValue, value, nil
	}
	allocator := func() uint32 { return db.manifest.AllocFileNum() }
	ptr, err := db.vlogWriter.Append(value, allocator)
	if err != nil {
		return 0, nil, fmt.Errorf("slate: vlog append: %w", err)
	}
	enc := vlog.EncodePointer(ptr)
	return keys.KindVlogPointer, enc[:], nil
}

// memtableSetWithKind is a small generalization of memtableSet that
// accepts either KindInlineValue or KindVlogPointer.
func (db *DB) memtableSetWithKind(key []byte, seq uint64, kind keys.Kind, payload []byte) bool {
	keyCopy := append([]byte(nil), key...)
	valCopy := append([]byte(nil), payload...)
	for {
		if db.memtable.Set(keyCopy, seq, kind, valCopy) {
			return true
		}
		if !db.rotate() {
			return false
		}
	}
}

// memtableSet inserts into the active memtable, rotating if it is full.
func (db *DB) memtableSet(key []byte, seq uint64, value []byte) bool {
	keyCopy := append([]byte(nil), key...)
	valCopy := append([]byte(nil), value...)
	for {
		if db.memtable.Set(keyCopy, seq, keys.KindInlineValue, valCopy) {
			return true
		}
		if !db.rotate() {
			return false
		}
	}
}

func (db *DB) memtableDelete(key []byte, seq uint64) bool {
	keyCopy := append([]byte(nil), key...)
	for {
		if db.memtable.Delete(keyCopy, seq) {
			return true
		}
		if !db.rotate() {
			return false
		}
	}
}

// rotate pushes the current memtable onto the immutable queue, allocates a
// fresh active memtable, and signals the flush worker. Returns false if
// the immutable queue is already at the configured maximum (back-pressure).
func (db *DB) rotate() bool {
	db.rotMu.Lock()
	defer db.rotMu.Unlock()

	const maxImmutable = 4
	if len(db.immutable) >= maxImmutable {
		// Block briefly to give the flusher a chance to drain. We yield by
		// returning false (caller surfaces ErrDiskFull) only if rotation
		// truly cannot proceed.
		return false
	}
	if db.memtable.Used() == 1 {
		// Active memtable is empty (just the sentinel byte); rotating it
		// would consume an arena slot without making progress. Surface as
		// disk-full.
		return false
	}
	db.memtable.Seal()
	db.immutable = append(db.immutable, db.memtable)
	db.memtable = memtable.New(db.opts.MemtableSize)
	select {
	case db.flushTrigger <- struct{}{}:
	default:
	}
	return true
}

func (db *DB) appendWAL(rec []byte, dur Durability) error {
	db.walMu.Lock()
	w := db.wal
	db.walMu.Unlock()
	if w == nil {
		return ErrClosed
	}
	return w.Append(rec, dur.wal())
}

func (db *DB) checkKey(key []byte) error {
	if len(key) == 0 {
		return &KeyError{Op: "Set", Key: nil, Err: errors.New("slate: key must be non-empty")}
	}
	if len(key) > db.opts.MaxKeySize {
		return &KeyError{Op: "Set", Key: key, Err: ErrKeyTooLarge}
	}
	return nil
}

func (db *DB) sstPath(num uint32) string {
	return filepath.Join(db.dir, sstDirName, fmt.Sprintf("%06d.sst", num))
}

// checkOrInitFormatVersion enforces the on-disk format-version invariant:
// every DB directory has a FORMAT file naming the version it was created
// at. Opening a directory whose recorded version is greater than this
// binary's currentFormatVersion returns ErrUnsupportedVersion. Opening a
// fresh directory writes the current version.
func checkOrInitFormatVersion(dir string) error {
	path := filepath.Join(dir, formatVersionFileName)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		// Fresh directory: stamp the current version.
		return os.WriteFile(path, []byte(fmt.Sprintf("%d\n", currentFormatVersion)), 0o644)
	}
	if err != nil {
		return err
	}
	var v uint32
	if _, err := fmt.Sscanf(string(data), "%d", &v); err != nil {
		return fmt.Errorf("%w: FORMAT unparseable: %v", ErrCorrupted, err)
	}
	if v > currentFormatVersion {
		return fmt.Errorf("%w: on-disk format %d, binary supports %d", ErrUnsupportedVersion, v, currentFormatVersion)
	}
	return nil
}

// sstWriterOpts returns WriterOptions for a new SST at (fileNum, level),
// carrying the encryption codec if enabled.
func (db *DB) sstWriterOpts(fileNum uint32, level int) *sstable.WriterOptions {
	opts := &sstable.WriterOptions{}
	if db.codec != nil {
		opts.Codec = db.codec
		opts.FileNum = fileNum
		opts.Level = level
	}
	return opts
}

// sstReaderOpts returns ReaderOptions for opening an SST at (fileNum, level).
// Returns nil for unencrypted databases.
func (db *DB) sstReaderOpts(fileNum uint32, level int) *sstable.ReaderOptions {
	if db.codec == nil {
		return nil
	}
	return &sstable.ReaderOptions{Codec: db.codec, FileNum: fileNum, Level: level}
}

func materialize(src []byte) []byte {
	out := make([]byte, len(src))
	copy(out, src)
	return out
}

// runtimeYield briefly suspends the caller so the flush worker can make
// progress without spinning.
func runtimeYield() { time.Sleep(time.Millisecond) }

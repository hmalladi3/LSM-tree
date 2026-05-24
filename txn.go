package slate

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/harimalladi/slate/internal/keys"
	"github.com/harimalladi/slate/internal/manifest"
	"github.com/harimalladi/slate/internal/record"
)

// Txn is a database transaction. It provides a consistent snapshot view of
// the database at the moment Begin was called; writes are buffered until
// Commit and become visible atomically thereafter.
//
// Isolation level is Serializable Snapshot Isolation (SSI). A transaction
// that reads a key and then commits will be aborted with ErrConflict if a
// concurrent transaction committed a write to that key after our read_ts.
//
// Txns are not safe for concurrent use by multiple goroutines; create one
// per goroutine.
type Txn struct {
	db       *DB
	readTS   uint64
	readOnly bool

	// readSet tracks user keys read from the LSM (not own-writes). Empty
	// for read-only transactions, which never need conflict detection.
	readSet map[string]struct{}

	// writes buffers pending point mutations. The map key is the user key
	// as a string; pendingOp.value holds the bytes (nil for tombstones).
	writes map[string]*pendingOp

	// rangeDels buffers DeleteRange calls. Each becomes a memtable range
	// tombstone at commit time. Read-path inside the txn consults these
	// before falling back to the LSM.
	rangeDels []bufferedRange

	// bufferBytes accumulates the rough byte cost of pending writes so we
	// can enforce TxnMaxBytes without scanning the buffer each call.
	bufferBytes int

	// durability overrides db.opts.DefaultDurability for this transaction's
	// commit. Set via SetDurability.
	durability    Durability
	durabilitySet bool

	closed bool
}

// SetDurability overrides the durability mode used by this transaction's
// commit. Must be called before Commit. Read-only transactions ignore this
// setting.
func (t *Txn) SetDurability(d Durability) {
	t.durability = d
	t.durabilitySet = true
}

type pendingOp struct {
	kind  keys.Kind
	value []byte
}

// bufferedRange records a [start, end) deletion buffered inside a
// transaction. The memtable's range-tombstone skiplist is updated only
// at commit time.
type bufferedRange struct {
	start []byte
	end   []byte
}

// Snapshot is a long-lived read-only view of the database at the sequence
// number it was created. Unlike a View transaction, a Snapshot is intended
// to outlive a single function call: it pins the manifest Version and
// holds a slot in the oracle's active-reads set, so SSTs and vlog segments
// visible at the snapshot's read_ts will not be deleted by compaction
// while it is alive.
//
// Snapshots must be Close()d to release the pin. Failing to Close leaks
// memory and prevents disk reclamation indefinitely.
type Snapshot struct {
	db     *DB
	readTS uint64
	ver    *manifest.Version
	closed bool
}

// NewSnapshot returns a Snapshot pinned at the current visible sequence.
// Reads via the returned Snapshot observe a stable view of the database
// even as new writes commit.
func (db *DB) NewSnapshot() *Snapshot {
	readTS := db.visibleTS.Load()
	db.oracle.acquireRead(readTS)
	v := db.manifest.Current()
	return &Snapshot{db: db, readTS: readTS, ver: v}
}

// ReadTS returns the snapshot's pinned sequence number.
func (s *Snapshot) ReadTS() uint64 { return s.readTS }

// Get returns the value visible at the snapshot's read_ts, or ErrNotFound.
// Does not record the read for any conflict detection (snapshots are
// passive observers).
func (s *Snapshot) Get(key []byte) ([]byte, error) {
	if s.closed {
		return nil, ErrClosed
	}
	if err := s.db.checkKey(key); err != nil {
		return nil, err
	}
	return s.db.snapshotGet(key, s.readTS)
}

// NewIterator returns an iterator over the snapshot at the pinned read_ts.
func (s *Snapshot) NewIterator(opts *IterOptions) *Iterator {
	if s.closed {
		return &Iterator{err: ErrClosed}
	}
	return s.db.newIteratorAt(opts, s.readTS)
}

// Close releases the snapshot's pin on the manifest Version and the
// oracle active-reads set. Idempotent.
func (s *Snapshot) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.ver != nil {
		s.ver.Unref()
		s.ver = nil
	}
	s.db.oracle.releaseRead(s.readTS)
	return nil
}

// BeginTxn starts a new transaction with the given read/write mode.
// Callers must eventually Commit or Rollback the returned handle.
//
// read_ts is captured from the database's visible-watermark so the txn
// only ever observes commits whose data has fully published into the
// memtable.
func (db *DB) BeginTxn(readWrite bool) *Txn {
	ts := db.visibleTS.Load()
	db.oracle.acquireRead(ts)
	t := &Txn{
		db:       db,
		readTS:   ts,
		readOnly: !readWrite,
	}
	if !t.readOnly {
		t.readSet = make(map[string]struct{})
		t.writes = make(map[string]*pendingOp)
	}
	return t
}

// View runs fn in a fresh read-only transaction. The transaction is
// released when fn returns.
func (db *DB) View(fn func(txn *Txn) error) error {
	if db.closed.Load() {
		return ErrClosed
	}
	t := db.BeginTxn(false)
	defer t.Rollback()
	return fn(t)
}

// Update runs fn in a fresh read-write transaction. On commit conflict,
// fn is retried with a new read_ts up to 10 times before the error is
// surfaced. Other errors are not retried.
func (db *DB) Update(fn func(txn *Txn) error) error {
	if db.closed.Load() {
		return ErrClosed
	}
	const maxRetries = 10
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0x9e3779b1))
	for attempt := 0; ; attempt++ {
		t := db.BeginTxn(true)
		if err := fn(t); err != nil {
			t.Rollback()
			return err
		}
		err := t.Commit()
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrConflict) {
			return err
		}
		if attempt >= maxRetries {
			return err
		}
		// Backoff before retry.
		base := time.Duration(100*(1<<uint(attempt))) * time.Microsecond
		if base > 10*time.Millisecond {
			base = 10 * time.Millisecond
		}
		jitter := time.Duration(rng.Int64N(int64(base) / 2))
		time.Sleep(base + jitter)
	}
}

// Get returns the value for key visible to this transaction's snapshot.
// Reads consult the transaction's buffer first (read-your-writes), then
// fall back to the LSM at read_ts.
//
// In a read-write transaction, Get records the key in the read set for
// conflict detection at commit time.
func (t *Txn) Get(key []byte) ([]byte, error) {
	if t.closed {
		return nil, ErrTxnClosed
	}
	if err := t.db.checkKey(key); err != nil {
		return nil, err
	}
	// 1) Buffer (read-your-writes).
	if t.writes != nil {
		if op, ok := t.writes[string(key)]; ok {
			if op.kind == keys.KindDeletion {
				return nil, ErrNotFound
			}
			out := make([]byte, len(op.value))
			copy(out, op.value)
			return out, nil
		}
	}
	// 1b) Buffered range tombstone? Per TXN-OP-007 a covering DeleteRange
	// shadows the LSM even without a point-write entry in the buffer.
	if t.rangeDeletesKey(key) {
		return nil, ErrNotFound
	}
	// 2) LSM at read_ts. Record the read for SSI in a R/W txn.
	if !t.readOnly {
		t.readSet[string(key)] = struct{}{}
	}
	return t.db.snapshotGet(key, t.readTS)
}

// Set buffers a write. The write is not visible to other transactions
// until Commit succeeds.
//
// Returns ErrTxnTooLarge if the transaction has hit options.TxnMaxKeys
// or options.TxnMaxBytes.
func (t *Txn) Set(key, value []byte) error {
	if t.closed {
		return ErrTxnClosed
	}
	if t.readOnly {
		return ErrReadOnly
	}
	if err := t.db.checkKey(key); err != nil {
		return err
	}
	if len(value) > t.db.opts.MaxValueSize {
		return &KeyError{Op: "Set", Key: key, Err: ErrValueTooLarge}
	}
	if err := t.checkSizeLimit(key, len(value)); err != nil {
		return err
	}
	keyCopy := append([]byte(nil), key...)
	valCopy := append([]byte(nil), value...)
	t.writeBuffered(keyCopy, &pendingOp{kind: keys.KindInlineValue, value: valCopy})
	return nil
}

// Delete buffers a tombstone for key.
func (t *Txn) Delete(key []byte) error {
	if t.closed {
		return ErrTxnClosed
	}
	if t.readOnly {
		return ErrReadOnly
	}
	if err := t.db.checkKey(key); err != nil {
		return err
	}
	if err := t.checkSizeLimit(key, 0); err != nil {
		return err
	}
	keyCopy := append([]byte(nil), key...)
	t.writeBuffered(keyCopy, &pendingOp{kind: keys.KindDeletion})
	return nil
}

// DeleteRange buffers a tombstone covering all user keys in [start, end).
// Subsequent reads inside the transaction (Get, iterator) see the range as
// deleted; at Commit the range is written as a memtable range tombstone.
//
// Returns immediately when start >= end (empty range, no-op).
func (t *Txn) DeleteRange(start, end []byte) error {
	if t.closed {
		return ErrTxnClosed
	}
	if t.readOnly {
		return ErrReadOnly
	}
	if err := t.db.checkKey(start); err != nil {
		return err
	}
	if len(end) == 0 {
		return &KeyError{Op: "DeleteRange", Key: end, Err: errors.New("slate: end key must be non-empty")}
	}
	if bytes.Compare(start, end) >= 0 {
		return nil
	}
	t.rangeDels = append(t.rangeDels, bufferedRange{
		start: append([]byte(nil), start...),
		end:   append([]byte(nil), end...),
	})
	return nil
}

// rangeDeletesKey reports whether any buffered DeleteRange covers `key`.
func (t *Txn) rangeDeletesKey(key []byte) bool {
	for _, r := range t.rangeDels {
		if bytes.Compare(r.start, key) <= 0 && bytes.Compare(key, r.end) < 0 {
			return true
		}
	}
	return false
}

// checkSizeLimit enforces TxnMaxKeys and TxnMaxBytes. A repeat write to an
// existing buffered key does not count toward TxnMaxKeys (the buffer
// collapses to the latest op per key).
func (t *Txn) checkSizeLimit(key []byte, valueLen int) error {
	if t.db.opts.TxnMaxKeys > 0 {
		if _, exists := t.writes[string(key)]; !exists {
			if len(t.writes)+1 > t.db.opts.TxnMaxKeys {
				return ErrTxnTooLarge
			}
		}
	}
	if t.db.opts.TxnMaxBytes > 0 {
		// Estimate the post-add buffer size. We use a rough additive
		// model: prior bufferBytes + (key + value + small overhead).
		prior := t.bufferBytes
		if existing, ok := t.writes[string(key)]; ok {
			// Replacing — subtract the existing entry's cost first.
			prior -= len(key) + len(existing.value)
		}
		after := prior + len(key) + valueLen
		if after > t.db.opts.TxnMaxBytes {
			return ErrTxnTooLarge
		}
	}
	return nil
}

// writeBuffered installs op for keyCopy and updates bufferBytes accounting.
// keyCopy must already be a defensively-copied slice.
func (t *Txn) writeBuffered(keyCopy []byte, op *pendingOp) {
	if existing, ok := t.writes[string(keyCopy)]; ok {
		t.bufferBytes -= len(keyCopy) + len(existing.value)
	}
	t.writes[string(keyCopy)] = op
	t.bufferBytes += len(keyCopy) + len(op.value)
}

// Commit attempts to commit the transaction. On rw-antidependency conflict,
// returns ErrConflict and the caller must Rollback (or retry the whole txn).
//
// A read-only Commit always succeeds and is equivalent to Rollback.
func (t *Txn) Commit() error {
	if t.closed {
		return ErrTxnClosed
	}
	if t.readOnly {
		t.cleanup()
		return nil
	}
	if len(t.writes) == 0 && len(t.rangeDels) == 0 {
		t.cleanup()
		return nil
	}

	writeSet := make(map[string]struct{}, len(t.writes))
	for k := range t.writes {
		writeSet[k] = struct{}{}
	}
	// Range deletes contribute their `start` keys to the write set for
	// conflict detection. (Conservative: any reader of a key in [start,
	// end) would have to coordinate.)
	for _, r := range t.rangeDels {
		writeSet[string(r.start)] = struct{}{}
	}

	// Hold writeMu through the full publication cycle so the visibleTS
	// advance happens only after the memtable insert; concurrent reads
	// never observe a commit_ts whose data isn't yet present.
	t.db.writeMu.Lock()
	commitTS, err := t.db.oracle.commit(t.readTS, t.readSet, writeSet)
	if err != nil {
		t.db.writeMu.Unlock()
		t.cleanup()
		return err
	}

	dur := t.db.opts.DefaultDurability
	if t.durabilitySet {
		dur = t.durability
	}
	if err := t.db.applyTxnWrites(t.writes, commitTS, dur); err != nil {
		t.db.writeMu.Unlock()
		t.cleanup()
		return err
	}
	if err := t.db.applyTxnRangeDeletes(t.rangeDels, commitTS, dur); err != nil {
		t.db.writeMu.Unlock()
		t.cleanup()
		return err
	}
	t.db.visibleTS.Store(commitTS)
	t.db.writeMu.Unlock()
	t.cleanup()
	return nil
}

// NewIterator returns an iterator over the database's snapshot at this
// transaction's read_ts, layered with the transaction's pending writes.
//
// Reads inside the iterator see the txn's buffered Set/Delete operations
// (read-your-writes) ahead of any LSM state. The iterator borrows the
// transaction's snapshot pin — it becomes invalid when the parent
// transaction is Committed or Rolled back, so Close it before committing
// to avoid surfacing stale state to the caller.
//
// For read-only transactions the iterator presents the LSM at read_ts
// directly.
func (t *Txn) NewIterator(opts *IterOptions) *Iterator {
	if t.closed {
		return &Iterator{err: ErrTxnClosed}
	}
	it := t.db.newIteratorAt(opts, t.readTS)
	if it.err != nil || t.readOnly || len(t.writes) == 0 {
		return it
	}
	// Wrap the LSM iterator with a txn-buffer overlay. The overlay
	// presents the buffer's entries in sorted order and shadows LSM
	// entries on the same user key.
	return wrapWithTxnBuffer(it, t.writes)
}

// Rollback discards the transaction. Idempotent.
func (t *Txn) Rollback() error {
	if t.closed {
		return nil
	}
	t.cleanup()
	return nil
}

func (t *Txn) cleanup() {
	if t.closed {
		return
	}
	t.closed = true
	t.db.oracle.releaseRead(t.readTS)
	t.readSet = nil
	t.writes = nil
}

// applyTxnWrites stamps every pending op with commitTS, appends ONE
// batched WAL record carrying every op, then inserts into the memtable.
// Called only by Txn.Commit after the oracle has authorized the commit.
//
// The batched WAL record is the atomicity guarantee: either every op of
// this transaction is recovered after a crash (the record's CRC validates)
// or none of them is (the record is discarded by replayWAL on CRC
// failure). A torn middle is impossible.
func (db *DB) applyTxnWrites(writes map[string]*pendingOp, commitTS uint64, dur Durability) error {
	keysSorted := make([]string, 0, len(writes))
	for k := range writes {
		keysSorted = append(keysSorted, k)
	}
	sort.Strings(keysSorted)

	// First pass: resolve placement (inline vs vlog pointer) and build the
	// batched WAL record AND the memtable mutation plan.
	type planEntry struct {
		key   []byte
		kind  keys.Kind
		value []byte
	}
	batchOps := make([]record.Op, 0, len(keysSorted))
	plan := make([]planEntry, 0, len(keysSorted))
	for _, k := range keysSorted {
		op := writes[k]
		key := []byte(k)
		switch op.kind {
		case keys.KindInlineValue:
			storedKind, storedValue, err := db.placeValue(op.value)
			if err != nil {
				return err
			}
			batchOps = append(batchOps, record.Op{
				Kind:  storedKind,
				Key:   key,
				Value: storedValue,
			})
			plan = append(plan, planEntry{key: key, kind: storedKind, value: storedValue})
		case keys.KindDeletion:
			batchOps = append(batchOps, record.Op{
				Kind: keys.KindDeletion,
				Key:  key,
			})
			plan = append(plan, planEntry{key: key, kind: keys.KindDeletion})
		default:
			return errors.New("slate: internal: unsupported pending op kind")
		}
	}

	if len(batchOps) > 0 {
		rec := record.EncodeBatch(nil, commitTS, batchOps)
		if err := db.appendWAL(rec, dur); err != nil {
			return err
		}
	}

	// Now the WAL guarantees durability of this commit — install into the
	// memtable. (Memtable insert never fails on a sane allocation, but the
	// rotation path can; ErrDiskFull surfaces from that case.)
	for _, p := range plan {
		switch p.kind {
		case keys.KindDeletion:
			if !db.memtableDeleteAt(p.key, commitTS) {
				return ErrDiskFull
			}
		default:
			if !db.memtableSetWithKindAt(p.key, commitTS, p.kind, p.value) {
				return ErrDiskFull
			}
		}
	}
	return nil
}

// applyTxnRangeDeletes writes one range-tombstone record per buffered
// DeleteRange, both to the WAL (for crash recovery) and to the memtable
// (for visibility). All deletes share the txn's commit_ts.
func (db *DB) applyTxnRangeDeletes(ranges []bufferedRange, commitTS uint64, dur Durability) error {
	for _, r := range ranges {
		// Encode as a record where key=start and value=end. The record
		// codec carries arbitrary value bytes for non-KindDeletion ops,
		// so KindRangeDeletion serializes naturally.
		rec := record.EncodeBatch(nil, commitTS, []record.Op{{Kind: keys.KindRangeDeletion, Key: r.start, Value: r.end}})
		if err := db.appendWAL(rec, dur); err != nil {
			return err
		}
		if !db.memtableDeleteRangeAt(r.start, r.end, commitTS) {
			return ErrDiskFull
		}
	}
	return nil
}

// memtableDeleteRangeAt writes a range tombstone, rotating the memtable
// if necessary.
func (db *DB) memtableDeleteRangeAt(start, end []byte, seq uint64) bool {
	startCopy := append([]byte(nil), start...)
	endCopy := append([]byte(nil), end...)
	for {
		if db.memtable.DeleteRange(startCopy, endCopy, seq) {
			return true
		}
		if !db.rotate() {
			return false
		}
	}
}

// memtableSetWithKindAt is memtableSetAt with an explicit kind tag (used
// to insert either KindInlineValue or KindVlogPointer).
func (db *DB) memtableSetWithKindAt(key []byte, seq uint64, kind keys.Kind, payload []byte) bool {
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

// memtableSetAt is like memtableSet but uses the supplied seq instead of
// allocating from the oracle.
func (db *DB) memtableSetAt(key []byte, seq uint64, value []byte) bool {
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

func (db *DB) memtableDeleteAt(key []byte, seq uint64) bool {
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

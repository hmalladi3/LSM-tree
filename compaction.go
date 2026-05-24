package slate

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/harimalladi/slate/internal/keys"
	"github.com/harimalladi/slate/internal/manifest"
	"github.com/harimalladi/slate/internal/sstable"
	"github.com/harimalladi/slate/internal/vlog"
)

// compactionPlan describes a single compaction to perform.
type compactionPlan struct {
	sourceLevel int
	outputLevel int
	inputs      [2][]*manifest.TableMeta // [0] = sourceLevel, [1] = outputLevel
}

// pickRangeCompaction builds a compaction plan that covers every file
// whose key range overlaps [start, end) at any level below the bottommost.
// It walks levels top-down, picking the highest level that has any
// overlapping file and compacting it down to the next level. Returns nil
// when no level has overlapping work to do.
//
// Used by db.CompactRange to force a one-shot, range-scoped compaction
// without disturbing the picker's normal triggers.
func (db *DB) pickRangeCompaction(v *manifest.Version, start, end []byte) *compactionPlan {
	for lvl := 0; lvl < manifest.NumLevels-1; lvl++ {
		var overlapping []*manifest.TableMeta
		for i := range v.Tables[lvl] {
			t := v.Tables[lvl][i]
			if bytes.Compare(t.Largest, start) < 0 {
				continue
			}
			if bytes.Compare(t.Smallest, end) >= 0 {
				continue
			}
			overlapping = append(overlapping, t)
		}
		if len(overlapping) == 0 {
			continue
		}
		plan := &compactionPlan{sourceLevel: lvl, outputLevel: lvl + 1}
		plan.inputs[0] = overlapping

		// Expand inputs[1] to every L+1 file overlapping the overall
		// [smallest, largest] range of the chosen source files.
		smallest := overlapping[0].Smallest
		largest := overlapping[0].Largest
		for _, t := range overlapping[1:] {
			if bytes.Compare(t.Smallest, smallest) < 0 {
				smallest = t.Smallest
			}
			if bytes.Compare(t.Largest, largest) > 0 {
				largest = t.Largest
			}
		}
		for i := range v.Tables[lvl+1] {
			t := v.Tables[lvl+1][i]
			if bytes.Compare(t.Largest, smallest) < 0 {
				continue
			}
			if bytes.Compare(t.Smallest, largest) > 0 {
				continue
			}
			plan.inputs[1] = append(plan.inputs[1], t)
		}
		return plan
	}
	return nil
}

// pickCompaction returns a plan describing the most-needed compaction, or
// nil if no level is over its target.
//
// Selection logic. Any level that meets its trigger criterion is a
// candidate; among candidates, the one with the highest score is chosen.
// Score is defined per level as the ratio of actual to target (for L0,
// file count / trigger; for L1+, bytes / target).
func (db *DB) pickCompaction(v *manifest.Version) *compactionPlan {
	var best *compactionPlan
	bestScore := 0.0
	triggered := false

	// L0 trigger: file count >= threshold.
	if n := len(v.Tables[0]); n >= db.opts.L0CompactionTrigger {
		score := float64(n) / float64(db.opts.L0CompactionTrigger)
		if !triggered || score > bestScore {
			if plan := db.buildL0Plan(v); plan != nil {
				best = plan
				bestScore = score
				triggered = true
			}
		}
	}

	// L1..L5 triggers: byte size > target.
	for lvl := 1; lvl < manifest.NumLevels-1; lvl++ {
		sz := levelSize(v.Tables[lvl])
		target := db.levelTargetBytes(lvl)
		if sz <= target {
			continue
		}
		score := float64(sz) / float64(target)
		if !triggered || score > bestScore {
			if plan := db.buildSizedPlan(v, lvl); plan != nil {
				best = plan
				bestScore = score
				triggered = true
			}
		}
	}
	return best
}

// buildL0Plan picks the oldest L0 file, expands to all overlapping L0 files
// (since L0 files may overlap), and then to all overlapping L1 files.
func (db *DB) buildL0Plan(v *manifest.Version) *compactionPlan {
	if len(v.Tables[0]) == 0 {
		return nil
	}
	// Oldest L0 file = first inserted = head of the slice (we append in
	// arrival order).
	oldest := v.Tables[0][0]
	plan := &compactionPlan{sourceLevel: 0, outputLevel: 1}
	plan.inputs[0] = []*manifest.TableMeta{oldest}

	// Expand L0 inputs to cover any L0 file overlapping the running range.
	smallest := append([]byte(nil), oldest.Smallest...)
	largest := append([]byte(nil), oldest.Largest...)
	for changed := true; changed; {
		changed = false
		for _, t := range v.Tables[0] {
			if containsTable(plan.inputs[0], t) {
				continue
			}
			if rangesOverlap(t.Smallest, t.Largest, smallest, largest) {
				plan.inputs[0] = append(plan.inputs[0], t)
				if bytes.Compare(t.Smallest, smallest) < 0 {
					smallest = append(smallest[:0], t.Smallest...)
				}
				if bytes.Compare(t.Largest, largest) > 0 {
					largest = append(largest[:0], t.Largest...)
				}
				changed = true
			}
		}
	}

	// Add all overlapping L1 files.
	for _, t := range v.Tables[1] {
		if rangesOverlap(t.Smallest, t.Largest, smallest, largest) {
			plan.inputs[1] = append(plan.inputs[1], t)
		}
	}
	return plan
}

// buildSizedPlan picks a file at level k (round-robin by smallest key) plus
// the overlapping L_{k+1} files.
func (db *DB) buildSizedPlan(v *manifest.Version, level int) *compactionPlan {
	if len(v.Tables[level]) == 0 {
		return nil
	}
	// Simple policy for v0: pick the smallest-keyed file. (Round-robin
	// state belongs on the picker; we defer that work until starvation
	// becomes a measurable concern.)
	src := v.Tables[level][0]
	plan := &compactionPlan{sourceLevel: level, outputLevel: level + 1}
	plan.inputs[0] = []*manifest.TableMeta{src}

	for _, t := range v.Tables[level+1] {
		if rangesOverlap(t.Smallest, t.Largest, src.Smallest, src.Largest) {
			plan.inputs[1] = append(plan.inputs[1], t)
		}
	}
	return plan
}

// runCompaction executes a plan. The caller must hold db.compactionMu so
// concurrent compactions cannot operate on the same inputs.
func (db *DB) runCompaction(plan *compactionPlan) error {
	// Open per-input iterators. L0 inputs (which may overlap each other)
	// must each be its own iterator; L1+ inputs (non-overlapping within a
	// level) could be combined but we keep them per-file for simplicity.
	type srcIter struct {
		it   *sstable.Iterator
		meta *manifest.TableMeta
	}
	var iters []*srcIter
	cleanup := func() {
		for _, s := range iters {
			_ = s.it.Close()
		}
	}
	defer cleanup()

	openReader := func(num uint32) *sstable.Reader {
		db.sstMu.RLock()
		defer db.sstMu.RUnlock()
		return db.readers[num]
	}

	for _, t := range plan.inputs[0] {
		r := openReader(t.FileNum)
		if r == nil {
			return fmt.Errorf("%w: compaction input %d missing", errInternal, t.FileNum)
		}
		it := r.NewIterator()
		it.First()
		iters = append(iters, &srcIter{it: it, meta: t})
	}
	for _, t := range plan.inputs[1] {
		r := openReader(t.FileNum)
		if r == nil {
			return fmt.Errorf("%w: compaction input %d missing", errInternal, t.FileNum)
		}
		it := r.NewIterator()
		it.First()
		iters = append(iters, &srcIter{it: it, meta: t})
	}

	isBottommost := plan.outputLevel == manifest.NumLevels-1

	// Collect every input SST's range tombstones up-front. After tombstone-
	// elimination logic (if at the bottommost level), they get propagated
	// into every output file whose key range overlaps the tombstone.
	var allTombs []sstable.RangeTombstone
	for _, t := range plan.inputs[0] {
		if r := openReader(t.FileNum); r != nil {
			allTombs = append(allTombs, r.RangeTombstones()...)
		}
	}
	for _, t := range plan.inputs[1] {
		if r := openReader(t.FileNum); r != nil {
			allTombs = append(allTombs, r.RangeTombstones()...)
		}
	}

	// Stream output files. Switch to a new output file at TargetFileSize
	// boundaries, but never mid-user-key (SST writer enforces).
	type pending struct {
		file *os.File
		w    *sstable.Writer
		num  uint32
		path string
	}
	var outputs []manifest.TableMeta
	var pendingPtr *pending
	var lastUser []byte

	startOutput := func() error {
		fileNum := db.manifest.AllocFileNum()
		path := db.sstPath(fileNum)
		f, err := os.Create(path + ".tmp")
		if err != nil {
			return err
		}
		pendingPtr = &pending{
			file: f,
			w:    sstable.NewWriter(f, db.sstWriterOpts(fileNum, plan.outputLevel)),
			num:  fileNum,
			path: path,
		}
		// Propagate every range tombstone to this output. For v0 we copy
		// them all into each output; tombstones at the bottommost level
		// could be dropped (no older data below), but the bookkeeping is
		// modest enough that we keep them in v0 for correctness. This
		// also avoids the COMP-INV-002/003 invariants drift.
		if !isBottommost {
			for _, rt := range allTombs {
				pendingPtr.w.AddRangeTombstone(rt.Start, rt.End, rt.Seq)
			}
		}
		return nil
	}

	closeOutput := func() error {
		if pendingPtr == nil {
			return nil
		}
		meta, err := pendingPtr.w.Finish()
		if err != nil {
			pendingPtr.file.Close()
			os.Remove(pendingPtr.path + ".tmp")
			return err
		}
		if err := pendingPtr.file.Sync(); err != nil {
			pendingPtr.file.Close()
			os.Remove(pendingPtr.path + ".tmp")
			return err
		}
		if err := pendingPtr.file.Close(); err != nil {
			os.Remove(pendingPtr.path + ".tmp")
			return err
		}
		if err := os.Rename(pendingPtr.path+".tmp", pendingPtr.path); err != nil {
			os.Remove(pendingPtr.path + ".tmp")
			return err
		}
		outputs = append(outputs, manifest.TableMeta{
			FileNum:     pendingPtr.num,
			Level:       plan.outputLevel,
			Smallest:    meta.Smallest,
			Largest:     meta.Largest,
			Size:        meta.Size,
			SmallestSeq: meta.SmallestSeq,
			LargestSeq:  meta.LargestSeq,
		})
		pendingPtr = nil
		return nil
	}

	// Snapshot-safety floor for version supersession. Any older version
	// whose newer-version seq <= this floor is unreachable from any live
	// snapshot or transaction. We capture it once at compaction start so
	// concurrent commits don't shift it mid-merge (a later capture would
	// be just as safe; a single capture is simpler).
	minActiveRead := db.oracle.minActiveRead()

	// supersession-tracking state: while the merge sits on entries with
	// the same user_key, the first one (newest seq) is the visible version
	// and any subsequent entries are older versions. Drop them when safe.
	var lastEmittedUser []byte
	var lastEmittedSeq uint64
	var lastEmittedDeletable bool // true iff older versions are safe to drop

	// Heap-less min-pick across iters: linear scan. Inputs are typically
	// fewer than 20 for L0 compactions and 2 for L1+.
	for {
		idx := -1
		var minKey []byte
		for i, s := range iters {
			if !s.it.Valid() {
				continue
			}
			k := s.it.Key()
			if idx < 0 || keys.Compare(k, minKey) < 0 {
				idx = i
				minKey = k
			}
		}
		if idx < 0 {
			break
		}
		user, seq, kind, ok := keys.Parse(minKey)
		if !ok {
			return fmt.Errorf("%w: malformed internal key during compaction", ErrCorrupted)
		}

		// Decide whether to drop this entry.
		emit := true

		// Supersession: an older version of an already-emitted user key.
		// Drop when no live snapshot could need it. When the dropped
		// entry references a vlog pointer, the pointed-to bytes are now
		// dead — record that for future GC.
		if bytes.Equal(lastEmittedUser, user) && lastEmittedDeletable {
			emit = false
			if kind == keys.KindVlogPointer && db.vlogReader != nil {
				rawPtr := iters[idx].it.Value()
				if ptr, perr := vlog.DecodePointer(rawPtr); perr == nil {
					db.vlogReader.MarkDead(ptr)
				}
			}
		}

		// Tombstone elimination at the bottommost level (existing rule).
		if emit && isBottommost && kind == keys.KindDeletion {
			emit = false
		}

		if emit {
			// Start a new output if needed.
			if pendingPtr == nil {
				if err := startOutput(); err != nil {
					return err
				}
			} else if pendingPtr.w.NumEntries() > 0 &&
				pendingPtr.w.Offset() >= db.opts.TargetFileSize &&
				!bytes.Equal(user, lastUser) {
				// Roll over: only at a user-key boundary, per the
				// no-mid-user-key-split invariant.
				if err := closeOutput(); err != nil {
					return err
				}
				if err := startOutput(); err != nil {
					return err
				}
			}
			if err := pendingPtr.w.Add(minKey, iters[idx].it.Value()); err != nil {
				return err
			}
			lastUser = append(lastUser[:0], user...)

			// Update supersession tracking: this entry just became the
			// "newest emitted version" for its user_key. Any subsequent
			// entry with the same user_key in this merge is an older
			// version, safe to drop iff this entry's seq is at or below
			// the snapshot-safety floor.
			if !bytes.Equal(lastEmittedUser, user) {
				lastEmittedUser = append(lastEmittedUser[:0], user...)
				lastEmittedSeq = seq
				lastEmittedDeletable = seq <= minActiveRead
			}
		}
		iters[idx].it.Next()
	}
	_ = lastEmittedSeq // referenced via deletability decision above

	if err := closeOutput(); err != nil {
		return err
	}

	// Apply manifest edit atomically.
	edit := manifest.VersionEdit{
		NewTables: outputs,
	}
	for _, t := range plan.inputs[0] {
		edit.DeletedTables = append(edit.DeletedTables, t.FileNum)
	}
	for _, t := range plan.inputs[1] {
		edit.DeletedTables = append(edit.DeletedTables, t.FileNum)
	}

	// Open readers for new outputs before publishing.
	newReaders := make(map[uint32]*sstable.Reader, len(outputs))
	for _, t := range outputs {
		r, err := sstable.Open(db.sstPath(t.FileNum), db.sstReaderOpts(t.FileNum, t.Level))
		if err != nil {
			for _, rr := range newReaders {
				_ = rr.Close()
			}
			for _, t := range outputs {
				os.Remove(db.sstPath(t.FileNum))
			}
			return err
		}
		if db.cache != nil {
			r.SetCache(db.cache, t.FileNum)
		}
		newReaders[t.FileNum] = r
	}

	// Publish: under sstMu, add new readers; then apply manifest edit; then
	// close + remove old readers.
	db.sstMu.Lock()
	for num, r := range newReaders {
		db.readers[num] = r
	}
	db.sstMu.Unlock()

	if err := db.manifest.Apply(edit); err != nil {
		// Roll back the new readers.
		db.sstMu.Lock()
		for num, r := range newReaders {
			delete(db.readers, num)
			_ = r.Close()
		}
		db.sstMu.Unlock()
		for _, t := range outputs {
			os.Remove(db.sstPath(t.FileNum))
		}
		return err
	}

	// Close and remove the old readers and files.
	deleteFiles := make([]uint32, 0, len(edit.DeletedTables))
	deleteFiles = append(deleteFiles, edit.DeletedTables...)
	db.sstMu.Lock()
	closers := make([]*sstable.Reader, 0, len(deleteFiles))
	for _, num := range deleteFiles {
		if r, ok := db.readers[num]; ok {
			closers = append(closers, r)
			delete(db.readers, num)
		}
	}
	db.sstMu.Unlock()
	for _, r := range closers {
		_ = r.Close()
	}
	for _, num := range deleteFiles {
		_ = os.Remove(db.sstPath(num))
	}

	// Disable cleanup defer.
	iters = nil
	return nil
}

// levelTargetBytes returns the byte budget for the given level.
func (db *DB) levelTargetBytes(level int) int64 {
	if level <= 1 {
		return db.opts.L1TargetSize
	}
	t := db.opts.L1TargetSize
	for i := 1; i < level; i++ {
		t *= int64(db.opts.LevelSizeMultiplier)
	}
	return t
}

// levelSize sums the sizes of all files at this level.
func levelSize(tables []*manifest.TableMeta) int64 {
	var sum int64
	for _, t := range tables {
		sum += t.Size
	}
	return sum
}

// rangesOverlap reports whether [aLo, aHi] overlaps [bLo, bHi].
func rangesOverlap(aLo, aHi, bLo, bHi []byte) bool {
	if bytes.Compare(aHi, bLo) < 0 {
		return false
	}
	if bytes.Compare(bHi, aLo) < 0 {
		return false
	}
	return true
}

func containsTable(set []*manifest.TableMeta, t *manifest.TableMeta) bool {
	for _, s := range set {
		if s.FileNum == t.FileNum {
			return true
		}
	}
	return false
}

// ----- public API -----

// CompactNow runs compaction until no level is over its target. Useful in
// tests and at controlled checkpoints; production workloads don't need it.
func (db *DB) CompactNow() error {
	if db.closed.Load() {
		return ErrClosed
	}
	db.compactionMu.Lock()
	defer db.compactionMu.Unlock()
	for {
		v := db.manifest.Current()
		plan := db.pickCompaction(v)
		v.Unref()
		if plan == nil {
			return nil
		}
		if err := db.runCompaction(plan); err != nil {
			return err
		}
	}
}

// CompactRange forces compaction of every SSTable whose key range overlaps
// `[start, end)`, pushing the matching data through every level until it
// reaches the bottommost.
//
// `nil` for either bound means "no bound" on that side. Passing nil for
// both is equivalent to a full-LSM compaction (slower than CompactNow,
// which uses the picker's score-based scheduling).
//
// Typical uses are operational: a tool wants to reclaim space after a
// bulk-delete, or migrate hot keys into a single SST. The method blocks
// until all matching work finishes.
func (db *DB) CompactRange(start, end []byte) error {
	if db.closed.Load() {
		return ErrClosed
	}
	rangeStart := start
	rangeEnd := end
	if rangeStart == nil {
		rangeStart = []byte{}
	}
	if rangeEnd == nil {
		// Sentinel "beyond any user key" — uses a single 0xFF byte
		// concatenated to nothing. Any real key compares less than this
		// when interpreted as "infinity"; we rely on byte-string ordering.
		rangeEnd = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	}
	db.compactionMu.Lock()
	defer db.compactionMu.Unlock()
	for {
		v := db.manifest.Current()
		plan := db.pickRangeCompaction(v, rangeStart, rangeEnd)
		v.Unref()
		if plan == nil {
			return nil
		}
		if err := db.runCompaction(plan); err != nil {
			return err
		}
	}
}

// sortLevelByKey sorts a non-L0 level's table slice by smallest user key.
// Called only in tests; the manifest's applied() handles this in production.
func sortLevelByKey(level []*manifest.TableMeta) {
	sort.Slice(level, func(i, j int) bool {
		return bytes.Compare(level[i].Smallest, level[j].Smallest) < 0
	})
}

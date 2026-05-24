package slate

import (
	"fmt"
	"os"

	"github.com/harimalladi/slate/internal/keys"
	"github.com/harimalladi/slate/internal/manifest"
	"github.com/harimalladi/slate/internal/memtable"
	"github.com/harimalladi/slate/internal/sstable"
)

// flusherLoop drains the immutable queue, writing each memtable to an L0
// SSTable and applying the corresponding manifest edit. The loop exits when
// flushTrigger is closed and the immutable queue is empty.
func (db *DB) flusherLoop() {
	defer close(db.flusherDone)
	for {
		_, open := <-db.flushTrigger
		// Drain everything available before checking whether we were closed.
		db.drainImmutables()
		if !open {
			// One last drain in case rotate raced our drain above.
			db.drainImmutables()
			return
		}
	}
}

func (db *DB) drainImmutables() {
	for {
		db.rotMu.Lock()
		if len(db.immutable) == 0 {
			db.rotMu.Unlock()
			break
		}
		mt := db.immutable[0]
		db.rotMu.Unlock()
		if err := db.flushOne(mt); err != nil {
			// In v0 we cannot continue past a flush failure: subsequent
			// writes would race the failed memtable. Log and bail; future
			// versions will transition the engine to the broken state.
			fmt.Fprintf(os.Stderr, "slate: flush failed: %v\n", err)
			return
		}
		db.rotMu.Lock()
		// Pop the just-flushed memtable from the head.
		db.immutable = db.immutable[1:]
		db.rotMu.Unlock()
	}
	// After a flush sequence, opportunistically run compaction until no
	// level is over its target. The compaction mutex is held only while
	// pick+run executes — concurrent CompactNow callers wait for us.
	const maxCompactionsPerDrain = 32
	db.compactionMu.Lock()
	defer db.compactionMu.Unlock()
	for i := 0; i < maxCompactionsPerDrain; i++ {
		v := db.manifest.Current()
		plan := db.pickCompaction(v)
		v.Unref()
		if plan == nil {
			break
		}
		if err := db.runCompaction(plan); err != nil {
			fmt.Fprintf(os.Stderr, "slate: compaction failed: %v\n", err)
			break
		}
	}
}

// flushOne writes the given memtable to an L0 SSTable and applies the
// manifest edit. It is safe to call concurrently with reads (the L0 file is
// not visible to Get until the manifest is updated).
func (db *DB) flushOne(mt *memtable.Memtable) error {
	if mt.MaxSeq() == 0 {
		// Empty memtable — nothing to flush, just drop it.
		return nil
	}
	fileNum := db.manifest.AllocFileNum()
	path := db.sstPath(fileNum)
	tmpPath := path + ".tmp"

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	w := sstable.NewWriter(f, db.sstWriterOpts(fileNum, 0))
	it := mt.NewRawIterator()
	for it.First(); it.Valid(); it.Next() {
		if err := w.Add(it.Key(), it.Value()); err != nil {
			f.Close()
			os.Remove(tmpPath)
			return err
		}
	}
	// Propagate range tombstones from the memtable's parallel skiplist.
	// Without this, DeleteRange would silently lose its effect once the
	// memtable is flushed and the originating WAL segment truncated.
	for _, rt := range mt.RangeTombstones() {
		w.AddRangeTombstone(rt.Start, rt.End, rt.Seq)
	}
	meta, err := w.Finish()
	if err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Open a reader against the new file and publish it before the manifest
	// edit so a concurrent Get observes a consistent file set.
	reader, err := sstable.Open(path, db.sstReaderOpts(fileNum, 0))
	if err != nil {
		os.Remove(path)
		return err
	}
	if db.cache != nil {
		reader.SetCache(db.cache, fileNum)
	}
	db.sstMu.Lock()
	db.readers[fileNum] = reader
	db.sstMu.Unlock()

	tm := manifest.TableMeta{
		FileNum:     fileNum,
		Level:       0,
		Smallest:    meta.Smallest,
		Largest:     meta.Largest,
		Size:        meta.Size,
		SmallestSeq: meta.SmallestSeq,
		LargestSeq:  meta.LargestSeq,
	}
	walFileNum := uint32(0)
	if db.wal != nil {
		walFileNum = db.wal.ActiveFileNum()
	}
	edit := manifest.VersionEdit{
		NewTables:       []manifest.TableMeta{tm},
		HasLastSequence: true,
		LastSequence:    mt.MaxSeq(),
		HasFlushedWAL:   true,
		FlushedWAL:      manifest.WALCheckpoint{FileNum: walFileNum, Seq: mt.MaxSeq()},
		HasNextFileNum:  true,
		NextFileNum:     fileNum + 1,
	}
	if err := db.manifest.Apply(edit); err != nil {
		// Manifest apply failed: roll back the reader and unlink the file.
		db.sstMu.Lock()
		delete(db.readers, fileNum)
		db.sstMu.Unlock()
		_ = reader.Close()
		os.Remove(path)
		return err
	}
	// Truncate WAL segments strictly older than the active one at flush
	// time. Records in those segments are durable in SSTables now, so
	// they're no longer needed for recovery.
	if walFileNum > 0 && db.wal != nil {
		db.wal.DeleteBefore(walFileNum)
	}

	// Memtable arena is now safe to release. We don't pool yet — let the GC
	// reclaim it when no iterator references remain.
	_ = keys.KindInvalid // package retention; keys is used elsewhere
	return nil
}

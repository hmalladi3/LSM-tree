package slate

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// waitForFlush waits for the immutable queue to drain. Used by tests that
// trigger memtable rotation and then need the resulting L0 SST visible.
func waitForFlush(t *testing.T, db *DB) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		db.rotMu.Lock()
		n := len(db.immutable)
		db.rotMu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("flush did not drain within 5s")
}

// countSSTFiles returns the number of *.sst files in the database directory.
func countSSTFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "sst"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		t.Fatal(err)
	}
	count := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			count++
		}
	}
	return count
}

// Force a memtable rotation by writing enough bytes to exceed the configured
// MemtableSize. Returns when the rotation has actually occurred.
func forceRotation(t *testing.T, db *DB) {
	t.Helper()
	// Each entry costs ~ (key + value + overhead) bytes. We aim to overflow
	// the active memtable by writing a generous multiple of its capacity.
	target := db.memtable.Cap() * 2
	written := 0
	for written < target {
		k := []byte(fmt.Sprintf("fill-%010d", written))
		v := bytes.Repeat([]byte{'x'}, 256)
		if err := db.Set(k, v); err != nil {
			t.Fatalf("forceRotation: %v", err)
		}
		written += len(k) + len(v) + 32
	}
}

func TestMemtableRotation_ProducesL0File(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10 // tiny memtable to force frequent flush
	opts.DefaultDurability = NoSync
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}

	forceRotation(t, db)
	waitForFlush(t, db)

	if count := countSSTFiles(t, dir); count == 0 {
		t.Fatalf("expected at least one SST file after rotation, found %d", count)
	}
	db.Close()
}

func TestGet_AfterFlush_ReadsFromL0(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)

	const n = 200
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k-%05d", i))
		val := []byte(fmt.Sprintf("v-%05d", i))
		if err := db.Set(key, val); err != nil {
			t.Fatalf("Set(%d): %v", i, err)
		}
	}
	forceRotation(t, db)
	waitForFlush(t, db)

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k-%05d", i))
		want := []byte(fmt.Sprintf("v-%05d", i))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%s) = %q want %q", key, got, want)
		}
	}
	db.Close()
}

func TestRecoveryAfterFlush_ReadsFromL0(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)

	const n = 200
	for i := 0; i < n; i++ {
		db.Set([]byte(fmt.Sprintf("k-%05d", i)), []byte(fmt.Sprintf("v-%05d", i)))
	}
	forceRotation(t, db)
	waitForFlush(t, db)
	db.Sync()
	db.Close()

	// Re-open and read every key. With the flushed L0 in place, the WAL
	// replay should observe LastFlushedWAL and skip the already-flushed
	// records; reads then resolve via L0.
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k-%05d", i))
		want := []byte(fmt.Sprintf("v-%05d", i))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("post-recovery Get(%s): %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%s) = %q want %q", key, got, want)
		}
	}
}

func TestDeletion_PersistsThroughFlush(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)

	db.Set([]byte("k"), []byte("v"))
	db.Delete([]byte("k"))
	forceRotation(t, db)
	waitForFlush(t, db)
	db.Sync()
	db.Close()

	db, _ = Open(dir, opts)
	defer db.Close()
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Errorf("deleted key visible after flush + recovery: %v", err)
	}
}

func TestOverwrite_LatestVisibleAcrossLayers(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()

	// First version goes to L0 after forced rotation.
	db.Set([]byte("k"), []byte("v1"))
	forceRotation(t, db)
	waitForFlush(t, db)
	// Second version lives in the active memtable (newer layer).
	db.Set([]byte("k"), []byte("v2"))

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v2")) {
		t.Errorf("after overwrite, Get = %q, want v2", got)
	}
}

func TestManyFlushes_ProduceManyL0Files(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Three full passes through the memtable.
	for round := 0; round < 3; round++ {
		for i := 0; i < 200; i++ {
			db.Set([]byte(fmt.Sprintf("r%d-k-%04d", round, i)), bytes.Repeat([]byte{'x'}, 128))
		}
		forceRotation(t, db)
		waitForFlush(t, db)
	}

	if count := countSSTFiles(t, dir); count < 2 {
		t.Errorf("expected multiple SST files, got %d", count)
	}

	// Every key from every round must still be readable.
	for round := 0; round < 3; round++ {
		for i := 0; i < 200; i++ {
			key := []byte(fmt.Sprintf("r%d-k-%04d", round, i))
			if _, err := db.Get(key); err != nil {
				t.Errorf("round=%d key=%d: %v", round, i, err)
			}
		}
	}
}

func TestWALReplay_SkipsAlreadyFlushedRecords(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)

	// Two batches separated by a forced flush.
	for i := 0; i < 100; i++ {
		db.Set([]byte(fmt.Sprintf("a-%05d", i)), []byte("v"))
	}
	forceRotation(t, db)
	waitForFlush(t, db)
	// At this point, the manifest's LastFlushedWAL should be advanced and
	// the WAL records for the "a-" batch should be skipped on next replay.

	for i := 0; i < 50; i++ {
		db.Set([]byte(fmt.Sprintf("b-%05d", i)), []byte("v"))
	}
	db.Sync()
	db.Close()

	db, _ = Open(dir, opts)
	defer db.Close()
	// Both batches should be visible. The first via L0, the second via the
	// recovered memtable.
	for i := 0; i < 100; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("a-%05d", i))); err != nil {
			t.Errorf("'a' batch lost: %v", err)
		}
	}
	for i := 0; i < 50; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("b-%05d", i))); err != nil {
			t.Errorf("'b' batch lost: %v", err)
		}
	}
}

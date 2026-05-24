package slate

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/harimalladi/slate/internal/manifest"
)

// countFilesPerLevel returns the file count at each LSM level.
func countFilesPerLevel(t *testing.T, db *DB) [manifest.NumLevels]int {
	t.Helper()
	v := db.manifest.Current()
	defer v.Unref()
	var out [manifest.NumLevels]int
	for i, lvl := range v.Tables {
		out[i] = len(lvl)
	}
	return out
}

func TestCompaction_L0ToL1_HappensAutomatically(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 2 // trigger after just two L0 files
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Three flushes; the picker should compact L0 → L1 as soon as the L0
	// file count reaches the trigger.
	for round := 0; round < 3; round++ {
		for i := 0; i < 50; i++ {
			db.Set([]byte(fmt.Sprintf("k-%03d", i)), []byte(fmt.Sprintf("r%d", round)))
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
	}

	c := countFilesPerLevel(t, db)
	if c[0] >= opts.L0CompactionTrigger {
		t.Errorf("L0 file count %d still >= trigger %d after Flush", c[0], opts.L0CompactionTrigger)
	}
	if c[1] == 0 {
		t.Errorf("expected at least one L1 file after compaction, got %d", c[1])
	}
}

func TestCompaction_KeysRemainReadable(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 2
	db, _ := Open(dir, opts)
	defer db.Close()

	const n = 500
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k-%05d", i))
		val := []byte(fmt.Sprintf("v-%05d", i))
		db.Set(key, val)
		if i%100 == 99 {
			db.Flush()
		}
	}
	if err := db.CompactNow(); err != nil {
		t.Fatal(err)
	}

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
}

func TestCompaction_OverwriteResolvesToLatest(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 2
	db, _ := Open(dir, opts)
	defer db.Close()

	for round := 0; round < 3; round++ {
		for i := 0; i < 10; i++ {
			key := []byte(fmt.Sprintf("k-%02d", i))
			val := []byte(fmt.Sprintf("r%d-v", round))
			db.Set(key, val)
		}
		db.Flush()
	}
	db.CompactNow()

	// Every key should resolve to the latest write (round 2).
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("k-%02d", i))
		got, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "r2-v" {
			t.Errorf("Get(%s) = %q, want r2-v", key, got)
		}
	}
}

func TestCompaction_RangeTombstones_SurviveL0ToL1(t *testing.T) {
	// Range tombstones written via Txn.DeleteRange must survive both the
	// memtable flush AND the subsequent L0→L1 compaction. Regression
	// check covering the SST format-v2 range_del block path.
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 2
	db, _ := Open(dir, opts)
	defer db.Close()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		db.Set([]byte(k), []byte(k))
	}
	db.Update(func(txn *Txn) error {
		return txn.DeleteRange([]byte("b"), []byte("e"))
	})
	db.Flush()
	db.Flush() // second flush to push to L1 via compaction
	if err := db.CompactNow(); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"b", "c", "d"} {
		if _, err := db.Get([]byte(k)); !errors.Is(err, ErrNotFound) {
			t.Errorf("post-compaction %s should be deleted: %v", k, err)
		}
	}
	for _, k := range []string{"a", "e"} {
		if _, err := db.Get([]byte(k)); err != nil {
			t.Errorf("post-compaction %s should be visible: %v", k, err)
		}
	}
}

func TestCompaction_SupersedesAndMarksVlogDead(t *testing.T) {
	// Overwriting the same key many times produces many old versions in
	// the LSM and old vlog entries. Compaction should drop the older
	// versions AND mark their vlog bytes dead.
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 2
	opts.ValueThreshold = 64 // force vlog spill
	db, _ := Open(dir, opts)
	defer db.Close()

	bigVal := bytes.Repeat([]byte("X"), 512)
	for round := 0; round < 5; round++ {
		val := append([]byte{}, bigVal...)
		val[0] = byte('a' + round)
		db.Set([]byte("hot-key"), val)
		db.Flush()
	}
	if err := db.CompactNow(); err != nil {
		t.Fatal(err)
	}

	// The newest value must still resolve.
	got, err := db.Get([]byte("hot-key"))
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != byte('a'+4) {
		t.Errorf("Get(hot-key)[0] = %q, want %q", got[0], byte('a'+4))
	}

	// Some vlog dead-bytes must have been recorded — the 4 superseded
	// versions each cost > value-size bytes.
	st := db.Stats()
	if st.VlogDeadBytes == 0 {
		t.Errorf("expected non-zero VlogDeadBytes after compaction, got 0 (stats=%+v)", st)
	}
}

func TestCompaction_TombstonesShadowOlderVersions(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 2
	db, _ := Open(dir, opts)
	defer db.Close()

	db.Set([]byte("k1"), []byte("v1"))
	db.Set([]byte("k2"), []byte("v2"))
	db.Flush()
	db.Delete([]byte("k1"))
	db.Set([]byte("k3"), []byte("v3"))
	db.Flush()
	if err := db.CompactNow(); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get([]byte("k1")); !errors.Is(err, ErrNotFound) {
		t.Errorf("k1 reappeared after compaction: %v", err)
	}
	if v, _ := db.Get([]byte("k2")); !bytes.Equal(v, []byte("v2")) {
		t.Errorf("k2 = %q", v)
	}
	if v, _ := db.Get([]byte("k3")); !bytes.Equal(v, []byte("v3")) {
		t.Errorf("k3 = %q", v)
	}
}

func TestCompaction_L1FilesAreNonOverlapping(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 2
	opts.TargetFileSize = 16 << 10 // small to force multiple L1 outputs
	db, _ := Open(dir, opts)
	defer db.Close()

	// Write enough data to force multiple flushes and at least one L1
	// compaction with multiple outputs.
	const n = 1000
	for i := 0; i < n; i++ {
		db.Set([]byte(fmt.Sprintf("k-%05d", i)), bytes.Repeat([]byte("v"), 64))
		if i%100 == 99 {
			db.Flush()
		}
	}
	if err := db.CompactNow(); err != nil {
		t.Fatal(err)
	}

	v := db.manifest.Current()
	defer v.Unref()
	// At L1, files must be non-overlapping and sorted by smallest key.
	prevLargest := []byte{}
	for _, t1 := range v.Tables[1] {
		if len(prevLargest) > 0 && bytes.Compare(prevLargest, t1.Smallest) >= 0 {
			t.Fatalf("L1 overlap: prevLargest=%q next.Smallest=%q", prevLargest, t1.Smallest)
		}
		prevLargest = t1.Largest
	}
}

func TestCompaction_RecoveryAcrossCompaction(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 2
	db, _ := Open(dir, opts)

	for i := 0; i < 500; i++ {
		db.Set([]byte(fmt.Sprintf("k-%05d", i)), []byte(fmt.Sprintf("v-%05d", i)))
		if i%100 == 99 {
			db.Flush()
		}
	}
	if err := db.CompactNow(); err != nil {
		t.Fatal(err)
	}
	db.Sync()
	db.Close()

	// Re-open: the manifest should describe the post-compaction file set
	// and every key must still be readable.
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 500; i++ {
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

func TestCompactRange_MovesOverlappingFiles(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 8 // keep things at L0 so CompactRange has work
	db, _ := Open(dir, opts)
	defer db.Close()

	for i := 0; i < 200; i++ {
		db.Set([]byte(fmt.Sprintf("k-%05d", i)), []byte(fmt.Sprintf("v-%05d", i)))
		if i%40 == 39 {
			db.Flush()
		}
	}

	pre := countFilesPerLevel(t, db)
	if pre[0] < 2 {
		t.Fatalf("expected multiple L0 files before CompactRange, got %d", pre[0])
	}

	if err := db.CompactRange([]byte("k-00000"), []byte("k-00100")); err != nil {
		t.Fatal(err)
	}

	// After CompactRange every k in [0,100) must still be readable.
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("k-%05d", i))
		want := []byte(fmt.Sprintf("v-%05d", i))
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%s) = %q want %q", k, got, want)
		}
	}
	// Any level below L0 holding data implies the range moved down.
	post := countFilesPerLevel(t, db)
	movedDown := false
	for lvl := 1; lvl < manifest.NumLevels; lvl++ {
		if post[lvl] > 0 {
			movedDown = true
			break
		}
	}
	if !movedDown {
		t.Errorf("CompactRange did not produce any output below L0: %v", post)
	}
}

func TestCompactRange_NilBoundsCompactsEverything(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 8
	db, _ := Open(dir, opts)
	defer db.Close()

	for i := 0; i < 200; i++ {
		db.Set([]byte(fmt.Sprintf("k-%05d", i)), []byte("v"))
		if i%40 == 39 {
			db.Flush()
		}
	}
	if err := db.CompactRange(nil, nil); err != nil {
		t.Fatal(err)
	}
	post := countFilesPerLevel(t, db)
	if post[0] != 0 {
		t.Errorf("CompactRange(nil, nil) left %d L0 files", post[0])
	}
}

func TestCompaction_IteratorAfterCompaction(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 2
	db, _ := Open(dir, opts)
	defer db.Close()

	const n = 200
	for i := 0; i < n; i++ {
		db.Set([]byte(fmt.Sprintf("k-%04d", i)), []byte(fmt.Sprintf("v-%04d", i)))
		if i%50 == 49 {
			db.Flush()
		}
	}
	if err := db.CompactNow(); err != nil {
		t.Fatal(err)
	}

	it := db.NewIterator(nil)
	defer it.Close()
	count := 0
	prev := ""
	for it.First(); it.Valid(); it.Next() {
		k := string(it.Key())
		if prev != "" && prev >= k {
			t.Fatalf("out of order: %s then %s", prev, k)
		}
		prev = k
		count++
	}
	if count != n {
		t.Errorf("iterated %d, want %d", count, n)
	}
}

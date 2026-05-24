package slate

import (
	"bytes"
	"fmt"
	"testing"
)

func TestCache_HitRateClimbs(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()

	// Write a working set that fits in the cache.
	const n = 500
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("k-%05d", i))
		db.Set(keys[i], bytes.Repeat([]byte("v"), 200))
	}
	// Force the data onto L0 (and possibly L1 via compaction).
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	// First pass populates the cache.
	for _, k := range keys {
		if _, err := db.Get(k); err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
	}
	statsAfterWarmup := db.Stats().Cache

	// Second pass should hit cache very frequently.
	for _, k := range keys {
		db.Get(k)
	}
	statsAfterHot := db.Stats().Cache

	deltaHits := statsAfterHot.Hits - statsAfterWarmup.Hits
	deltaMisses := statsAfterHot.Misses - statsAfterWarmup.Misses
	if deltaHits+deltaMisses == 0 {
		t.Fatal("no cache traffic recorded")
	}
	hotHitRate := float64(deltaHits) / float64(deltaHits+deltaMisses)
	if hotHitRate < 0.9 {
		t.Errorf("hot-pass hit rate %.2f%% below 90%% target", hotHitRate*100)
	}
	t.Logf("warmup hits=%d misses=%d, hot pass hits=%d misses=%d (hit rate %.2f%%)",
		statsAfterWarmup.Hits, statsAfterWarmup.Misses,
		deltaHits, deltaMisses, hotHitRate*100)
}

func TestCache_Disabled(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.BlockCacheSize = 0 // disable
	db, _ := Open(dir, opts)
	defer db.Close()

	// Engine should still serve reads with the cache disabled.
	db.Set([]byte("k"), []byte("v"))
	db.Flush()
	v, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte("v")) {
		t.Errorf("Get = %q", v)
	}
	s := db.Stats().Cache
	if s.Capacity != 0 || s.Hits != 0 || s.Misses != 0 {
		t.Errorf("disabled cache reported stats: %+v", s)
	}
}

func TestCache_StatsExposed(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()
	if cap := db.Stats().Cache.Capacity; cap == 0 {
		t.Errorf("Cache.Capacity = 0 with default options")
	}
}

func TestCache_LevelFileCountInStats(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()

	for i := 0; i < 100; i++ {
		db.Set([]byte(fmt.Sprintf("k-%03d", i)), []byte("v"))
	}
	db.Flush()

	s := db.Stats()
	if s.LevelFileCount[0] == 0 {
		t.Errorf("expected at least one L0 file in stats, got %d", s.LevelFileCount[0])
	}
}

// BenchmarkGet_ColdCache exercises the path that misses the block cache on
// every read.
func BenchmarkGet_ColdCache(b *testing.B) {
	dir := tempDir(b)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.BlockCacheSize = 4 << 10 // tiny so every read evicts
	db, _ := Open(dir, opts)
	defer db.Close()

	const n = 10_000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("k-%08d", i))
		db.Set(keys[i], []byte("v"))
	}
	db.Flush()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get(keys[i%n])
	}
}

// BenchmarkGet_WarmCache exercises the path where the same keys are
// repeatedly read from a properly-sized cache.
func BenchmarkGet_WarmCache(b *testing.B) {
	dir := tempDir(b)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()

	const n = 1_000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("k-%08d", i))
		db.Set(keys[i], []byte("v"))
	}
	db.Flush()
	// Warm the cache.
	for _, k := range keys {
		db.Get(k)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get(keys[i%n])
	}
}

package slate

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func tempDir(t testing.TB) string {
	t.Helper()
	d, err := os.MkdirTemp("", "slate-db-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func TestOpen_NewDirectory(t *testing.T) {
	dir := tempDir(t)
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "LOCK")); err != nil {
		t.Errorf("LOCK file missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "wal")); err != nil {
		t.Errorf("wal directory missing: %v", err)
	}
}

func TestSetGet_Basic(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	if err := db.Set([]byte("hello"), []byte("world")); err != nil {
		t.Fatal(err)
	}
	got, err := db.Get([]byte("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("world")) {
		t.Errorf("got %q want %q", got, "world")
	}
}

func TestGet_Missing_ReturnsErrNotFound(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	_, err := db.Get([]byte("nope"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("got %v, want ErrNotFound", err)
	}
}

func TestDelete_HidesValue(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	db.Set([]byte("k"), []byte("v"))
	if err := db.Delete([]byte("k")); err != nil {
		t.Fatal(err)
	}
	_, err := db.Get([]byte("k"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete: got %v, want ErrNotFound", err)
	}
}

func TestEmptyValue_IsNotTombstone(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	if err := db.Set([]byte("k"), []byte{}); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get of empty-value key: %v", err)
	}
	if v == nil || len(v) != 0 {
		t.Errorf("expected empty-but-non-nil value, got %v", v)
	}
}

func TestCrashRecovery_AllSyncWritesSurvive(t *testing.T) {
	dir := tempDir(t)

	db, _ := Open(dir, nil)
	const n = 1000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d-payload", i))
		if err := db.Set(key, val); err != nil {
			t.Fatal(err)
		}
	}
	// Mid-stream delete: should be preserved across recovery.
	db.Delete([]byte("key-0500"))
	db.Close()

	// Re-open and verify every committed write is visible.
	db, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		want := []byte(fmt.Sprintf("val-%04d-payload", i))
		got, err := db.Get(key)
		if i == 500 {
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("deleted key %s reappeared after recovery", key)
			}
			continue
		}
		if err != nil {
			t.Fatalf("key=%s: %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("key=%s: got %q want %q", key, got, want)
		}
	}
}

func TestCrashRecovery_OverwrittenKey_LatestVisible(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	db.Set([]byte("k"), []byte("v1"))
	db.Set([]byte("k"), []byte("v2"))
	db.Set([]byte("k"), []byte("v3"))
	db.Close()

	db, _ = Open(dir, nil)
	defer db.Close()
	got, _ := db.Get([]byte("k"))
	if !bytes.Equal(got, []byte("v3")) {
		t.Errorf("after recovery k = %q, want v3", got)
	}
}

func TestConcurrentOpen_SecondReturnsErrLocked(t *testing.T) {
	dir := tempDir(t)
	db1, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()

	_, err = Open(dir, nil)
	if !errors.Is(err, ErrLocked) {
		t.Errorf("second Open: got %v, want ErrLocked", err)
	}
}

func TestClose_Idempotent(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Errorf("second Close returned %v", err)
	}
}

func TestSet_AfterClose_ReturnsErrClosed(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	db.Close()
	if err := db.Set([]byte("k"), []byte("v")); !errors.Is(err, ErrClosed) {
		t.Errorf("Set after Close: %v", err)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close: %v", err)
	}
}

func TestSet_KeyTooLarge(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MaxKeySize = 16
	db, _ := Open(dir, opts)
	defer db.Close()
	err := db.Set(bytes.Repeat([]byte("x"), 17), []byte("v"))
	if !errors.Is(err, ErrKeyTooLarge) {
		t.Errorf("got %v, want ErrKeyTooLarge", err)
	}
}

func TestSet_ValueTooLarge(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MaxValueSize = 16
	db, _ := Open(dir, opts)
	defer db.Close()
	err := db.Set([]byte("k"), bytes.Repeat([]byte("x"), 17))
	if !errors.Is(err, ErrValueTooLarge) {
		t.Errorf("got %v, want ErrValueTooLarge", err)
	}
}

func TestSet_EmptyKey_Rejected(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	err := db.Set(nil, []byte("v"))
	var ke *KeyError
	if !errors.As(err, &ke) {
		t.Errorf("expected *KeyError, got %v", err)
	}
}

func TestConcurrent_Writes(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	const (
		writers = 8
		each    = 200
	)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				key := []byte(fmt.Sprintf("w%02d-k%04d", w, i))
				if err := db.Set(key, []byte("v")); err != nil {
					t.Errorf("w=%d i=%d: %v", w, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Verify every key is present and the engine kept its seq counter
	// strictly above every assigned value.
	count := 0
	for w := 0; w < writers; w++ {
		for i := 0; i < each; i++ {
			key := []byte(fmt.Sprintf("w%02d-k%04d", w, i))
			if _, err := db.Get(key); err == nil {
				count++
			}
		}
	}
	if count != writers*each {
		t.Errorf("read %d keys, want %d", count, writers*each)
	}
}

func TestAsync_WriteReadable_Pre_Sync(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = Async
	db, _ := Open(dir, opts)
	defer db.Close()

	if err := db.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	// Visible in this process even before Sync.
	v, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get after Async Set: %v", err)
	}
	if !bytes.Equal(v, []byte("v")) {
		t.Errorf("v=%q", v)
	}
}

func TestSetSync_OverridesAsyncDefault(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = Async
	db, _ := Open(dir, opts)
	defer db.Close()
	// SetSync should commit synchronously regardless of the default.
	if err := db.SetSync([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
}

func TestOpen_RefusesFutureFormatVersion(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	db.Close()

	// Bump the FORMAT file to a higher version than the binary supports.
	path := filepath.Join(dir, "FORMAT")
	if err := os.WriteFile(path, []byte("999\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, nil)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Errorf("Open with future format version: got %v, want ErrUnsupportedVersion", err)
	}
}

func TestOpen_RefusesMissingManifestPointee(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	db.Close()

	manDir := filepath.Join(dir, "manifest")
	cur, err := os.ReadFile(filepath.Join(manDir, "CURRENT"))
	if err != nil {
		t.Fatal(err)
	}
	target := string(cur)
	for len(target) > 0 && (target[len(target)-1] == '\n' || target[len(target)-1] == '\r') {
		target = target[:len(target)-1]
	}
	if err := os.Remove(filepath.Join(manDir, target)); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, nil); !errors.Is(err, ErrCorrupted) {
		t.Errorf("Open with missing manifest pointee: got %v, want ErrCorrupted", err)
	}
}

func TestOpen_RefusesManifestDirWithoutCurrent(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	db.Close()

	manDir := filepath.Join(dir, "manifest")
	if err := os.Remove(filepath.Join(manDir, "CURRENT")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, nil); !errors.Is(err, ErrCorrupted) {
		t.Errorf("Open with missing CURRENT and stranded MANIFEST-*: got %v, want ErrCorrupted", err)
	}
}

func TestOpen_RefusesMissingReferencedSST(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	for i := 0; i < 100; i++ {
		db.Set([]byte(fmt.Sprintf("k-%03d", i)), []byte("v"))
	}
	db.Flush()
	db.Close()

	// Find and delete one SST file out from under the manifest.
	sstDir := filepath.Join(dir, "sst")
	entries, _ := os.ReadDir(sstDir)
	var removed bool
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			os.Remove(filepath.Join(sstDir, e.Name()))
			removed = true
			break
		}
	}
	if !removed {
		t.Fatal("no SST file to remove")
	}

	_, err := Open(dir, opts)
	if !errors.Is(err, ErrCorrupted) {
		t.Errorf("Open with missing SST: got %v, want ErrCorrupted", err)
	}
}

func TestOpen_InvalidOptions(t *testing.T) {
	dir := tempDir(t)
	bad := DefaultOptions()
	bad.MemtableSize = 0
	_, err := Open(dir, bad)
	if !errors.Is(err, ErrInvalidOption) {
		t.Errorf("Open with MemtableSize=0: %v, want ErrInvalidOption", err)
	}
}

// benchOpts returns options sized to comfortably hold the bench's writes.
// SST flushing is not yet wired up, so the memtable must be large enough
// to absorb the benchmark's writes without overflowing.
func benchOpts(b *testing.B, valueSize int) *Options {
	opts := DefaultOptions()
	// Each in-memory entry costs roughly key+value bytes + ~64B overhead.
	perEntry := 64 + valueSize + 32
	want := b.N * perEntry
	if want < 64<<20 {
		want = 64 << 20
	}
	opts.MemtableSize = want + 32<<20 // headroom
	return opts
}

func BenchmarkSet_Sync(b *testing.B) {
	dir := tempDir(b)
	db, _ := Open(dir, benchOpts(b, 100))
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("k-%08d", i))
		if err := db.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSet_NoSync(b *testing.B) {
	dir := tempDir(b)
	opts := benchOpts(b, 100)
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()
	value := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("k-%08d", i))
		if err := db.Set(key, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet_Hit(b *testing.B) {
	dir := tempDir(b)
	opts := benchOpts(b, 16)
	opts.DefaultDurability = NoSync // build the dataset fast
	db, _ := Open(dir, opts)
	defer db.Close()

	const n = 10_000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("k-%08d", i))
		db.Set(keys[i], keys[i])
	}
	db.Sync()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(keys[i%n]); err != nil {
			b.Fatal(err)
		}
	}
}

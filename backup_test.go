package slate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestBackupRestore_RoundTrip(t *testing.T) {
	src := tempDir(t)

	// Populate the source database with a mix of inline + vlog values.
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.MemtableSize = 64 << 10
	opts.ValueThreshold = 256
	db, err := Open(src, opts)
	if err != nil {
		t.Fatal(err)
	}
	const n = 200
	for i := 0; i < n; i++ {
		var val []byte
		if i%2 == 0 {
			val = []byte(fmt.Sprintf("small-%05d", i))
		} else {
			val = bytes.Repeat([]byte{'L'}, 1024) // forces vlog
		}
		if err := db.Set([]byte(fmt.Sprintf("k-%05d", i)), val); err != nil {
			t.Fatal(err)
		}
	}
	db.Flush() // get some data into L0 SSTs
	db.Sync()

	// Take a backup into an in-memory buffer.
	var buf bytes.Buffer
	if err := db.Backup(context.Background(), &buf); err != nil {
		t.Fatalf("Backup: %v", err)
	}
	db.Close()

	// Restore into a fresh directory.
	dst := tempDir(t)
	if err := Restore(context.Background(), dst, &buf, nil); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	// Open the restored DB and verify every key.
	db2, err := Open(dst, opts)
	if err != nil {
		t.Fatalf("Open restored: %v", err)
	}
	defer db2.Close()
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k-%05d", i))
		got, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Get(%d) on restored DB: %v", i, err)
		}
		var want []byte
		if i%2 == 0 {
			want = []byte(fmt.Sprintf("small-%05d", i))
		} else {
			want = bytes.Repeat([]byte{'L'}, 1024)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("restored Get(%d) length=%d, want length=%d", i, len(got), len(want))
		}
	}
}

func TestBackup_PinsVersionAgainstCompaction(t *testing.T) {
	// While Backup is running, compaction may run too. The pinned Version
	// keeps the files we're streaming alive; the restored DB must see the
	// data that was visible at backup-start, not whatever compaction
	// produced midway.
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.MemtableSize = 64 << 10
	opts.L0CompactionTrigger = 2
	db, _ := Open(dir, opts)
	defer db.Close()

	for i := 0; i < 500; i++ {
		db.Set([]byte(fmt.Sprintf("k-%04d", i)), []byte(fmt.Sprintf("v-%04d", i)))
		if i%100 == 99 {
			db.Flush()
		}
	}

	var buf bytes.Buffer
	if err := db.Backup(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	if buf.Len() == 0 {
		t.Fatal("backup is empty")
	}

	// Run compaction post-backup; the backup buffer is already filled and
	// must not depend on the source's current file set.
	if err := db.CompactNow(); err != nil {
		t.Fatal(err)
	}

	dst := tempDir(t)
	if err := Restore(context.Background(), dst, &buf, nil); err != nil {
		t.Fatalf("Restore after compaction: %v", err)
	}
	rdb, err := Open(dst, opts)
	if err != nil {
		t.Fatalf("Open after compaction-then-restore: %v", err)
	}
	defer rdb.Close()
	for i := 0; i < 500; i++ {
		key := []byte(fmt.Sprintf("k-%04d", i))
		got, err := rdb.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		want := []byte(fmt.Sprintf("v-%04d", i))
		if !bytes.Equal(got, want) {
			t.Errorf("key %d mismatch", i)
		}
	}
}

func TestRestore_RejectsExistingDatabase(t *testing.T) {
	// Create a real DB, then try to restore over it.
	src := tempDir(t)
	srcDB, _ := Open(src, nil)
	srcDB.Set([]byte("k"), []byte("v"))
	var buf bytes.Buffer
	if err := srcDB.Backup(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	srcDB.Close()

	// Target dir contains a live database.
	dst := tempDir(t)
	live, _ := Open(dst, nil)
	live.Set([]byte("x"), []byte("y"))
	live.Close()
	// Mark restore complete to simulate a previous successful restore +
	// open. Open removes RESTORE_OK after a clean Open; we re-add it for
	// the test's purposes.
	os.WriteFile(filepath.Join(dst, "RESTORE_OK"), nil, 0o644)

	if err := Restore(context.Background(), dst, &buf, nil); !errors.Is(err, ErrDirectoryNotEmpty) {
		t.Errorf("Restore over live DB: got %v, want ErrDirectoryNotEmpty", err)
	}
}

func TestRestore_AutoCleansTornDir(t *testing.T) {
	src := tempDir(t)
	srcDB, _ := Open(src, nil)
	srcDB.Set([]byte("a"), []byte("alpha"))
	var buf bytes.Buffer
	srcDB.Backup(context.Background(), &buf)
	srcDB.Close()

	// Target dir has stray files but no RESTORE_OK and no CURRENT.
	dst := tempDir(t)
	os.WriteFile(filepath.Join(dst, "junk.txt"), []byte("leftover"), 0o644)
	os.MkdirAll(filepath.Join(dst, "sst"), 0o755)
	os.WriteFile(filepath.Join(dst, "sst", "000001.sst.tmp"), []byte("partial"), 0o644)

	if err := Restore(context.Background(), dst, &buf, nil); err != nil {
		t.Fatalf("Restore should auto-clean: %v", err)
	}
	db, err := Open(dst, nil)
	if err != nil {
		t.Fatalf("Open restored: %v", err)
	}
	defer db.Close()
	v, _ := db.Get([]byte("a"))
	if !bytes.Equal(v, []byte("alpha")) {
		t.Errorf("Get after auto-clean restore: got %q", v)
	}
}

func TestRestore_BadMagic(t *testing.T) {
	dst := tempDir(t)
	err := Restore(context.Background(), dst, bytes.NewReader([]byte("not a backup")), nil)
	if !errors.Is(err, ErrCorrupted) {
		t.Errorf("Restore bad magic: got %v, want ErrCorrupted", err)
	}
}

func TestRestore_DetectsTruncatedStream(t *testing.T) {
	src := tempDir(t)
	srcDB, _ := Open(src, nil)
	for i := 0; i < 10; i++ {
		srcDB.Set([]byte(fmt.Sprintf("k-%d", i)), []byte("v"))
	}
	var buf bytes.Buffer
	srcDB.Backup(context.Background(), &buf)
	srcDB.Close()

	full := buf.Bytes()
	truncated := full[:len(full)-50] // chop off the tail (including CRC)

	dst := tempDir(t)
	err := Restore(context.Background(), dst, bytes.NewReader(truncated), nil)
	if err == nil {
		t.Error("Restore of truncated stream should fail")
	}
}

func TestBackup_EncryptedDB_PreservesEncryption(t *testing.T) {
	src := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.EncryptionKey = freshKey(t)
	opts.MemtableSize = 64 << 10
	db, _ := Open(src, opts)
	for i := 0; i < 50; i++ {
		db.Set([]byte(fmt.Sprintf("k-%02d", i)), []byte(fmt.Sprintf("secret-%02d", i)))
	}
	db.Flush()
	var buf bytes.Buffer
	if err := db.Backup(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}
	db.Close()

	dst := tempDir(t)
	if err := Restore(context.Background(), dst, &buf, nil); err != nil {
		t.Fatal(err)
	}
	// Re-open with the same key — restored IDENTITY carries the source's
	// db_uuid + key_id, so HKDF derives the same DEK.
	rdb, err := Open(dst, opts)
	if err != nil {
		t.Fatalf("Open restored encrypted: %v", err)
	}
	defer rdb.Close()
	for i := 0; i < 50; i++ {
		got, err := rdb.Get([]byte(fmt.Sprintf("k-%02d", i)))
		if err != nil {
			t.Fatal(err)
		}
		want := []byte(fmt.Sprintf("secret-%02d", i))
		if !bytes.Equal(got, want) {
			t.Errorf("key %d: got %q want %q", i, got, want)
		}
	}
}

func TestBackup_ContextCancellation(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	for i := 0; i < 50; i++ {
		db.Set([]byte(fmt.Sprintf("k-%02d", i)), []byte("v"))
	}
	db.Flush()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before Backup runs
	err := db.Backup(ctx, &bytes.Buffer{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

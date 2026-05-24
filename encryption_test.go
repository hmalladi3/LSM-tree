package slate

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func freshKey(t testing.TB) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

func TestEncryption_RoundTripsThroughFlushAndReopen(t *testing.T) {
	dir := tempDir(t)
	key := freshKey(t)

	// First open: create the DB with encryption, write data, flush to L0,
	// close.
	{
		opts := DefaultOptions()
		opts.MemtableSize = 64 << 10
		opts.DefaultDurability = NoSync
		opts.EncryptionKey = key
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 200; i++ {
			db.Set([]byte(fmt.Sprintf("k-%05d", i)), []byte(fmt.Sprintf("v-%05d", i)))
		}
		if err := db.Flush(); err != nil {
			t.Fatal(err)
		}
		db.Sync()
		db.Close()
	}

	// Re-open with the same key — all data must still be visible.
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.EncryptionKey = key
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 200; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("k-%05d", i)))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		want := []byte(fmt.Sprintf("v-%05d", i))
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%d) = %q want %q", i, got, want)
		}
	}
}

func TestEncryption_WrongKey_Rejected(t *testing.T) {
	dir := tempDir(t)
	key1 := freshKey(t)

	{
		opts := DefaultOptions()
		opts.EncryptionKey = key1
		db, err := Open(dir, opts)
		if err != nil {
			t.Fatal(err)
		}
		db.Set([]byte("k"), []byte("v"))
		db.Close()
	}

	// Wrong key.
	opts := DefaultOptions()
	opts.EncryptionKey = freshKey(t)
	_, err := Open(dir, opts)
	if !errors.Is(err, ErrEncryptionKeyMismatch) {
		t.Errorf("Open with wrong key: got %v, want ErrEncryptionKeyMismatch", err)
	}
}

func TestEncryption_OpenEncryptedDBWithoutKey_Rejected(t *testing.T) {
	dir := tempDir(t)
	{
		opts := DefaultOptions()
		opts.EncryptionKey = freshKey(t)
		db, _ := Open(dir, opts)
		db.Set([]byte("k"), []byte("v"))
		db.Close()
	}
	// Open with no key.
	_, err := Open(dir, nil)
	if !errors.Is(err, ErrEncryptionKeyMismatch) {
		t.Errorf("Open without key: got %v", err)
	}
}

func TestEncryption_OpenUnencryptedDBWithKey_Rejected(t *testing.T) {
	dir := tempDir(t)
	{
		db, _ := Open(dir, nil)
		db.Set([]byte("k"), []byte("v"))
		db.Close()
	}
	opts := DefaultOptions()
	opts.EncryptionKey = freshKey(t)
	_, err := Open(dir, opts)
	if !errors.Is(err, ErrEncryptionKeyMismatch) {
		t.Errorf("Open with key on unencrypted DB: got %v", err)
	}
}

func TestEncryption_InvalidKey_Rejected(t *testing.T) {
	dir := tempDir(t)
	for _, n := range []int{1, 16, 31, 33, 64} {
		opts := DefaultOptions()
		opts.EncryptionKey = make([]byte, n)
		_, err := Open(dir, opts)
		if !errors.Is(err, ErrInvalidOption) {
			t.Errorf("EncryptionKey size %d: got %v, want ErrInvalidOption", n, err)
		}
	}
}

func TestEncryption_DiskBytesAreCiphertext(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.EncryptionKey = freshKey(t)
	db, _ := Open(dir, opts)
	plaintext := []byte("super-secret-value-not-on-disk-in-the-clear")
	db.Set([]byte("k"), plaintext)
	db.Flush()
	db.Close()

	// Scan every .sst file and assert the plaintext does NOT appear.
	sstDir := filepath.Join(dir, "sst")
	entries, _ := os.ReadDir(sstDir)
	if len(entries) == 0 {
		t.Fatal("no SST files produced")
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".sst" {
			continue
		}
		data, _ := os.ReadFile(filepath.Join(sstDir, e.Name()))
		if bytes.Contains(data, plaintext) {
			t.Errorf("plaintext leaked in %s", e.Name())
		}
	}
}

func TestEncryption_Compaction_PreservesEncryption(t *testing.T) {
	// Compaction reads inputs through the codec, writes outputs through
	// the codec — round-trip must preserve plaintext.
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	opts.L0CompactionTrigger = 2
	opts.EncryptionKey = freshKey(t)
	db, _ := Open(dir, opts)
	defer db.Close()

	const n = 300
	for i := 0; i < n; i++ {
		db.Set([]byte(fmt.Sprintf("k-%04d", i)), []byte(fmt.Sprintf("v-%04d", i)))
		if i%50 == 49 {
			db.Flush()
		}
	}
	if err := db.CompactNow(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("k-%04d", i))
		want := []byte(fmt.Sprintf("v-%04d", i))
		got, err := db.Get(key)
		if err != nil {
			t.Fatalf("post-compaction Get(%d): %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%d) = %q want %q", i, got, want)
		}
	}
}

func TestEncryption_IdentityFileLayout(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.EncryptionKey = freshKey(t)
	db, _ := Open(dir, opts)
	db.Close()

	data, err := os.ReadFile(filepath.Join(dir, "IDENTITY"))
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 32 {
		t.Errorf("IDENTITY size = %d, want 32", len(data))
	}
	// key_id (bytes 16-32) must NOT be all zero in an encrypted DB.
	allZero := true
	for _, b := range data[16:32] {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		t.Error("encrypted DB's IDENTITY has all-zero key_id")
	}
}

func TestEncryption_UnencryptedDB_HasZeroKeyID(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	db.Close()
	data, _ := os.ReadFile(filepath.Join(dir, "IDENTITY"))
	for i := 16; i < 32; i++ {
		if data[i] != 0 {
			t.Errorf("unencrypted DB key_id[%d] = %d, want 0", i-16, data[i])
		}
	}
}

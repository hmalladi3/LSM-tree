package slate

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestVlog_SmallValues_StayInline(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.ValueThreshold = 1024
	db, _ := Open(dir, opts)
	defer db.Close()

	smallValue := []byte("small")
	for i := 0; i < 100; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k-%03d", i)), smallValue); err != nil {
			t.Fatal(err)
		}
	}
	db.Flush()

	// vlog directory may have a (newly-opened) empty active segment, but
	// the total bytes written to vlog files should equal the empty-file
	// overhead — i.e., zero entries.
	entries, _ := os.ReadDir(filepath.Join(dir, "vlog"))
	totalBytes := int64(0)
	for _, e := range entries {
		info, _ := e.Info()
		totalBytes += info.Size()
	}
	if totalBytes != 0 {
		t.Errorf("expected 0 bytes in vlog for small-value workload, got %d", totalBytes)
	}
}

func TestVlog_LargeValues_RouteToVlog(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.ValueThreshold = 64 // tiny threshold to force vlog usage
	db, _ := Open(dir, opts)
	defer db.Close()

	largeValue := bytes.Repeat([]byte("X"), 4096)
	for i := 0; i < 50; i++ {
		if err := db.Set([]byte(fmt.Sprintf("k-%02d", i)), largeValue); err != nil {
			t.Fatal(err)
		}
	}

	// Read every key back through the engine.
	for i := 0; i < 50; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("k-%02d", i)))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if !bytes.Equal(got, largeValue) {
			t.Errorf("Get(%d) length = %d, want %d", i, len(got), len(largeValue))
		}
	}

	// Vlog file should contain roughly N * (len(value) + 8) bytes.
	entries, _ := os.ReadDir(filepath.Join(dir, "vlog"))
	var totalBytes int64
	for _, e := range entries {
		info, _ := e.Info()
		totalBytes += info.Size()
	}
	expectedMin := int64(50 * (len(largeValue) + 8))
	if totalBytes < expectedMin {
		t.Errorf("vlog bytes = %d, want >= %d", totalBytes, expectedMin)
	}
}

func TestVlog_SurvivesFlushAndReopen(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.ValueThreshold = 64
	opts.MemtableSize = 64 << 10

	largeValue := bytes.Repeat([]byte("L"), 2048)
	{
		db, _ := Open(dir, opts)
		for i := 0; i < 100; i++ {
			db.Set([]byte(fmt.Sprintf("k-%05d", i)), largeValue)
		}
		db.Flush()
		db.Sync()
		db.Close()
	}

	// Re-open and verify every key still reads back the original bytes.
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 100; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("k-%05d", i)))
		if err != nil {
			t.Fatalf("post-reopen Get(%d): %v", i, err)
		}
		if !bytes.Equal(got, largeValue) {
			t.Errorf("Get(%d) wrong bytes", i)
		}
	}
}

func TestVlog_LargeAndSmallMixed(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.ValueThreshold = 256
	db, _ := Open(dir, opts)
	defer db.Close()

	smallVal := []byte("small")
	largeVal := bytes.Repeat([]byte("L"), 1024)

	for i := 0; i < 100; i++ {
		var val []byte
		if i%2 == 0 {
			val = smallVal
		} else {
			val = largeVal
		}
		db.Set([]byte(fmt.Sprintf("k-%03d", i)), val)
	}

	for i := 0; i < 100; i++ {
		got, _ := db.Get([]byte(fmt.Sprintf("k-%03d", i)))
		want := smallVal
		if i%2 == 1 {
			want = largeVal
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%d) length = %d, want %d", i, len(got), len(want))
		}
	}
}

func TestVlog_TransactionalCommit(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.ValueThreshold = 128
	db, _ := Open(dir, opts)
	defer db.Close()

	largeVal := bytes.Repeat([]byte("v"), 512)
	err := db.Update(func(t *Txn) error {
		for i := 0; i < 50; i++ {
			if err := t.Set([]byte(fmt.Sprintf("k-%02d", i)), largeVal); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		got, _ := db.Get([]byte(fmt.Sprintf("k-%02d", i)))
		if !bytes.Equal(got, largeVal) {
			t.Errorf("txn-committed Get(%d) length = %d, want %d", i, len(got), len(largeVal))
		}
	}
}

func TestVlog_EncryptedRoundTrip(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.ValueThreshold = 64
	opts.EncryptionKey = freshKey(t)
	db, _ := Open(dir, opts)

	plaintext := []byte("super-secret-vlog-value-not-on-disk-in-clear")
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("k-%02d", i))
		val := append([]byte{}, plaintext...)
		val = append(val, byte(i))
		if err := db.Set(key, val); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	// Inspect every vlog segment on disk — none should contain the
	// plaintext prefix.
	entries, _ := os.ReadDir(filepath.Join(dir, "vlog"))
	if len(entries) == 0 {
		t.Fatal("no vlog segments produced")
	}
	for _, e := range entries {
		data, _ := os.ReadFile(filepath.Join(dir, "vlog", e.Name()))
		if bytes.Contains(data, plaintext) {
			t.Errorf("plaintext leaked in vlog segment %s", e.Name())
		}
	}

	// Round-trip every key through the engine.
	for i := 0; i < 20; i++ {
		got, err := db.Get([]byte(fmt.Sprintf("k-%02d", i)))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		want := append([]byte{}, plaintext...)
		want = append(want, byte(i))
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%d): plaintext mismatch", i)
		}
	}
	db.Close()
}

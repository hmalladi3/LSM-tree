package slate

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestTxn_BasicSetGet(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	err := db.Update(func(txn *Txn) error {
		return txn.Set([]byte("k"), []byte("v"))
	})
	if err != nil {
		t.Fatal(err)
	}

	var got []byte
	err = db.View(func(txn *Txn) error {
		v, e := txn.Get([]byte("k"))
		if e != nil {
			return e
		}
		got = v
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Errorf("got %q", got)
	}
}

func TestTxn_ReadYourWrites(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	err := db.Update(func(txn *Txn) error {
		if err := txn.Set([]byte("k"), []byte("v1")); err != nil {
			return err
		}
		// Read should see the buffered write.
		v, err := txn.Get([]byte("k"))
		if err != nil {
			return err
		}
		if !bytes.Equal(v, []byte("v1")) {
			t.Errorf("read-your-writes broken: got %q", v)
		}
		// Overwrite — read should now reflect the new value.
		if err := txn.Set([]byte("k"), []byte("v2")); err != nil {
			return err
		}
		v, _ = txn.Get([]byte("k"))
		if !bytes.Equal(v, []byte("v2")) {
			t.Errorf("after overwrite: got %q", v)
		}
		// Delete then read.
		if err := txn.Delete([]byte("k")); err != nil {
			return err
		}
		_, err = txn.Get([]byte("k"))
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("after delete in same txn: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxn_ReadOnlyRejectsWrites(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	err := db.View(func(txn *Txn) error {
		if err := txn.Set([]byte("k"), []byte("v")); !errors.Is(err, ErrReadOnly) {
			t.Errorf("Set on View txn: got %v", err)
		}
		if err := txn.Delete([]byte("k")); !errors.Is(err, ErrReadOnly) {
			t.Errorf("Delete on View txn: got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxn_RollbackDiscardsWrites(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	txn := db.BeginTxn(true)
	txn.Set([]byte("k"), []byte("v"))
	txn.Rollback()

	_, err := db.Get([]byte("k"))
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("after Rollback, key visible: %v", err)
	}
}

func TestTxn_SSIConflict_Detected(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	// Seed.
	db.Update(func(txn *Txn) error {
		return txn.Set([]byte("counter"), []byte("0"))
	})

	// Two concurrent read-modify-write transactions on the same key.
	// At least one must observe ErrConflict (or be retried).
	t1 := db.BeginTxn(true)
	t2 := db.BeginTxn(true)

	v1, _ := t1.Get([]byte("counter"))
	v2, _ := t2.Get([]byte("counter"))

	t1.Set([]byte("counter"), append(v1, '1'))
	t2.Set([]byte("counter"), append(v2, '2'))

	// First commit wins; second sees ErrConflict because its readSet
	// includes "counter" and t1's writeSet includes "counter".
	if err := t1.Commit(); err != nil {
		t.Fatalf("t1.Commit: %v", err)
	}
	err := t2.Commit()
	if !errors.Is(err, ErrConflict) {
		t.Errorf("t2.Commit: got %v, want ErrConflict", err)
	}
}

func TestTxn_NoConflictOnDisjointWrites(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	t1 := db.BeginTxn(true)
	t2 := db.BeginTxn(true)

	// t1 reads "a" and writes "b"; t2 reads "c" and writes "d". No overlap.
	t1.Get([]byte("a"))
	t1.Set([]byte("b"), []byte("from-t1"))

	t2.Get([]byte("c"))
	t2.Set([]byte("d"), []byte("from-t2"))

	if err := t1.Commit(); err != nil {
		t.Errorf("t1.Commit: %v", err)
	}
	if err := t2.Commit(); err != nil {
		t.Errorf("t2.Commit: %v", err)
	}
}

func TestTxn_NoConflict_WriteOnlyTxns(t *testing.T) {
	// A write-only txn (no reads) cannot conflict with any other write.
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	t1 := db.BeginTxn(true)
	t2 := db.BeginTxn(true)
	t1.Set([]byte("k"), []byte("from-t1"))
	t2.Set([]byte("k"), []byte("from-t2"))

	if err := t1.Commit(); err != nil {
		t.Errorf("t1.Commit: %v", err)
	}
	if err := t2.Commit(); err != nil {
		t.Errorf("t2.Commit: %v", err)
	}
	// The "later" commit_ts wins. We can't assert WHICH committed last
	// without exposing oracle internals, but Get must return one of them.
	v, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte("from-t1")) && !bytes.Equal(v, []byte("from-t2")) {
		t.Errorf("Get returned unexpected value %q", v)
	}
}

func TestTxn_UpdateRetriesOnConflict(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	db.Update(func(t *Txn) error { return t.Set([]byte("counter"), []byte("0")) })

	var attempts atomic.Int32
	const workers = 8
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := db.Update(func(t *Txn) error {
				attempts.Add(1)
				v, err := t.Get([]byte("counter"))
				if err != nil {
					return err
				}
				return t.Set([]byte("counter"), append(v, 'x'))
			})
			if err != nil {
				t.Errorf("Update worker: %v", err)
			}
		}()
	}
	wg.Wait()

	v, _ := db.Get([]byte("counter"))
	if len(v) != 1+workers {
		t.Errorf("counter length %d, want %d (some Update lost)", len(v), 1+workers)
	}
	t.Logf("attempts = %d for %d workers (retries = %d)", attempts.Load(), workers, int(attempts.Load())-workers)
}

func TestTxn_AfterCommit_RejectsOps(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	txn := db.BeginTxn(true)
	txn.Set([]byte("k"), []byte("v"))
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Get([]byte("k")); !errors.Is(err, ErrTxnClosed) {
		t.Errorf("Get after Commit: %v", err)
	}
	if err := txn.Set([]byte("k"), []byte("v2")); !errors.Is(err, ErrTxnClosed) {
		t.Errorf("Set after Commit: %v", err)
	}
}

func TestTxn_RespectsTxnMaxKeys(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.TxnMaxKeys = 5
	db, _ := Open(dir, opts)
	defer db.Close()

	err := db.Update(func(txn *Txn) error {
		for i := 0; i < 5; i++ {
			if err := txn.Set([]byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
				return err
			}
		}
		// 6th key should trip the limit.
		return txn.Set([]byte("k5"), []byte("v"))
	})
	if !errors.Is(err, ErrTxnTooLarge) {
		t.Errorf("got %v, want ErrTxnTooLarge", err)
	}
}

func TestTxn_RepeatSetDoesNotCountTowardKeyLimit(t *testing.T) {
	// Repeated writes to the same key should not push us over TxnMaxKeys.
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.TxnMaxKeys = 2
	db, _ := Open(dir, opts)
	defer db.Close()

	err := db.Update(func(txn *Txn) error {
		txn.Set([]byte("a"), []byte("1"))
		txn.Set([]byte("a"), []byte("2"))
		txn.Set([]byte("a"), []byte("3"))
		txn.Set([]byte("b"), []byte("1"))
		// Two distinct keys; should be fine.
		return nil
	})
	if err != nil {
		t.Errorf("repeat-key path: %v", err)
	}
}

func TestTxn_RespectsTxnMaxBytes(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.TxnMaxBytes = 1024
	db, _ := Open(dir, opts)
	defer db.Close()

	val := bytes.Repeat([]byte("v"), 600)
	err := db.Update(func(txn *Txn) error {
		if err := txn.Set([]byte("k1"), val); err != nil {
			return err
		}
		// Second 600-byte value crosses the 1024 limit.
		return txn.Set([]byte("k2"), val)
	})
	if !errors.Is(err, ErrTxnTooLarge) {
		t.Errorf("got %v, want ErrTxnTooLarge", err)
	}
}

func TestTxn_SetDurability_Overrides(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = Sync // slow default
	db, _ := Open(dir, opts)
	defer db.Close()

	// Override to NoSync for this txn — fast path.
	txn := db.BeginTxn(true)
	txn.SetDurability(NoSync)
	if err := txn.Set([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get([]byte("k"))
	if err != nil || !bytes.Equal(v, []byte("v")) {
		t.Errorf("Get after NoSync txn: v=%q err=%v", v, err)
	}
}

func TestTxn_NewIterator_ReadYourWrites(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	db.Set([]byte("a"), []byte("from-lsm"))
	db.Set([]byte("c"), []byte("from-lsm"))

	err := db.Update(func(txn *Txn) error {
		// Mutate two entries inside the txn.
		txn.Set([]byte("a"), []byte("from-txn"))
		txn.Set([]byte("b"), []byte("new-from-txn"))
		txn.Delete([]byte("c"))

		// Iterator should reflect: a=from-txn, b=new-from-txn, c is gone.
		it := txn.NewIterator(nil)
		defer it.Close()

		got := map[string]string{}
		for it.First(); it.Valid(); it.Next() {
			got[string(it.Key())] = string(it.Value())
		}
		if err := it.Error(); err != nil {
			return err
		}
		want := map[string]string{
			"a": "from-txn",
			"b": "new-from-txn",
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("Get(%s) via txn iter = %q, want %q", k, got[k], v)
			}
		}
		if _, present := got["c"]; present {
			t.Errorf("deleted key 'c' visible in txn iterator")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxn_DeleteRange(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	for _, k := range []string{"a", "b", "c", "d", "e"} {
		db.Set([]byte(k), []byte(k))
	}

	err := db.Update(func(txn *Txn) error {
		// Inside the txn, delete [b, d). Read-your-writes should hide b and c.
		if err := txn.DeleteRange([]byte("b"), []byte("d")); err != nil {
			return err
		}
		if _, err := txn.Get([]byte("a")); err != nil {
			t.Errorf("a: %v", err)
		}
		if _, err := txn.Get([]byte("b")); !errors.Is(err, ErrNotFound) {
			t.Errorf("b: got %v, want ErrNotFound", err)
		}
		if _, err := txn.Get([]byte("c")); !errors.Is(err, ErrNotFound) {
			t.Errorf("c: got %v, want ErrNotFound", err)
		}
		if _, err := txn.Get([]byte("d")); err != nil {
			t.Errorf("d: %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// After commit, the range tombstone must persist.
	for _, k := range []string{"b", "c"} {
		if _, err := db.Get([]byte(k)); !errors.Is(err, ErrNotFound) {
			t.Errorf("post-commit %s: got %v, want ErrNotFound", k, err)
		}
	}
	for _, k := range []string{"a", "d", "e"} {
		if _, err := db.Get([]byte(k)); err != nil {
			t.Errorf("post-commit %s should be visible: %v", k, err)
		}
	}
}

func TestTxn_DeleteRange_SurvivesFlush(t *testing.T) {
	// Range tombstones written via Txn.DeleteRange must remain effective
	// after the memtable holding them flushes to L0. Regression check:
	// the memtable's range-tombstone skiplist is separate from the point
	// entries, so the flush path has to propagate both.
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	for _, k := range []string{"a", "b", "c", "d"} {
		db.Set([]byte(k), []byte(k))
	}
	if err := db.Update(func(txn *Txn) error {
		return txn.DeleteRange([]byte("b"), []byte("d"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"b", "c"} {
		if _, err := db.Get([]byte(k)); !errors.Is(err, ErrNotFound) {
			t.Errorf("post-flush %s should remain deleted: got %v", k, err)
		}
	}
	for _, k := range []string{"a", "d"} {
		if _, err := db.Get([]byte(k)); err != nil {
			t.Errorf("post-flush %s should be visible: %v", k, err)
		}
	}
}

func TestTxn_DeleteRange_PersistsAcrossReopen(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = Sync
	{
		db, _ := Open(dir, opts)
		for _, k := range []string{"a", "b", "c"} {
			db.Set([]byte(k), []byte(k))
		}
		db.Update(func(txn *Txn) error { return txn.DeleteRange([]byte("a"), []byte("c")) })
		db.Close()
	}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
		t.Errorf("a after reopen: got %v", err)
	}
	if _, err := db.Get([]byte("b")); !errors.Is(err, ErrNotFound) {
		t.Errorf("b after reopen: got %v", err)
	}
	if _, err := db.Get([]byte("c")); err != nil {
		t.Errorf("c after reopen: should be visible: %v", err)
	}
}

func TestTxn_NewIterator_OnReadOnly(t *testing.T) {
	// A View transaction's iterator should just stream the LSM at read_ts.
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	for _, k := range []string{"a", "b", "c"} {
		db.Set([]byte(k), []byte(k))
	}
	err := db.View(func(txn *Txn) error {
		it := txn.NewIterator(nil)
		defer it.Close()
		var got []string
		for it.First(); it.Valid(); it.Next() {
			got = append(got, string(it.Key()))
		}
		want := []string{"a", "b", "c"}
		if !equalStringSlices(got, want) {
			t.Errorf("got %v want %v", got, want)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestTxn_MultiOpRecoveryIsAtomic(t *testing.T) {
	// A multi-op transaction must appear all-or-nothing after a crash.
	// Verified by: write 50 ops in one Update, Sync to disk, reopen, every
	// op must be visible.
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = Sync
	{
		db, _ := Open(dir, opts)
		if err := db.Update(func(txn *Txn) error {
			for i := 0; i < 50; i++ {
				key := []byte(fmt.Sprintf("k-%02d", i))
				if err := txn.Set(key, []byte(fmt.Sprintf("v-%02d", i))); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		db.Close()
	}
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("k-%02d", i))
		want := []byte(fmt.Sprintf("v-%02d", i))
		got, err := db.Get(k)
		if err != nil {
			t.Fatalf("Get(%s): %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("Get(%s) = %q, want %q", k, got, want)
		}
	}
}

func TestSnapshot_StableViewAcrossWrites(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	db.Set([]byte("k"), []byte("v1"))
	snap := db.NewSnapshot()
	defer snap.Close()

	db.Set([]byte("k"), []byte("v2"))
	db.Delete([]byte("k"))

	got, err := snap.Get([]byte("k"))
	if err != nil {
		t.Fatalf("snapshot.Get: %v", err)
	}
	if string(got) != "v1" {
		t.Errorf("snapshot.Get = %q, want v1", got)
	}
	if _, err := db.Get([]byte("k")); !errors.Is(err, ErrNotFound) {
		t.Errorf("db.Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestSnapshot_IteratorIsStable(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	for _, k := range []string{"a", "b", "c"} {
		db.Set([]byte(k), []byte("snap"))
	}
	snap := db.NewSnapshot()
	defer snap.Close()

	db.Set([]byte("d"), []byte("post"))
	db.Delete([]byte("a"))

	it := snap.NewIterator(nil)
	defer it.Close()
	var seen []string
	for it.First(); it.Valid(); it.Next() {
		seen = append(seen, string(it.Key()))
	}
	want := []string{"a", "b", "c"}
	if !equalStringSlices(seen, want) {
		t.Errorf("snapshot iterator saw %v, want %v", seen, want)
	}
}

func TestSnapshot_CloseIsIdempotent(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	snap := db.NewSnapshot()
	if err := snap.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snap.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if _, err := snap.Get([]byte("anything")); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close: got %v, want ErrClosed", err)
	}
}

func TestTxn_OnClosedDB(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	db.Close()
	if err := db.View(func(t *Txn) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Errorf("View on closed DB: %v", err)
	}
	if err := db.Update(func(t *Txn) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Errorf("Update on closed DB: %v", err)
	}
}

func TestTxn_SnapshotIsolation(t *testing.T) {
	// Reads in an open transaction never observe writes that commit after
	// Begin, even though the key/value pair is present in the LSM.
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	db.Update(func(t *Txn) error { return t.Set([]byte("k"), []byte("initial")) })

	reader := db.BeginTxn(false)
	defer reader.Rollback()

	// Concurrent commit: writes a new version.
	db.Update(func(t *Txn) error { return t.Set([]byte("k"), []byte("updated")) })

	// reader's snapshot still sees the initial value.
	v, err := reader.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v, []byte("initial")) {
		t.Errorf("snapshot leak: reader saw %q, want 'initial'", v)
	}
}

func TestTxn_ManyKeysInSingleCommit(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()

	const n = 1000
	err := db.Update(func(t *Txn) error {
		for i := 0; i < n; i++ {
			if err := t.Set([]byte(fmt.Sprintf("k-%04d", i)), []byte("v")); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if _, err := db.Get([]byte(fmt.Sprintf("k-%04d", i))); err != nil {
			t.Errorf("key %d missing: %v", i, err)
		}
	}
}

func BenchmarkTxn_PointWrite(b *testing.B) {
	dir := tempDir(b)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.MemtableSize = 128 << 20
	db, _ := Open(dir, opts)
	defer db.Close()

	value := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("k-%08d", i))
		err := db.Update(func(t *Txn) error { return t.Set(key, value) })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTxn_BatchedWrites(b *testing.B) {
	dir := tempDir(b)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.MemtableSize = 128 << 20
	db, _ := Open(dir, opts)
	defer db.Close()

	const batch = 100
	value := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		err := db.Update(func(t *Txn) error {
			for j := 0; j < batch; j++ {
				if err := t.Set([]byte(fmt.Sprintf("k-%08d-%03d", i, j)), value); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "ops/sec")
}

package slate

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"testing"
)

func collect(t *testing.T, db *DB, opts *IterOptions) []string {
	t.Helper()
	it := db.NewIterator(opts)
	defer it.Close()
	var out []string
	for it.First(); it.Valid(); it.Next() {
		out = append(out, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	return out
}

func TestIterator_Empty(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	if got := collect(t, db, nil); len(got) != 0 {
		t.Errorf("empty DB iter returned %v", got)
	}
}

func TestIterator_MemtableOnly(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	pairs := map[string]string{
		"alpha": "a", "beta": "b", "delta": "d", "gamma": "g",
	}
	for k, v := range pairs {
		db.Set([]byte(k), []byte(v))
	}
	got := collect(t, db, nil)
	want := []string{"alpha=a", "beta=b", "delta=d", "gamma=g"}
	if !equalStringSlices(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestIterator_L0Only(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()
	for i := 0; i < 100; i++ {
		k := []byte(fmt.Sprintf("k-%03d", i))
		db.Set(k, []byte(fmt.Sprintf("v-%03d", i)))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	got := collect(t, db, nil)
	if len(got) != 100 {
		t.Fatalf("got %d entries, want 100", len(got))
	}
	for i := 0; i < 100; i++ {
		want := fmt.Sprintf("k-%03d=v-%03d", i, i)
		if got[i] != want {
			t.Errorf("[%d] %s != %s", i, got[i], want)
		}
	}
}

func TestIterator_MixedMemtableAndL0(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()

	// First batch → flushed to L0.
	for i := 0; i < 50; i += 2 {
		db.Set([]byte(fmt.Sprintf("k-%03d", i)), []byte("L0"))
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	// Second batch → stays in active memtable. Inserts the odd-numbered keys.
	for i := 1; i < 50; i += 2 {
		db.Set([]byte(fmt.Sprintf("k-%03d", i)), []byte("mem"))
	}

	got := collect(t, db, nil)
	if len(got) != 50 {
		t.Fatalf("got %d entries, want 50", len(got))
	}
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("k-%03d", i)
		want := "L0"
		if i%2 == 1 {
			want = "mem"
		}
		expected := fmt.Sprintf("%s=%s", key, want)
		if got[i] != expected {
			t.Errorf("[%d] %s != %s", i, got[i], expected)
		}
	}
}

func TestIterator_MemtableShadowsL0(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()

	db.Set([]byte("k"), []byte("L0"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	db.Set([]byte("k"), []byte("mem"))

	got := collect(t, db, nil)
	want := []string{"k=mem"}
	if !equalStringSlices(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestIterator_DeleteShadowsL0(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()

	db.Set([]byte("k1"), []byte("v1"))
	db.Set([]byte("k2"), []byte("v2"))
	db.Set([]byte("k3"), []byte("v3"))
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	db.Delete([]byte("k2"))

	got := collect(t, db, nil)
	want := []string{"k1=v1", "k3=v3"}
	if !equalStringSlices(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestIterator_SeekGE(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	for _, k := range []string{"alpha", "beta", "delta", "gamma"} {
		db.Set([]byte(k), []byte(k))
	}
	cases := []struct {
		target, want string
		valid        bool
	}{
		{"a", "alpha", true},
		{"alpha", "alpha", true},
		{"alphab", "beta", true},
		{"c", "delta", true},
		{"gamma", "gamma", true},
		{"z", "", false},
	}
	for _, tc := range cases {
		it := db.NewIterator(nil)
		it.SeekGE([]byte(tc.target))
		if it.Valid() != tc.valid {
			t.Errorf("SeekGE(%q).Valid = %v want %v", tc.target, it.Valid(), tc.valid)
			it.Close()
			continue
		}
		if tc.valid && string(it.Key()) != tc.want {
			t.Errorf("SeekGE(%q).Key = %q want %q", tc.target, it.Key(), tc.want)
		}
		it.Close()
	}
}

func TestIterator_Bounds(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "g"} {
		db.Set([]byte(k), []byte(k))
	}

	got := collect(t, db, &IterOptions{Lower: []byte("c"), Upper: []byte("f")})
	want := []string{"c=c", "d=d", "e=e"}
	if !equalStringSlices(got, want) {
		t.Errorf("[c, f) got %v want %v", got, want)
	}

	// Lower only.
	got = collect(t, db, &IterOptions{Lower: []byte("e")})
	want = []string{"e=e", "f=f", "g=g"}
	if !equalStringSlices(got, want) {
		t.Errorf("[e, inf) got %v want %v", got, want)
	}

	// Upper only.
	got = collect(t, db, &IterOptions{Upper: []byte("c")})
	want = []string{"a=a", "b=b"}
	if !equalStringSlices(got, want) {
		t.Errorf("(inf, c) got %v want %v", got, want)
	}
}

func TestIterator_ManyKeysAcrossManyFlushes(t *testing.T) {
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.MemtableSize = 64 << 10
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()

	const total = 1000
	for i := 0; i < total; i++ {
		db.Set([]byte(fmt.Sprintf("k-%05d", i)), []byte(fmt.Sprintf("v-%05d", i)))
		// Force occasional rotations to spread keys across L0 files.
		if i%200 == 199 {
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
		}
	}
	got := collect(t, db, nil)
	if len(got) != total {
		t.Fatalf("got %d entries, want %d", len(got), total)
	}
	// Verify lexicographic order is preserved across the heterogeneous mix.
	for i := 0; i < total; i++ {
		want := fmt.Sprintf("k-%05d=v-%05d", i, i)
		if got[i] != want {
			t.Errorf("[%d] %s != %s", i, got[i], want)
			break
		}
	}
	// Verify it equals a sorted reconstruction.
	wantList := make([]string, total)
	for i := 0; i < total; i++ {
		wantList[i] = fmt.Sprintf("k-%05d=v-%05d", i, i)
	}
	sort.Strings(wantList)
	if !equalStringSlices(got, wantList) {
		t.Errorf("iteration does not produce sorted output")
	}
}

func TestIterator_DereferencesVlogPointers(t *testing.T) {
	// Regression: previously iterator.Value() returned raw vlog pointer
	// bytes (16 bytes) instead of the actual stored value for any key
	// whose value had been spilled to the vlog. Make sure scans return
	// the real bytes for both inline and vlog-stored values.
	dir := tempDir(t)
	opts := DefaultOptions()
	opts.DefaultDurability = NoSync
	opts.ValueThreshold = 64
	opts.MemtableSize = 64 << 10
	db, _ := Open(dir, opts)
	defer db.Close()

	smallVal := []byte("small-inline")
	largeVal := bytes.Repeat([]byte("L"), 512) // forces vlog spill
	for i := 0; i < 20; i++ {
		key := []byte(fmt.Sprintf("k-%02d", i))
		if i%2 == 0 {
			db.Set(key, smallVal)
		} else {
			db.Set(key, largeVal)
		}
	}
	// Flush so the L0 path is exercised by the scan.
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}

	it := db.NewIterator(nil)
	defer it.Close()
	seen := 0
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		v := it.Value()
		idx := 0
		fmt.Sscanf(string(k), "k-%d", &idx)
		var want []byte
		if idx%2 == 0 {
			want = smallVal
		} else {
			want = largeVal
		}
		if !bytes.Equal(v, want) {
			t.Errorf("Value(%s): len=%d want len=%d", k, len(v), len(want))
		}
		seen++
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	if seen != 20 {
		t.Errorf("scanned %d entries, want 20", seen)
	}
}

func TestIterator_StickyError(t *testing.T) {
	// Simulate a sticky error: stuff the iterator with an err and verify
	// every public method falls back to its zero behavior thereafter.
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	db.Set([]byte("k"), []byte("v"))

	it := db.NewIterator(nil)
	defer it.Close()
	it.First()
	if !it.Valid() {
		t.Fatal("expected valid before injecting error")
	}
	// Inject the error directly. (In production this happens via a source
	// returning a non-nil error during advance.)
	it.err = errors.New("simulated source error")
	if it.Next() {
		t.Error("Next() returned true after sticky error")
	}
	if it.Valid() {
		t.Error("Valid() returned true after sticky error")
	}
	if it.Error() == nil {
		t.Error("Error() returned nil after sticky error injected")
	}
}

func TestIterator_PostClose_IsNoop(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	db.Set([]byte("a"), []byte("alpha"))
	db.Set([]byte("b"), []byte("beta"))

	it := db.NewIterator(nil)
	it.First()
	if !it.Valid() {
		t.Fatal("expected valid before Close")
	}
	it.Close()

	// After Close, all advance/Valid/Key/Value should behave gracefully.
	if it.Valid() {
		t.Error("Valid() returned true after Close")
	}
	if it.Key() != nil {
		t.Errorf("Key() = %v after Close", it.Key())
	}
	if it.Value() != nil {
		t.Errorf("Value() = %v after Close", it.Value())
	}
	if it.Next() {
		t.Error("Next() returned true after Close")
	}
	if it.First() {
		// First() resets but synthetic list is nil after Close.
		t.Error("First() returned true after Close")
	}
}

func TestIterator_Close_Idempotent(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	db.Set([]byte("k"), []byte("v"))
	it := db.NewIterator(nil)
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestIterator_OnClosedDB(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	db.Close()
	it := db.NewIterator(nil)
	defer it.Close()
	if it.First() {
		t.Error("First on closed DB returned valid")
	}
	if it.Error() == nil {
		t.Error("expected sticky error on closed DB iterator")
	}
}

func TestIterator_ValueLifetime_CopyAcrossNext(t *testing.T) {
	dir := tempDir(t)
	db, _ := Open(dir, nil)
	defer db.Close()
	db.Set([]byte("a"), []byte("alpha"))
	db.Set([]byte("b"), []byte("beta"))

	it := db.NewIterator(nil)
	defer it.Close()
	it.First()
	v1 := append([]byte(nil), it.Value()...) // copy
	it.Next()
	v2 := append([]byte(nil), it.Value()...)
	if !bytes.Equal(v1, []byte("alpha")) {
		t.Errorf("v1 after Next = %q", v1)
	}
	if !bytes.Equal(v2, []byte("beta")) {
		t.Errorf("v2 = %q", v2)
	}
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func BenchmarkIterator_RangeScan(b *testing.B) {
	dir := tempDir(b)
	opts := benchOpts(b, 64)
	opts.DefaultDurability = NoSync
	db, _ := Open(dir, opts)
	defer db.Close()

	const n = 100_000
	for i := 0; i < n; i++ {
		db.Set([]byte(fmt.Sprintf("k-%08d", i)), []byte("v"))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := db.NewIterator(nil)
		count := 0
		for it.First(); it.Valid(); it.Next() {
			count++
		}
		it.Close()
		if count != n {
			b.Fatalf("scanned %d, want %d", count, n)
		}
	}
}

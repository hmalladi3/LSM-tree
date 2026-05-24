package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/harimalladi/slate/internal/keys"
)

func tempFile(t testing.TB) string {
	t.Helper()
	d, err := os.MkdirTemp("", "slate-sst-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return filepath.Join(d, "test.sst")
}

// buildSST writes an SSTable containing the given (user, value) pairs at
// monotonically increasing sequence numbers, then opens it.
func buildSST(t testing.TB, path string, pairs []struct{ key, val string }) *Reader {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := NewWriter(f, nil)
	for i, p := range pairs {
		ik := keys.Encode(nil, []byte(p.key), uint64(i+1), keys.KindInlineValue)
		if err := w.Add(ik, []byte(p.val)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestWriteRead_RoundTrip(t *testing.T) {
	pairs := []struct{ key, val string }{
		{"alpha", "a"},
		{"beta", "b"},
		{"delta", "d"},
		{"gamma", "g"},
	}
	r := buildSST(t, tempFile(t), pairs)
	for _, p := range pairs {
		v, _, ok, err := r.Get([]byte(p.key), 100)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Errorf("Get(%s) ok=false", p.key)
			continue
		}
		if !bytes.Equal(v, []byte(p.val)) {
			t.Errorf("Get(%s) = %q want %q", p.key, v, p.val)
		}
	}
}

func TestGet_MissingKey_BloomShortCircuits(t *testing.T) {
	r := buildSST(t, tempFile(t), []struct{ key, val string }{
		{"alpha", "a"},
		{"beta", "b"},
	})
	for _, k := range []string{"absent", "zzz", "", "alphaa"} {
		_, _, ok, err := r.Get([]byte(k), 100)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			t.Errorf("Get(%q) ok=true on absent key", k)
		}
	}
}

func TestGet_AcrossBlocks(t *testing.T) {
	// Many entries to force multiple data blocks.
	path := tempFile(t)
	f, _ := os.Create(path)
	w := NewWriter(f, &WriterOptions{BlockSize: 256, RestartInterval: 4})
	const n = 500
	expected := make([]string, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		val := fmt.Sprintf("val-%05d-padding", i)
		expected[i] = val
		ik := keys.Encode(nil, []byte(key), uint64(i+1), keys.KindInlineValue)
		if err := w.Add(ik, []byte(val)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	r, err := Open(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%05d", i)
		v, _, ok, err := r.Get([]byte(key), 1000)
		if err != nil {
			t.Fatal(err)
		}
		if !ok || string(v) != expected[i] {
			t.Errorf("Get(%s) = (%q, %v), want (%s, true)", key, v, ok, expected[i])
		}
	}
}

func TestGet_SnapshotVisibility(t *testing.T) {
	// Same user key at different sequence numbers; older versions are
	// visible at older snapshots.
	path := tempFile(t)
	f, _ := os.Create(path)
	w := NewWriter(f, nil)
	// Internal keys for "k" at seq=3, 7, 10 (added in ascending internal-
	// key order, which is DESCENDING seq because of the inverted trailer).
	for _, seq := range []uint64{10, 7, 3} {
		ik := keys.Encode(nil, []byte("k"), seq, keys.KindInlineValue)
		val := fmt.Sprintf("v%d", seq)
		w.Add(ik, []byte(val))
	}
	w.Finish()
	f.Close()

	r, _ := Open(path, nil)
	defer r.Close()

	cases := []struct {
		snap uint64
		want string
	}{
		{2, ""},
		{3, "v3"},
		{5, "v3"},
		{7, "v7"},
		{9, "v7"},
		{10, "v10"},
		{1000, "v10"},
	}
	for _, tc := range cases {
		v, _, ok, _ := r.Get([]byte("k"), tc.snap)
		if tc.want == "" {
			if ok {
				t.Errorf("snap=%d: got %q, want absent", tc.snap, v)
			}
			continue
		}
		if !ok || string(v) != tc.want {
			t.Errorf("snap=%d: got %q (ok=%v), want %s", tc.snap, v, ok, tc.want)
		}
	}
}

func TestGet_DeletedKeyReturnsAbsent(t *testing.T) {
	path := tempFile(t)
	f, _ := os.Create(path)
	w := NewWriter(f, nil)
	// k @ seq=10 = tombstone (internal-key ordering puts higher seq first).
	w.Add(keys.Encode(nil, []byte("k"), 10, keys.KindDeletion), nil)
	w.Add(keys.Encode(nil, []byte("k"), 5, keys.KindInlineValue), []byte("v"))
	w.Finish()
	f.Close()

	r, _ := Open(path, nil)
	defer r.Close()

	// At snap >= 10: tombstone wins.
	if _, _, ok, _ := r.Get([]byte("k"), 100); ok {
		t.Error("expected absent at snap=100")
	}
	// At snap < 10: old value still visible.
	v, _, ok, _ := r.Get([]byte("k"), 7)
	if !ok || string(v) != "v" {
		t.Errorf("snap=7: got (%q, %v)", v, ok)
	}
}

func TestIterator_FullScan(t *testing.T) {
	pairs := []struct{ key, val string }{
		{"alpha", "a"},
		{"beta", "b"},
		{"delta", "d"},
		{"gamma", "g"},
	}
	r := buildSST(t, tempFile(t), pairs)

	it := r.NewIterator()
	defer it.Close()
	got := []string{}
	for it.First(); it.Valid(); it.Next() {
		user, _, _, _ := keys.Parse(it.Key())
		got = append(got, fmt.Sprintf("%s=%s", user, it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	want := []string{"alpha=a", "beta=b", "delta=d", "gamma=g"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("[%d] %s vs %s", i, got[i], want[i])
		}
	}
}

func TestFinish_Empty_ReturnsErrEmpty(t *testing.T) {
	f, _ := os.Create(tempFile(t))
	w := NewWriter(f, nil)
	_, err := w.Finish()
	if !errors.Is(err, ErrEmpty) {
		t.Errorf("Finish on empty writer returned %v", err)
	}
}

func TestOpen_ShortFile_ReturnsErrCorrupted(t *testing.T) {
	path := tempFile(t)
	os.WriteFile(path, []byte("xx"), 0o644)
	_, err := Open(path, nil)
	if !errors.Is(err, ErrCorrupted) {
		t.Errorf("Open of 2-byte file returned %v", err)
	}
}

func TestOpen_BadMagic_ReturnsErrCorrupted(t *testing.T) {
	path := tempFile(t)
	// Build a real SST, then clobber the magic.
	buildSST(t, path, []struct{ key, val string }{{"a", "1"}}).Close()
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	stat, _ := f.Stat()
	f.WriteAt([]byte("BADMAGIC"), stat.Size()-8)
	f.Close()
	_, err := Open(path, nil)
	if !errors.Is(err, ErrCorrupted) {
		t.Errorf("Open with bad magic returned %v", err)
	}
}

func TestPrefixCompression_Decodes(t *testing.T) {
	// Keys with substantial shared prefixes exercise the prefix-compression
	// path; verify Get still finds them.
	pairs := []struct{ key, val string }{
		{"common-prefix-001", "1"},
		{"common-prefix-002", "2"},
		{"common-prefix-003", "3"},
		{"common-prefix-100", "100"},
		{"different-prefix-001", "X"},
	}
	r := buildSST(t, tempFile(t), pairs)
	for _, p := range pairs {
		v, _, ok, _ := r.Get([]byte(p.key), 100)
		if !ok || string(v) != p.val {
			t.Errorf("Get(%s) = (%q, %v)", p.key, v, ok)
		}
	}
}

func TestMetadata_BoundsAndSeq(t *testing.T) {
	path := tempFile(t)
	f, _ := os.Create(path)
	w := NewWriter(f, nil)
	w.Add(keys.Encode(nil, []byte("alpha"), 5, keys.KindInlineValue), []byte("a"))
	w.Add(keys.Encode(nil, []byte("zeta"), 100, keys.KindInlineValue), []byte("z"))
	meta, err := w.Finish()
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	if !bytes.Equal(meta.Smallest, []byte("alpha")) || !bytes.Equal(meta.Largest, []byte("zeta")) {
		t.Errorf("bounds: %s..%s", meta.Smallest, meta.Largest)
	}
	if meta.SmallestSeq != 5 || meta.LargestSeq != 100 {
		t.Errorf("seq range: %d..%d", meta.SmallestSeq, meta.LargestSeq)
	}
	if meta.NumEntries != 2 {
		t.Errorf("NumEntries = %d", meta.NumEntries)
	}
}

func BenchmarkGet_Hit(b *testing.B) {
	path := tempFile(b)
	f, _ := os.Create(path)
	w := NewWriter(f, nil)
	const n = 10_000
	keysList := make([][]byte, n)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("k-%08d", i))
		keysList[i] = k
		ik := keys.Encode(nil, k, uint64(i+1), keys.KindInlineValue)
		w.Add(ik, []byte("v"))
	}
	w.Finish()
	f.Close()
	r, _ := Open(path, nil)
	defer r.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, _, err := r.Get(keysList[i%n], uint64(n)); err != nil {
			b.Fatal(err)
		}
	}
}

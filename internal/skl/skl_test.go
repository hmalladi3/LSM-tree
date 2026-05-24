package skl

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"

	"github.com/harimalladi/slate/internal/arena"
)

func newTestSkl(t testing.TB, cap int) *Skl {
	t.Helper()
	return New(arena.New(cap))
}

func TestInsertGet_SingleKey(t *testing.T) {
	s := newTestSkl(t, 4096)
	if !s.Insert([]byte("hello"), []byte("world")) {
		t.Fatal("Insert returned false")
	}
	k, v, ok := s.Get([]byte("hello"))
	if !ok {
		t.Fatal("Get reported absent")
	}
	if !bytes.Equal(k, []byte("hello")) {
		t.Errorf("key=%q", k)
	}
	if !bytes.Equal(v, []byte("world")) {
		t.Errorf("value=%q", v)
	}
}

func TestGet_Missing(t *testing.T) {
	s := newTestSkl(t, 4096)
	if _, _, ok := s.Get([]byte("nope")); ok {
		t.Error("Get reported present on empty list")
	}
	s.Insert([]byte("b"), []byte("vb"))
	// "a" < "b" → Get("a") finds "b" (first key >= "a").
	k, _, ok := s.Get([]byte("a"))
	if !ok || !bytes.Equal(k, []byte("b")) {
		t.Errorf("expected b, got %q (ok=%v)", k, ok)
	}
	// "c" > "b" → no key >= "c".
	if _, _, ok := s.Get([]byte("c")); ok {
		t.Error("Get should be absent past largest key")
	}
}

func TestInsertGet_MultipleKeys(t *testing.T) {
	s := newTestSkl(t, 64*1024)
	keys := []string{"d", "a", "c", "b", "f", "e"}
	for _, k := range keys {
		if !s.Insert([]byte(k), []byte("v"+k)) {
			t.Fatalf("Insert(%q) returned false", k)
		}
	}
	for _, k := range keys {
		gotK, v, ok := s.Get([]byte(k))
		if !ok || !bytes.Equal(gotK, []byte(k)) || !bytes.Equal(v, []byte("v"+k)) {
			t.Errorf("Get(%q) = (%q, %q, %v)", k, gotK, v, ok)
		}
	}
}

func TestIterator_AscendingOrder(t *testing.T) {
	s := newTestSkl(t, 64*1024)
	keys := []string{"d", "a", "c", "b", "f", "e"}
	for _, k := range keys {
		s.Insert([]byte(k), []byte("v"+k))
	}
	sort.Strings(keys)

	it := s.NewIterator()
	var got []string
	for it.First(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	if len(got) != len(keys) {
		t.Fatalf("iterator visited %d, want %d", len(got), len(keys))
	}
	for i, k := range keys {
		if got[i] != k {
			t.Errorf("[%d]: got %q want %q", i, got[i], k)
		}
	}
}

func TestIterator_SeekGE(t *testing.T) {
	s := newTestSkl(t, 64*1024)
	for _, k := range []string{"alpha", "beta", "delta", "gamma"} {
		s.Insert([]byte(k), []byte(k))
	}
	cases := []struct {
		target string
		want   string
		valid  bool
	}{
		{"a", "alpha", true},
		{"alpha", "alpha", true},
		{"alphab", "beta", true},
		{"c", "delta", true},
		{"gamma", "gamma", true},
		{"z", "", false},
	}
	for _, tc := range cases {
		it := s.NewIterator()
		it.SeekGE([]byte(tc.target))
		if it.Valid() != tc.valid {
			t.Errorf("SeekGE(%q).Valid() = %v, want %v", tc.target, it.Valid(), tc.valid)
			continue
		}
		if tc.valid && string(it.Key()) != tc.want {
			t.Errorf("SeekGE(%q) = %q, want %q", tc.target, it.Key(), tc.want)
		}
	}
}

func TestInsert_FailsWhenArenaFull(t *testing.T) {
	// 8 KiB is enough for ~ a head node (~88B) plus a handful of small
	// entries. Insert many to provoke a full-arena failure.
	s := newTestSkl(t, 8*1024)
	successes := 0
	for i := 0; i < 10000; i++ {
		var k [16]byte
		binary.BigEndian.PutUint64(k[:], uint64(i))
		if !s.Insert(k[:], k[:]) {
			break
		}
		successes++
	}
	if successes == 0 {
		t.Fatal("no inserts succeeded")
	}
	if successes >= 10000 {
		t.Fatal("expected the arena to fill")
	}
	t.Logf("filled arena after %d inserts", successes)
}

func TestInsert_AfterSealReturnsFalse(t *testing.T) {
	s := newTestSkl(t, 4096)
	s.arena.Seal()
	if s.Insert([]byte("k"), []byte("v")) {
		t.Error("Insert should fail after Seal")
	}
}

func TestConcurrent_InsertDistinctKeys(t *testing.T) {
	const (
		writers       = 16
		keysPerWriter = 500
	)
	s := newTestSkl(t, 64*1024*1024)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < keysPerWriter; i++ {
				var buf [16]byte
				binary.BigEndian.PutUint64(buf[:8], uint64(w))
				binary.BigEndian.PutUint64(buf[8:], uint64(i))
				if !s.Insert(buf[:], []byte("v")) {
					t.Errorf("insert failed at w=%d i=%d", w, i)
					return
				}
			}
		}(w)
	}
	wg.Wait()

	// Verify every inserted key is readable and the list is sorted.
	expected := writers * keysPerWriter
	count := 0
	var prev []byte
	it := s.NewIterator()
	for it.First(); it.Valid(); it.Next() {
		k := it.Key()
		if prev != nil && bytes.Compare(prev, k) >= 0 {
			t.Fatalf("out-of-order: %v then %v", prev, k)
		}
		prev = append(prev[:0], k...)
		count++
	}
	if count != expected {
		t.Errorf("counted %d, want %d", count, expected)
	}
}

func TestConcurrent_ReadAlongsideInsert(t *testing.T) {
	const (
		nInsert   = 5000
		readers   = 4
		readsEach = 5000
	)
	s := newTestSkl(t, 16*1024*1024)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < nInsert; i++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(i))
			s.Insert(k[:], k[:])
		}
	}()

	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b1))
			for i := 0; i < readsEach; i++ {
				var k [8]byte
				binary.BigEndian.PutUint64(k[:], rng.Uint64()%nInsert)
				_, _, _ = s.Get(k[:])
			}
		}(uint64(r))
	}
	wg.Wait()
}

func TestRandomHeight_DistributionShape(t *testing.T) {
	// Sample many random keys; on aggregate the heights should follow
	// the geometric distribution (p = 1/4). We assert weak bounds rather
	// than the exact ratio because we use deterministic hashing.
	const n = 100_000
	counts := make(map[int]int)
	for i := 0; i < n; i++ {
		var k [16]byte
		binary.LittleEndian.PutUint64(k[:8], uint64(i))
		binary.LittleEndian.PutUint64(k[8:], uint64(i)*2654435761)
		h := randomHeight(k[:])
		counts[h]++
	}
	// Roughly 75% should be height-1, 18.75% height-2, etc.
	if counts[1] < n/2 || counts[1] > 9*n/10 {
		t.Errorf("height=1 count %d outside [%d, %d]", counts[1], n/2, 9*n/10)
	}
	if counts[2] < n/10 || counts[2] > n/3 {
		t.Errorf("height=2 count %d outside [%d, %d]", counts[2], n/10, n/3)
	}
	// MaxHeight should be vanishingly rare but not always 0 with 100K samples.
	t.Logf("height distribution: %v", counts)
}

func BenchmarkInsert(b *testing.B) {
	s := newTestSkl(b, max(64*1024*1024, b.N*64))
	keys := make([][]byte, b.N)
	for i := range keys {
		keys[i] = make([]byte, 16)
		binary.BigEndian.PutUint64(keys[i], uint64(i))
		binary.BigEndian.PutUint64(keys[i][8:], uint64(i)*2654435761)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Insert(keys[i], keys[i])
	}
}

func BenchmarkGet_Hit(b *testing.B) {
	const n = 10_000
	s := newTestSkl(b, 8*1024*1024)
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%08d", i))
		s.Insert(keys[i], []byte("v"))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Get(keys[i%n])
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

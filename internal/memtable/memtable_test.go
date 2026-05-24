package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/harimalladi/slate/internal/keys"
)

func TestSetGet_Basic(t *testing.T) {
	m := New(64 * 1024)
	if !m.Set([]byte("hello"), 1, keys.KindInlineValue, []byte("world")) {
		t.Fatal("Set failed")
	}
	v, kind, ok := m.Get([]byte("hello"), 10)
	if !ok || kind != keys.KindInlineValue || !bytes.Equal(v, []byte("world")) {
		t.Errorf("Get = (%q, %v, %v)", v, kind, ok)
	}
}

func TestGet_NotPresent(t *testing.T) {
	m := New(64 * 1024)
	if _, _, ok := m.Get([]byte("missing"), 10); ok {
		t.Error("Get reported present on empty memtable")
	}
	m.Set([]byte("k"), 1, keys.KindInlineValue, []byte("v"))
	if _, _, ok := m.Get([]byte("other"), 10); ok {
		t.Error("Get reported present for absent key")
	}
}

func TestGet_FutureSeq_NotVisible(t *testing.T) {
	m := New(64 * 1024)
	m.Set([]byte("k"), 10, keys.KindInlineValue, []byte("future"))
	if _, _, ok := m.Get([]byte("k"), 5); ok {
		t.Error("Get at seq=5 saw entry with seq=10")
	}
	if _, _, ok := m.Get([]byte("k"), 10); !ok {
		t.Error("Get at seq=10 missed entry with seq=10")
	}
}

func TestGet_LatestVisibleVersion(t *testing.T) {
	m := New(64 * 1024)
	m.Set([]byte("k"), 1, keys.KindInlineValue, []byte("v1"))
	m.Set([]byte("k"), 5, keys.KindInlineValue, []byte("v5"))
	m.Set([]byte("k"), 10, keys.KindInlineValue, []byte("v10"))

	v, _, _ := m.Get([]byte("k"), 7)
	if !bytes.Equal(v, []byte("v5")) {
		t.Errorf("snapshot=7 saw %q, want v5", v)
	}
	v, _, _ = m.Get([]byte("k"), 100)
	if !bytes.Equal(v, []byte("v10")) {
		t.Errorf("snapshot=100 saw %q, want v10", v)
	}
}

func TestDelete_ShadowsValue(t *testing.T) {
	m := New(64 * 1024)
	m.Set([]byte("k"), 1, keys.KindInlineValue, []byte("v1"))
	m.Delete([]byte("k"), 5)
	if _, _, ok := m.Get([]byte("k"), 10); ok {
		t.Error("Get after Delete should not see the value")
	}
	// Older snapshot still sees the value.
	v, _, ok := m.Get([]byte("k"), 3)
	if !ok || !bytes.Equal(v, []byte("v1")) {
		t.Errorf("Get at older snapshot = (%q, %v)", v, ok)
	}
}

func TestDeleteRange_ShadowsCoveredKeys(t *testing.T) {
	m := New(64 * 1024)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		m.Set([]byte(k), 1, keys.KindInlineValue, []byte("v"+k))
	}
	m.DeleteRange([]byte("b"), []byte("d"), 5)

	cases := []struct {
		key  string
		seen bool
	}{
		{"a", true},
		{"b", false},
		{"c", false},
		{"d", true}, // end is exclusive
		{"e", true},
	}
	for _, tc := range cases {
		_, _, ok := m.Get([]byte(tc.key), 10)
		if ok != tc.seen {
			t.Errorf("Get(%q) = %v, want %v", tc.key, ok, tc.seen)
		}
	}
}

func TestDeleteRange_OlderPointWins(t *testing.T) {
	m := New(64 * 1024)
	m.Set([]byte("k"), 10, keys.KindInlineValue, []byte("v"))
	m.DeleteRange([]byte("a"), []byte("z"), 5) // earlier seq
	v, _, ok := m.Get([]byte("k"), 100)
	if !ok || !bytes.Equal(v, []byte("v")) {
		t.Errorf("point at seq=10 should win over range tomb at seq=5: got (%q, %v)", v, ok)
	}
}

func TestDeleteRange_EmptyRange(t *testing.T) {
	m := New(64 * 1024)
	// start == end should be a no-op.
	if !m.DeleteRange([]byte("a"), []byte("a"), 1) {
		t.Fatal("DeleteRange should accept empty range as no-op")
	}
}

func TestIterator_VisibleVersionsOnly(t *testing.T) {
	m := New(64 * 1024)
	m.Set([]byte("a"), 1, keys.KindInlineValue, []byte("a-old"))
	m.Set([]byte("a"), 10, keys.KindInlineValue, []byte("a-new"))
	m.Set([]byte("b"), 5, keys.KindInlineValue, []byte("b"))
	m.Delete([]byte("c"), 3)

	it := m.NewIterator(20)
	defer func() {}()

	var got []string
	for it.First(); it.Valid(); it.Next() {
		got = append(got, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
	}
	sort.Strings(got)
	want := []string{"a=a-new", "b=b"}
	if len(got) != len(want) || !equalStrings(got, want) {
		t.Errorf("iterator: got %v want %v", got, want)
	}
}

func TestIterator_RangeTombstoneSkipsCoveredKeys(t *testing.T) {
	m := New(64 * 1024)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		m.Set([]byte(k), 1, keys.KindInlineValue, []byte(k))
	}
	m.DeleteRange([]byte("b"), []byte("d"), 5)

	it := m.NewIterator(10)
	var got []string
	for it.First(); it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}
	want := []string{"a", "d", "e"}
	if len(got) != len(want) || !equalStrings(got, want) {
		t.Errorf("iterator with range tomb: got %v want %v", got, want)
	}
}

func TestSetVlogPointer(t *testing.T) {
	m := New(64 * 1024)
	ptr := make([]byte, 16)
	binary.BigEndian.PutUint32(ptr[0:4], 7) // file num
	binary.BigEndian.PutUint64(ptr[4:12], 1024)
	binary.BigEndian.PutUint32(ptr[12:16], 4096)
	if !m.Set([]byte("k"), 1, keys.KindVlogPointer, ptr) {
		t.Fatal("Set failed")
	}
	v, kind, ok := m.Get([]byte("k"), 10)
	if !ok || kind != keys.KindVlogPointer || !bytes.Equal(v, ptr) {
		t.Errorf("Get = (%v, %v, %v)", v, kind, ok)
	}
}

func TestSeqTracking(t *testing.T) {
	m := New(64 * 1024)
	m.Set([]byte("k1"), 10, keys.KindInlineValue, []byte("v"))
	m.Set([]byte("k2"), 5, keys.KindInlineValue, []byte("v"))
	m.Set([]byte("k3"), 20, keys.KindInlineValue, []byte("v"))
	if m.MinSeq() != 5 {
		t.Errorf("MinSeq = %d, want 5", m.MinSeq())
	}
	if m.MaxSeq() != 20 {
		t.Errorf("MaxSeq = %d, want 20", m.MaxSeq())
	}
}

func TestSeal_RejectsWrites(t *testing.T) {
	m := New(64 * 1024)
	m.Set([]byte("a"), 1, keys.KindInlineValue, []byte("v"))
	m.Seal()
	if !m.Sealed() {
		t.Fatal("Sealed = false after Seal")
	}
	if m.Set([]byte("b"), 2, keys.KindInlineValue, []byte("v")) {
		t.Error("Set succeeded after Seal")
	}
	// Reads still work.
	if _, _, ok := m.Get([]byte("a"), 10); !ok {
		t.Error("Get failed on sealed memtable")
	}
}

func TestConcurrent_WritesAndReads(t *testing.T) {
	const (
		writers      = 8
		opsPerWriter = 500
	)
	m := New(32 * 1024 * 1024)

	var seq atomic.Uint64
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < opsPerWriter; i++ {
				k := fmt.Sprintf("w%02d-k%04d", w, i)
				s := seq.Add(1)
				if !m.Set([]byte(k), s, keys.KindInlineValue, []byte("v")) {
					t.Errorf("Set failed")
					return
				}
			}
		}(w)
	}

	// Reader goroutine — should not crash even if it reads in-flight writes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5000; i++ {
			_, _, _ = m.Get([]byte("w03-k0100"), seq.Load()+1)
		}
	}()
	wg.Wait()

	if m.MaxSeq() != uint64(writers*opsPerWriter) {
		t.Errorf("MaxSeq = %d, want %d", m.MaxSeq(), writers*opsPerWriter)
	}
}

func equalStrings(a, b []string) bool {
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

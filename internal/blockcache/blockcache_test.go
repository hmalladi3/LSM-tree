package blockcache

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestGet_Miss(t *testing.T) {
	c := New(1<<20, 8)
	if _, ok := c.Get(Key{1, 0}); ok {
		t.Error("Get on empty cache returned hit")
	}
}

func TestPutGet_RoundTrip(t *testing.T) {
	c := New(1<<20, 8)
	val := []byte("hello world")
	if !c.Put(Key{1, 0}, val) {
		t.Fatal("Put rejected")
	}
	got, ok := c.Get(Key{1, 0})
	if !ok {
		t.Fatal("Get reported miss after Put")
	}
	if !bytes.Equal(got, val) {
		t.Errorf("Get = %q want %q", got, val)
	}
}

func TestGet_ReturnsIndependentCopy(t *testing.T) {
	// Mutating the returned slice must not corrupt the cache.
	c := New(1<<20, 8)
	c.Put(Key{1, 0}, []byte("immutable"))
	got, _ := c.Get(Key{1, 0})
	for i := range got {
		got[i] = 'x'
	}
	got2, _ := c.Get(Key{1, 0})
	if !bytes.Equal(got2, []byte("immutable")) {
		t.Errorf("cache corrupted: got %q", got2)
	}
}

func TestEviction_OldestFirst(t *testing.T) {
	// 1024-byte total capacity, 1 shard, 256-byte entries → fits 4.
	// Inserting a 5th evicts the oldest.
	c := New(1024, 1)
	for i := 0; i < 4; i++ {
		c.Put(Key{1, uint64(i)}, bytes.Repeat([]byte{'a' + byte(i)}, 256))
	}
	// All four hit.
	for i := 0; i < 4; i++ {
		if _, ok := c.Get(Key{1, uint64(i)}); !ok {
			t.Errorf("expected hit for key %d", i)
		}
	}
	// Reset LRU order by touching #0 (most recent). Then insert #4.
	c.Get(Key{1, 0})
	c.Put(Key{1, 4}, bytes.Repeat([]byte{'e'}, 256))

	// Key 1 was the oldest in LRU order after the Get(0) touch — should be evicted.
	if _, ok := c.Get(Key{1, 1}); ok {
		t.Error("expected key 1 to be evicted")
	}
	for _, k := range []uint64{0, 2, 3, 4} {
		if _, ok := c.Get(Key{1, k}); !ok {
			t.Errorf("expected key %d still present", k)
		}
	}
}

func TestOversize_Rejected(t *testing.T) {
	c := New(1024, 1)
	big := make([]byte, 4096)
	if c.Put(Key{1, 0}, big) {
		t.Error("oversize Put should be rejected")
	}
	s := c.Stats()
	if s.OversizeRejected != 1 {
		t.Errorf("OversizeRejected = %d", s.OversizeRejected)
	}
	if s.BytesUsed != 0 {
		t.Errorf("BytesUsed = %d after rejected oversize", s.BytesUsed)
	}
}

func TestStats_HitsAndMisses(t *testing.T) {
	c := New(1<<20, 4)
	c.Put(Key{1, 0}, []byte("v"))
	for i := 0; i < 5; i++ {
		c.Get(Key{1, 0}) // hit
	}
	for i := 0; i < 3; i++ {
		c.Get(Key{2, uint64(i)}) // miss
	}
	s := c.Stats()
	if s.Hits != 5 {
		t.Errorf("Hits = %d, want 5", s.Hits)
	}
	if s.Misses != 3 {
		t.Errorf("Misses = %d, want 3", s.Misses)
	}
}

func TestReplace_ExistingKey(t *testing.T) {
	c := New(1<<20, 4)
	c.Put(Key{1, 0}, []byte("v1"))
	c.Put(Key{1, 0}, []byte("v2-larger"))
	got, _ := c.Get(Key{1, 0})
	if !bytes.Equal(got, []byte("v2-larger")) {
		t.Errorf("got %q", got)
	}
}

func TestConcurrent_HighContention(t *testing.T) {
	const (
		writers      = 16
		opsEach      = 1000
		distinctKeys = 100
	)
	c := New(64*1024, 16)
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			val := []byte(fmt.Sprintf("v-%02d", w))
			for i := 0; i < opsEach; i++ {
				k := Key{uint32(w % 4), uint64(i % distinctKeys)}
				if i%3 == 0 {
					c.Put(k, val)
				} else {
					c.Get(k)
				}
			}
		}(w)
	}
	wg.Wait()
	s := c.Stats()
	if s.Hits+s.Misses == 0 {
		t.Errorf("no gets recorded")
	}
}

func TestNew_RejectsBadShardCount(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on non-power-of-two shard count")
		}
	}()
	_ = New(1024, 3)
}

func TestClose_Releases(t *testing.T) {
	c := New(1<<20, 4)
	c.Put(Key{1, 0}, []byte("v"))
	c.Close()
	if _, ok := c.Get(Key{1, 0}); ok {
		t.Error("entry visible after Close")
	}
}

func BenchmarkGet_Hit(b *testing.B) {
	c := New(64<<20, 32)
	key := Key{1, 0}
	c.Put(key, bytes.Repeat([]byte("x"), 4096))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(key)
	}
}

func BenchmarkPut(b *testing.B) {
	c := New(64<<20, 32)
	val := bytes.Repeat([]byte("x"), 4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Put(Key{1, uint64(i)}, val)
	}
}

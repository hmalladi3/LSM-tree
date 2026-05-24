// Package blockcache implements a sharded LRU cache for raw SSTable block
// bytes. Caching post-decode (post-decompression, post-decryption) bytes
// means a hot block is read once, paid for once, then reused by every
// subsequent Get and iterator step that lands on it.
//
// Concurrency. The cache is split into N independent shards (N = power of
// two). Each shard owns its own mutex, LRU list, and map. Operations are
// dispatched to a shard via FNV-1a hash of (file_num, offset); contention
// scales with the number of shards.
//
// Eviction. Each shard is a strict LRU bounded by bytes (not entry count).
// Inserts evict the oldest entries until the new payload fits. An oversize
// insert — bigger than a shard's capacity — is silently rejected and the
// metric `engine.cache.oversize_rejected` is incremented.
package blockcache

import (
	"container/list"
	"sync"
	"sync/atomic"
)

// Key identifies a cache entry. (FileNum, Offset) is globally unique by the
// manifest invariant that file numbers are never reused.
type Key struct {
	FileNum uint32
	Offset  uint64
}

// Cache is a sharded LRU. Construct via New.
type Cache struct {
	shards []*shard
	mask   uint32
}

// Stats summarizes cache activity. Copyable; no internal pointers.
type Stats struct {
	Hits             int64
	Misses           int64
	Evictions        int64
	OversizeRejected int64
	BytesUsed        int64
	Capacity         int64
}

// New returns a cache with `numShards` shards (must be a power of two)
// and total capacity `bytes`. Per-shard capacity is `bytes/numShards`.
func New(bytes int64, numShards int) *Cache {
	if numShards < 1 || numShards&(numShards-1) != 0 {
		panic("blockcache: numShards must be a power of two")
	}
	if bytes < int64(numShards) {
		bytes = int64(numShards)
	}
	per := bytes / int64(numShards)
	shards := make([]*shard, numShards)
	for i := range shards {
		shards[i] = newShard(per)
	}
	return &Cache{
		shards: shards,
		mask:   uint32(numShards - 1),
	}
}

// Get returns a copy of the cached bytes for key, if any. The returned
// slice is independent of the cache and remains valid indefinitely.
//
// We deliberately copy on Get to keep the cache's eviction story simple —
// no refcounts, no handles. The cost is one allocation + copy per cache
// hit; for hot keys the hit rate amortizes it.
func (c *Cache) Get(key Key) ([]byte, bool) {
	s := c.shards[c.shardFor(key)]
	return s.get(key)
}

// Put inserts value under key. Returns true if the value was admitted,
// false if it was too large for the shard's capacity.
//
// The cache takes ownership of `value` — callers must not retain or
// mutate the slice after Put.
func (c *Cache) Put(key Key, value []byte) bool {
	s := c.shards[c.shardFor(key)]
	return s.put(key, value)
}

// Close releases all entries. Safe to call multiple times.
func (c *Cache) Close() error {
	for _, s := range c.shards {
		s.clear()
	}
	return nil
}

// Stats returns a snapshot of cumulative cache counters.
func (c *Cache) Stats() Stats {
	var out Stats
	for _, s := range c.shards {
		out.Hits += s.hits.Load()
		out.Misses += s.misses.Load()
		out.Evictions += s.evictions.Load()
		out.OversizeRejected += s.oversizeRejected.Load()
		s.mu.Lock()
		out.BytesUsed += s.used
		out.Capacity += s.capacity
		s.mu.Unlock()
	}
	return out
}

func (c *Cache) shardFor(key Key) uint32 {
	// FNV-1a over the 12 bytes of the key.
	const (
		offset uint32 = 2166136261
		prime  uint32 = 16777619
	)
	h := offset
	h ^= uint32(key.FileNum)
	h *= prime
	h ^= uint32(key.Offset)
	h *= prime
	h ^= uint32(key.Offset >> 32)
	h *= prime
	return h & c.mask
}

// ----- shard -----

type shard struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	lru      *list.List
	table    map[Key]*list.Element

	// Counters live outside the mutex so Stats() can read them without
	// blocking the shard.
	hits             atomic.Int64
	misses           atomic.Int64
	evictions        atomic.Int64
	oversizeRejected atomic.Int64
}

type cacheEntry struct {
	key   Key
	value []byte
}

func newShard(capacity int64) *shard {
	return &shard{
		capacity: capacity,
		lru:      list.New(),
		table:    make(map[Key]*list.Element),
	}
}

func (s *shard) get(key Key) ([]byte, bool) {
	s.mu.Lock()
	elt, ok := s.table[key]
	if !ok {
		s.mu.Unlock()
		s.misses.Add(1)
		return nil, false
	}
	s.lru.MoveToFront(elt)
	entry := elt.Value.(*cacheEntry)
	out := make([]byte, len(entry.value))
	copy(out, entry.value)
	s.mu.Unlock()
	s.hits.Add(1)
	return out, true
}

func (s *shard) put(key Key, value []byte) bool {
	if int64(len(value)) > s.capacity {
		s.oversizeRejected.Add(1)
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if elt, ok := s.table[key]; ok {
		// Replace existing entry's bytes (rare — callers typically check
		// Get first).
		entry := elt.Value.(*cacheEntry)
		s.used -= int64(len(entry.value))
		entry.value = value
		s.used += int64(len(value))
		s.lru.MoveToFront(elt)
		s.evictUntilFitsLocked()
		return true
	}

	for s.used+int64(len(value)) > s.capacity {
		back := s.lru.Back()
		if back == nil {
			break
		}
		s.removeLocked(back)
		s.evictions.Add(1)
	}
	entry := &cacheEntry{key: key, value: value}
	elt := s.lru.PushFront(entry)
	s.table[key] = elt
	s.used += int64(len(value))
	return true
}

func (s *shard) evictUntilFitsLocked() {
	for s.used > s.capacity {
		back := s.lru.Back()
		if back == nil {
			return
		}
		s.removeLocked(back)
		s.evictions.Add(1)
	}
}

func (s *shard) removeLocked(elt *list.Element) {
	entry := elt.Value.(*cacheEntry)
	delete(s.table, entry.key)
	s.lru.Remove(elt)
	s.used -= int64(len(entry.value))
}

func (s *shard) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lru.Init()
	for k := range s.table {
		delete(s.table, k)
	}
	s.used = 0
}

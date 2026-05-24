package slate

import (
	"sync"
	"sync/atomic"
)

// oracle is the engine's single source of truth for sequence numbers and
// concurrency control. It hands out monotonic timestamps for both reads
// (snapshots) and writes (commit ts), and detects rw-antidependency
// conflicts at commit time.
//
// Reads call snapshot() at Begin; writes call commit() at Commit. Active
// reads are tracked so committed-write history can be pruned once it can
// no longer be required by any live snapshot.
type oracle struct {
	// nextTS is the monotonic counter. It is bumped by commit() and read
	// directly by snapshot(). Atomic for lock-free reads.
	nextTS atomic.Uint64

	mu sync.Mutex
	// committed records concluded transactions whose read sets might be
	// invalidated by a future commit. Pruned by pruneBefore.
	committed []commitRecord

	// activeReads tracks the read_ts of every still-open snapshot/txn so
	// pruneBefore knows the safe high-water mark.
	activeMu sync.Mutex
	active   map[uint64]int // read_ts → refcount
}

type commitRecord struct {
	commitTS uint64
	writeSet map[string]struct{}
}

func newOracle(seed uint64) *oracle {
	o := &oracle{
		active: make(map[uint64]int),
	}
	o.nextTS.Store(seed)
	return o
}

// snapshot returns the current monotonic counter — the largest ts that any
// committed transaction may have been assigned. A txn beginning at this
// ts will see every commit with commit_ts ≤ snapshot.
func (o *oracle) snapshot() uint64 { return o.nextTS.Load() }

// acquireRead registers a live read at ts so pruneBefore won't drop the
// commit history below it.
func (o *oracle) acquireRead(ts uint64) {
	o.activeMu.Lock()
	o.active[ts]++
	o.activeMu.Unlock()
}

// releaseRead unregisters a previously-acquired read.
func (o *oracle) releaseRead(ts uint64) {
	o.activeMu.Lock()
	if n, ok := o.active[ts]; ok {
		if n <= 1 {
			delete(o.active, ts)
		} else {
			o.active[ts] = n - 1
		}
	}
	o.activeMu.Unlock()
}

// activeReaderCount returns the number of distinct active read_ts entries
// (each entry can hold multiple references via its counter). Useful for
// db.Stats — a high value indicates many open snapshots/transactions
// pinning compaction targets.
func (o *oracle) activeReaderCount() int {
	o.activeMu.Lock()
	defer o.activeMu.Unlock()
	n := 0
	for _, count := range o.active {
		n += int(count)
	}
	return n
}

// minActiveRead returns the smallest active read_ts (or nextTS if no reads
// are open). Pruning happens up to this value.
func (o *oracle) minActiveRead() uint64 {
	o.activeMu.Lock()
	defer o.activeMu.Unlock()
	if len(o.active) == 0 {
		return o.nextTS.Load()
	}
	var min uint64 = ^uint64(0)
	for ts := range o.active {
		if ts < min {
			min = ts
		}
	}
	return min
}

// commit attempts to commit a transaction with the given read set / write
// set. On success, it returns a fresh commit_ts and appends a commit
// record. On a rw-antidependency conflict it returns ErrConflict and the
// caller must abort.
//
// readSet may be nil for purely-write operations (no read tracking).
func (o *oracle) commit(readTS uint64, readSet, writeSet map[string]struct{}) (uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Conflict check: any committed write since readTS that touches our
	// read set invalidates this txn.
	if len(readSet) > 0 {
		for i := range o.committed {
			c := &o.committed[i]
			if c.commitTS <= readTS {
				continue
			}
			for k := range c.writeSet {
				if _, hit := readSet[k]; hit {
					return 0, ErrConflict
				}
			}
		}
	}

	commitTS := o.nextTS.Add(1)
	if len(writeSet) > 0 {
		// Copy the writeSet — the caller's map may be reused.
		ws := make(map[string]struct{}, len(writeSet))
		for k := range writeSet {
			ws[k] = struct{}{}
		}
		o.committed = append(o.committed, commitRecord{commitTS: commitTS, writeSet: ws})
	}

	// Opportunistically prune history that no live reader requires. This
	// runs under oracle.mu so it's cheap on the commit path; the
	// active-reads structure tracks the floor independently.
	if floor := o.minActiveRead(); floor > 0 && len(o.committed) > 0 {
		o.pruneBeforeLocked(floor)
	}
	return commitTS, nil
}

// pruneBeforeLocked drops committed records with commitTS ≤ ts. Caller
// must hold o.mu.
func (o *oracle) pruneBeforeLocked(ts uint64) {
	i := 0
	for i < len(o.committed) && o.committed[i].commitTS <= ts {
		i++
	}
	if i == 0 {
		return
	}
	o.committed = append(o.committed[:0], o.committed[i:]...)
}

// (pruneBeforeLocked, called from commit() under oracle.mu, replaced the
// previous standalone pruneBefore — we always have the lock at the point
// we want to prune, so the explicit re-acquire was wasteful.)

// observeSeq seeds nextTS to at least seq+1. Used on Open to recover the
// counter from manifest/WAL state.
func (o *oracle) observeSeq(seq uint64) {
	for {
		cur := o.nextTS.Load()
		if cur > seq {
			return
		}
		if o.nextTS.CompareAndSwap(cur, seq+1) {
			return
		}
	}
}

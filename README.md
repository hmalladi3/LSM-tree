# slate

**slate** is an embedded log-structured merge-tree key-value engine for Go. It provides durable, ordered key-value storage for single-process applications, with WAL-backed crash recovery, configurable durability per write, and zero external runtime dependencies.

```go
db, err := slate.Open("/var/lib/myapp/data", nil)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Transactional API — Serializable Snapshot Isolation.
err = db.Update(func(txn *slate.Txn) error {
    return txn.Set([]byte("hello"), []byte("world"))
})

var got []byte
err = db.View(func(txn *slate.Txn) error {
    v, err := txn.Get([]byte("hello"))
    got = v
    return err
})

// Or the one-shot shortcuts when transactions are unnecessary:
db.Set([]byte("k"), []byte("v"))
v, _ := db.Get([]byte("k"))
```

## Features

- **Lock-free concurrent memtable.** Inserts use CAS on per-level skiplist next pointers; reads never block writes. Arena allocator gives zero-GC-pressure on the hot path.
- **Block-framed WAL with group commit.** A dedicated commit thread amortizes `fdatasync` across all in-flight writers; per-call durability (`Sync`, `Async`, `NoSync`) lets each commit choose its own latency/durability trade-off.
- **Crash-safe by construction.** Every Sync write survives any single-fault crash. On `Open`, the engine replays the WAL into a fresh memtable; torn tails are detected via CRC-32C and discarded silently.
- **Strict resource discipline.** Single-process access enforced via POSIX `flock`; idempotent `Close`; sticky failure state for unrecoverable I/O errors.
- **Predictable defaults.** A one-line `Open` gives you a working database; tuning is opt-in via `Options`.
- **Zero external runtime dependencies.** Standard library only.

## Performance

Measured on Apple M4 (darwin/arm64), Go 1.23.

| Operation | Latency | Allocations |
| --- | --- | --- |
| `Set` (Sync — fdatasync per commit) | 4.8 ms | 15 |
| `Set` (NoSync) | 1.88 µs | 15 |
| `Update` (single-key txn, NoSync) | 1.97 µs | 26 |
| `Update` (100-key batched txn, NoSync) | **1.5M ops/sec** | — |
| `Get` (memtable hit) | 239 ns | 3 |
| `Get` (warm L0 cache hit) | 730 ns | 14 |
| `Get` (cold L0 — cache miss) | 1.37 µs | 11 |
| Range scan (100K keys) | 25.8 ns/key | — |

Internal-component micro-benchmarks:

| Operation | Latency |
| --- | --- |
| CRC-32C (1 KiB block) | 80 ns — 12.8 GB/s |
| Bloom filter `Contains` | 16.7 ns |
| Concurrent skiplist `Insert` | 166 ns, 0 allocs |
| Concurrent skiplist `Get` | 100 ns, 0 allocs |
| Block cache `Get` (hit) | sub-µs (sharded LRU, 32 shards) |

The warm-cache speedup over cold is roughly **2×** on representative workloads. Hit rate on hot working sets reliably exceeds 99 %.

### YCSB workloads

Measured with the bundled `slate-bench` tool on the same hardware. NoSync durability; 50 000 records loaded; 200 000 ops in the run phase; 4 client threads; 16-byte keys, 100-byte values; Zipfian (θ=0.99) key distribution.

| Workload | Mix | Throughput | Read p50 / p99 | Write p50 / p99 |
| --- | --- | --- | --- | --- |
| YCSB-A | 50% read / 50% update | **1.74 M ops/sec** | 167 ns / 583 ns | 500 ns / 58 µs |
| YCSB-B | 95% read /  5% update | **5.69 M ops/sec** | 166 ns / 792 ns | 834 ns / 63 µs |
| YCSB-C | 100% read | **8.15 M ops/sec** | 167 ns / 750 ns | — |
| YCSB-E | 95% scan (1–100 keys) /  5% insert | 8.7 K scans/sec | scan p50 468 µs / p99 757 µs | 3.6 µs / 14 µs |

Sync durability (fdatasync per commit) is bound by the filesystem's sync rate. On macOS APFS, single-threaded sync writes run at ~118 ops/sec; on Linux ext4 with a modern NVMe drive, the same workload typically reaches 5–10× that.

Reproduce on your hardware:

```sh
go build -o slate-bench ./cmd/slate-bench
./slate-bench -workload=c -records=100000 -ops=1000000 -threads=8
```

Run `make bench` for the Go micro-benchmarks.

## Status

This repository is **pre-v1.0**. The on-disk format and the exported API may change between minor versions. The engine is reliable for the workloads it's been tested on, but has known gaps relative to a production-class system. The table below is honest about what's done, what's a known shortcut, and what's deliberately out of scope for this version.

| Subsystem | What works today | Known shortcuts (deferred) |
| --- | --- | --- |
| WAL | block-framed records, group commit, segment rotation, Sync/Async/NoSync durability, recovery with torn-tail detection | each WAL record is one op (no `WriteBatch` framing); flushed segments aren't garbage-collected; broken-state transitions on fsync errors |
| Memtable | lock-free concurrent skiplist, MVCC reads, range-tombstone shadowing, atomic rotation | no arena pool reuse; queue back-pressure is hard-rejection rather than soft-stall |
| Transactions | SSI conflict detection, retry-with-backoff (`Update`), read-your-writes, snapshot isolation | no per-txn `SetDurability`, no `Txn.NewIterator`, no `Txn.DeleteRange`, no `db.NewSnapshot` |
| SSTable | LevelDB-style blocks, prefix compression, restart array, Bloom filter, AEAD-wrapped blocks | no compression (zstd/snappy); no range-deletion block; no prefix bloom |
| Manifest | atomic edit log, snapshot rotation, monotonic non-reusable file numbers, CRC-verified replay | no group-commit batching; no per-file refcounts (only per-Version); no orphan-manifest cleanup on Open |
| Block cache | 32-shard LRU, oversize rejection, post-decryption byte caching, ~2× speedup on warm sets | strict LRU only (no Clock-Pro / TinyLFU) |
| Iterator | merged scans across memtable + immutables + L0 + L1..L6, MVCC + tombstones, bounds, vlog dereference | no `SeekLT` / reverse iteration; no prefetch; no txn-scoped iterator |
| Compaction | leveled, score-based picker, atomic manifest swap, tombstone elimination at L6, no-mid-user-key splits | no rate limiter; no `CompactRange`; no per-worker metrics |
| Encryption | AES-256-GCM for every SST block and every vlog entry; HKDF-SHA-256 subkeys; position-bound AD with domain separation; `IDENTITY` file with key-ID check | WAL is currently plaintext (encrypt-at-the-filesystem if this matters) |
| Vlog (WiscKey) | append-only segments, 16-byte pointer in the LSM, threshold-based placement, AEAD-wrapped values, rotation | no live-byte tracking or GC; vlog grows monotonically (this is the v0.2 roadmap item) |
| Backup / Restore | consistent snapshot (memtable flushed + Version pinned), streaming, auto-cleans torn restore targets, refuses to overwrite a live DB, encrypted round-trip | restore-time options-cross-check is permissive |
| Public API | `Open` / `Close` / `Sync` / `Flush` / `CompactNow` / `Stats` / `View` / `Update` / `BeginTxn` / `Set` / `SetSync` / `Get` / `Delete` / `NewIterator` / `Backup`, single-writer dir lock | no `IngestSST` (planned); close grace period is not a graceful force-rollback yet |

### v0.2 roadmap

- **Value-log GC** — live-byte tracking + segment rewrite. The vlog grows monotonically until this lands.
- **Subcompaction parallelism** for L0→L1.
- **WAL encryption** — symmetric to the SST/vlog AEAD paths.
- **Linux-specific I/O** (`fallocate`, `O_DIRECT` opt-in).
- **Range scans inside transactions** (`Txn.NewIterator`, `Txn.DeleteRange`).
- **Reverse iteration** (`Iterator.SeekLT` / `Prev`).

## Build

```sh
git clone https://github.com/harimalladi/slate
cd slate
make test
```

Requires Go 1.22 or newer. The default `make test` target runs every package under the race detector.

## Quick demo with the CLI

```sh
go build -o /tmp/slatectl ./cmd/slatectl

# Basic operations.
DB=/tmp/demo
/tmp/slatectl "$DB" set hello world
/tmp/slatectl "$DB" set foo bar
/tmp/slatectl "$DB" get hello   # → world
/tmp/slatectl "$DB" delete foo
/tmp/slatectl "$DB" get foo     # → exit code 3 (not found)

# Streaming backup and restore.
/tmp/slatectl "$DB" backup > /tmp/snapshot.bin
/tmp/slatectl /tmp/restored restore < /tmp/snapshot.bin
/tmp/slatectl /tmp/restored get hello   # → world
```

## Why another KV store?

The Go ecosystem has three established embedded KV engines, each with structural trade-offs:

- **bbolt** is a single B+-tree with a global write lock — unsuited for write-heavy workloads, no compression, no built-in encryption.
- **BadgerDB** implements a strict WiscKey LSM; small-value scans pay one random read per value, and its garbage collector has historically been the source of operational pain.
- **Pebble** is excellent but is engineered as CockroachDB's internal storage engine; its surface area reflects that posture rather than that of a friendly embeddable library.

slate aims for the gap between them: the write throughput of an LSM, the small-value scan performance of inline storage, and an honest, embeddable API with clean documentation.

## Layout

```
slate/
├── db.go              public DB type and lifecycle
├── flush.go           background memtable → L0 flush worker
├── options.go         configurable options
├── errors.go          exported error sentinels
├── doc.go             package-level godoc
├── example_test.go    runnable godoc examples
├── cmd/slatectl/      CLI for ad-hoc inspection
├── cmd/slate-bench/   YCSB-style benchmark harness
├── compaction.go      leveled-compaction picker and worker
├── iterator.go        merged iterator across memtable + L0 + L1..L6
├── leveliter.go       per-level concat iterator for L1..L6
├── oracle.go          SSI oracle: monotonic counter + commit history
├── txn.go             Txn type + View/Update/BeginTxn
├── identity.go        IDENTITY file + encryption-mode validation
└── internal/
    ├── arena/         bump allocator for the memtable
    ├── blockcache/    sharded LRU cache for SST block bytes
    ├── bloom/         double-hashing Bloom filter
    ├── crc/           CRC-32C (Castagnoli)
    ├── encryption/    AES-256-GCM AEAD + HKDF subkey derivation
    ├── keys/          internal-key encoding (bit-inverted seq)
    ├── manifest/      atomic edit log + Version tracking
    ├── memtable/      MVCC memtable on top of skl
    ├── record/        WAL record codec
    ├── skl/           lock-free concurrent skiplist
    ├── sstable/       LevelDB-style block format with bloom + AEAD
    ├── vlog/          WiscKey-style append-only value log
    └── wal/           block-framed write-ahead log
```

## License

MIT — see `LICENSE`.

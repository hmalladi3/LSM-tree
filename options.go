package slate

import (
	"errors"
	"fmt"

	"github.com/harimalladi/slate/internal/wal"
)

// Durability classifies how a Set or Delete waits on persistence.
type Durability uint8

const (
	// Sync (the default) blocks until the operation is fdatasynced to disk.
	// A successful Sync write survives any single-fault crash.
	Sync Durability = iota

	// Async returns after the WAL Write syscall completes but before
	// fdatasync. Subsequent reads in the same process see the write,
	// but a crash before the next fdatasync may lose it.
	Async

	// NoSync returns as soon as the WAL append is enqueued. Lower latency
	// but survives only an orderly Close.
	NoSync
)

// Options configures a slate database.
//
// Construct via DefaultOptions and override fields as needed; call Validate
// to surface configuration mistakes early.
type Options struct {
	// MemtableSize is the byte budget for one in-memory memtable. When the
	// active memtable's arena reaches this size, the database rotates to a
	// fresh active memtable. Default 64 MiB.
	MemtableSize int

	// WALSegmentSize is the byte budget for one WAL segment file. Default
	// 64 MiB.
	WALSegmentSize int64

	// DefaultDurability is the durability mode used by Set / Delete and
	// by the convenience helpers (View, Update) when no per-call mode is
	// specified. Default Sync.
	DefaultDurability Durability

	// MaxKeySize is the largest accepted key size in bytes. Writes with
	// larger keys fail with ErrKeyTooLarge. Default 64 KiB.
	MaxKeySize int

	// MaxValueSize is the largest accepted value size in bytes. Writes with
	// larger values fail with ErrValueTooLarge. Default 16 MiB while value-
	// log support is pending; once vlog ships this will rise to 512 MiB.
	MaxValueSize int

	// L0CompactionTrigger is the L0 file count at which compaction is
	// scheduled. Default 4.
	L0CompactionTrigger int

	// LevelSizeMultiplier is the geometric size growth factor between
	// successive levels. Default 10.
	LevelSizeMultiplier int

	// L1TargetSize is the byte budget for L1. Each subsequent level's
	// budget is `LevelSizeMultiplier` times the previous. Default 64 MiB.
	L1TargetSize int64

	// TargetFileSize is the byte target for individual SSTable files at
	// levels L1+. Compaction closes the current output file once its size
	// exceeds this target (modulo the no-mid-user-key-split invariant).
	// Default 4 MiB.
	TargetFileSize int64

	// BlockCacheSize is the total byte budget for the shared block cache.
	// A value of zero disables caching. Default 64 MiB.
	BlockCacheSize int64

	// BlockCacheShards is the number of independent shards inside the
	// block cache. Must be a power of two. Default 32.
	BlockCacheShards int

	// EncryptionKey, if non-nil, enables AES-256-GCM encryption at rest.
	// Must be exactly 32 bytes. The same key must be supplied on every
	// Open of the same directory; mismatches return
	// ErrEncryptionKeyMismatch. Encryption mode is immutable after a
	// directory is created.
	EncryptionKey []byte

	// ValueThreshold is the value-byte size at or above which writes are
	// spilled to the value log (WiscKey-style key/value separation).
	// Smaller values stay inline in the SSTable. Default 1 KiB. Set to a
	// very large value to disable; values up to MaxValueSize are then
	// always inlined.
	ValueThreshold int

	// VlogSegmentSize is the byte cap on one value-log segment file. Once
	// exceeded, the writer rotates to a fresh segment. Default 1 GiB.
	VlogSegmentSize int64

	// TxnMaxKeys caps the number of distinct keys mutated in a single
	// transaction. Exceeding the cap returns ErrTxnTooLarge at the
	// offending Set/Delete. A value of 0 disables the cap. Default 10 000.
	TxnMaxKeys int

	// TxnMaxBytes caps the buffered byte cost of a single transaction.
	// Default 16 MiB; must be strictly below MemtableSize so a committed
	// txn fits in the active memtable with room to spare.
	TxnMaxBytes int
}

// DefaultOptions returns a sensible-defaults Options value.
func DefaultOptions() *Options {
	return &Options{
		MemtableSize:        64 << 20,
		WALSegmentSize:      64 << 20,
		DefaultDurability:   Sync,
		MaxKeySize:          64 << 10,
		MaxValueSize:        16 << 20,
		L0CompactionTrigger: 4,
		LevelSizeMultiplier: 10,
		L1TargetSize:        64 << 20,
		TargetFileSize:      4 << 20,
		BlockCacheSize:      64 << 20,
		BlockCacheShards:    32,
		ValueThreshold:      1 << 10, // 1 KiB
		VlogSegmentSize:     1 << 30, // 1 GiB
		TxnMaxKeys:          10_000,
		TxnMaxBytes:         16 << 20, // 16 MiB
	}
}

// Validate returns nil if the options are internally consistent, or an
// error wrapping ErrInvalidOption naming the offending field.
func (o *Options) Validate() error {
	if o.MemtableSize < 64<<10 {
		return fmt.Errorf("%w: MemtableSize must be >= 64 KiB", ErrInvalidOption)
	}
	if o.WALSegmentSize < 4<<10 {
		return fmt.Errorf("%w: WALSegmentSize must be >= 4 KiB", ErrInvalidOption)
	}
	if o.MaxKeySize < 1 {
		return fmt.Errorf("%w: MaxKeySize must be >= 1", ErrInvalidOption)
	}
	if o.MaxValueSize < 0 {
		return fmt.Errorf("%w: MaxValueSize must be >= 0", ErrInvalidOption)
	}
	if o.MaxValueSize > int(o.WALSegmentSize)/2 {
		return fmt.Errorf("%w: MaxValueSize (%d) must be <= WALSegmentSize/2 (%d)", ErrInvalidOption, o.MaxValueSize, int(o.WALSegmentSize)/2)
	}
	switch o.DefaultDurability {
	case Sync, Async, NoSync:
	default:
		return fmt.Errorf("%w: DefaultDurability invalid", ErrInvalidOption)
	}
	if o.L0CompactionTrigger < 1 {
		return fmt.Errorf("%w: L0CompactionTrigger must be >= 1", ErrInvalidOption)
	}
	if o.LevelSizeMultiplier < 2 {
		return fmt.Errorf("%w: LevelSizeMultiplier must be >= 2", ErrInvalidOption)
	}
	if o.L1TargetSize < 1 {
		return fmt.Errorf("%w: L1TargetSize must be >= 1", ErrInvalidOption)
	}
	if o.TargetFileSize < 1 {
		return fmt.Errorf("%w: TargetFileSize must be >= 1", ErrInvalidOption)
	}
	if o.BlockCacheSize < 0 {
		return fmt.Errorf("%w: BlockCacheSize must be >= 0", ErrInvalidOption)
	}
	if o.BlockCacheSize > 0 {
		if o.BlockCacheShards < 1 || o.BlockCacheShards&(o.BlockCacheShards-1) != 0 {
			return fmt.Errorf("%w: BlockCacheShards must be a power of two", ErrInvalidOption)
		}
	}
	if o.EncryptionKey != nil && len(o.EncryptionKey) != 32 {
		return fmt.Errorf("%w: EncryptionKey must be 32 bytes (got %d)", ErrInvalidOption, len(o.EncryptionKey))
	}
	if o.ValueThreshold < 0 {
		return fmt.Errorf("%w: ValueThreshold must be >= 0", ErrInvalidOption)
	}
	if o.VlogSegmentSize < 1 {
		return fmt.Errorf("%w: VlogSegmentSize must be >= 1", ErrInvalidOption)
	}
	if o.TxnMaxKeys < 0 {
		return fmt.Errorf("%w: TxnMaxKeys must be >= 0", ErrInvalidOption)
	}
	if o.TxnMaxBytes < 0 {
		return fmt.Errorf("%w: TxnMaxBytes must be >= 0", ErrInvalidOption)
	}
	if o.TxnMaxBytes > 0 && o.TxnMaxBytes >= o.MemtableSize {
		// Auto-cap to half the memtable so a single committed txn always
		// fits with room to spare. Surface the adjustment via the field
		// so callers can inspect it.
		o.TxnMaxBytes = o.MemtableSize / 2
		if o.TxnMaxBytes < 1 {
			o.TxnMaxBytes = 1
		}
	}
	return nil
}

func (d Durability) wal() wal.Durability {
	switch d {
	case Sync:
		return wal.Sync
	case Async:
		return wal.Async
	default:
		return wal.NoSync
	}
}

var errInternal = errors.New("slate: internal error")

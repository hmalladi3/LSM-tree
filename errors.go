package slate

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound                 = errors.New("slate: key not found")
	ErrConflict                 = errors.New("slate: transaction conflict")
	ErrClosed                   = errors.New("slate: database is closed")
	ErrReadOnly                 = errors.New("slate: transaction is read-only")
	ErrTxnClosed                = errors.New("slate: transaction is closed")
	ErrTxnTooLarge              = errors.New("slate: transaction exceeds size limit")
	ErrValueTooLarge            = errors.New("slate: value exceeds maximum size")
	ErrKeyTooLarge              = errors.New("slate: key exceeds maximum size")
	ErrDiskFull                 = errors.New("slate: disk full")
	ErrCorrupted                = errors.New("slate: data is corrupted")
	ErrEngineFailed             = errors.New("slate: engine is in a failed state")
	ErrLocked                   = errors.New("slate: another process holds the database lock")
	ErrUnsupportedVersion       = errors.New("slate: on-disk format version is not supported")
	ErrInvalidOption            = errors.New("slate: invalid option")
	ErrEncryptionKeyMismatch    = errors.New("slate: encryption key does not match this database")
	ErrBackupInProgress         = errors.New("slate: another backup is in progress")
	ErrDirectoryNotEmpty        = errors.New("slate: restore directory is not empty")
	ErrUnsupportedBackupVersion = errors.New("slate: backup stream version is not supported")
	ErrIngestOverlap            = errors.New("slate: ingested SSTable overlaps existing data")
)

// KeyError wraps an underlying error with the operation and key it concerns.
// Returned for per-key failures inside an otherwise-healthy transaction.
type KeyError struct {
	Op  string
	Key []byte
	Err error
}

func (e *KeyError) Error() string {
	if len(e.Key) == 0 {
		return fmt.Sprintf("slate: %s: %v", e.Op, e.Err)
	}
	return fmt.Sprintf("slate: %s %q: %v", e.Op, e.Key, e.Err)
}

func (e *KeyError) Unwrap() error { return e.Err }

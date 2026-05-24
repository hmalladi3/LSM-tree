//go:build linux || darwin || freebsd || netbsd || openbsd

package slate

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// acquireLock takes an exclusive non-blocking lock on path. The caller owns
// the returned file and must call releaseLock to drop it.
func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("slate: opening lock file: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("slate: locking: %w", err)
	}
	return f, nil
}

func releaseLock(f *os.File) error {
	if f == nil {
		return nil
	}
	// Closing the descriptor releases the flock. Explicit unlock first gives
	// us a precise error if the kernel disagrees.
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return f.Close()
}

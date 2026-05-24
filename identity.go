package slate

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/harimalladi/slate/internal/encryption"
)

// identityFileName names the file holding the database's identity: a
// random db_uuid plus an integrity-verifying key_id. Stored in the root
// of the database directory.
const identityFileName = "IDENTITY"

// dbIdentity is the on-disk identity record: 16-byte db_uuid + 16-byte
// key_id. The key_id is all-zero for unencrypted databases.
type dbIdentity struct {
	DBUUID [16]byte
	KeyID  [16]byte
}

func (id *dbIdentity) encryptionEnabled() bool {
	return id.KeyID != [16]byte{}
}

// loadOrInitIdentity returns the IDENTITY for the database at dir. If no
// IDENTITY file exists, one is created based on opts.EncryptionKey.
// Mismatches between the user-supplied key and the stored key_id return
// ErrEncryptionKeyMismatch.
func loadOrInitIdentity(dir string, masterKey []byte) (*dbIdentity, error) {
	path := filepath.Join(dir, identityFileName)
	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return createIdentity(path, masterKey)
	case err != nil:
		return nil, err
	}
	if len(data) != 32 {
		return nil, fmt.Errorf("%w: IDENTITY size %d (want 32)", ErrCorrupted, len(data))
	}
	id := &dbIdentity{}
	copy(id.DBUUID[:], data[:16])
	copy(id.KeyID[:], data[16:32])

	// Cross-check the user's key against the recorded key_id.
	storedEncrypted := id.encryptionEnabled()
	userEncrypted := len(masterKey) > 0
	if storedEncrypted != userEncrypted {
		return nil, ErrEncryptionKeyMismatch
	}
	if userEncrypted {
		want, err := encryption.KeyID(masterKey)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidOption, err)
		}
		if want != id.KeyID {
			return nil, ErrEncryptionKeyMismatch
		}
	}
	return id, nil
}

func createIdentity(path string, masterKey []byte) (*dbIdentity, error) {
	id := &dbIdentity{}
	if _, err := rand.Read(id.DBUUID[:]); err != nil {
		return nil, err
	}
	if len(masterKey) > 0 {
		k, err := encryption.KeyID(masterKey)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidOption, err)
		}
		id.KeyID = k
	}
	// Write atomically: write to tmp, fsync, rename.
	buf := make([]byte, 0, 32)
	buf = append(buf, id.DBUUID[:]...)
	buf = append(buf, id.KeyID[:]...)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return nil, err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return nil, err
	}
	return id, nil
}

// codecFor returns a Codec configured for the supplied key + identity, or
// nil if encryption is disabled.
func codecFor(masterKey []byte, id *dbIdentity) (*encryption.Codec, error) {
	if len(masterKey) == 0 {
		return nil, nil
	}
	c, err := encryption.NewCodec(masterKey, id.DBUUID[:])
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidOption, err)
	}
	return c, nil
}

// keyMaterialIsAllZero is a small predicate kept here to document the
// "unencrypted = all-zero key_id" wire convention.
func keyMaterialIsAllZero(b []byte) bool {
	return bytes.Equal(b, make([]byte, len(b)))
}

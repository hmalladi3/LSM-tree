// Package encryption wraps AES-256-GCM with deterministic nonces for slate's
// at-rest encryption layer.
//
// Threat model. The cipher protects on-disk content against an attacker
// with read access to the filesystem (a stolen disk, a leaked backup).
// It does NOT protect against an attacker with read access to live process
// memory.
//
// Construction. The master key is a 32-byte secret supplied by the caller
// at DB-open time. Per-component data-encryption keys (DEKs) are derived
// via HKDF-SHA-256:
//
//	DEK_SST = HKDF(master, salt=db_uuid, info="slate-sst-key-v1")
//
// Nonces are deterministic from on-disk position — for each SST block,
// nonce = [file_num: 4B big-endian][block_offset: 8B big-endian]. Because
// the manifest never reuses file numbers across a database's lifetime
// (see internal/manifest, MAN-INV-001), and offsets within one file are
// unique by construction, the (key, nonce) pair is never repeated. This
// is the load-bearing invariant that makes AES-GCM safe to use here
// without an explicit per-block nonce store.
//
// Associated data binds the ciphertext to its on-disk position so a
// misplaced block (corruption, swap with another file) fails AEAD
// verification rather than producing wrong plaintext.
package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
)

// KeyLen is the master-key length: 32 bytes for AES-256.
const KeyLen = 32

// Codec encodes and decodes block payloads. A nil *Codec acts as a
// pass-through (no encryption).
type Codec struct {
	sst cipher.AEAD
}

// ErrAuth is returned by Decrypt when the AEAD authentication tag does not
// match. Treat as ErrCorrupted at the engine layer.
var ErrAuth = errors.New("encryption: authentication failed")

// ErrInvalidKey is returned by NewCodec when the supplied master key is the
// wrong size.
var ErrInvalidKey = errors.New("encryption: master key must be 32 bytes")

// NewCodec constructs a codec for SST blocks. salt should be the DB UUID
// (16 bytes recommended); it scopes derived keys to a single DB so that
// two databases sharing the same master key do not produce the same
// ciphertext for the same plaintext.
func NewCodec(masterKey, salt []byte) (*Codec, error) {
	if len(masterKey) != KeyLen {
		return nil, ErrInvalidKey
	}
	dek := hkdfExpand(masterKey, salt, []byte("slate-sst-key-v1"), 32)
	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Codec{sst: gcm}, nil
}

// SSTBlockNonce returns the deterministic nonce for an SST data block at
// (fileNum, blockOffset).
func SSTBlockNonce(fileNum uint32, blockOffset uint64) [12]byte {
	var n [12]byte
	binary.BigEndian.PutUint32(n[0:4], fileNum)
	binary.BigEndian.PutUint64(n[4:12], blockOffset)
	return n
}

// VlogEntryNonce returns the deterministic nonce for a vlog entry at
// (fileNum, entryOffset). Domain-separated from SSTBlockNonce by the AD
// (different prefix), so we can reuse the same DEK across scopes.
func VlogEntryNonce(fileNum uint32, entryOffset uint64) [12]byte {
	var n [12]byte
	binary.BigEndian.PutUint32(n[0:4], fileNum)
	binary.BigEndian.PutUint64(n[4:12], entryOffset)
	return n
}

// VlogEntryAD returns the associated-data bytes used when sealing/opening
// a vlog entry. The "vlog-v1" prefix prevents a vlog frame from ever
// successfully decrypting if confused with an SST block (whose AD prefix
// is "sst-v1"). Prefixes match EARS specs ENC-SST-003 and ENC-VLOG-003.
func VlogEntryAD(fileNum uint32, entryOffset uint64) []byte {
	var buf [len("vlog-v1") + 4 + 8]byte
	copy(buf[:len("vlog-v1")], "vlog-v1")
	pos := len("vlog-v1")
	binary.BigEndian.PutUint32(buf[pos:pos+4], fileNum)
	pos += 4
	binary.BigEndian.PutUint64(buf[pos:pos+8], entryOffset)
	return buf[:]
}

// SSTBlockAD returns the associated-data bytes used when sealing/opening an
// SST block at the given position. Binding ciphertext to position prevents
// a misplaced block from decrypting silently.
//
// Prefix matches the EARS spec (ENC-SST-003): "sst-v1".
func SSTBlockAD(fileNum uint32, level int, blockOffset uint64) []byte {
	var buf [len("sst-v1") + 4 + 1 + 8]byte
	copy(buf[:len("sst-v1")], "sst-v1")
	pos := len("sst-v1")
	binary.BigEndian.PutUint32(buf[pos:pos+4], fileNum)
	pos += 4
	buf[pos] = byte(level)
	pos++
	binary.BigEndian.PutUint64(buf[pos:pos+8], blockOffset)
	return buf[:]
}

// Seal encrypts plaintext under the SST DEK and prepends the nonce to the
// returned ciphertext. Result layout: [nonce:12B][gcm_tag:16B][ciphertext].
// The total size is len(plaintext) + 28 bytes.
func (c *Codec) Seal(plaintext []byte, fileNum uint32, level int, blockOffset uint64) []byte {
	if c == nil {
		return plaintext
	}
	nonce := SSTBlockNonce(fileNum, blockOffset)
	ad := SSTBlockAD(fileNum, level, blockOffset)
	out := make([]byte, 12, 12+len(plaintext)+c.sst.Overhead())
	copy(out[:12], nonce[:])
	out = c.sst.Seal(out, nonce[:], plaintext, ad)
	return out
}

// Open inverses Seal. ciphertext is the on-disk bytes starting with the
// embedded nonce.
func (c *Codec) Open(ciphertext []byte, fileNum uint32, level int, blockOffset uint64) ([]byte, error) {
	if c == nil {
		return ciphertext, nil
	}
	if len(ciphertext) < 12+c.sst.Overhead() {
		return nil, fmt.Errorf("%w: ciphertext too short", ErrAuth)
	}
	nonce := ciphertext[:12]
	body := ciphertext[12:]
	ad := SSTBlockAD(fileNum, level, blockOffset)
	plaintext, err := c.sst.Open(nil, nonce, body, ad)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAuth, err)
	}
	return plaintext, nil
}

// Overhead returns the extra bytes a Seal adds to a plaintext: 12 (nonce) +
// 16 (GCM tag). Useful for offset arithmetic.
func (c *Codec) Overhead() int {
	if c == nil {
		return 0
	}
	return 12 + c.sst.Overhead()
}

// SealVlog encrypts a vlog value bytes with deterministic nonce / AD. The
// result layout is `[nonce:12B][gcm_tag:16B][ciphertext]` (the same shape
// SealSST produces).
func (c *Codec) SealVlog(plaintext []byte, fileNum uint32, entryOffset uint64) []byte {
	if c == nil {
		return plaintext
	}
	nonce := VlogEntryNonce(fileNum, entryOffset)
	ad := VlogEntryAD(fileNum, entryOffset)
	out := make([]byte, 12, 12+len(plaintext)+c.sst.Overhead())
	copy(out[:12], nonce[:])
	out = c.sst.Seal(out, nonce[:], plaintext, ad)
	return out
}

// OpenVlog is the inverse of SealVlog.
func (c *Codec) OpenVlog(ciphertext []byte, fileNum uint32, entryOffset uint64) ([]byte, error) {
	if c == nil {
		return ciphertext, nil
	}
	if len(ciphertext) < 12+c.sst.Overhead() {
		return nil, fmt.Errorf("%w: vlog ciphertext too short", ErrAuth)
	}
	nonce := ciphertext[:12]
	body := ciphertext[12:]
	ad := VlogEntryAD(fileNum, entryOffset)
	plaintext, err := c.sst.Open(nil, nonce, body, ad)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAuth, err)
	}
	return plaintext, nil
}

// KeyID returns a deterministic 16-byte identifier for a master key. It
// proves possession of the key without revealing it; the DB stores this
// value in the IDENTITY file and compares against the user-supplied key
// at every Open.
func KeyID(masterKey []byte) ([16]byte, error) {
	if len(masterKey) != KeyLen {
		return [16]byte{}, ErrInvalidKey
	}
	mac := hmac.New(sha256.New, masterKey)
	mac.Write([]byte("slate-key-id-v1"))
	sum := mac.Sum(nil)
	var out [16]byte
	copy(out[:], sum[:16])
	return out, nil
}

// hkdfExpand is a minimal HKDF-SHA256 implementation (RFC 5869). We avoid
// pulling in golang.org/x/crypto/hkdf to keep slate's runtime dependency
// list empty.
func hkdfExpand(secret, salt, info []byte, length int) []byte {
	// Extract.
	if len(salt) == 0 {
		salt = make([]byte, sha256.Size)
	}
	mac := hmac.New(sha256.New, salt)
	mac.Write(secret)
	prk := mac.Sum(nil)

	// Expand.
	out := make([]byte, 0, length)
	var prev []byte
	for counter := byte(1); len(out) < length; counter++ {
		mac := hmac.New(sha256.New, prk)
		mac.Write(prev)
		mac.Write(info)
		mac.Write([]byte{counter})
		prev = mac.Sum(nil)
		out = append(out, prev...)
	}
	return out[:length]
}

package encryption

import (
	"bytes"
	"crypto/rand"
	"errors"
	"testing"
)

func randomKey(t testing.TB) []byte {
	t.Helper()
	k := make([]byte, KeyLen)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

func TestNewCodec_RejectsWrongKeySize(t *testing.T) {
	for _, n := range []int{0, 1, 16, 31, 33, 64} {
		_, err := NewCodec(make([]byte, n), nil)
		if !errors.Is(err, ErrInvalidKey) {
			t.Errorf("NewCodec(%d bytes) = %v", n, err)
		}
	}
}

func TestSealOpen_RoundTrip(t *testing.T) {
	c, err := NewCodec(randomKey(t), []byte("salt-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	plaintexts := [][]byte{
		nil,
		{},
		[]byte("a"),
		[]byte("hello world"),
		bytes.Repeat([]byte("x"), 4096),
	}
	for _, pt := range plaintexts {
		ct := c.Seal(pt, 7, 1, 1024)
		got, err := c.Open(ct, 7, 1, 1024)
		if err != nil {
			t.Fatalf("Open(%d bytes): %v", len(pt), err)
		}
		if !bytes.Equal(got, pt) {
			t.Errorf("plaintext mismatch: got %v want %v", got, pt)
		}
	}
}

func TestOpen_DetectsTamper(t *testing.T) {
	c, _ := NewCodec(randomKey(t), []byte("salt"))
	pt := []byte("important data")
	ct := c.Seal(pt, 1, 0, 100)
	// Flip a bit inside the ciphertext body (after the 12-byte nonce).
	ct[20] ^= 0x01
	if _, err := c.Open(ct, 1, 0, 100); !errors.Is(err, ErrAuth) {
		t.Errorf("Open of tampered ciphertext: got %v, want ErrAuth", err)
	}
}

func TestOpen_DetectsMisplacement(t *testing.T) {
	// A block sealed at (fileNum=1, blockOffset=100) must fail to open at
	// any other (fileNum, blockOffset) — the associated data binds it.
	c, _ := NewCodec(randomKey(t), []byte("salt"))
	pt := []byte("payload")
	ct := c.Seal(pt, 1, 0, 100)

	if _, err := c.Open(ct, 2, 0, 100); !errors.Is(err, ErrAuth) {
		t.Errorf("wrong file_num: got %v", err)
	}
	if _, err := c.Open(ct, 1, 0, 200); !errors.Is(err, ErrAuth) {
		t.Errorf("wrong block_offset: got %v", err)
	}
	if _, err := c.Open(ct, 1, 5, 100); !errors.Is(err, ErrAuth) {
		t.Errorf("wrong level: got %v", err)
	}
}

func TestOpen_DetectsWrongKey(t *testing.T) {
	c1, _ := NewCodec(randomKey(t), []byte("salt"))
	c2, _ := NewCodec(randomKey(t), []byte("salt"))
	ct := c1.Seal([]byte("plaintext"), 1, 0, 0)
	if _, err := c2.Open(ct, 1, 0, 0); !errors.Is(err, ErrAuth) {
		t.Errorf("Open with wrong key: got %v", err)
	}
}

func TestNilCodec_IsPassThrough(t *testing.T) {
	var c *Codec
	pt := []byte("plain")
	if got := c.Seal(pt, 0, 0, 0); !bytes.Equal(got, pt) {
		t.Errorf("nil Seal not pass-through: got %v", got)
	}
	got, err := c.Open(pt, 0, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, pt) {
		t.Errorf("nil Open not pass-through: got %v", got)
	}
	if c.Overhead() != 0 {
		t.Errorf("nil Overhead = %d", c.Overhead())
	}
}

func TestKeyID_StableAndDistinct(t *testing.T) {
	k1 := randomKey(t)
	k2 := randomKey(t)
	id1, err := KeyID(k1)
	if err != nil {
		t.Fatal(err)
	}
	id1b, _ := KeyID(k1)
	id2, _ := KeyID(k2)
	if id1 != id1b {
		t.Error("KeyID not stable across calls")
	}
	if id1 == id2 {
		t.Error("KeyID collided across distinct keys")
	}
}

func TestSSTBlockNonce_DeterministicAndUnique(t *testing.T) {
	n1 := SSTBlockNonce(1, 0)
	n2 := SSTBlockNonce(1, 0)
	if n1 != n2 {
		t.Error("nonce not deterministic")
	}
	if SSTBlockNonce(1, 0) == SSTBlockNonce(1, 1) {
		t.Error("nonce collision across offsets")
	}
	if SSTBlockNonce(1, 0) == SSTBlockNonce(2, 0) {
		t.Error("nonce collision across file_nums")
	}
}

func TestOverhead_ConsistentWithSeal(t *testing.T) {
	c, _ := NewCodec(randomKey(t), []byte("salt"))
	pt := bytes.Repeat([]byte("x"), 100)
	ct := c.Seal(pt, 1, 0, 0)
	if len(ct) != len(pt)+c.Overhead() {
		t.Errorf("Overhead = %d, but Seal grew %d", c.Overhead(), len(ct)-len(pt))
	}
}

func TestHKDF_StandardVector(t *testing.T) {
	// RFC 5869 Test Case 1.
	ikm := []byte{0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b}
	salt := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	info := []byte{0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9}
	expected := []byte{0x3c, 0xb2, 0x5f, 0x25, 0xfa, 0xac, 0xd5, 0x7a, 0x90, 0x43, 0x4f, 0x64, 0xd0, 0x36, 0x2f, 0x2a, 0x2d, 0x2d, 0x0a, 0x90, 0xcf, 0x1a, 0x5a, 0x4c, 0x5d, 0xb0, 0x2d, 0x56, 0xec, 0xc4, 0xc5, 0xbf, 0x34, 0x00, 0x72, 0x08, 0xd5, 0xb8, 0x87, 0x18, 0x58, 0x65}
	got := hkdfExpand(ikm, salt, info, len(expected))
	if !bytes.Equal(got, expected) {
		t.Errorf("HKDF RFC vector mismatch")
	}
}

func BenchmarkSeal(b *testing.B) {
	c, _ := NewCodec(randomKey(b), []byte("salt"))
	pt := bytes.Repeat([]byte("x"), 4096)
	b.SetBytes(int64(len(pt)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Seal(pt, 1, 0, uint64(i))
	}
}

func BenchmarkOpen(b *testing.B) {
	c, _ := NewCodec(randomKey(b), []byte("salt"))
	pt := bytes.Repeat([]byte("x"), 4096)
	ct := c.Seal(pt, 1, 0, 0)
	b.SetBytes(int64(len(pt)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Open(ct, 1, 0, 0); err != nil {
			b.Fatal(err)
		}
	}
}

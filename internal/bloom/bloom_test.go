package bloom

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

func TestRoundTrip_AllAddedKeysContained(t *testing.T) {
	b := NewBuilder(10)
	keys := [][]byte{
		[]byte("alpha"),
		[]byte("beta"),
		[]byte("gamma"),
		[]byte("delta"),
		[]byte(""),
		[]byte("with\x00null"),
	}
	for _, k := range keys {
		b.Add(k)
	}
	f := NewFilter(b.Bytes())
	for _, k := range keys {
		if !f.Contains(k) {
			t.Errorf("filter missing key %q", k)
		}
	}
}

func TestFalsePositiveRate_BelowExpected(t *testing.T) {
	const (
		n          = 10_000
		bitsPerKey = 10
		probes     = 100_000
		// Expected ≈ 0.82%. Allow 3x headroom for variance — if we see >3%
		// the filter is broken.
		maxFPRate = 0.03
	)
	rng := rand.New(rand.NewSource(1))
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = make([]byte, 16)
		binary.LittleEndian.PutUint64(keys[i], uint64(i))
		rng.Read(keys[i][8:])
	}
	b := NewBuilder(bitsPerKey)
	for _, k := range keys {
		b.Add(k)
	}
	f := NewFilter(b.Bytes())

	fp := 0
	probe := make([]byte, 16)
	for i := 0; i < probes; i++ {
		// Probe with keys NOT in the set: derive from i^huge offset.
		binary.LittleEndian.PutUint64(probe, uint64(i)+1<<40)
		rng.Read(probe[8:])
		if f.Contains(probe) {
			fp++
		}
	}
	rate := float64(fp) / float64(probes)
	if rate > maxFPRate {
		t.Fatalf("FP rate %.4f exceeds bound %.4f", rate, maxFPRate)
	}
	t.Logf("FP rate at %d bits/key with %d keys: %.4f (%d/%d)", bitsPerKey, n, rate, fp, probes)
}

func TestEmpty_NoFalseMatches(t *testing.T) {
	b := NewBuilder(10)
	f := NewFilter(b.Bytes())
	if f == nil {
		t.Fatal("nil filter from empty builder")
	}
	if f.Contains([]byte("anything")) {
		t.Error("empty filter must not match")
	}
}

func TestNew_Malformed(t *testing.T) {
	for _, buf := range [][]byte{nil, {}, {0xff}} {
		if NewFilter(buf) != nil {
			t.Errorf("NewFilter(%v) returned non-nil", buf)
		}
	}
}

func TestBuilder_Reset(t *testing.T) {
	b := NewBuilder(10)
	b.Add([]byte("x"))
	if b.NumKeys() != 1 {
		t.Fatal("Add not counted")
	}
	b.Reset()
	if b.NumKeys() != 0 {
		t.Fatal("Reset did not clear")
	}
	b.Add([]byte("y"))
	f := NewFilter(b.Bytes())
	if !f.Contains([]byte("y")) {
		t.Error("post-reset filter broken")
	}
	if f.Contains([]byte("x")) {
		// Not guaranteed false, but high probability — the FP rate at one
		// key is essentially 0 because we hash and probe specific bits.
		t.Logf("note: x still present after reset (false positive, allowed)")
	}
}

func TestBitsPerKey_Clamped(t *testing.T) {
	b := NewBuilder(0)
	if b == nil {
		t.Fatal()
	}
	b = NewBuilder(1000)
	if b == nil {
		t.Fatal()
	}
}

func BenchmarkContains_Hit(b *testing.B) {
	builder := NewBuilder(10)
	keys := make([][]byte, 10_000)
	for i := range keys {
		keys[i] = make([]byte, 16)
		binary.LittleEndian.PutUint64(keys[i], uint64(i))
		builder.Add(keys[i])
	}
	f := NewFilter(builder.Bytes())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = f.Contains(keys[i%len(keys)])
	}
}

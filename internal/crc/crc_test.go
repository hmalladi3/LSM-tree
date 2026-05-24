package crc

import "testing"

func TestCompute_KnownVectors(t *testing.T) {
	// Verified against external CRC-32C calculators.
	cases := []struct {
		in   string
		want uint32
	}{
		{"", 0x00000000},
		{"a", 0xc1d04330},
		{"abc", 0x364b3fb7},
		{"123456789", 0xe3069283},
	}
	for _, tc := range cases {
		got := Compute([]byte(tc.in))
		if got != tc.want {
			t.Errorf("Compute(%q) = 0x%08x, want 0x%08x", tc.in, got, tc.want)
		}
	}
}

func TestUpdate_Streaming(t *testing.T) {
	whole := Compute([]byte("hello, world"))
	streamed := Update(0, []byte("hello"))
	streamed = Update(streamed, []byte(", "))
	streamed = Update(streamed, []byte("world"))
	if whole != streamed {
		t.Fatalf("streamed (%08x) != whole (%08x)", streamed, whole)
	}
}

func BenchmarkCompute_1KiB(b *testing.B) {
	buf := make([]byte, 1024)
	for i := range buf {
		buf[i] = byte(i)
	}
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Compute(buf)
	}
}

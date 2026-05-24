package arena

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestNew_SentinelReserved(t *testing.T) {
	a := New(64)
	if a.Used() != 1 {
		t.Errorf("Used = %d, want 1 (sentinel byte)", a.Used())
	}
	if a.Cap() != 64 {
		t.Errorf("Cap = %d, want 64", a.Cap())
	}
}

func TestAlloc_Sequential(t *testing.T) {
	a := New(128)
	off1 := a.Alloc(8, 1)
	off2 := a.Alloc(8, 1)
	if off1 == InvalidOff || off2 == InvalidOff {
		t.Fatal("alloc failed")
	}
	if off1 == off2 {
		t.Fatal("alloc returned overlapping offsets")
	}
	if off2 < off1+8 {
		t.Fatal("second alloc overlaps first")
	}
}

func TestAlloc_Alignment(t *testing.T) {
	a := New(128)
	// Burn 5 bytes to make subsequent aligned allocs visible.
	a.Alloc(5, 1)
	off := a.Alloc(8, 8)
	if off%8 != 0 {
		t.Errorf("off=%d not 8-aligned", off)
	}
	off = a.Alloc(4, 4)
	if off%4 != 0 {
		t.Errorf("off=%d not 4-aligned", off)
	}
}

func TestAlloc_Overflow(t *testing.T) {
	a := New(32)
	// One byte is reserved, so 31 bytes are usable.
	if got := a.Alloc(31, 1); got == InvalidOff {
		t.Fatal("first alloc should succeed")
	}
	if got := a.Alloc(1, 1); got != InvalidOff {
		t.Fatalf("overflow alloc should fail, got %d", got)
	}
}

func TestAlloc_ZeroSize(t *testing.T) {
	a := New(32)
	if got := a.Alloc(0, 1); got != InvalidOff {
		t.Errorf("zero-size alloc should return InvalidOff")
	}
}

func TestSeal_RejectsFurtherAlloc(t *testing.T) {
	a := New(128)
	a.Alloc(16, 1)
	a.Seal()
	if !a.Sealed() {
		t.Fatal("Sealed reported false")
	}
	if got := a.Alloc(16, 1); got != InvalidOff {
		t.Fatalf("post-seal alloc returned %d", got)
	}
}

func TestAlloc_Concurrent(t *testing.T) {
	const (
		goroutines   = 8
		allocsPerG   = 1000
		sizePerAlloc = 16
	)
	a := New(goroutines*allocsPerG*sizePerAlloc + 1024)

	var wg sync.WaitGroup
	var firstByte sync.Mutex
	seen := make(map[uint32]struct{}, goroutines*allocsPerG)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]uint32, 0, allocsPerG)
			for i := 0; i < allocsPerG; i++ {
				off := a.Alloc(sizePerAlloc, 8)
				if off == InvalidOff {
					t.Errorf("alloc failed under contention")
					return
				}
				local = append(local, off)
			}
			firstByte.Lock()
			for _, off := range local {
				if _, dup := seen[off]; dup {
					t.Errorf("duplicate offset %d", off)
				}
				seen[off] = struct{}{}
			}
			firstByte.Unlock()
		}()
	}
	wg.Wait()

	if len(seen) != goroutines*allocsPerG {
		t.Errorf("expected %d unique offsets, got %d", goroutines*allocsPerG, len(seen))
	}
}

func TestAlloc_RaceFreeViaCAS(t *testing.T) {
	// 1000 goroutines each try to allocate from a tight arena; the total
	// successful allocations should exactly fill the arena.
	const (
		goroutines = 1000
		bytesEach  = 8
	)
	a := New(goroutines*bytesEach + 1)
	var ok atomic.Uint32
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if a.Alloc(bytesEach, 1) != InvalidOff {
				ok.Add(1)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != goroutines {
		t.Errorf("expected %d successful allocs, got %d", goroutines, ok.Load())
	}
}

func TestAlloc_BadAlignment(t *testing.T) {
	a := New(32)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on non-power-of-two align")
		}
	}()
	a.Alloc(8, 3)
}

package vlog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func tempDir(t testing.TB) string {
	t.Helper()
	d, err := os.MkdirTemp("", "slate-vlog-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func TestPointer_RoundTrip(t *testing.T) {
	p := Pointer{FileNum: 7, Offset: 1024, Length: 256}
	enc := EncodePointer(p)
	got, err := DecodePointer(enc[:])
	if err != nil {
		t.Fatal(err)
	}
	if got != p {
		t.Errorf("round-trip: %+v != %+v", got, p)
	}
}

func TestDecodePointer_TooShort(t *testing.T) {
	_, err := DecodePointer(make([]byte, 8))
	if !errors.Is(err, ErrCorrupted) {
		t.Errorf("got %v, want ErrCorrupted", err)
	}
}

func TestAppendDereference_RoundTrip(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWriter(dir, 1, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	values := [][]byte{
		[]byte("alpha"),
		bytes.Repeat([]byte("x"), 1024),
		[]byte(""),
		[]byte("with\x00null bytes"),
	}
	ptrs := make([]Pointer, len(values))
	for i, v := range values {
		p, err := w.Append(v, func() uint32 { return 99 })
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		ptrs[i] = p
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}

	r := NewReader(dir)
	defer r.Close()
	for i, p := range ptrs {
		got, err := r.Dereference(p)
		if err != nil {
			t.Fatalf("Dereference[%d]: %v", i, err)
		}
		if !bytes.Equal(got, values[i]) {
			t.Errorf("[%d] got %q want %q", i, got, values[i])
		}
	}
}

func TestRotation_AllocateNextOnOverflow(t *testing.T) {
	dir := tempDir(t)
	w, err := NewWriter(dir, 1, 64) // tiny segment to force rotation
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	var nextNum atomic.Uint32
	nextNum.Store(2)
	allocator := func() uint32 { return nextNum.Add(1) - 1 }

	var ptrs []Pointer
	for i := 0; i < 20; i++ {
		val := bytes.Repeat([]byte{'v'}, 32)
		p, err := w.Append(val, allocator)
		if err != nil {
			t.Fatal(err)
		}
		ptrs = append(ptrs, p)
	}
	w.Sync()

	// Several distinct file numbers should appear in the pointer set.
	seen := map[uint32]struct{}{}
	for _, p := range ptrs {
		seen[p.FileNum] = struct{}{}
	}
	if len(seen) < 2 {
		t.Errorf("expected rotation; saw %d distinct segments", len(seen))
	}

	r := NewReader(dir)
	defer r.Close()
	for i, p := range ptrs {
		got, err := r.Dereference(p)
		if err != nil {
			t.Fatalf("Dereference across rotation [%d]: %v", i, err)
		}
		if !bytes.Equal(got, bytes.Repeat([]byte{'v'}, 32)) {
			t.Errorf("value mismatch at %d", i)
		}
	}
}

func TestReopen_DiscoversExistingSegments(t *testing.T) {
	dir := tempDir(t)
	w1, _ := NewWriter(dir, 1, 1<<20)
	ptr, _ := w1.Append([]byte("hello"), nil)
	w1.Close()

	w2, err := NewWriter(dir, 2, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	if len(w2.ExistingSegments()) == 0 {
		t.Error("reopen failed to discover prior segment")
	}

	r := NewReader(dir)
	defer r.Close()
	got, err := r.Dereference(ptr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Errorf("got %q", got)
	}
}

func TestDereference_DetectsTamper(t *testing.T) {
	dir := tempDir(t)
	w, _ := NewWriter(dir, 1, 1<<20)
	ptr, _ := w.Append([]byte("original value"), nil)
	w.Sync()
	w.Close()

	// Flip a byte inside the value bytes on disk.
	path := filepath.Join(dir, "000001.vlog")
	f, _ := os.OpenFile(path, os.O_RDWR, 0)
	f.WriteAt([]byte{'X'}, int64(ptr.Offset)+4) // +4 to skip the length prefix
	f.Close()

	r := NewReader(dir)
	defer r.Close()
	_, err := r.Dereference(ptr)
	if !errors.Is(err, ErrCorrupted) {
		t.Errorf("Dereference of tampered value: got %v, want ErrCorrupted", err)
	}
}

func TestAppend_ConcurrentWriters(t *testing.T) {
	dir := tempDir(t)
	w, _ := NewWriter(dir, 1, 64<<20)
	defer w.Close()

	const (
		workers = 8
		each    = 500
	)
	type result struct {
		ptr   Pointer
		value []byte
	}
	results := make([][]result, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		results[i] = make([]result, each)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < each; j++ {
				val := []byte(fmt.Sprintf("w%02d-v%05d", workerID, j))
				if _, err := w.Append(val, nil); err != nil {
					t.Errorf("Append: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

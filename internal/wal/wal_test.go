package wal

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func newTmpDir(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "slate-wal-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func TestAppendRead_SingleRecord(t *testing.T) {
	dir := newTmpDir(t)
	w, err := NewWriter(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("hello, world")
	if err := w.Append(payload, Sync); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, err := NewReader(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	got, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("got %q want %q", got, payload)
	}
	if _, err := r.Next(); !EOF(err) {
		t.Errorf("expected EOF, got %v", err)
	}
}

func TestAppendRead_ManyRecords(t *testing.T) {
	dir := newTmpDir(t)
	w, err := NewWriter(dir, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	const n = 1000
	expected := make([][]byte, n)
	for i := 0; i < n; i++ {
		expected[i] = []byte(fmt.Sprintf("record-%04d", i))
		if err := w.Append(expected[i], Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	r, _ := NewReader(dir, 0)
	defer r.Close()
	for i := 0; i < n; i++ {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if !bytes.Equal(got, expected[i]) {
			t.Errorf("record %d: got %q want %q", i, got, expected[i])
		}
	}
	if _, err := r.Next(); !EOF(err) {
		t.Errorf("expected EOF after %d records, got %v", n, err)
	}
}

func TestAppendRead_LargeRecord_SpansBlocks(t *testing.T) {
	dir := newTmpDir(t)
	w, _ := NewWriter(dir, 1<<20)
	// A record three blocks long must be emitted as kFirst + kMiddle + kLast.
	large := make([]byte, 3*BlockSize+1234)
	if _, err := rand.Read(large); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(large, Sync); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, _ := NewReader(dir, 0)
	defer r.Close()
	got, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, large) {
		t.Errorf("payload mismatch: len got=%d want=%d", len(got), len(large))
	}
}

func TestAppendRead_AcrossSegmentRotation(t *testing.T) {
	dir := newTmpDir(t)
	w, _ := NewWriter(dir, 64*1024) // tiny segment to force rotation
	expected := make([][]byte, 0)
	// Each record ~ 1 KiB → ~64 records per segment.
	for i := 0; i < 200; i++ {
		payload := make([]byte, 1024)
		for j := range payload {
			payload[j] = byte(i)
		}
		expected = append(expected, payload)
		if err := w.Append(payload, Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Multiple segment files should now exist.
	entries, _ := os.ReadDir(dir)
	count := 0
	for _, e := range entries {
		if _, ok := parseSegmentName(e.Name()); ok {
			count++
		}
	}
	if count < 2 {
		t.Errorf("expected >= 2 segments, got %d", count)
	}

	r, _ := NewReader(dir, 0)
	defer r.Close()
	for i, want := range expected {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("record %d mismatch", i)
		}
	}
}

func TestReader_SkipsTornTail(t *testing.T) {
	dir := newTmpDir(t)
	w, _ := NewWriter(dir, 1<<20)
	for i := 0; i < 5; i++ {
		w.Append([]byte(fmt.Sprintf("rec-%d", i)), Sync)
	}
	w.Close()

	// Corrupt the active segment by appending junk that doesn't form a
	// valid fragment header.
	files, _ := os.ReadDir(dir)
	var segPath string
	for _, f := range files {
		if _, ok := parseSegmentName(f.Name()); ok {
			segPath = filepath.Join(dir, f.Name())
			break
		}
	}
	fh, err := os.OpenFile(segPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	fh.Write([]byte("xxxxxx")) // less than a full header
	fh.Close()

	r, _ := NewReader(dir, 0)
	defer r.Close()
	count := 0
	for {
		_, err := r.Next()
		if EOF(err) {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}
	if count != 5 {
		t.Errorf("read %d records, want 5", count)
	}
}

func TestAppend_ConcurrentSyncCommits(t *testing.T) {
	dir := newTmpDir(t)
	w, _ := NewWriter(dir, 1<<20)
	const (
		writers = 16
		each    = 200
	)
	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				payload := []byte(fmt.Sprintf("g=%02d i=%04d", g, i))
				if err := w.Append(payload, Sync); err != nil {
					t.Errorf("g=%d i=%d: %v", g, i, err)
				}
			}
		}(g)
	}
	wg.Wait()
	w.Close()

	r, _ := NewReader(dir, 0)
	defer r.Close()
	count := 0
	for {
		_, err := r.Next()
		if EOF(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != writers*each {
		t.Errorf("read %d records, want %d", count, writers*each)
	}
}

func TestClose_Idempotent(t *testing.T) {
	dir := newTmpDir(t)
	w, _ := NewWriter(dir, 1<<20)
	w.Append([]byte("x"), Sync)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second Close returned %v", err)
	}
}

func TestAppend_AfterClose(t *testing.T) {
	dir := newTmpDir(t)
	w, _ := NewWriter(dir, 1<<20)
	w.Close()
	if err := w.Append([]byte("x"), Sync); err != os.ErrClosed {
		t.Errorf("Append after Close = %v, want os.ErrClosed", err)
	}
}

func TestSync_AfterAsyncAppend(t *testing.T) {
	dir := newTmpDir(t)
	w, _ := NewWriter(dir, 1<<20)
	for i := 0; i < 100; i++ {
		w.Append([]byte(fmt.Sprintf("async-%d", i)), Async)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	w.Close()

	r, _ := NewReader(dir, 0)
	defer r.Close()
	for i := 0; i < 100; i++ {
		_, err := r.Next()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
}

func TestNewReader_MinNum_SkipsLowerSegments(t *testing.T) {
	dir := newTmpDir(t)
	w, _ := NewWriter(dir, 2*1024) // tiny segments to force many rotations
	const total = 500
	for i := 0; i < total; i++ {
		w.Append([]byte(fmt.Sprintf("record-%05d-payload-to-force-segment-rotation", i)), Sync)
	}
	w.Close()

	files, _ := os.ReadDir(dir)
	var segCount int
	for _, f := range files {
		if _, ok := parseSegmentName(f.Name()); ok {
			segCount++
		}
	}
	if segCount < 2 {
		t.Fatalf("expected multiple segments, got %d", segCount)
	}

	r, err := NewReader(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	count := 0
	for {
		_, err := r.Next()
		if EOF(err) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count == 0 {
		t.Error("min-num scan returned no records")
	}
	if count >= total {
		t.Errorf("scan returned all %d records — first segment not skipped", count)
	}
}

func TestNewReader_EmptyDir(t *testing.T) {
	dir := newTmpDir(t)
	r, err := NewReader(dir, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Next(); !EOF(err) {
		t.Errorf("Next on empty dir returned %v, want EOF", err)
	}
}

func BenchmarkAppend_Sync(b *testing.B) {
	dir := newTmpDir(b)
	w, _ := NewWriter(dir, 64<<20)
	defer w.Close()
	payload := make([]byte, 256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(payload, Sync); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppend_NoSync(b *testing.B) {
	dir := newTmpDir(b)
	w, _ := NewWriter(dir, 64<<20)
	defer w.Close()
	payload := make([]byte, 256)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(payload, NoSync); err != nil {
			b.Fatal(err)
		}
	}
}

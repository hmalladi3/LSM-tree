// Package wal implements the engine's write-ahead log.
//
// The WAL is the engine's durability boundary. A transaction is durable iff
// its records have been appended to a WAL segment and that segment has been
// fdatasynced. Every other on-disk artifact (SSTables, vlog segments,
// manifest edits) is derivable from WAL-recorded state.
//
// Layout. The WAL is a stream of records split into fixed 32 KiB blocks for
// resilience: a torn write in one block does not corrupt records in adjacent
// blocks. Records that span block boundaries are emitted as a sequence of
// (kFirst, kMiddle..., kLast) fragments — the LevelDB / RocksDB convention.
//
// Fragment header: 4-byte CRC-32C || 2-byte length || 1-byte type.
// Block trailer < 8 bytes is zero-filled and skipped on read.
//
// Group commit. Writes from many goroutines are queued on a single commit
// thread. Each loop iteration drains the queue, performs one Write syscall
// for the combined buffer, and (when any batch in the group requested Sync
// durability) one fdatasync. Throughput on a busy writer is therefore
// bounded by (fdatasync time) / (batch group size), not by per-batch syscall
// cost.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/harimalladi/slate/internal/crc"
)

const (
	// BlockSize is the framing unit. Inside each block, fragments carry
	// records or partial records.
	BlockSize = 32 * 1024

	// HeaderSize is the per-fragment header: 4B CRC, 2B length, 1B type.
	HeaderSize = 7

	// MaxFragmentPayload is the largest payload a single fragment can hold.
	// A fragment header consumes HeaderSize bytes; the payload is bounded
	// to fit in a single block.
	MaxFragmentPayload = BlockSize - HeaderSize
)

// Record kinds.
const (
	kZeroType   uint8 = 0 // block-end padding sentinel
	kFullType   uint8 = 1
	kFirstType  uint8 = 2
	kMiddleType uint8 = 3
	kLastType   uint8 = 4
)

// Durability classifies how a commit waits on persistence.
type Durability uint8

const (
	Sync   Durability = 0 // wait for fdatasync
	Async  Durability = 1 // wait for Write syscall but not fdatasync
	NoSync Durability = 2 // return as soon as enqueued
)

// ErrCorrupted is returned by the reader when a fragment fails integrity
// checks (CRC mismatch or bad framing).
var ErrCorrupted = errors.New("wal: record is corrupted")

// ----- Writer -----

// Writer serializes WAL records to disk segments under group commit.
type Writer struct {
	dir         string
	segmentSize int64

	mu        sync.Mutex
	cond      *sync.Cond
	queue     []*commit
	active    *segmentFile
	nextNum   uint32
	closed    bool
	closeOnce sync.Once
	wg        sync.WaitGroup
	err       error // sticky write error
}

type commit struct {
	payload []byte
	dur     Durability
	done    chan error // nil = success; non-nil = error
}

// NewWriter opens (or creates) the WAL directory and starts the commit
// goroutine. Existing segment files are NOT replayed here — see Reader.
//
// segmentSize is the byte budget per segment; default 64 MiB.
func NewWriter(dir string, segmentSize int64) (*Writer, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	if segmentSize <= 0 {
		segmentSize = 64 << 20
	}
	w := &Writer{
		dir:         dir,
		segmentSize: segmentSize,
	}
	w.cond = sync.NewCond(&w.mu)

	// Find the next free segment number.
	num, err := nextSegmentNum(dir)
	if err != nil {
		return nil, err
	}
	w.nextNum = num

	if err := w.rotateLocked(); err != nil {
		return nil, err
	}

	w.wg.Add(1)
	go w.commitLoop()
	return w, nil
}

// Append enqueues payload as a single record and waits per dur. Returns
// when the chosen durability is satisfied.
//
// payload is copied internally; the caller may reuse the slice after Append
// returns.
func (w *Writer) Append(payload []byte, dur Durability) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return os.ErrClosed
	}
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return err
	}
	c := &commit{
		payload: append([]byte(nil), payload...),
		dur:     dur,
		done:    make(chan error, 1),
	}
	w.queue = append(w.queue, c)
	w.cond.Signal()
	w.mu.Unlock()

	if dur == NoSync {
		// Caller returns immediately; the commit completes asynchronously.
		// Errors will surface on the next Append via w.err.
		return nil
	}
	return <-c.done
}

// ActiveFileNum returns the file number of the segment currently being
// appended to. Used by the engine to record `LastFlushedWAL.FileNum` in
// the manifest at flush time.
func (w *Writer) ActiveFileNum() uint32 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.active == nil {
		return 0
	}
	return w.active.num
}

// Sync forces an fdatasync of the active segment.
func (w *Writer) Sync() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return os.ErrClosed
	}
	if w.active == nil {
		w.mu.Unlock()
		return nil
	}
	f := w.active.file
	w.mu.Unlock()
	return f.Sync()
}

// DeleteBefore unlinks every sealed WAL segment whose file number is
// strictly less than minNum. The caller is responsible for ensuring those
// segments are no longer needed (typically, the manifest's
// LastFlushedWAL.FileNum has advanced past them and their records are
// durable in SSTables).
//
// Errors are logged at stderr but otherwise swallowed — a failed unlink
// just delays cleanup, it never compromises correctness.
func (w *Writer) DeleteBefore(minNum uint32) {
	w.mu.Lock()
	dir := w.dir
	active := w.active
	w.mu.Unlock()

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		num, ok := parseSegmentName(e.Name())
		if !ok {
			continue
		}
		if num >= minNum {
			continue
		}
		if active != nil && num == active.num {
			continue
		}
		_ = os.Remove(filepath.Join(dir, e.Name()))
	}
}

// Close stops the commit thread, fsyncs the active segment, and releases
// file handles. Subsequent calls return nil.
func (w *Writer) Close() error {
	var err error
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		w.cond.Broadcast()
		w.mu.Unlock()
		w.wg.Wait()
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.active != nil {
			err = w.active.close()
			w.active = nil
		}
	})
	return err
}

func (w *Writer) commitLoop() {
	defer w.wg.Done()
	for {
		w.mu.Lock()
		for !w.closed && len(w.queue) == 0 {
			w.cond.Wait()
		}
		if w.closed && len(w.queue) == 0 {
			w.mu.Unlock()
			return
		}
		batch := w.queue
		w.queue = nil
		w.mu.Unlock()

		w.flushBatch(batch)
	}
}

func (w *Writer) flushBatch(batch []*commit) {
	var anySync bool
	for _, c := range batch {
		if c.dur == Sync {
			anySync = true
		}
	}

	// Serialize all payloads into block-framed bytes; rotate segments as needed.
	for _, c := range batch {
		if err := w.writeRecord(c.payload); err != nil {
			w.fail(err)
			for _, c2 := range batch {
				if c2.dur != NoSync {
					c2.done <- err
				}
			}
			return
		}
	}

	if anySync && w.active != nil {
		if err := w.active.file.Sync(); err != nil {
			w.fail(err)
			for _, c := range batch {
				if c.dur != NoSync {
					c.done <- err
				}
			}
			return
		}
	}

	for _, c := range batch {
		if c.dur != NoSync {
			c.done <- nil
		}
	}
}

func (w *Writer) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
}

// writeRecord splits payload into one or more block-aligned fragments.
func (w *Writer) writeRecord(payload []byte) error {
	if w.active == nil {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}

	// Will this record's first fragment fit in the current segment? We
	// require room for at least one byte of payload plus the header.
	if w.active.size+int64(HeaderSize+1) > w.segmentSize {
		if err := w.rotateLocked(); err != nil {
			return err
		}
	}

	first := true
	for len(payload) > 0 || first {
		// Space remaining in the current block.
		blockUsed := w.active.size % BlockSize
		blockLeft := int64(BlockSize) - blockUsed
		if blockLeft < int64(HeaderSize) {
			// Zero-fill the trailer and start a new block.
			pad := make([]byte, blockLeft)
			if _, err := w.active.file.Write(pad); err != nil {
				return err
			}
			w.active.size += int64(len(pad))
			continue
		}
		payloadRoom := int(blockLeft) - HeaderSize
		take := len(payload)
		if take > payloadRoom {
			take = payloadRoom
		}
		isLast := take == len(payload)
		var typ uint8
		switch {
		case first && isLast:
			typ = kFullType
		case first && !isLast:
			typ = kFirstType
		case !first && !isLast:
			typ = kMiddleType
		case !first && isLast:
			typ = kLastType
		}

		var header [HeaderSize]byte
		binary.LittleEndian.PutUint16(header[4:6], uint16(take))
		header[6] = typ
		// CRC over (type || payload[:take]).
		crcVal := crc.Update(0, []byte{typ})
		crcVal = crc.Update(crcVal, payload[:take])
		binary.LittleEndian.PutUint32(header[0:4], crcVal)

		if _, err := w.active.file.Write(header[:]); err != nil {
			return err
		}
		if take > 0 {
			if _, err := w.active.file.Write(payload[:take]); err != nil {
				return err
			}
		}
		w.active.size += int64(HeaderSize + take)
		payload = payload[take:]
		first = false

		// If we've filled the segment, rotate to a new one for any remaining
		// fragments. This means a single record may straddle segments — the
		// reader handles continuation by following kMiddle/kLast across files.
		if w.active.size >= w.segmentSize && len(payload) > 0 {
			if err := w.rotateLocked(); err != nil {
				return err
			}
		}
	}
	return nil
}

// rotateLocked closes the current segment (if any) and opens the next.
func (w *Writer) rotateLocked() error {
	if w.active != nil {
		if err := w.active.file.Sync(); err != nil {
			return err
		}
		if err := w.active.close(); err != nil {
			return err
		}
		w.active = nil
	}
	num := w.nextNum
	w.nextNum++
	name := segmentName(num)
	path := filepath.Join(w.dir, name)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	// Best-effort directory fsync after creating the first segment of this
	// session so a crash before the first record still leaves a recoverable
	// state.
	if dir, derr := os.Open(w.dir); derr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	w.active = &segmentFile{file: f, num: num}
	return nil
}

type segmentFile struct {
	file *os.File
	num  uint32
	size int64
}

func (s *segmentFile) close() error { return s.file.Close() }

// ----- Reader -----

// Reader scans WAL segments in ascending file-number order, yielding each
// logical record.
type Reader struct {
	files []string
	dir   string

	cur       *os.File
	buf       [BlockSize]byte
	blockLen  int
	blockPos  int
	pending   []byte
	scratch   []byte
	doneFiles bool
	curIndex  int
}

// NewReader opens the WAL directory and prepares to scan segments whose
// number is >= minNum. Segments lower than minNum are skipped. If no
// segments match, Next immediately returns EOF.
func NewReader(dir string, minNum uint32) (*Reader, error) {
	files, err := listSegments(dir, minNum)
	if err != nil {
		return nil, err
	}
	r := &Reader{
		dir:   dir,
		files: files,
	}
	if len(files) > 0 {
		if err := r.openNext(); err != nil && err != errEOF {
			return nil, err
		}
	}
	return r, nil
}

// Next reads the next logical record. Returns (record, nil) on success;
// (nil, io.EOF) at end; (nil, ErrCorrupted) for CRC failure.
//
// The returned slice references reader scratch space and is overwritten by
// the next call; callers retaining it must copy.
func (r *Reader) Next() ([]byte, error) {
	r.pending = r.pending[:0]
	for {
		if r.cur == nil {
			return nil, errEOF
		}
		if r.blockPos >= r.blockLen {
			if err := r.fillBlock(); err != nil {
				if err == errSegmentEnd {
					if err := r.openNext(); err != nil {
						return nil, err
					}
					continue
				}
				return nil, err
			}
		}
		typ, payload, ok := r.nextFragment()
		if !ok {
			// Skip to next block (zero padding or torn tail).
			r.blockPos = r.blockLen
			continue
		}
		switch typ {
		case kFullType:
			r.pending = append(r.pending[:0], payload...)
			return r.pending, nil
		case kFirstType:
			r.pending = append(r.pending[:0], payload...)
		case kMiddleType:
			r.pending = append(r.pending, payload...)
		case kLastType:
			r.pending = append(r.pending, payload...)
			return r.pending, nil
		case kZeroType:
			// Padding sentinel; skip.
			continue
		default:
			return nil, ErrCorrupted
		}
	}
}

var errEOF = errors.New("wal: end of log")
var errSegmentEnd = errors.New("wal: end of segment")

// EOF reports whether err is the reader's end-of-log sentinel.
func EOF(err error) bool { return err == errEOF }

// Close releases the current segment file.
func (r *Reader) Close() error {
	if r.cur != nil {
		err := r.cur.Close()
		r.cur = nil
		return err
	}
	return nil
}

func (r *Reader) openNext() error {
	if r.cur != nil {
		if err := r.cur.Close(); err != nil {
			return err
		}
		r.cur = nil
	}
	if r.curIndex >= len(r.files) {
		return errEOF
	}
	path := r.files[r.curIndex]
	r.curIndex++
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	r.cur = f
	r.blockLen = 0
	r.blockPos = 0
	return nil
}

func (r *Reader) fillBlock() error {
	if r.cur == nil {
		return errSegmentEnd
	}
	n, err := r.cur.Read(r.buf[:])
	if n > 0 {
		r.blockLen = n
		r.blockPos = 0
		return nil
	}
	if err == nil {
		return errSegmentEnd
	}
	// io.EOF surfaces from os.File.Read after the file's content is
	// exhausted; in that case n==0, err==io.EOF, and we treat it as
	// segment end.
	return errSegmentEnd
}

// nextFragment reads one fragment from the current block buffer. Returns
// (type, payload, ok=true) on success; ok=false signals that the remaining
// bytes in this block are padding or torn and should be skipped.
func (r *Reader) nextFragment() (typ uint8, payload []byte, ok bool) {
	if r.blockLen-r.blockPos < HeaderSize {
		return 0, nil, false
	}
	hdr := r.buf[r.blockPos : r.blockPos+HeaderSize]
	crcVal := binary.LittleEndian.Uint32(hdr[0:4])
	length := int(binary.LittleEndian.Uint16(hdr[4:6]))
	typ = hdr[6]
	if length == 0 && typ == kZeroType {
		// Block-end padding sentinel; consume the rest of the block.
		return kZeroType, nil, true
	}
	if r.blockPos+HeaderSize+length > r.blockLen {
		// Fragment extends past the buffered block — torn.
		return 0, nil, false
	}
	payload = r.buf[r.blockPos+HeaderSize : r.blockPos+HeaderSize+length]
	// Verify CRC: covers (type || payload).
	check := crc.Update(0, []byte{typ})
	check = crc.Update(check, payload)
	if check != crcVal {
		return 0, nil, false
	}
	r.blockPos += HeaderSize + length
	return typ, payload, true
}

// ----- Helpers -----

func segmentName(num uint32) string {
	return fmt.Sprintf("%06d.log", num)
}

func parseSegmentName(name string) (uint32, bool) {
	var n uint32
	if _, err := fmt.Sscanf(name, "%06d.log", &n); err != nil {
		return 0, false
	}
	return n, true
}

func listSegments(dir string, minNum uint32) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type pair struct {
		num  uint32
		path string
	}
	var out []pair
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		num, ok := parseSegmentName(e.Name())
		if !ok || num < minNum {
			continue
		}
		out = append(out, pair{num: num, path: filepath.Join(dir, e.Name())})
	}
	// Sort ascending by num.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].num > out[j].num; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	paths := make([]string, len(out))
	for i, p := range out {
		paths[i] = p.path
	}
	return paths, nil
}

func nextSegmentNum(dir string) (uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var max uint32
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if num, ok := parseSegmentName(e.Name()); ok && num >= max {
			max = num + 1
		}
	}
	return max, nil
}

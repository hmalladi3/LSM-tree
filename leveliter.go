package slate

import (
	"github.com/harimalladi/slate/internal/manifest"
	"github.com/harimalladi/slate/internal/sstable"
)

// levelIter walks an L1+ level by concatenating per-file SST iterators in
// ascending file order. Files within an L1+ level are non-overlapping by
// construction (see manifest invariants), so the concatenation produces
// internal keys in ascending order.
type levelIter struct {
	files []*manifest.TableMeta
	open  func(num uint32) *sstable.Reader

	curIdx  int
	curIter *sstable.Iterator
	err     error
}

func newLevelIter(files []*manifest.TableMeta, open func(num uint32) *sstable.Reader) *levelIter {
	return &levelIter{files: files, open: open, curIdx: -1}
}

func (l *levelIter) first() {
	l.curIdx = 0
	l.advanceFile()
	if l.curIter != nil {
		l.curIter.First()
		l.skipExhausted()
	}
}

func (l *levelIter) seekGE(target []byte) {
	// Skip files whose largest user key < target_user_key.
	user := userPrefix(target)
	idx := 0
	for ; idx < len(l.files); idx++ {
		if bytesLessOrEqual(user, l.files[idx].Largest) {
			break
		}
	}
	l.curIdx = idx
	l.advanceFile()
	if l.curIter != nil {
		l.curIter.SeekGE(target)
		l.skipExhausted()
	}
}

func (l *levelIter) next() {
	if l.curIter == nil {
		return
	}
	l.curIter.Next()
	l.skipExhausted()
}

func (l *levelIter) valid() bool {
	return l.err == nil && l.curIter != nil && l.curIter.Valid()
}

func (l *levelIter) key() []byte {
	if !l.valid() {
		return nil
	}
	return l.curIter.Key()
}

func (l *levelIter) value() []byte {
	if !l.valid() {
		return nil
	}
	return l.curIter.Value()
}

func (l *levelIter) close() error {
	if l.curIter != nil {
		_ = l.curIter.Close()
		l.curIter = nil
	}
	return l.err
}

func (l *levelIter) error() error {
	if l.err != nil {
		return l.err
	}
	if l.curIter != nil {
		return l.curIter.Error()
	}
	return nil
}

// advanceFile opens the iterator for files[curIdx]. Returns with curIter
// nil if curIdx is past the end.
func (l *levelIter) advanceFile() {
	if l.curIter != nil {
		_ = l.curIter.Close()
		l.curIter = nil
	}
	if l.curIdx < 0 || l.curIdx >= len(l.files) {
		return
	}
	r := l.open(l.files[l.curIdx].FileNum)
	if r == nil {
		l.err = errInternal
		return
	}
	l.curIter = r.NewIterator()
}

// skipExhausted: if the current file iterator is exhausted, move to the
// next file (if any) and position at its first entry.
func (l *levelIter) skipExhausted() {
	for l.curIter != nil && !l.curIter.Valid() {
		l.curIdx++
		l.advanceFile()
		if l.curIter == nil {
			return
		}
		l.curIter.First()
	}
}

// userPrefix returns the user-key portion of an internal key.
func userPrefix(internalKey []byte) []byte {
	if len(internalKey) < 8 {
		return internalKey
	}
	return internalKey[:len(internalKey)-8]
}

// bytesLessOrEqual: a <= b
func bytesLessOrEqual(a, b []byte) bool {
	// Inline comparison to avoid pulling in bytes here.
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) <= len(b)
}

// ----- adapter so levelIter satisfies iterSource -----

type levelSource struct {
	*levelIter
}

func (s *levelSource) first()          { s.levelIter.first() }
func (s *levelSource) seekGE(k []byte) { s.levelIter.seekGE(k) }
func (s *levelSource) next()           { s.levelIter.next() }
func (s *levelSource) valid() bool     { return s.levelIter.valid() }
func (s *levelSource) key() []byte     { return s.levelIter.key() }
func (s *levelSource) value() []byte   { return s.levelIter.value() }
func (s *levelSource) close() error    { return s.levelIter.close() }
func (s *levelSource) error() error    { return s.levelIter.error() }

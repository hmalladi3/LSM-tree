package manifest

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func tempDir(t testing.TB) string {
	t.Helper()
	d, err := os.MkdirTemp("", "slate-man-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })
	return d
}

func TestOpen_NewDirectory(t *testing.T) {
	dir := tempDir(t)
	m, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	v := m.Current()
	defer v.Unref()
	if v.NextFileNum != 1 {
		t.Errorf("NextFileNum = %d, want 1", v.NextFileNum)
	}
	// CURRENT file should exist.
	if _, err := os.Stat(filepath.Join(dir, "CURRENT")); err != nil {
		t.Errorf("CURRENT missing: %v", err)
	}
}

func TestApply_AddAndDeleteTables(t *testing.T) {
	dir := tempDir(t)
	m, _ := Open(dir)
	defer m.Close()

	t1 := TableMeta{FileNum: 10, Level: 0, Smallest: []byte("a"), Largest: []byte("m"), Size: 1024, SmallestSeq: 1, LargestSeq: 5}
	t2 := TableMeta{FileNum: 11, Level: 0, Smallest: []byte("n"), Largest: []byte("z"), Size: 2048, SmallestSeq: 6, LargestSeq: 9}
	err := m.Apply(VersionEdit{NewTables: []TableMeta{t1, t2}, HasLastSequence: true, LastSequence: 9})
	if err != nil {
		t.Fatal(err)
	}
	v := m.Current()
	defer v.Unref()
	if len(v.Tables[0]) != 2 {
		t.Errorf("L0 table count = %d, want 2", len(v.Tables[0]))
	}
	if v.LastSequence != 9 {
		t.Errorf("LastSequence = %d, want 9", v.LastSequence)
	}

	// Now delete one.
	err = m.Apply(VersionEdit{DeletedTables: []uint32{10}})
	if err != nil {
		t.Fatal(err)
	}
	v2 := m.Current()
	defer v2.Unref()
	if len(v2.Tables[0]) != 1 {
		t.Errorf("after delete, L0 count = %d, want 1", len(v2.Tables[0]))
	}
	if v2.Tables[0][0].FileNum != 11 {
		t.Errorf("remaining table = %d, want 11", v2.Tables[0][0].FileNum)
	}
}

func TestApply_Reopen_StateSurvives(t *testing.T) {
	dir := tempDir(t)
	m, _ := Open(dir)
	t1 := TableMeta{FileNum: 5, Level: 0, Smallest: []byte("a"), Largest: []byte("m"), Size: 256, SmallestSeq: 10, LargestSeq: 20}
	m.Apply(VersionEdit{NewTables: []TableMeta{t1}, HasLastSequence: true, LastSequence: 20, HasFlushedWAL: true, FlushedWAL: WALCheckpoint{FileNum: 3, Seq: 20}})
	m.Close()

	m2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	v := m2.Current()
	defer v.Unref()

	if len(v.Tables[0]) != 1 {
		t.Errorf("L0 count after reopen = %d", len(v.Tables[0]))
	}
	got := v.Tables[0][0]
	if got.FileNum != 5 || !bytes.Equal(got.Smallest, []byte("a")) || got.LargestSeq != 20 {
		t.Errorf("table after reopen = %+v", got)
	}
	if v.LastSequence != 20 {
		t.Errorf("LastSequence after reopen = %d", v.LastSequence)
	}
	if v.FlushedWAL.FileNum != 3 || v.FlushedWAL.Seq != 20 {
		t.Errorf("FlushedWAL after reopen = %+v", v.FlushedWAL)
	}
}

func TestApply_TornTail_TruncatedSilently(t *testing.T) {
	dir := tempDir(t)
	m, _ := Open(dir)
	m.Apply(VersionEdit{HasLastSequence: true, LastSequence: 100})
	m.Close()

	// Append garbage to the manifest file to simulate a torn write.
	curName, _ := readCurrent(dir)
	path := filepath.Join(dir, curName)
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte("garbage that does not parse as a record"))
	f.Close()

	m2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	v := m2.Current()
	defer v.Unref()
	if v.LastSequence != 100 {
		t.Errorf("torn-tail recovery lost state: LastSequence = %d", v.LastSequence)
	}
}

func TestVersion_Refcount(t *testing.T) {
	v := &Version{}
	v.Ref()
	if got := v.refs.Load(); got != 1 {
		t.Errorf("after Ref, refs=%d", got)
	}
	v.Ref()
	if v.Unref() != 1 {
		t.Errorf("after two Refs and one Unref, refs should be 1")
	}
	v.Unref()
	if v.refs.Load() != 0 {
		t.Errorf("refs should drop to 0")
	}
}

func TestCurrent_AcquiresReference(t *testing.T) {
	dir := tempDir(t)
	m, _ := Open(dir)
	defer m.Close()

	v1 := m.Current()
	v2 := m.Current()
	if v1 != v2 {
		t.Fatal("Current returned distinct versions")
	}
	if v1.refs.Load() < 2 {
		t.Errorf("refs should be >= 2; got %d", v1.refs.Load())
	}
	v1.Unref()
	v2.Unref()
}

func TestApplied_NoMutation(t *testing.T) {
	// Apply returns a new Version without mutating the prior.
	v := &Version{NextFileNum: 1}
	v.Tables[0] = append(v.Tables[0], &TableMeta{FileNum: 1})
	v2 := v.applied(VersionEdit{NewTables: []TableMeta{{FileNum: 2, Level: 0}}})
	if len(v.Tables[0]) != 1 {
		t.Errorf("original mutated: L0 size = %d", len(v.Tables[0]))
	}
	if len(v2.Tables[0]) != 2 {
		t.Errorf("derived L0 size = %d, want 2", len(v2.Tables[0]))
	}
}

func TestRotation_AfterManyEdits(t *testing.T) {
	dir := tempDir(t)
	m, _ := Open(dir)
	defer m.Close()
	// Trigger the snapshot rotation (default 1000 edits).
	for i := 0; i < 1500; i++ {
		m.Apply(VersionEdit{HasLastSequence: true, LastSequence: uint64(i + 1)})
	}
	v := m.Current()
	defer v.Unref()
	if v.LastSequence != 1500 {
		t.Errorf("LastSequence = %d, want 1500", v.LastSequence)
	}
	// There should be more than one MANIFEST-* file (snapshot rotated).
	entries, _ := os.ReadDir(dir)
	count := 0
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) >= len("MANIFEST-") && e.Name()[:9] == "MANIFEST-" {
			count++
		}
	}
	if count < 2 {
		t.Errorf("after 1500 edits, manifest files = %d (rotation did not run)", count)
	}
}

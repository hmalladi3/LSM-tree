package slate

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/harimalladi/slate/internal/crc"
	"github.com/harimalladi/slate/internal/manifest"
)

// backupMagic identifies a slate backup stream.
var backupMagic = []byte("SLATEBAK")

const (
	backupVersion uint32 = 1

	// Record types in the stream.
	recFile uint8 = 1
	recEnd  uint8 = 0xFF
)

// restoreOKMarker is the sentinel written at the end of a successful
// Restore. Its absence on a non-empty directory signals a torn restore.
const restoreOKMarker = "RESTORE_OK"

// Backup writes a consistent snapshot of the database to w.
//
// The stream is a sequence of framed file records covering every SSTable,
// every vlog segment, the IDENTITY file, and the active manifest. While
// Backup runs it pins the current manifest Version so files in the snapshot
// cannot be deleted by compaction; writes proceed normally and their
// effects do not appear in the backup.
//
// Compatible with Restore.
func (db *DB) Backup(ctx context.Context, w io.Writer) error {
	if db.closed.Load() {
		return ErrClosed
	}

	// Flush the active memtable so unflushed writes are captured by the
	// snapshot. Backup represents only data durable in the LSM, so any
	// in-memory state must be rotated to L0 before we pin a Version.
	if err := db.Flush(); err != nil {
		return err
	}

	// Pin a snapshot of the current Version. compaction may delete other
	// files but it cannot delete files referenced by ver.
	ver := db.manifest.Current()
	defer ver.Unref()

	// Buffer the writer for efficient framing.
	bw := bufio.NewWriterSize(w, 64<<10)

	// Header: magic + version.
	if _, err := bw.Write(backupMagic); err != nil {
		return err
	}
	if err := writeU32(bw, backupVersion); err != nil {
		return err
	}

	rollingCRC := crc.Compute(backupMagic)
	var verBuf [4]byte
	binary.LittleEndian.PutUint32(verBuf[:], backupVersion)
	rollingCRC = crc.Update(rollingCRC, verBuf[:])

	// Sync writes so we capture all committed Sync-durability data.
	if err := db.Sync(); err != nil {
		return err
	}
	if err := db.vlogWriter.Sync(); err != nil {
		return err
	}

	// Determine which files to back up:
	//   1) IDENTITY (encryption key id + db uuid)
	//   2) Active manifest log + CURRENT
	//   3) Every SST listed in the pinned Version
	//   4) Every vlog segment present in the vlog directory
	files, err := db.backupFileList(ver)
	if err != nil {
		return err
	}

	for _, rel := range files {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		full := filepath.Join(db.dir, rel)
		if err := streamFileRecord(bw, rel, full, &rollingCRC); err != nil {
			return err
		}
	}

	// End marker followed by the stream CRC.
	if err := bw.WriteByte(recEnd); err != nil {
		return err
	}
	rollingCRC = crc.Update(rollingCRC, []byte{recEnd})
	if err := writeU32(bw, rollingCRC); err != nil {
		return err
	}
	return bw.Flush()
}

// backupFileList returns the relative paths of every file the backup needs
// to capture. Order is deterministic.
func (db *DB) backupFileList(ver *manifest.Version) ([]string, error) {
	var files []string

	// IDENTITY (encryption mode marker, always present).
	files = append(files, identityFileName)

	// Manifest: CURRENT + the file it names.
	curName, err := os.ReadFile(filepath.Join(db.dir, manifestDirName, "CURRENT"))
	if err != nil {
		return nil, err
	}
	curStr := stripNewline(string(curName))
	files = append(files,
		filepath.Join(manifestDirName, "CURRENT"),
		filepath.Join(manifestDirName, curStr),
	)

	// SSTs from every level.
	for _, level := range ver.Tables {
		for _, t := range level {
			files = append(files,
				filepath.Join(sstDirName, fmt.Sprintf("%06d.sst", t.FileNum)),
			)
		}
	}

	// Vlog segments: read directory.
	vlogDir := filepath.Join(db.dir, vlogDirName)
	if entries, err := os.ReadDir(vlogDir); err == nil {
		var names []string
		for _, e := range entries {
			if !e.IsDir() && filepath.Ext(e.Name()) == ".vlog" {
				names = append(names, e.Name())
			}
		}
		sort.Strings(names)
		for _, n := range names {
			files = append(files, filepath.Join(vlogDirName, n))
		}
	}

	return files, nil
}

// streamFileRecord writes one file record into bw and updates the rolling
// CRC. Layout: [type=recFile:u8][path_len:u16][path][size:u64][bytes].
func streamFileRecord(bw *bufio.Writer, relPath, fullPath string, rollingCRC *uint32) error {
	f, err := os.Open(fullPath)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if len(relPath) > 0xffff {
		return fmt.Errorf("slate: backup path too long: %s", relPath)
	}

	// Frame header.
	if err := bw.WriteByte(recFile); err != nil {
		return err
	}
	if err := writeU16(bw, uint16(len(relPath))); err != nil {
		return err
	}
	if _, err := bw.WriteString(relPath); err != nil {
		return err
	}
	if err := writeU64(bw, uint64(fi.Size())); err != nil {
		return err
	}

	// Update CRC over the header bytes.
	var hdrBuf [11]byte
	hdrBuf[0] = recFile
	binary.LittleEndian.PutUint16(hdrBuf[1:3], uint16(len(relPath)))
	*rollingCRC = crc.Update(*rollingCRC, hdrBuf[:3])
	*rollingCRC = crc.Update(*rollingCRC, []byte(relPath))
	binary.LittleEndian.PutUint64(hdrBuf[3:11], uint64(fi.Size()))
	*rollingCRC = crc.Update(*rollingCRC, hdrBuf[3:11])

	// Stream the body and tee through CRC.
	buf := make([]byte, 64<<10)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			if _, werr := bw.Write(buf[:n]); werr != nil {
				return werr
			}
			*rollingCRC = crc.Update(*rollingCRC, buf[:n])
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}
	return nil
}

// Restore reads a stream produced by Backup into dir.
//
// If dir does not exist it is created. If dir is non-empty but lacks both a
// completed RESTORE_OK marker and a parseable manifest CURRENT, it is
// auto-cleaned and Restore proceeds. A directory holding a real database
// returns ErrDirectoryNotEmpty.
func Restore(ctx context.Context, dir string, r io.Reader, opts *Options) error {
	// Auto-clean / refuse logic.
	if err := prepareRestoreDir(dir); err != nil {
		return err
	}

	br := bufio.NewReaderSize(r, 64<<10)

	// Header.
	magic := make([]byte, len(backupMagic))
	if _, err := io.ReadFull(br, magic); err != nil {
		return fmt.Errorf("%w: reading magic: %v", ErrCorrupted, err)
	}
	for i := range magic {
		if magic[i] != backupMagic[i] {
			return fmt.Errorf("%w: bad backup magic", ErrCorrupted)
		}
	}
	rollingCRC := crc.Compute(backupMagic)

	ver, err := readU32(br)
	if err != nil {
		return err
	}
	if ver != backupVersion {
		return fmt.Errorf("%w: unsupported backup version %d", ErrUnsupportedBackupVersion, ver)
	}
	var verBuf [4]byte
	binary.LittleEndian.PutUint32(verBuf[:], ver)
	rollingCRC = crc.Update(rollingCRC, verBuf[:])

	// Stream records until kEnd.
	written := 0
	for {
		if ctx != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		typ, err := br.ReadByte()
		if err != nil {
			return fmt.Errorf("%w: reading record type: %v", ErrCorrupted, err)
		}
		rollingCRC = crc.Update(rollingCRC, []byte{typ})
		switch typ {
		case recFile:
			if err := readFileRecord(br, dir, &rollingCRC); err != nil {
				return err
			}
			written++
		case recEnd:
			// Verify final CRC.
			expected, err := readU32(br)
			if err != nil {
				return err
			}
			if expected != rollingCRC {
				return fmt.Errorf("%w: backup CRC mismatch", ErrCorrupted)
			}
			if written == 0 {
				return fmt.Errorf("%w: empty backup", ErrCorrupted)
			}
			// Write the RESTORE_OK marker so a future Open knows the dir
			// is complete.
			marker := filepath.Join(dir, restoreOKMarker)
			if err := os.WriteFile(marker, nil, 0o644); err != nil {
				return err
			}
			return nil
		default:
			return fmt.Errorf("%w: unknown record type %d", ErrCorrupted, typ)
		}
	}
}

func readFileRecord(br *bufio.Reader, dir string, rollingCRC *uint32) error {
	plen, err := readU16(br)
	if err != nil {
		return err
	}
	pathBytes := make([]byte, plen)
	if _, err := io.ReadFull(br, pathBytes); err != nil {
		return err
	}
	size, err := readU64(br)
	if err != nil {
		return err
	}
	var hdr [3]byte
	binary.LittleEndian.PutUint16(hdr[0:2], plen)
	*rollingCRC = crc.Update(*rollingCRC, hdr[:2])
	*rollingCRC = crc.Update(*rollingCRC, pathBytes)
	var sz [8]byte
	binary.LittleEndian.PutUint64(sz[:], size)
	*rollingCRC = crc.Update(*rollingCRC, sz[:])

	rel := string(pathBytes)
	if !isSafeBackupPath(rel) {
		return fmt.Errorf("%w: unsafe path %q", ErrCorrupted, rel)
	}
	full := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	tmp := full + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	buf := make([]byte, 64<<10)
	remaining := int64(size)
	for remaining > 0 {
		take := int64(len(buf))
		if take > remaining {
			take = remaining
		}
		if _, err := io.ReadFull(br, buf[:take]); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		*rollingCRC = crc.Update(*rollingCRC, buf[:take])
		if _, err := f.Write(buf[:take]); err != nil {
			f.Close()
			os.Remove(tmp)
			return err
		}
		remaining -= take
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, full); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// prepareRestoreDir ensures the target directory is ready to receive a
// fresh restore. Returns ErrDirectoryNotEmpty if the directory holds a
// valid open database; auto-cleans torn restores.
func prepareRestoreDir(dir string) error {
	info, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return os.MkdirAll(dir, 0o755)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%w: %s is not a directory", ErrInvalidOption, dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	// Probe for an intact database.
	_, manErr := os.Stat(filepath.Join(dir, manifestDirName, "CURRENT"))
	_, okErr := os.Stat(filepath.Join(dir, restoreOKMarker))
	if manErr == nil && okErr == nil {
		return ErrDirectoryNotEmpty
	}
	// Torn / partial state — auto-clean.
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// isSafeBackupPath blocks paths that would escape the restore directory.
func isSafeBackupPath(p string) bool {
	if filepath.IsAbs(p) {
		return false
	}
	cleaned := filepath.Clean(p)
	if cleaned == ".." || filepath.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return false
	}
	return true
}

func stripNewline(s string) string {
	for len(s) > 0 && (s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}

// ----- varint helpers -----

func writeU16(w *bufio.Writer, v uint16) error {
	var b [2]byte
	binary.LittleEndian.PutUint16(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeU32(w *bufio.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeU64(w *bufio.Writer, v uint64) error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func readU16(r io.Reader) (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b[:]), nil
}

func readU32(r io.Reader) (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func readU64(r io.Reader) (uint64, error) {
	var b [8]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b[:]), nil
}

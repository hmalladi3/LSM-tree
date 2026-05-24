// slatectl is a small command-line interface to a slate database. It exists
// for manual inspection and scripted maintenance; production applications
// embed the slate package directly.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/harimalladi/slate"
)

func main() {
	flag.Usage = usage
	flag.Parse()
	if flag.NArg() < 2 {
		usage()
		os.Exit(2)
	}
	dir := flag.Arg(0)
	cmd := flag.Arg(1)
	args := flag.Args()[2:]

	var err error
	switch cmd {
	case "set":
		err = cmdSet(dir, args)
	case "get":
		err = cmdGet(dir, args)
	case "delete":
		err = cmdDelete(dir, args)
	case "load":
		err = cmdLoad(dir)
	case "backup":
		err = cmdBackup(dir)
	case "restore":
		err = cmdRestore(dir)
	case "stats":
		err = cmdStats(dir)
	case "compact":
		err = cmdCompact(dir, args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `slatectl - inspect and manipulate a slate database

Usage:
  slatectl <dir> set <key> <value>      Write a key.
  slatectl <dir> get <key>              Read a key. Exits 3 if absent.
  slatectl <dir> delete <key>           Delete a key.
  slatectl <dir> load                   Read 'key value' lines from stdin
                                        and Set each. One line per pair.
  slatectl <dir> backup                 Stream a consistent snapshot of the
                                        database to stdout.
  slatectl <dir> restore                Read a backup stream from stdin and
                                        materialise it into <dir>.
  slatectl <dir> stats                  Print operational stats (level files
                                        and bytes, vlog segments, cache hits,
                                        active readers).
  slatectl <dir> compact [start end]    Compact the database. With no args,
                                        runs full compaction. With a key
                                        range, compacts that range only.`)
}

func openDB(dir string) (*slate.DB, error) {
	return slate.Open(dir, nil)
}

func cmdSet(dir string, args []string) error {
	if len(args) != 2 {
		return errors.New("set requires <key> <value>")
	}
	db, err := openDB(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Set([]byte(args[0]), []byte(args[1]))
}

func cmdGet(dir string, args []string) error {
	if len(args) != 1 {
		return errors.New("get requires <key>")
	}
	db, err := openDB(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	v, err := db.Get([]byte(args[0]))
	if errors.Is(err, slate.ErrNotFound) {
		os.Exit(3)
	}
	if err != nil {
		return err
	}
	os.Stdout.Write(v)
	if len(v) > 0 && v[len(v)-1] != '\n' {
		os.Stdout.Write([]byte{'\n'})
	}
	return nil
}

func cmdDelete(dir string, args []string) error {
	if len(args) != 1 {
		return errors.New("delete requires <key>")
	}
	db, err := openDB(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Delete([]byte(args[0]))
}

func cmdBackup(dir string) error {
	db, err := openDB(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Backup(context.Background(), os.Stdout)
}

func cmdRestore(dir string) error {
	return slate.Restore(context.Background(), dir, os.Stdin, nil)
}

func cmdStats(dir string) error {
	db, err := openDB(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	s := db.Stats()
	fmt.Printf("Level files / bytes:\n")
	for i := 0; i < 7; i++ {
		if s.LevelFileCount[i] == 0 {
			continue
		}
		fmt.Printf("  L%d  %4d files  %s\n", i, s.LevelFileCount[i], humanBytes(s.LevelBytes[i]))
	}
	fmt.Printf("\nValue log:\n  %d segments  %s\n", s.VlogSegments, humanBytes(s.VlogBytes))
	fmt.Printf("\nBlock cache:\n  used %s / cap %s\n  hits=%d  misses=%d  hit_rate=%.1f%%\n  evictions=%d  oversize_rejected=%d\n",
		humanBytes(s.Cache.BytesUsed), humanBytes(s.Cache.Capacity),
		s.Cache.Hits, s.Cache.Misses, s.Cache.HitRate()*100,
		s.Cache.Evictions, s.Cache.OversizeRejected)
	fmt.Printf("\nActive readers (txns + snapshots): %d\n", s.ActiveReaders)
	return nil
}

func cmdCompact(dir string, args []string) error {
	db, err := openDB(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	switch len(args) {
	case 0:
		return db.CompactNow()
	case 2:
		return db.CompactRange([]byte(args[0]), []byte(args[1]))
	default:
		return errors.New("compact takes either no args or [start end]")
	}
}

// humanBytes renders a byte count with KiB/MiB/GiB suffix.
func humanBytes(n int64) string {
	const k = 1024
	switch {
	case n < k:
		return fmt.Sprintf("%dB", n)
	case n < k*k:
		return fmt.Sprintf("%.1fKiB", float64(n)/float64(k))
	case n < k*k*k:
		return fmt.Sprintf("%.1fMiB", float64(n)/float64(k*k))
	default:
		return fmt.Sprintf("%.2fGiB", float64(n)/float64(k*k*k))
	}
}

func cmdLoad(dir string) error {
	db, err := openDB(dir)
	if err != nil {
		return err
	}
	defer db.Close()

	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	line := 0
	for sc.Scan() {
		line++
		text := sc.Text()
		var k, v string
		for i := 0; i < len(text); i++ {
			if text[i] == ' ' || text[i] == '\t' {
				k = text[:i]
				v = text[i+1:]
				break
			}
		}
		if k == "" {
			return fmt.Errorf("line %d: expected 'key value'", line)
		}
		if err := db.Set([]byte(k), []byte(v)); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
	}
	return sc.Err()
}

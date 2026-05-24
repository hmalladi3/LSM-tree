package slate_test

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/harimalladi/slate"
)

// The simplest possible use of slate: open, write, read, close.
func Example() {
	dir, _ := os.MkdirTemp("", "slate-example-")
	defer os.RemoveAll(dir)

	db, err := slate.Open(dir, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Set([]byte("hello"), []byte("world")); err != nil {
		log.Fatal(err)
	}
	v, err := db.Get([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", v)
	// Output: world
}

// Deleted keys are indistinguishable from never-written keys: both return
// ErrNotFound.
func ExampleDB_Delete() {
	dir, _ := os.MkdirTemp("", "slate-example-")
	defer os.RemoveAll(dir)

	db, _ := slate.Open(dir, nil)
	defer db.Close()

	db.Set([]byte("k"), []byte("v"))
	db.Delete([]byte("k"))

	_, err := db.Get([]byte("k"))
	fmt.Println(errors.Is(err, slate.ErrNotFound))
	// Output: true
}

// Writes are recovered after a crash. Re-opening a database replays the
// WAL and exposes every Sync-committed write.
func ExampleOpen_recovery() {
	dir, _ := os.MkdirTemp("", "slate-example-")
	defer os.RemoveAll(dir)

	db, _ := slate.Open(dir, nil)
	db.Set([]byte("durable"), []byte("v"))
	db.Close()

	// Simulate process restart: open the same directory and read back.
	db, _ = slate.Open(dir, nil)
	defer db.Close()
	v, _ := db.Get([]byte("durable"))
	fmt.Printf("%s\n", v)
	// Output: v
}

// Range scan with bounds. Iterators present a consistent snapshot of the
// database; they walk the merged LSM (memtable + every immutable memtable
// + every L0 SSTable) in ascending user-key order.
func ExampleDB_NewIterator() {
	dir, _ := os.MkdirTemp("", "slate-example-")
	defer os.RemoveAll(dir)

	db, _ := slate.Open(dir, nil)
	defer db.Close()
	for _, k := range []string{"apple", "banana", "cherry", "date", "elder"} {
		db.Set([]byte(k), []byte(k))
	}

	it := db.NewIterator(&slate.IterOptions{
		Lower: []byte("b"),
		Upper: []byte("e"), // exclusive
	})
	defer it.Close()
	for it.First(); it.Valid(); it.Next() {
		fmt.Printf("%s\n", it.Key())
	}
	// Output:
	// banana
	// cherry
	// date
}

// Async durability acknowledges the write before fdatasync. The write is
// visible immediately in the same process but may be lost on a crash
// before the next fdatasync.
func ExampleDB_SetSync() {
	dir, _ := os.MkdirTemp("", "slate-example-")
	defer os.RemoveAll(dir)

	opts := slate.DefaultOptions()
	opts.DefaultDurability = slate.Async
	db, _ := slate.Open(dir, opts)
	defer db.Close()

	// SetSync overrides the default Async durability for this one write.
	if err := db.SetSync([]byte("k"), []byte("v")); err != nil {
		log.Fatal(err)
	}
	fmt.Println("ok")
	// Output: ok
}

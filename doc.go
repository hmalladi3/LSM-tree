// Package slate is an embedded log-structured merge-tree key-value engine.
//
// slate provides durable, transactional, ordered key-value storage for
// single-process Go applications. It uses threshold-based hybrid value
// placement: small values are stored inline in SSTables while large values
// spill to a separate append-only value log.
//
// # Opening a database
//
//	db, err := slate.Open("/var/lib/app/data", slate.DefaultOptions())
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer db.Close()
//
// # Transactions
//
// All reads and writes go through a transaction. Use View for read-only
// transactions and Update for read-write transactions. Update retries
// automatically on commit conflicts.
//
//	err := db.Update(func(txn *slate.Txn) error {
//	    return txn.Set([]byte("k"), []byte("v"))
//	})
//
// For explicit lifetime control, call BeginTxn and Commit or Rollback
// manually.
//
// # Iteration
//
// Iterators must be created from an open transaction and must be closed
// before the transaction commits.
//
//	db.View(func(txn *slate.Txn) error {
//	    it := txn.NewIterator(nil)
//	    defer it.Close()
//	    for it.First(); it.Valid(); it.Next() {
//	        fmt.Printf("%s = %s\n", it.Key(), mustValue(it))
//	    }
//	    return it.Error()
//	})
//
// # Durability
//
// Every commit defaults to Sync durability: the transaction is durable on
// disk before Commit returns. Callers willing to trade durability for
// latency can call Txn.SetDurability with Async or NoSync.
package slate

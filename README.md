# slate

An embedded log-structured merge-tree key-value engine for Go.

```go
db, err := slate.Open("/var/lib/myapp/data", nil)
if err != nil {
    log.Fatal(err)
}
defer db.Close()
```

slate is single-process, single-writer storage with a WAL for crash recovery and zero runtime dependencies. Work in progress.

See `LICENSE` (MIT).

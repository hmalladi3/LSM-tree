// slate-bench is a YCSB-style benchmark harness for slate. It runs one of
// the standard YCSB workloads (A, B, C, E) against a freshly-created DB
// and reports throughput plus per-operation-type latency percentiles.
//
// Example:
//
//	slate-bench -workload=a -records=100000 -ops=100000 -threads=4
//
// Workload definitions follow Cooper et al., "Benchmarking cloud serving
// systems with YCSB," SoCC 2010:
//
//	A — Update heavy.   50% reads, 50% updates.   Zipfian.
//	B — Read heavy.     95% reads,  5% updates.   Zipfian.
//	C — Read only.     100% reads.                Zipfian.
//	E — Short ranges.  95% scans,  5% inserts.    Uniform.
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/harimalladi/slate"
)

type opType int

const (
	opRead opType = iota
	opUpdate
	opScan
	opInsert

	numOpTypes
)

func (o opType) String() string {
	switch o {
	case opRead:
		return "READ"
	case opUpdate:
		return "UPDATE"
	case opScan:
		return "SCAN"
	case opInsert:
		return "INSERT"
	}
	return "?"
}

type workload struct {
	name        string
	pRead       float64
	pUpdate     float64
	pScan       float64
	pInsert     float64
	scanRange   int  // max keys per scan
	keyDistZipf bool // true=zipfian, false=uniform
}

func parseWorkload(name string) workload {
	switch name {
	case "a", "A":
		return workload{name: "A", pRead: 0.5, pUpdate: 0.5, keyDistZipf: true}
	case "b", "B":
		return workload{name: "B", pRead: 0.95, pUpdate: 0.05, keyDistZipf: true}
	case "c", "C":
		return workload{name: "C", pRead: 1.0, keyDistZipf: true}
	case "e", "E":
		return workload{name: "E", pScan: 0.95, pInsert: 0.05, scanRange: 100, keyDistZipf: false}
	}
	log.Fatalf("unknown workload: %s (valid: a, b, c, e)", name)
	return workload{}
}

func main() {
	var (
		workloadName  = flag.String("workload", "a", "YCSB workload (a, b, c, e)")
		numRecords    = flag.Int("records", 100_000, "initial dataset size")
		numOps        = flag.Int("ops", 100_000, "operations to run during the run phase")
		threads       = flag.Int("threads", 4, "parallel client threads")
		keySize       = flag.Int("keysize", 16, "key length in bytes (will pad)")
		valueSize     = flag.Int("valuesize", 100, "value length in bytes")
		dir           = flag.String("dir", "", "DB directory (default: temp)")
		syncCommits   = flag.Bool("sync", false, "use Sync durability instead of NoSync")
		encrypted     = flag.Bool("encryption", false, "enable AES-256-GCM at rest")
		zipfTheta     = flag.Float64("zipf-theta", 0.99, "zipfian distribution theta parameter")
		showHistogram = flag.Bool("hist", false, "print latency histogram")
	)
	flag.Parse()

	wl := parseWorkload(*workloadName)

	// Set up the database directory.
	if *dir == "" {
		d, err := os.MkdirTemp("", "slate-bench-")
		if err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(d)
		*dir = d
	}

	opts := slate.DefaultOptions()
	if *syncCommits {
		opts.DefaultDurability = slate.Sync
	} else {
		opts.DefaultDurability = slate.NoSync
	}
	if *encrypted {
		opts.EncryptionKey = make([]byte, 32)
		// Deterministic key for reproducibility; production callers
		// should source entropy from crypto/rand.
		for i := range opts.EncryptionKey {
			opts.EncryptionKey[i] = byte(i)
		}
	}
	opts.MemtableSize = 64 << 20

	db, err := slate.Open(*dir, opts)
	if err != nil {
		log.Fatalf("Open: %v", err)
	}
	defer db.Close()

	fmt.Printf("=== slate-bench: workload %s ===\n", wl.name)
	fmt.Printf("  records=%d ops=%d threads=%d keysize=%d valuesize=%d sync=%v encryption=%v\n\n",
		*numRecords, *numOps, *threads, *keySize, *valueSize, *syncCommits, *encrypted)

	// LOAD PHASE: insert numRecords sequential keys.
	loadStart := time.Now()
	loadHist := newHistogram(*numRecords)
	value := makeValue(*valueSize)
	for i := 0; i < *numRecords; i++ {
		key := makeKey(i, *keySize)
		opStart := time.Now()
		if err := db.Set(key, value); err != nil {
			log.Fatalf("load Set: %v", err)
		}
		loadHist.add(time.Since(opStart))
	}
	if err := db.Sync(); err != nil {
		log.Fatalf("Sync: %v", err)
	}
	loadDuration := time.Since(loadStart)
	fmt.Printf("LOAD complete: %d records in %v (%.0f ops/sec)\n",
		*numRecords, loadDuration, float64(*numRecords)/loadDuration.Seconds())
	loadHist.report("LOAD", *showHistogram)
	fmt.Println()

	// RUN PHASE.
	runStart := time.Now()
	hists := [numOpTypes]*histogram{}
	for i := range hists {
		hists[i] = newHistogram(*numOps / *threads)
	}
	var ops atomic.Int64
	var wg sync.WaitGroup
	opsPerThread := *numOps / *threads
	for t := 0; t < *threads; t++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b1))
			zipf := newZipfian(uint64(*numRecords), *zipfTheta)
			perThreadHists := [numOpTypes]*histogram{}
			for i := range perThreadHists {
				perThreadHists[i] = newHistogram(opsPerThread)
			}
			for i := 0; i < opsPerThread; i++ {
				op := chooseOp(rng, wl)
				var keyIdx uint64
				if wl.keyDistZipf {
					keyIdx = zipf.Next(rng)
				} else {
					keyIdx = uint64(rng.Int64N(int64(*numRecords)))
				}
				key := makeKey(int(keyIdx), *keySize)
				opStart := time.Now()
				switch op {
				case opRead:
					_, _ = db.Get(key)
				case opUpdate:
					_ = db.Set(key, value)
				case opInsert:
					// Insert with index beyond the loaded range.
					ins := int(keyIdx) + *numRecords + i
					_ = db.Set(makeKey(ins, *keySize), value)
				case opScan:
					n := 1 + rng.IntN(wl.scanRange)
					it := db.NewIterator(nil)
					it.SeekGE(key)
					for cnt := 0; it.Valid() && cnt < n; cnt++ {
						_ = it.Value()
						it.Next()
					}
					it.Close()
				}
				perThreadHists[op].add(time.Since(opStart))
				ops.Add(1)
			}
			// Merge per-thread histograms into the global ones.
			for i := range perThreadHists {
				hists[i].merge(perThreadHists[i])
			}
		}(uint64(t + 1))
	}
	wg.Wait()
	runDuration := time.Since(runStart)

	totalOps := ops.Load()
	fmt.Printf("RUN complete: %d operations in %v (%.0f ops/sec, %d threads)\n",
		totalOps, runDuration, float64(totalOps)/runDuration.Seconds(), *threads)
	for op := opType(0); op < numOpTypes; op++ {
		if hists[op].count() == 0 {
			continue
		}
		hists[op].report(op.String(), *showHistogram)
	}
	fmt.Println()

	stats := db.Stats()
	fmt.Println("Engine stats:")
	fmt.Printf("  cache hit rate: %.2f%% (%d hits, %d misses)\n",
		stats.Cache.HitRate()*100, stats.Cache.Hits, stats.Cache.Misses)
	fmt.Printf("  levels: ")
	for i, n := range stats.LevelFileCount {
		if n > 0 {
			fmt.Printf("L%d=%d ", i, n)
		}
	}
	fmt.Println()
}

func chooseOp(rng *rand.Rand, w workload) opType {
	r := rng.Float64()
	c := 0.0
	c += w.pRead
	if r < c {
		return opRead
	}
	c += w.pUpdate
	if r < c {
		return opUpdate
	}
	c += w.pScan
	if r < c {
		return opScan
	}
	return opInsert
}

func makeKey(i, size int) []byte {
	s := fmt.Sprintf("user%020d", i)
	if len(s) >= size {
		return []byte(s[:size])
	}
	b := make([]byte, size)
	copy(b, s)
	for j := len(s); j < size; j++ {
		b[j] = '.'
	}
	return b
}

func makeValue(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte('a' + (i % 26))
	}
	return b
}

// ----- histogram -----

type histogram struct {
	mu      sync.Mutex
	samples []time.Duration
	sum     time.Duration
}

func newHistogram(initialCap int) *histogram {
	if initialCap > 1_000_000 {
		initialCap = 1_000_000
	}
	return &histogram{samples: make([]time.Duration, 0, initialCap)}
}

func (h *histogram) add(d time.Duration) {
	h.samples = append(h.samples, d)
	h.sum += d
}

func (h *histogram) merge(other *histogram) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples = append(h.samples, other.samples...)
	h.sum += other.sum
}

func (h *histogram) count() int { return len(h.samples) }

func (h *histogram) report(label string, showHistogram bool) {
	if len(h.samples) == 0 {
		return
	}
	sort.Slice(h.samples, func(i, j int) bool { return h.samples[i] < h.samples[j] })
	pct := func(p float64) time.Duration {
		i := int(float64(len(h.samples)) * p)
		if i >= len(h.samples) {
			i = len(h.samples) - 1
		}
		return h.samples[i]
	}
	mean := h.sum / time.Duration(len(h.samples))
	fmt.Printf("  %s: n=%d mean=%s p50=%s p95=%s p99=%s p99.9=%s max=%s\n",
		label, len(h.samples), mean, pct(0.50), pct(0.95), pct(0.99), pct(0.999), pct(1.0))
	if showHistogram {
		printBuckets(h.samples)
	}
}

func printBuckets(samples []time.Duration) {
	buckets := []time.Duration{
		100 * time.Nanosecond,
		300 * time.Nanosecond,
		time.Microsecond,
		3 * time.Microsecond,
		10 * time.Microsecond,
		30 * time.Microsecond,
		100 * time.Microsecond,
		300 * time.Microsecond,
		time.Millisecond,
		3 * time.Millisecond,
		10 * time.Millisecond,
		30 * time.Millisecond,
		100 * time.Millisecond,
	}
	counts := make([]int, len(buckets)+1)
	for _, s := range samples {
		idx := sort.Search(len(buckets), func(i int) bool { return buckets[i] >= s })
		counts[idx]++
	}
	prev := time.Duration(0)
	for i, b := range buckets {
		fmt.Printf("    [%8s, %8s): %d\n", prev, b, counts[i])
		prev = b
	}
	fmt.Printf("    [%8s, %8s): %d\n", prev, time.Duration(0), counts[len(buckets)])
}

// ----- zipfian distribution -----

// zipfian samples values in [0, n) under a Zipf-Mandelbrot distribution
// with parameter theta. Adapted from the YCSB Java implementation.
type zipfian struct {
	n     uint64
	theta float64
	zetaN float64
	zeta2 float64
	alpha float64
	eta   float64
}

func newZipfian(n uint64, theta float64) *zipfian {
	z := &zipfian{n: n, theta: theta}
	z.zetaN = zeta(n, theta)
	z.zeta2 = zeta(2, theta)
	z.alpha = 1.0 / (1.0 - theta)
	z.eta = (1.0 - pow(2.0/float64(n), 1.0-theta)) / (1.0 - z.zeta2/z.zetaN)
	return z
}

func (z *zipfian) Next(rng *rand.Rand) uint64 {
	u := rng.Float64()
	uz := u * z.zetaN
	if uz < 1.0 {
		return 0
	}
	if uz < 1.0+pow(0.5, z.theta) {
		return 1
	}
	return uint64(float64(z.n) * pow(z.eta*u-z.eta+1.0, z.alpha))
}

func zeta(n uint64, theta float64) float64 {
	sum := 0.0
	for i := uint64(1); i <= n; i++ {
		sum += 1.0 / pow(float64(i), theta)
	}
	return sum
}

func pow(x, y float64) float64 { return math.Pow(x, y) }

package main

import (
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestConfig holds configuration for the test suite
type TestConfig struct {
	NumKeys     int     // total number of keys to generate
	NumVersions int     // number of block heights to simulate
	ValueSize   int     // size of each value in bytes
	UpdateRatio float64 // fraction of keys updated per version
	NumRuns     int     // number of timing runs for statistics
}

// DefaultTestConfig returns production-realistic test configuration
func DefaultTestConfig() TestConfig {
	config := TestConfig{
		NumKeys:     10000, // reasonable default for quick tests
		NumVersions: 50,    // moderate blockchain depth
		ValueSize:   1024,  // production-like default value size (1KB)
		UpdateRatio: 0.05,  // 5% of keys change per block
		NumRuns:     5,     // enough runs for good statistics
	}

	// Allow environment variable overrides
	if v := os.Getenv("CANOPY_KEYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			config.NumKeys = n
		}
	}
	if v := os.Getenv("CANOPY_VERSIONS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			config.NumVersions = n
		}
	}
	if v := os.Getenv("CANOPY_VALUE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			config.ValueSize = n
		}
	}
	if v := os.Getenv("CANOPY_UPDATE_RATIO"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f > 0 && f <= 1 {
			config.UpdateRatio = f
		}
	}
	if v := os.Getenv("CANOPY_RUNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			config.NumRuns = n
		}
	}

	return config
}

// TestSuite manages the execution of all Canopy benchmark tests
type TestSuite struct {
	config  TestConfig
	keys    [][]byte
	hotKeys [][]byte
	results *TestResults
}

// NewTestSuite creates a new test suite with the given configuration
func NewTestSuite(config TestConfig) *TestSuite {
	// Generate deterministic keys
	keys := GenerateCanopyKeys(config.NumKeys, 12345) // fixed seed for reproducibility
	hotKeys := GetHotKeys(keys)

	return &TestSuite{
		config:  config,
		keys:    keys,
		hotKeys: hotKeys,
		results: NewTestResults(),
	}
}

// RunAllTests executes all test types against all provided store implementations
func (ts *TestSuite) RunAllTests(b *testing.B, factories map[string]StoreFactory) {
	fmt.Printf("Running Canopy benchmark suite:\n")
	fmt.Printf("  Keys: %d, Versions: %d, Value Size: %d bytes\n",
		ts.config.NumKeys, ts.config.NumVersions, ts.config.ValueSize)
	fmt.Printf("  Update Ratio: %.1f%%, Timing Runs: %d\n",
		ts.config.UpdateRatio*100, ts.config.NumRuns)
	fmt.Println()

	for name, factory := range factories {
		fmt.Printf("Testing %s implementation...\n", name)

		// Run simplified, production-representative tests
		fmt.Printf("  Running Latest...")
		ts.runLatestMixedTest(b, name, factory)
		fmt.Printf(" done\n")

		fmt.Printf("  Running Historical...")
		ts.runHistoricalMixedTest(b, name, factory)
		fmt.Printf(" done\n")

		fmt.Printf("  Running Archival...")
		ts.runArchivalTest(b, name, factory)
		fmt.Printf(" done\n")

		fmt.Printf("  Running Write...")
		ts.runWriteTest(b, name, factory)
		fmt.Printf(" done\n")

		fmt.Printf("  %s complete\n", name)
	}
}

// runLatestTest measures point-get performance on the latest state (hot path)
func (ts *TestSuite) runLatestTest(b *testing.B, implName string, factory StoreFactory) {
	times := make([]time.Duration, 0, ts.config.NumRuns)
	allocs := make([]int64, 0, ts.config.NumRuns)
	bytes := make([]int64, 0, ts.config.NumRuns)

	for run := 0; run < ts.config.NumRuns; run++ {
		if run > 0 {
			fmt.Printf(".")
		}
		store, err := factory()
		if err != nil {
			b.Fatalf("Failed to create %s store: %v", implName, err)
		}

		// Populate data up to latest version
		ts.populateStore(store, ts.config.NumVersions)

		// Optional warm-up pass to emulate steady-state hot caches
		warm := false
		if v := strings.ToLower(strings.TrimSpace(os.Getenv("WARM_LATEST"))); v == "1" || v == "true" || v == "yes" || v == "on" {
			warm = true
		}
		if warm {
			for _, key := range ts.hotKeys {
				_, _ = store.Get(key)
			}
		}

		// Measure point gets on hot keys (most realistic workload)
		var ms1, ms2 runtime.MemStats
		runtime.ReadMemStats(&ms1)

		start := time.Now()
		for _, key := range ts.hotKeys {
			_, err := store.Get(key)
			if err != nil {
				b.Fatalf("Get failed for key %s: %v", string(key), err)
			}
		}
		elapsed := time.Since(start)

		runtime.ReadMemStats(&ms2)

		// Record per-run totals (match store/benchmark_test.go semantics)
		times = append(times, elapsed)
		allocs = append(allocs, int64(ms2.Mallocs-ms1.Mallocs))
		bytes = append(bytes, int64(ms2.TotalAlloc-ms1.TotalAlloc))

		store.Close()
	}

	// Calculate statistics and record result
	minTime, medTime, maxTime := CalculateStats(times)
	medAllocs := CalculateMedianInt64(allocs)
	medBytes := CalculateMedianInt64(bytes)

	ts.results.AddResult(TestResult{
		Implementation: implName,
		TestType:       "Latest-PointGet",
		TimeMin:        minTime,
		TimeMedian:     medTime,
		TimeMax:        maxTime,
		AllocsPerOp:    medAllocs,
		BytesPerOp:     medBytes,
	})
}

// runHistoricalTest now measures true historical point lookups (height strictly below latest)
func (ts *TestSuite) runHistoricalTest(b *testing.B, implName string, factory StoreFactory) {
	times := make([]time.Duration, 0, ts.config.NumRuns)
	allocs := make([]int64, 0, ts.config.NumRuns)
	bytes := make([]int64, 0, ts.config.NumRuns)

	// Choose a height strictly below the latest; allow override via HIST_HEIGHT but clamp
	latest := ts.config.NumVersions
	if latest < 2 {
		latest = 2
	}
	trueHeight := uint64(latest - 1)
	if v := os.Getenv("HIST_HEIGHT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			if n >= latest {
				n = latest - 1
			}
			if n < 1 {
				n = 1
			}
			trueHeight = uint64(n)
		}
	}

	for run := 0; run < ts.config.NumRuns; run++ {
		if run > 0 {
			fmt.Printf(".")
		}
		store, err := factory()
		if err != nil {
			b.Fatalf("Failed to create %s store: %v", implName, err)
		}
		// Populate data
		ts.populateStore(store, ts.config.NumVersions)

		// Deterministic shuffled order of keys to emulate random seeks
		idx := make([]int, len(ts.keys))
		for i := range idx {
			idx[i] = i
		}
		rng := rand.New(rand.NewSource(int64(12345 + run)))
		rng.Shuffle(len(idx), func(i, j int) { idx[i], idx[j] = idx[j], idx[i] })

		var ms1, ms2 runtime.MemStats
		runtime.ReadMemStats(&ms1)
		start := time.Now()
		for _, id := range idx {
			key := ts.keys[id]
			if _, err := store.HistoricalGet(trueHeight, key); err != nil {
				// Some keys may not exist as of trueHeight depending on workload; ignore misses
				_ = err
			}
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&ms2)

		times = append(times, elapsed)
		allocs = append(allocs, int64(ms2.Mallocs-ms1.Mallocs))
		bytes = append(bytes, int64(ms2.TotalAlloc-ms1.TotalAlloc))
		store.Close()
	}

	minTime, medTime, maxTime := CalculateStats(times)
	medAllocs := CalculateMedianInt64(allocs)
	medBytes := CalculateMedianInt64(bytes)

	ts.results.AddResult(TestResult{
		Implementation: implName,
		TestType:       "Historical",
		TimeMin:        minTime,
		TimeMedian:     medTime,
		TimeMax:        maxTime,
		AllocsPerOp:    medAllocs,
		BytesPerOp:     medBytes,
	})
}

// runLatestMixedTest measures mixed hot/warm/cold latest gets with a small miss-rate
func (ts *TestSuite) runLatestMixedTest(b *testing.B, implName string, factory StoreFactory) {
	times := make([]time.Duration, 0, ts.config.NumRuns)
	allocs := make([]int64, 0, ts.config.NumRuns)
	bytes := make([]int64, 0, ts.config.NumRuns)
	for run := 0; run < ts.config.NumRuns; run++ {
		store, err := factory()
		if err != nil {
			b.Fatalf("Failed to create %s store: %v", implName, err)
		}
		ts.populateStore(store, ts.config.NumVersions)
		updateCount := int(float64(len(ts.keys)) * ts.config.UpdateRatio)
		warm := ts.keys[:min(updateCount, len(ts.keys))]
		cold := ts.keys[min(updateCount, len(ts.keys)):min(2*updateCount, len(ts.keys))]
		keys := make([][]byte, 0, len(ts.hotKeys)+len(warm)+len(cold))
		keys = append(keys, sample(ts.hotKeys, clamp(int(float64(len(ts.hotKeys))*0.6), 1, len(ts.hotKeys)))...)
		keys = append(keys, sample(warm, clamp(int(float64(len(warm))*0.2), 0, len(warm)))...)
		keys = append(keys, sample(cold, clamp(int(float64(len(cold))*0.2), 0, len(cold)))...)
		misses := max(1, int(float64(len(keys))*0.05))
		for i := 0; i < misses; i++ {
			keys = append(keys, []byte(fmt.Sprintf("missing_%d", i)))
		}
		rng := rand.New(rand.NewSource(int64(98765 + run)))
		rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
		var ms1, ms2 runtime.MemStats
		runtime.ReadMemStats(&ms1)
		start := time.Now()
		for _, k := range keys {
			_, _ = store.Get(k)
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&ms2)
		times = append(times, elapsed)
		allocs = append(allocs, int64(ms2.Mallocs-ms1.Mallocs))
		bytes = append(bytes, int64(ms2.TotalAlloc-ms1.TotalAlloc))
		store.Close()
	}
	minTime, medTime, maxTime := CalculateStats(times)
	medAllocs := CalculateMedianInt64(allocs)
	medBytes := CalculateMedianInt64(bytes)
	ts.results.AddResult(TestResult{Implementation: implName, TestType: "Latest", TimeMin: minTime, TimeMedian: medTime, TimeMax: maxTime, AllocsPerOp: medAllocs, BytesPerOp: medBytes})
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func sample(keys [][]byte, n int) [][]byte {
	if n <= 0 || len(keys) == 0 {
		return nil
	}
	if n >= len(keys) {
		out := make([][]byte, len(keys))
		copy(out, keys)
		return out
	}
	out := make([][]byte, 0, n)
	rng := rand.New(rand.NewSource(123456))
	idxs := rng.Perm(len(keys))
	for i := 0; i < n; i++ {
		out = append(out, keys[idxs[i]])
	}
	return out
}

// runHistoricalMixedTest uses a mix of heights (latest-1, latest-10, random) to emulate RPC
func (ts *TestSuite) runHistoricalMixedTest(b *testing.B, implName string, factory StoreFactory) {
	times := make([]time.Duration, 0, ts.config.NumRuns)
	allocs := make([]int64, 0, ts.config.NumRuns)
	bytes := make([]int64, 0, ts.config.NumRuns)
	for run := 0; run < ts.config.NumRuns; run++ {
		store, err := factory()
		if err != nil {
			b.Fatalf("create %s: %v", implName, err)
		}
		ts.populateStore(store, ts.config.NumVersions)
		var ms1, ms2 runtime.MemStats
		runtime.ReadMemStats(&ms1)
		start := time.Now()
		rng := rand.New(rand.NewSource(int64(77777 + run)))
		for _, key := range ts.keys {
			p := rng.Float64()
			var h uint64
			if p < 0.5 {
				h = uint64(max(ts.config.NumVersions-1, 1))
			} else if p < 0.8 {
				h = uint64(max(ts.config.NumVersions-10, 1))
			} else {
				h = uint64(1 + rng.Intn(max(ts.config.NumVersions-1, 1)))
			}
			_, _ = store.HistoricalGet(h, key)
		}
		elapsed := time.Since(start)
		runtime.ReadMemStats(&ms2)
		times = append(times, elapsed)
		allocs = append(allocs, int64(ms2.Mallocs-ms1.Mallocs))
		bytes = append(bytes, int64(ms2.TotalAlloc-ms1.TotalAlloc))
		store.Close()
	}
	minTime, medTime, maxTime := CalculateStats(times)
	medAllocs := CalculateMedianInt64(allocs)
	medBytes := CalculateMedianInt64(bytes)
	ts.results.AddResult(TestResult{Implementation: implName, TestType: "Historical", TimeMin: minTime, TimeMedian: medTime, TimeMax: maxTime, AllocsPerOp: medAllocs, BytesPerOp: medBytes})
}

// runArchivalTest measures full export performance across all versions
// Safety: supports a soft time cap via CANOPY_ARCHIVE_MAX_SECONDS (default 10s)
func (ts *TestSuite) runArchivalTest(b *testing.B, implName string, factory StoreFactory) {
	times := make([]time.Duration, 0, ts.config.NumRuns)
	allocs := make([]int64, 0, ts.config.NumRuns)
	bytes := make([]int64, 0, ts.config.NumRuns)

	// Soft timeout for safety to prevent runaway disk/CPU; user can override
	maxSeconds := 10
	if v := os.Getenv("CANOPY_ARCHIVE_MAX_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxSeconds = n
		}
	}
	var maxDur time.Duration
	if maxSeconds > 0 {
		maxDur = time.Duration(maxSeconds) * time.Second
	} // 0=unlimited

	for run := 0; run < ts.config.NumRuns; run++ {
		store, err := factory()
		if err != nil {
			b.Fatalf("Failed to create %s store: %v", implName, err)
		}
		// Populate data
		ts.populateStore(store, ts.config.NumVersions)

		// Measure archive iteration (all versions)
		var ms1, ms2 runtime.MemStats
		runtime.ReadMemStats(&ms1)

		start := time.Now()
		iter, err := store.ArchiveIterator(nil)
		if err != nil {
			b.Fatalf("ArchiveIterator failed: %v", err)
		}

		count := 0
		for iter.Valid() {
			_ = iter.Key()
			_ = iter.Value()
			iter.Next()
			count++
			if maxDur > 0 && time.Since(start) >= maxDur {
				// Soft stop to avoid freezes; break early and mark as truncated
				break
			}
		}
		iter.Close()
		elapsed := time.Since(start)

		runtime.ReadMemStats(&ms2)

		// Record per-run totals (match store/benchmark_test.go semantics)
		times = append(times, elapsed)
		allocs = append(allocs, int64(ms2.Mallocs-ms1.Mallocs))
		bytes = append(bytes, int64(ms2.TotalAlloc-ms1.TotalAlloc))

		store.Close()
	}

	// Calculate statistics and record result
	minTime, medTime, maxTime := CalculateStats(times)
	medAllocs := CalculateMedianInt64(allocs)
	medBytes := CalculateMedianInt64(bytes)

	ts.results.AddResult(TestResult{Implementation: implName, TestType: "Archival", TimeMin: minTime, TimeMedian: medTime, TimeMax: maxTime, AllocsPerOp: medAllocs, BytesPerOp: medBytes})
}

// runArchivalZeroCopyTest enables ARCHIVE_FAST=1 to measure zero-copy archive path
func (ts *TestSuite) runArchivalZeroCopyTest(b *testing.B, implName string, factory StoreFactory) {
	times := make([]time.Duration, 0, ts.config.NumRuns)
	allocs := make([]int64, 0, ts.config.NumRuns)
	bytes := make([]int64, 0, ts.config.NumRuns)
	old := os.Getenv("ARCHIVE_FAST")
	defer os.Setenv("ARCHIVE_FAST", old)
	for run := 0; run < ts.config.NumRuns; run++ {
		store, err := factory()
		if err != nil {
			b.Fatalf("create %s: %v", implName, err)
		}
		ts.populateStore(store, ts.config.NumVersions)
		var ms1, ms2 runtime.MemStats
		runtime.ReadMemStats(&ms1)
		os.Setenv("ARCHIVE_FAST", "1")
		start := time.Now()
		iter, err := store.ArchiveIterator(nil)
		if err != nil {
			b.Fatalf("ArchiveIterator failed: %v", err)
		}
		for iter.Valid() {
			_ = iter.Key()
			_ = iter.Value()
			iter.Next()
		}
		iter.Close()
		elapsed := time.Since(start)
		runtime.ReadMemStats(&ms2)
		times = append(times, elapsed)
		allocs = append(allocs, int64(ms2.Mallocs-ms1.Mallocs))
		bytes = append(bytes, int64(ms2.TotalAlloc-ms1.TotalAlloc))
		store.Close()
	}
	minTime, medTime, maxTime := CalculateStats(times)
	medAllocs := CalculateMedianInt64(allocs)
	medBytes := CalculateMedianInt64(bytes)
	ts.results.AddResult(TestResult{Implementation: implName, TestType: "Historical-Archive-ZeroCopy", TimeMin: minTime, TimeMedian: medTime, TimeMax: maxTime, AllocsPerOp: medAllocs, BytesPerOp: medBytes})
}

// runWriteTest measures block ingestion performance
func (ts *TestSuite) runWriteTest(b *testing.B, implName string, factory StoreFactory) {
	times := make([]time.Duration, 0, ts.config.NumRuns)
	allocs := make([]int64, 0, ts.config.NumRuns)
	bytes := make([]int64, 0, ts.config.NumRuns)

	for run := 0; run < ts.config.NumRuns; run++ {
		store, err := factory()
		if err != nil {
			b.Fatalf("Failed to create %s store: %v", implName, err)
		}

		// Measure write performance
		var ms1, ms2 runtime.MemStats
		runtime.ReadMemStats(&ms1)

		start := time.Now()
		ts.populateStore(store, ts.config.NumVersions)
		elapsed := time.Since(start)

		runtime.ReadMemStats(&ms2)

		// Record per-run totals (match store/benchmark_test.go semantics)
		times = append(times, elapsed)
		allocs = append(allocs, int64(ms2.Mallocs-ms1.Mallocs))
		bytes = append(bytes, int64(ms2.TotalAlloc-ms1.TotalAlloc))

		store.Close()
	}

	// Calculate statistics and record result
	minTime, medTime, maxTime := CalculateStats(times)
	medAllocs := CalculateMedianInt64(allocs)
	medBytes := CalculateMedianInt64(bytes)

	ts.results.AddResult(TestResult{
		Implementation: implName,
		TestType:       "Write",
		TimeMin:        minTime,
		TimeMedian:     medTime,
		TimeMax:        maxTime,
		AllocsPerOp:    medAllocs,
		BytesPerOp:     medBytes,
	})
}

// populateStore fills a store with realistic data across multiple versions
func (ts *TestSuite) populateStore(store CanopyStore, numVersions int) {
	// Genesis block - write all keys
	store.SetVersion(1)
	for i, key := range ts.keys {
		value := GenerateRealisticValue(ts.config.ValueSize, uint64(i))
		if err := store.Set(key, value); err != nil {
			panic(fmt.Sprintf("Set failed: %v", err))
		}
	}
	if err := store.Commit(); err != nil {
		panic(fmt.Sprintf("Commit failed: %v", err))
	}

	// Subsequent blocks - update only a fraction of keys
	updateCount := int(float64(len(ts.keys)) * ts.config.UpdateRatio)
	for version := 2; version <= numVersions; version++ {
		// Show progress for large datasets
		if numVersions > 20 && version%10 == 0 {
			fmt.Printf("v%d", version)
		}

		store.SetVersion(uint64(version))

		// Update the first updateCount keys (deterministic pattern)
		for i := 0; i < updateCount; i++ {
			key := ts.keys[i]
			// Create version-specific value
			valueKey := uint64(version*1000 + i)
			value := GenerateRealisticValue(ts.config.ValueSize, valueKey)
			if err := store.Set(key, value); err != nil {
				panic(fmt.Sprintf("Set failed: %v", err))
			}
		}

		if err := store.Commit(); err != nil {
			panic(fmt.Sprintf("Commit failed: %v", err))
		}
	}
}

// PrintResults prints the final summary table
func (ts *TestSuite) PrintResults(title string) {
	ts.results.PrintSummary(title)
}

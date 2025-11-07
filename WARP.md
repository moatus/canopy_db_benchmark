# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Overview

This is a focused benchmark harness that emulates Canopy's storage access patterns to compare production-relevant storage variants:
- **Badger**: Reference baseline using Badger managed mode
- **MonoSpace**: Single Pebble DB in pure SeekLT layout

## Common Commands

### Running Benchmarks

```bash
# Basic benchmark run (all implementations, all tests)
go test -bench=BenchmarkCanopyStorage -benchtime=1x

# Verbose output for long tests
go test -bench=BenchmarkCanopyStorage -benchtime=1x -v

# With timeout for large datasets
go test -bench=BenchmarkCanopyStorage -benchtime=1x -timeout=30m
```

### Configuration Examples

```bash
# Larger dataset with more versions
CANOPY_KEYS=10000 CANOPY_VERSIONS=100 CANOPY_RUNS=3 \
  go test -bench=BenchmarkCanopyStorage -benchtime=1x

# Quick iteration with small dataset
CANOPY_KEYS=200 CANOPY_VERSIONS=5 CANOPY_RUNS=2 \
  go test -bench=BenchmarkCanopyStorage -benchtime=1x

# MonoSpace with value-log enabled
MONOSPACE_ENABLE_VALUE_LOG=1 MONOSPACE_INLINE_THRESHOLD=256 \
  go test -bench=BenchmarkCanopyStorage -benchtime=1x

# Disable fsync for exploration
PEBBLE_SYNC=0 go test -bench=BenchmarkCanopyStorage -benchtime=1x
```

### Build and Lint

```bash
# Build the module
go build ./...

# Format and vet
go fmt ./...
go vet ./...
```

### Individual Tests

```bash
# Run key generation validation
go test -run=TestCanopyKeyGeneration

# Test results formatting
go test -run=TestCanopyResultsFormatting
```

## Architecture

### High-Level Structure

This is a standalone Go module benchmarking three storage implementations through a common `CanopyStore` interface. Each implementation emulates different aspects of Canopy's production storage:

- **Badger**: Production baseline with LSS (`s/`) and HSS (`h/`) prefixes
- **MonoSpace**: Pure versioned key layout optimized for storage efficiency

### Key Components

1. **Test Suite (`canopy_tests.go`)**: Main orchestration with configurable workloads
2. **Store Implementations**: 
   - `badger_store.go`: Badger managed mode wrapper
   - `monospace_store.go`: MonoSpace (pure SeekLT with optional value-log)
- `dual_timeline_store.go`: Mirror of actual Canopy Pebble integration
3. **Supporting Components**:
   - `canopy_keys.go`: Realistic key generation (account_, validator_, committee_, hash_)
   - `canopy_results.go`: Statistics calculation and formatting
   - `valuelog.go`: Value-log implementation for large values
   - `hot_latest_cache.go`: Hot cache for latest reads

### Store Interface

All implementations satisfy:
```go
type CanopyStore interface {
    Set(key, value []byte) error
    Get(key []byte) ([]byte, error)
    HistoricalGet(height uint64, key []byte) ([]byte, error)
    Iterator(prefix []byte) (CanopyIterator, error)
    HistoricalIterator(height uint64, prefix []byte) (CanopyIterator, error)
    ArchiveIterator(prefix []byte) (CanopyIterator, error)
    SetVersion(height uint64)
    Commit() error
    Close() error
    Name() string
}
```

## Configuration Environment Variables

### Workload Configuration
- `CANOPY_KEYS`: Total keys (default 10000)
- `CANOPY_VERSIONS`: Number of heights (default 50)
- `CANOPY_VALUE_SIZE`: Bytes per value (default 1024)
- `CANOPY_UPDATE_RATIO`: Fraction of keys updated per block (default 0.05)
- `CANOPY_RUNS`: Timing repetitions for stats (default 5)
- `WARM_LATEST`: Warm hot keys before latest test
- `HIST_HEIGHT`: Override target height for historical test
- `CANOPY_ARCHIVE_MAX_SECONDS`: Soft cap for archive test (default 10s)

### Pebble Configuration
- `PEBBLE_CACHE_MB`: Block cache size (default varies by implementation)
- `PEBBLE_BLOOM_BITS`: Bloom filter bits per key (default 10)
- `PEBBLE_SYNC`: Enable/disable fsync (default true)

### MonoSpace Configuration
- `MONOSPACE_HOT_LATEST_MB`: Hot latest cache size (default 64MB, 0=disable)
- `MONOSPACE_INLINE_THRESHOLD`: Value-log inline threshold (default 2048)
- `MONOSPACE_BLOB_CACHE_MB`: Blob cache size for value-log (default 128MB)
- `MONOSPACE_VALUE_LOG_SHARDS`: Value-log sharding (default 8)
- `MONOSPACE_ENABLE_VALUE_LOG`: Enable value-log path (default off)
- `PEBBLE_MAX_OPEN_FILES`: Override max open files (if supported by options)

## Test Types

1. **Latest-PointGet**: Hot path performance on current state
2. **Historical-PointGet**: Point lookups at specific heights  
3. **Historical-Archive-AllVersions**: Full export performance across all versions
4. **Write**: Block ingestion performance

## Key Patterns

- Realistic key distribution: 60% account_, 20% validator_, 10% committee_, 10% hash_
- Hot keys (subset of account_ keys) used for latest read tests
- Deterministic update patterns for reproducible benchmarks
- Version-specific value generation for realistic historical queries

### Value Encoding

### MonoSpace  
- Pure versioned keys: `[userKey][^version]`
- Values: `[tombstone][kind][payload]` with optional value-log pointers

### Version Encoding
- Inverted timestamps: `^version` so MonoSpace finds latest versions first
- 8-byte big-endian encoding for proper lexicographic ordering

## Dependencies

This repository uses a local `mvcc` library replacement pointing to `../mvcc_library`. Key external dependencies:
- `github.com/cockroachdb/pebble/v2`: Primary storage engine
- `github.com/dgraph-io/badger/v4`: Reference implementation
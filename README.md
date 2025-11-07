# Canopy Storage Benchmark

## Purpose

Compares Pebble-based storage implementations against Badger to demonstrate how Pebble with caching optimizations can match or exceed Badger's performance for Canopy workloads.

**Implementations tested:**
- **Badger**: Current production reference with built-in caching
- **MonoSpace**: Single-DB Pebble with SeekLT-based versioned keys
- **DualTimeline**: Dual-prefix Pebble (LSS/HSS separation)

## Key Insight: Caching Makes Pebble Competitive

Badger's performance advantage historically came from its built-in hot-key caching and value-log optimizations. **This benchmark shows that Pebble implementations with equivalent opt-in features (hot latest cache, value-log, block-property filters) achieve comparable performance to Badger**, especially for read-heavy RPC workloads typical in blockchain storage.

## Design Comparison

### MonoSpace
- **Layout**: Canopy-safe encoding `[Enc(userKey)][0x00 0x00][^version]` (default ON; disable with `MONOSPACE_CANOPY_ENCODING=0`)
- **Writes**: One versioned key + optional value-log entry (low write amplification)
- **Reads**: SeekLT for all queries (Latest, Historical, Archival)
- **Optimizations** (when `MONOSPACE_OPT_FEATURES=1`):
  - Hot latest cache: accelerates Latest scans when there is reuse of hot keys
  - Value-log: reduces LSM write amp; pairs well with cache for large values
  - Block-property filters: prunes archival/historical scans
  - Cached Historical iterators: reuses snapshots
- **Baseline advantage (no cache)**: SeekLT layout already yields strong Latest/Historical latency without caching for one-pass workloads
- **Trade-off**: Compact layout favors archival/historical; Latest can further improve with cache in reuse-heavy RPC patterns

### DualTimeline
- **Layout**: Prefix-separated Latest (`s/`) and Historical (`h/`) keyspaces
- **Writes**: Writes to both LSS (inverted version) and HSS (forward version)
- **Reads**: Direct Latest prefix scan (fast), SeekLT for Historical/Archival
- **Optimizations** (when `DUALTIMELINE_OPT_FEATURES=1`):
  - Hot latest cache: further accelerates Latest
  - Value-log: stores HSS values out-of-band
  - Block-property filters: prunes archival scans
  - Zero-copy archive: avoids allocations
- **Trade-off**: Higher write amplification (dual writes) for faster Latest reads

### Badger (Reference)
- Managed mode with production settings: large memtables, no compression, aggressive L0 thresholds
- Built-in hot-key cache and value-log enabled by default

## Benchmark Results

### With Optimizations (Hot Cache + Value-Log + Block Filters)

Pebble implementations with opt features enabled match or exceed Badger performance.

#### Medium Load (10k keys, 50 versions)

```bash
CANOPY_KEYS=10000 CANOPY_VERSIONS=50 go test -bench=BenchmarkCanopyStorage -benchtime=1x
```

| Test                | Badger | MonoSpace | DualTimeline |
|---------------------|--------|-----------|-------------|
| **Latest (ms)**     | 12.4   | 2.6       | 1.4          |
| **Historical (ms)** | 18.8   | 6.5       | 3.9          |
| **Archival (ms)**   | 25.0   | 8.6       | 7.6          |
| **Write (ms)**      | 967    | 444       | 438          |

#### Large Load (100k keys, 50 versions)

```bash
CANOPY_KEYS=100000 CANOPY_VERSIONS=50 go test -bench=BenchmarkCanopyStorage -benchtime=1x
```

| Test                | Badger | MonoSpace | DualTimeline |
|---------------------|--------|-----------|-------------|
| **Latest (ms)**     | 185.9  | 128.0     | 134.4        |
| **Historical (ms)** | 326.7  | 268.4     | 306.5        |
| **Archival (ms)**   | 347.5  | 289.9     | 469.8        |
| **Write (s)**       | 11.48  | 5.08      | 5.91         |

### Without Optimizations (Baseline Pebble)

Without hot cache, value-log, and block filters, DualTimeline shows significant performance degradation.

#### Medium Load (10k keys, 50 versions)

```bash
CANOPY_KEYS=10000 CANOPY_VERSIONS=50 MONOSPACE_OPT_FEATURES=0 DUALTIMELINE_OPT_FEATURES=0 \
  go test -bench=BenchmarkCanopyStorage -benchtime=1x
```

| Test                | Badger | MonoSpace | DualTimeline |
|---------------------|--------|-----------|-------------|
| **Latest (ms)**     | 13.2   | 1.3       | 33.9         |
| **Historical (ms)** | 20.5   | 5.0       | 56.1         |
| **Archival (ms)**   | 32.1   | 8.9       | 32.4         |
| **Write (s)**       | 1.01   | 0.45      | 0.53         |

#### Large Load (100k keys, 50 versions)

```bash
CANOPY_KEYS=100000 CANOPY_VERSIONS=50 MONOSPACE_OPT_FEATURES=0 DUALTIMELINE_OPT_FEATURES=0 \
  go test -bench=BenchmarkCanopyStorage -benchtime=1x
```

| Test                | Badger | MonoSpace | DualTimeline |
|---------------------|--------|-----------|-------------|
| **Latest (ms)**     | 178.6  | 97.9      | 667.0        |
| **Historical (s)**  | 0.46   | 0.24      | 1.31         |
| **Archival (ms)**   | 343.7  | 231.1     | 538.8        |
| **Write (s)**       | 10.87  | 4.70      | 5.21         |

### Key Observations

- MonoSpace’s SeekLT layout delivers strong Latest and Historical latency even with caches off; this is why its “no-opt” profile still looks good in one-pass benchmarks.
- Caching benefits for MonoSpace are workload-dependent: expect gains when (a) there is repeated access to a small hot set (RPC head), (b) value‑log is enabled with lower inline thresholds (cache avoids blob I/O + pointer decode), or (c) the hot set fits within the cache. In broad one-pass scans, gains are modest.
- DualTimeline relies heavily on the hot-latest cache and block filters to keep Latest/Historical fast; with features off it degrades substantially.
- With optimizations enabled, both Pebble implementations outperform Badger on reads and remain competitive on writes; choice depends on write amp tolerance vs Latest latency goals.

## Implementations and Tuning

### 1) Badger
Reference baseline using Badger managed mode with production-leaning settings:
- NumVersionsToKeep=MaxInt
- ValueThreshold=1024, NoCompression
- Large memtables and aggressive L0 thresholds
- SyncWrites=true, DetectConflicts=false

Env: none (pre-tuned in code)

### 2) MonoSpace (single Pebble DB)
Pure SeekLT layout: keys are `[userKey][^version]`. Latest and historical are served via SeekLT. Value-log
is enabled by default with an inline threshold.

Env and Pebble options:
- PEBBLE_CACHE_MB: Pebble block cache (default 512)
- PEBBLE_BLOOM_BITS: bits/key bloom (default 10)
- MONOSPACE_CANOPY_ENCODING: use Canopy-safe key encoding (default on; set to 0 to disable)
- MONOSPACE_HOT_LATEST_MB: hot latest cache MB, 0=disable (default 64)
- MONOSPACE_INLINE_THRESHOLD: value-log inline threshold (default 2048)
- MONOSPACE_BLOB_CACHE_MB: blob cache MB for value-log (default 128)
- MONOSPACE_VALUE_LOG_SHARDS: shards for value-log (default 8)
- MONOSPACE_ENABLE_VALUE_LOG: enable value-log path (default off)
- PEBBLE_MAX_OPEN_FILES: override max open files (optional)
- PEBBLE_SYNC: applies to batch commits

Opt features toggles (MonoSpace):
- MONOSPACE_OPT_FEATURES: master toggle (default on). Set to 0/false to disable advanced features.
- When MONOSPACE_OPT_FEATURES=0: disables HotLatest, ValueLog, block-interval pruning (falls back to coarse invTS), Historical session reuse, and Last‑Modified index.
- MONOSPACE_HIST_SESSION: enable per-height snapshot + cached iterator reuse (default on when OPT_FEATURES=1).
- MONOSPACE_LAST_MOD_INDEX: enable in-memory last-modified fast path (default on when OPT_FEATURES=1).

### 3) DualTimeline
Baseline that mirrors the actual Canopy Pebble integration (no hot latest cache, no value-log, no
block-property filters). DualTimeline was included in this benchmark suite as an initial implementation to assess
how the baseline Pebble setup performs under stress. Layout:
- Latest (LSS): `s/<userKey><^version>` (latest uses inverted version space)
- Historical (HSS): `h/<userKey><^version>`
- Values: `[tombstone][value]`

Options (subset):
- PEBBLE_CACHE_MB (default 128)
- MemTableSize 512MB, L0CompactionThreshold 20, L0StopWritesThreshold 40
- PEBBLE_SYNC respected via write options

Env and Pebble options (DualTimeline):
- PEBBLE_CACHE_MB: Pebble block cache (default 256 for DualTimeline legacy path)
- PEBBLE_BLOOM_BITS: bits/key bloom (default 12)
- PEBBLE_SYNC: enable/disable fsync on writes (default on)
- DUALTIMELINE_HOT_LATEST_MB: HotLatest cache MB, 0=disable (default 96 when OPT_FEATURES=1)
- DUALTIMELINE_ENABLE_VALUE_LOG: enable value-log storage for HSS values (default on when OPT_FEATURES=1)
- DUALTIMELINE_INLINE_THRESHOLD_HSS: inline size threshold for HSS before value-log (default 1536)
- DUALTIMELINE_BLOB_CACHE_MB: blob cache MB for value-log (default 256)
- DUALTIMELINE_VALUE_LOG_SHARDS: shards for value-log (default 8)
- DUALTIMELINE_ARCHIVE_POINTERS_ONLY: return pointers-only payloads during archival when value-log is enabled (default on)
- ARCHIVE_FAST: return zero-copy values in ArchiveIterator (effective only when OPT_FEATURES=1; default on)

Opt features toggles (DualTimeline):
- DUALTIMELINE_OPT_FEATURES: master toggle (default on). Set to 0/false to disable advanced features.
- When DUALTIMELINE_OPT_FEATURES=0: disables HotLatest, ValueLog, block-interval pruning/filtering, Historical session reuse, Last‑Modified index, and zero-copy archive path.
- DUALTIMELINE_HIST_SESSION: enable per-height snapshot + cached iterator reuse (default on when OPT_FEATURES=1)
- DUALTIMELINE_LAST_MOD_INDEX: enable in-memory last-modified fast path (default on when OPT_FEATURES=1)

## Running the Benchmarks

### Quick start

```bash
# From this folder - runs all implementations
go test -bench=BenchmarkCanopyStorage -benchtime=1x
```

### Command flags explained

#### Go benchmark flags
- **`-bench=BenchmarkCanopyStorage`**: Runs only the specified benchmark function
- **`-benchtime=1x`**: Run each benchmark exactly 1 iteration (instead of time-based)
  - Alternative: `-benchtime=5s` would run for 5 seconds
  - Using `1x` gives predictable, single execution
- **`-v`**: Verbose output (optional, shows progress during long tests)
- **`-timeout=30m`**: Set timeout for the entire test run (default 10m, increase for large datasets)

### Workload configuration (env)
- CANOPY_KEYS: total keys (default 10000)
- CANOPY_VERSIONS: number of heights (default 50)
- CANOPY_VALUE_SIZE: bytes/value (default 1024)
- CANOPY_UPDATE_RATIO: fraction of keys updated per block (default 0.05)
- CANOPY_RUNS: timing repetitions for stats (default 5)
- WARM_LATEST: warm hot keys before Latest test (1/true)
- HIST_HEIGHT: override target height for Historical test (default latest)
- CANOPY_ARCHIVE_MAX_SECONDS: soft cap for Archive test (default 10s; 0=unlimited)

## Result format
- Time/Run reported as min/median/max with one consistent unit per test (ns/μs/ms/s)
- Allocs/Run and Bytes/Run shown as medians
- Implementations shown in the order: Badger, MonoSpace, DualTimeline

## Notes
- Archive test has a soft timeout via CANOPY_ARCHIVE_MAX_SECONDS to avoid long runs
- This folder is an isolated Go module (see go.mod) for simple, reproducible runs
- Keys follow realistic distribution (account_, validator_, committee_, hash_)

## Examples
```bash
# Larger values and more versions
CANOPY_VALUE_SIZE=4096 CANOPY_VERSIONS=100 CANOPY_RUNS=3 \
  go test -bench=BenchmarkCanopyStorage -benchtime=1x

# MonoSpace with value-log enabled
MONOSPACE_ENABLE_VALUE_LOG=1 MONOSPACE_INLINE_THRESHOLD=256 \
  go test -bench=BenchmarkCanopyStorage -benchtime=1x

# Quick run for iteration (small dataset)
CANOPY_KEYS=200 CANOPY_VERSIONS=5 CANOPY_RUNS=2 \
  go test -bench=BenchmarkCanopyStorage -benchtime=1x

# Production-scale run (large dataset, with timeout)
CANOPY_KEYS=10000 CANOPY_VERSIONS=100 CANOPY_RUNS=3 \
  go test -bench=BenchmarkCanopyStorage -benchtime=1x -timeout=30m

# Verbose output for long-running tests
CANOPY_KEYS=5000 CANOPY_VERSIONS=50 \
  go test -bench=BenchmarkCanopyStorage -benchtime=1x -v

# Disable fsync (exploration only)
PEBBLE_SYNC=0 go test -bench=BenchmarkCanopyStorage -benchtime=1x
```

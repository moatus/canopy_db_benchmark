package main

import (
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// Shared utility functions for all Pebble store implementations
// These functions are used across the SeekLT and DualTimeline stores

// Key encoding constants and functions

// keyLSS composes the Latest State Store key: s/<userKey>
func keyLSS(user []byte) []byte {
	out := make([]byte, 0, 2+len(user))
	out = append(out, 's', '/')
	out = append(out, user...)
	return out
}

// SeekLT constants for MVCC implementations
const (
	SeekLTVersionSize    = 8
	SeekLTDeadTombstone  = byte(1)
	SeekLTAliveTombstone = byte(0)
)

// MVCC SeekLT key/value encoding functions

// mvccValueWithTombstone prefixes value with tombstone marker
func mvccValueWithTombstone(tombstone byte, value []byte) []byte {
	v := make([]byte, 1+len(value))
	v[0] = tombstone
	if len(value) > 0 {
		copy(v[1:], value)
	}
	return v
}

// mvccParseValueWithTombstone extracts tombstone and value
func mvccParseValueWithTombstone(v []byte) (tombstone byte, value []byte) {
	if len(v) == 0 {
		return SeekLTDeadTombstone, nil
	}
	if len(v) > 1 {
		value = v[1:]
	}
	return v[0], value
}

// Utility functions for prefix handling

// prefixEnd returns the next key after all keys with the given prefix
func prefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // prefix is all 0xff bytes
}

// Pebble configuration helpers

// createProductionPebbleOptions creates Pebble options optimized for production-like performance
func createProductionPebbleOptions(cacheSizeMB int) *pebble.Options {
	cache := pebble.NewCache(int64(cacheSizeMB) << 20)
	// Optional columnar blocks for MonoSpace (env: MONOSPACE_COLUMNAR); default ON
	useColumnar := func() bool {
		v := strings.ToLower(strings.TrimSpace(os.Getenv("MONOSPACE_COLUMNAR")))
		if v == "0" || v == "false" || v == "no" || v == "off" {
			return false
		}
		return true
	}()
	format := pebble.FormatNewest
	if useColumnar {
		format = pebble.FormatColumnarBlocks
	}
	opts := &pebble.Options{
		DisableWAL:                  false,
		MemTableSize:                256 << 20, // 256MB to match Badger
		L0CompactionThreshold:       10,        // Align with Badger-ish thresholds
		L0StopWritesThreshold:       20,
		MemTableStopWritesThreshold: 16,
		LBaseMaxBytes:               512 << 20,
		BytesPerSync:                1 << 20,
		WALBytesPerSync:             1 << 20,
		MaxOpenFiles:                5000, // Fewer iterator file-open stalls
		Cache:                       cache,
		FormatMajorVersion:          format,
	}

	// Configure to mirror Canopy+Badger defaults: bloom on all levels, no compression, 64KB blocks
	opts.EnsureDefaults()
	// Allow env overrides for bloom bits and block sizes
	bloomBits := 12
	if v := strings.TrimSpace(os.Getenv("PEBBLE_BLOOM_BITS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			bloomBits = n
		}
	}
	blockKB := 64
	if v := strings.TrimSpace(os.Getenv("PEBBLE_BLOCK_KB")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			blockKB = n
		}
	}
	policy := bloom.FilterPolicy(bloomBits)
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = policy
		opts.Levels[i].Compression = func() *sstable.CompressionProfile { return sstable.NoCompression }
		opts.Levels[i].BlockSize = blockKB << 10
		opts.Levels[i].IndexBlockSize = blockKB << 10
	}

	// Optional: bump MaxOpenFiles via env
	if v := strings.TrimSpace(os.Getenv("PEBBLE_MAX_OPEN_FILES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			opts.MaxOpenFiles = n
		}
	}

	return opts
}

// Environment variable helpers

// isPebbleSyncEnabled checks if Pebble sync is enabled (default true for production parity)
func isPebbleSyncEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("PEBBLE_SYNC")))
	switch v {
	case "0", "false", "no", "n":
		return false
	default:
		return true
	}
}

// Sync option helper
func getSyncOption() pebble.WriteOptions {
	if isPebbleSyncEnabled() {
		return pebble.WriteOptions{Sync: true}
	}
	return pebble.WriteOptions{Sync: false}
}

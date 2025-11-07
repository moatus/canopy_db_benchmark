package main

import (
	"sync"
)

// HotLatestCache is a tiny byte-capped cache for latest (s/) values keyed by user key.
// It is intentionally simple (FIFO-ish) and optimized for benchmarks rather than
// perfect eviction policy.
// Values are copied on set to keep cached slices immutable; Get can return the
// cached slice directly or provide a defensive copy depending on configuration.
type HotLatestCache struct {
	capBytes     int64
	mu           sync.Mutex
	curBytes     int64
	items        map[string]*hotEntry
	order        []string // FIFO order for eviction
	returnCopies bool     // when true, Get returns defensive copies
}

type hotEntry struct {
	ver uint64
	val []byte
}

// Default max per-record admission to avoid a single value consuming the cache.
const hotLatestMaxRecordLen = 256 << 10 // 256KB

func NewHotLatestCache(megabytes int) *HotLatestCache {
	if megabytes <= 0 {
		return nil
	}
	return &HotLatestCache{capBytes: int64(megabytes) << 20, items: make(map[string]*hotEntry)}
}

// SetReturnCopies toggles whether Get returns defensive copies (default: false for zero-copy).
func (c *HotLatestCache) SetReturnCopies(copy bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.returnCopies = copy
	c.mu.Unlock()
}

// Get retrieves a cached value for key if present.
func (c *HotLatestCache) Get(key []byte) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	k := string(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[k]
	if !ok || e == nil || e.val == nil {
		return nil, false
	}
	if c.returnCopies {
		out := make([]byte, len(e.val))
		copy(out, e.val)
		return out, true
	}
	return e.val, true
}

// GetWithVersion returns (version, value, ok) for the key if present.
func (c *HotLatestCache) GetWithVersion(key []byte) (uint64, []byte, bool) {
	if c == nil {
		return 0, nil, false
	}
	k := string(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.items[k]
	if !ok || e == nil || e.val == nil {
		return 0, nil, false
	}
	if c.returnCopies {
		out := make([]byte, len(e.val))
		copy(out, e.val)
		return e.ver, out, true
	}
	return e.ver, e.val, true
}

// Set inserts or updates the cache entry for key at version ver with value bytes.
// Oversized values are skipped; the cache is byte-capped with FIFO eviction.
func (c *HotLatestCache) Set(key []byte, ver uint64, value []byte) {
	if c == nil || value == nil || len(value) == 0 {
		return
	}
	if int64(len(value)) > c.capBytes/2 || len(value) > hotLatestMaxRecordLen {
		// Skip very large values to preserve cache effectiveness
		return
	}
	k := string(key)
	c.mu.Lock()
	defer c.mu.Unlock()
	// If already present with same or newer version, do nothing.
	if e, ok := c.items[k]; ok && e != nil {
		if e.ver >= ver {
			return
		}
		c.curBytes -= int64(len(e.val))
		delete(c.items, k)
	}
	vv := make([]byte, len(value))
	copy(vv, value)
	// Evict until there is room
	for c.curBytes+int64(len(vv)) > c.capBytes && len(c.order) > 0 {
		evk := c.order[0]
		c.order = c.order[1:]
		if e, ok := c.items[evk]; ok {
			delete(c.items, evk)
			if e != nil {
				c.curBytes -= int64(len(e.val))
			}
		}
	}
	c.items[k] = &hotEntry{ver: ver, val: vv}
	c.order = append(c.order, k)
	c.curBytes += int64(len(vv))
}

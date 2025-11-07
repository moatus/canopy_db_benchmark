package main

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Pointer is a 20B reference into the append-only value log
// Layout: FileID u32 | Offset u64 | Length u32 | CRC32 u32
// All fields are big-endian encoded when serialized.
type Pointer struct {
	FileID uint32
	Offset uint64
	Length uint32
	CRC32  uint32
}

func encodePointer(p Pointer) [20]byte {
	var b [20]byte
	binary.BigEndian.PutUint32(b[0:4], p.FileID)
	binary.BigEndian.PutUint64(b[4:12], p.Offset)
	binary.BigEndian.PutUint32(b[12:16], p.Length)
	binary.BigEndian.PutUint32(b[16:20], p.CRC32)
	return b
}

func decodePointer(b []byte) (Pointer, error) {
	if len(b) < 20 {
		return Pointer{}, fmt.Errorf("pointer too short")
	}
	return Pointer{
		FileID: binary.BigEndian.Uint32(b[0:4]),
		Offset: binary.BigEndian.Uint64(b[4:12]),
		Length: binary.BigEndian.Uint32(b[12:16]),
		CRC32:  binary.BigEndian.Uint32(b[16:20]),
	}, nil
}

// BlobCache is a minimal byte-cap LRU-ish cache keyed by (fid, off, len).
// For simplicity, we use a simple FIFO eviction.
// This is intentionally lightweight for benchmarks.
type BlobCache struct {
	capBytes int64
	mu       sync.Mutex
	curBytes int64
	items    map[string][]byte
	order    []string // FIFO order for eviction
}

func NewBlobCache(mb int) *BlobCache {
	if mb <= 0 {
		return nil
	}
	return &BlobCache{capBytes: int64(mb) << 20, items: make(map[string][]byte)}
}

func (c *BlobCache) makeKey(fid uint32, off uint64, ln uint32) string {
	// Fixed formatting to avoid fmt.Sprint allocations
	var b [64]byte
	n := 0
	n += putU32(b[:], n, fid)
	b[n] = ':'
	n++
	n += putU64(b[:], n, off)
	b[n] = ':'
	n++
	n += putU32(b[:], n, ln)
	return string(b[:n])
}

func putU32(b []byte, n int, v uint32) int { // returns bytes written
	if v == 0 {
		b[n] = '0'
		return 1
	}
	start := n
	var tmp [10]byte
	i := 0
	for v > 0 {
		tmp[i] = byte('0' + v%10)
		v /= 10
		i++
	}
	for j := i - 1; j >= 0; j-- {
		b[n] = tmp[j]
		n++
	}
	return n - start
}
func putU64(b []byte, n int, v uint64) int {
	if v == 0 {
		b[n] = '0'
		return 1
	}
	start := n
	var tmp [20]byte
	i := 0
	for v > 0 {
		tmp[i] = byte('0' + v%10)
		v /= 10
		i++
	}
	for j := i - 1; j >= 0; j-- {
		b[n] = tmp[j]
		n++
	}
	return n - start
}

func (c *BlobCache) Get(fid uint32, off uint64, ln uint32) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	k := c.makeKey(fid, off, ln)
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.items[k]
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (c *BlobCache) Set(fid uint32, off uint64, ln uint32, val []byte) {
	if c == nil || int64(len(val)) > c.capBytes/2 {
		return
	} // skip enormous
	k := c.makeKey(fid, off, ln)
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.items[k]; exists {
		return
	}
	for c.curBytes+int64(len(val)) > c.capBytes && len(c.order) > 0 {
		evk := c.order[0]
		c.order = c.order[1:]
		if v, ok := c.items[evk]; ok {
			delete(c.items, evk)
			c.curBytes -= int64(len(v))
		}
	}
	vv := make([]byte, len(val))
	copy(vv, val)
	c.items[k] = vv
	c.order = append(c.order, k)
	c.curBytes += int64(len(vv))
}

// ValueLog is a sharded append-only blob store shared by LSS and HSS
// Not concurrency-optimized beyond a shard-level mutex which is sufficient for bench.
type ValueLog struct {
	shards []*vlogShard
	cache  *BlobCache
	verify bool // verify CRC on reads (default true; disable via VALUE_LOG_VERIFY=0)
}

const maxCacheRecordLen = 256 << 10 // 256KB; avoid caching huge values by default

type vlogShard struct {
	mu     sync.Mutex
	f      *os.File
	fileID uint32
	offset int64 // tracked current end offset
	dirty  bool  // needs fsync
}

func NewValueLog(dir string, shards int, cache *BlobCache) (*ValueLog, error) {
	if shards <= 0 {
		shards = 8
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	vl := &ValueLog{shards: make([]*vlogShard, shards), cache: cache}
	// CRC verification enabled by default; allow override via VALUE_LOG_VERIFY
	v := strings.ToLower(strings.TrimSpace(os.Getenv("VALUE_LOG_VERIFY")))
	vl.verify = !(v == "0" || v == "false" || v == "no" || v == "off")
	for i := 0; i < shards; i++ {
		p := filepath.Join(dir, fmt.Sprintf("blob.%02d.log", i))
		f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		st, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		vl.shards[i] = &vlogShard{f: f, fileID: uint32(i), offset: st.Size()}
	}
	return vl, nil
}

// shardFor returns shard index for a key hash (already computed) or user key.
func (vl *ValueLog) shardForKeyHash(h uint32) int {
	return int(h % uint32(len(vl.shards)))
}

// Append writes a record and returns a Pointer. Caller must call SyncPending() before committing pointers.
func (vl *ValueLog) Append(userKey []byte, val []byte) (Pointer, error) {
	h := crc32.ChecksumIEEE(userKey)
	s := vl.shards[vl.shardForKeyHash(h)]
	s.mu.Lock()
	defer s.mu.Unlock()

	// Record format: [keyHash u32][valueLen u32][value][crc32 u32]
	keyHash := h
	valueLen := uint32(len(val))
	recLen := int64(4 + 4 + len(val) + 4)
	off := s.offset

	// Build buffers
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header[0:4], keyHash)
	binary.BigEndian.PutUint32(header[4:8], valueLen)
	crc := crc32.ChecksumIEEE(val)
	trailer := make([]byte, 4)
	binary.BigEndian.PutUint32(trailer[0:4], crc)

	// Append
	if _, err := s.f.WriteAt(header, off); err != nil {
		return Pointer{}, err
	}
	if _, err := s.f.WriteAt(val, off+8); err != nil {
		return Pointer{}, err
	}
	if _, err := s.f.WriteAt(trailer, off+8+int64(len(val))); err != nil {
		return Pointer{}, err
	}
	s.offset += recLen
	s.dirty = true

	return Pointer{FileID: s.fileID, Offset: uint64(off), Length: valueLen, CRC32: crc}, nil
}

// SyncPending fsyncs all shards that were written since last call.
func (vl *ValueLog) SyncPending() error {
	for _, s := range vl.shards {
		s.mu.Lock()
		if s.dirty {
			if err := s.f.Sync(); err != nil {
				s.mu.Unlock()
				return err
			}
			s.dirty = false
		}
		s.mu.Unlock()
	}
	return nil
}

// Read returns the value for a pointer. It checks the cache first.
func (vl *ValueLog) Read(p Pointer) ([]byte, error) {
	if vl.cache != nil {
		if b, ok := vl.cache.Get(p.FileID, p.Offset, p.Length); ok {
			return b, nil
		}
	}
	if int(p.FileID) >= len(vl.shards) {
		return nil, fmt.Errorf("bad fid")
	}
	s := vl.shards[p.FileID]
	// Read header
	header := make([]byte, 8)
	if _, err := s.f.ReadAt(header, int64(p.Offset)); err != nil {
		return nil, err
	}
	ln := binary.BigEndian.Uint32(header[4:8])
	if ln != p.Length {
		return nil, fmt.Errorf("length mismatch")
	}
	// Read value + trailer
	buf := make([]byte, ln+4)
	if _, err := s.f.ReadAt(buf, int64(p.Offset)+8); err != nil {
		return nil, err
	}
	val := buf[:ln]
	if vl.verify {
		crc := binary.BigEndian.Uint32(buf[ln:])
		if crc != p.CRC32 || crc32.ChecksumIEEE(val) != p.CRC32 {
			return nil, fmt.Errorf("crc mismatch")
		}
	}
	if vl.cache != nil && p.Length <= maxCacheRecordLen {
		vl.cache.Set(p.FileID, p.Offset, p.Length, val)
	}
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// ReadBatch reads a batch of pointers from the same fileID using minimal merges.
// The returned values are ordered to match the input slice.
func (vl *ValueLog) ReadBatch(fid uint32, ptrs []Pointer) ([][]byte, error) {
	if len(ptrs) == 0 {
		return nil, nil
	}
	if int(fid) >= len(vl.shards) {
		return nil, fmt.Errorf("bad fid")
	}

	// Cache-fast path: if all in cache, return directly
	allCached := true
	cached := make([][]byte, len(ptrs))
	for i, p := range ptrs {
		if vl.cache != nil {
			if b, ok := vl.cache.Get(p.FileID, p.Offset, p.Length); ok {
				cached[i] = b
				continue
			}
		}
		allCached = false
	}
	if allCached {
		return cached, nil
	}

	// Prepare merged spans. Each record size = 8 + len + 4
	type span struct {
		start int64
		end   int64
	}
	type rec struct {
		p       Pointer
		idx     int
		spanIdx int
	}

	recs := make([]rec, len(ptrs))
	for i, p := range ptrs {
		recs[i] = rec{p: p, idx: i}
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].p.Offset < recs[j].p.Offset })

	spans := make([]span, 0, len(recs))
	// Map from rec to its span index
	for i := range recs {
		p := recs[i].p
		recStart := int64(p.Offset)
		recEnd := recStart + 8 + int64(p.Length) + 4
		if len(spans) == 0 {
			spans = append(spans, span{start: recStart, end: recEnd})
			recs[i].spanIdx = 0
			continue
		}
		last := &spans[len(spans)-1]
		if recStart == last.end { // contiguous
			last.end = recEnd
			recs[i].spanIdx = len(spans) - 1
		} else {
			spans = append(spans, span{start: recStart, end: recEnd})
			recs[i].spanIdx = len(spans) - 1
		}
	}

	// Read each span once
	s := vl.shards[fid]
	spanBufs := make([][]byte, len(spans))
	for i, sp := range spans {
		ln := sp.end - sp.start
		if ln <= 0 || ln > 64<<20 { // guard: 64MB per span
			return nil, fmt.Errorf("span too large or invalid")
		}
		buf := make([]byte, ln)
		if _, err := s.f.ReadAt(buf, sp.start); err != nil {
			return nil, err
		}
		spanBufs[i] = buf
	}

	// Extract each record from its span buffer
	out := make([][]byte, len(ptrs))
	for _, r := range recs {
		sp := spans[r.spanIdx]
		buf := spanBufs[r.spanIdx]
		recStart := int64(r.p.Offset) - sp.start
		// header
		if recStart < 0 || recStart+8 > int64(len(buf)) {
			return nil, fmt.Errorf("bad offsets")
		}
		hdr := buf[recStart : recStart+8]
		ln := binary.BigEndian.Uint32(hdr[4:8])
		if ln != r.p.Length {
			return nil, fmt.Errorf("length mismatch")
		}
		// value + crc
		valStart := recStart + 8
		valEnd := valStart + int64(ln)
		crcStart := valEnd
		crcEnd := crcStart + 4
		if crcEnd > int64(len(buf)) {
			return nil, fmt.Errorf("bad bounds")
		}
		val := buf[valStart:valEnd]
		if vl.verify {
			crc := binary.BigEndian.Uint32(buf[crcStart:crcEnd])
			if crc != r.p.CRC32 || crc32.ChecksumIEEE(val) != r.p.CRC32 {
				return nil, fmt.Errorf("crc mismatch")
			}
		}
		vv := make([]byte, len(val))
		copy(vv, val)
		out[r.idx] = vv
		if vl.cache != nil && r.p.Length <= maxCacheRecordLen {
			vl.cache.Set(r.p.FileID, r.p.Offset, r.p.Length, vv)
		}
	}

	return out, nil
}

// Close closes all shard files.
func (vl *ValueLog) Close() error {
	var firstErr error
	for _, s := range vl.shards {
		if s == nil || s.f == nil {
			continue
		}
		if err := s.f.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// --- Encoding helpers reused by stores ---
// Legacy helper names are retained to avoid touching existing call sites.
const (
	latestKindInline  = byte(0)
	latestKindPointer = byte(1)
)

func dualEncodeLatestInline(val []byte) []byte {
	out := make([]byte, 1+len(val))
	out[0] = latestKindInline
	copy(out[1:], val)
	return out
}

func dualEncodeLatestPointer(ptr Pointer) []byte {
	enc := encodePointer(ptr)
	out := make([]byte, 1+len(enc))
	out[0] = latestKindPointer
	copy(out[1:], enc[:])
	return out
}

func dualDecodeLatest(raw []byte, vl *ValueLog) ([]byte, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty value")
	}
	kind := raw[0]
	body := raw[1:]
	switch kind {
	case latestKindInline:
		v := make([]byte, len(body))
		copy(v, body)
		return v, nil
	case latestKindPointer:
		if len(body) < 20 {
			return nil, fmt.Errorf("pointer too short")
		}
		p, err := decodePointer(body[:20])
		if err != nil {
			return nil, err
		}
		if vl == nil {
			return nil, fmt.Errorf("value log not available")
		}
		return vl.Read(p)
	default:
		return nil, fmt.Errorf("unknown latest kind: %d", kind)
	}
}

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// DualTimeline mirrors the production Canopy Pebble integration as closely as possible
// for benchmarking, including Pebble BlockPropertyCollectors/Filters and production-like options.
// Layout:
//   - Latest state (LSS): keys under "s/" with inverted version suffix (use lssVersion)
//   - Historical state (HSS): keys under "h/" with inverted version suffix of real height
// Values are stored as [1-byte tombstone][value].

type dualTimelineStore struct {
	db            *pebble.DB
	batch         *pebble.Batch
	version       uint64
	dataDir       string
	removeOnClose bool

	// Hot latest cache (optional)
	hotLatest  *HotLatestCache
	pendingHot map[string][]byte

	// Historical session (snapshot + cached iterator per height)
	histSession    bool
	histSnap       *pebble.Snapshot
	histIter       *pebble.Iterator
	histIterHeight uint64

	// Last-modified index (in-memory) for no-change-since fast path
	lastModEnabled bool
	lastMod        map[string]uint64

	// Value-log for HSS (optional)
	vlog                *ValueLog
	inlineThresholdHSS  int
	pendingVlogSync     bool
	archivePointersOnly bool

	// Master features toggle
	optFeatures bool
}

// Master feature toggle for DualTimeline (default on)
func parseDualOptFeatures() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DUALTIMELINE_OPT_FEATURES")))
	if v == "0" || v == "false" || v == "no" || v == "off" {
		return false
	}
	return true
}
func parseDualHistSession() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DUALTIMELINE_HIST_SESSION")))
	if v == "0" || v == "false" || v == "no" || v == "off" {
		return false
	}
	return true
}
func parseDualLastModIndex() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DUALTIMELINE_LAST_MOD_INDEX")))
	if v == "0" || v == "false" || v == "no" || v == "off" {
		return false
	}
	return true
}
func parseDualHotLatestMB() int {
	v := strings.TrimSpace(os.Getenv("DUALTIMELINE_HOT_LATEST_MB"))
	if v == "" {
		return 96
	}
	if n, err := strconv.Atoi(v); err == nil && n >= 0 {
		return n
	}
	return 96
}
func parseDualEnableVlog() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DUALTIMELINE_ENABLE_VALUE_LOG")))
	// Default ON when features enabled
	return !(v == "0" || v == "false" || v == "no" || v == "off")
}
func parseDualInlineThresholdHSS() int {
	base := 1536
	if v := strings.TrimSpace(os.Getenv("DUALTIMELINE_INLINE_THRESHOLD_HSS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			base = n
		}
	}
	return base
}
func parseDualBlobCacheMB() int {
	v := strings.TrimSpace(os.Getenv("DUALTIMELINE_BLOB_CACHE_MB"))
	if v == "" {
		return 256
	}
	if n, err := strconv.Atoi(v); err == nil && n >= 0 {
		return n
	}
	return 256
}
func parseDualValueLogShards() int {
	v := strings.TrimSpace(os.Getenv("DUALTIMELINE_VALUE_LOG_SHARDS"))
	if v == "" {
		return 8
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return n
	}
	return 8
}
func parseDualArchivePointersOnly() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DUALTIMELINE_ARCHIVE_POINTERS_ONLY")))
	// Default ON for archival throughput
	return !(v == "0" || v == "false" || v == "no" || v == "off")
}

// Toggle to enable scan-friendly (MonoSpace-like) Pebble settings for archival throughput

func NewDualTimeline() (CanopyStore, error) {
	// Create temp dir for isolation
	dir, err := os.MkdirTemp("", "canopy-dualtimeline-")
	if err != nil {
		return nil, err
	}
	// Determine feature mode first
	optFeatures := parseDualOptFeatures()
	// Cache size (default 256MB, env override)
	cacheMB := 256
	if v := strings.TrimSpace(os.Getenv("PEBBLE_CACHE_MB")); v != "" {
		if n, e := strconv.Atoi(v); e == nil && n > 0 {
			cacheMB = n
		}
	}
	cache := pebble.NewCache(int64(cacheMB) << 20)
	// Build Pebble options based on feature mode
	var opts *pebble.Options
	if !optFeatures {
		// Production parity (match canopy integration defaults)
		lvl := pebble.LevelOptions{
			FilterPolicy:   nil,           // no blooms for scans
			BlockSize:      64 << 10,      // 64KB data blocks
			IndexBlockSize: 32 << 10,      // 32KB index blocks
			Compression: func() *sstable.CompressionProfile {
				return sstable.ZstdCompression
			},
		}
		opts = &pebble.Options{
			DisableWAL:            false,
			MemTableSize:          64 << 20,
			L0CompactionThreshold: 6,
			L0StopWritesThreshold: 12,
			MaxOpenFiles:          5000,
			Cache:                 cache,
			FormatMajorVersion:    pebble.FormatColumnarBlocks,
			LBaseMaxBytes:         512 << 20,
			Levels:                [7]pebble.LevelOptions{lvl, lvl, lvl, lvl, lvl, lvl, lvl},
			BlockPropertyCollectors: []func() pebble.BlockPropertyCollector{
				func() pebble.BlockPropertyCollector { return newVersionedPropertyCollector() },
			},
			WALMinSyncInterval: func() time.Duration { return 2 * time.Millisecond },
		}
	} else {
		// Benchmark/optimized defaults (legacy DT behavior)
		opts = &pebble.Options{
			DisableWAL:            false,
			MemTableSize:          512 << 20,
			L0CompactionThreshold: 20,
			L0StopWritesThreshold: 40,
			MaxOpenFiles:          5000,
			Cache:                 cache,
			FormatMajorVersion:    pebble.FormatColumnarBlocks,
			BlockPropertyCollectors: []func() pebble.BlockPropertyCollector{
				func() pebble.BlockPropertyCollector { return newVersionedPropertyCollector() },
			},
		}
		// Bloom filters on all levels
		for i := range opts.Levels {
			opts.Levels[i].FilterPolicy = bloom.FilterPolicy(12)
		}
	}
	db, err := pebble.Open(dir, opts)
	if err != nil {
		if opts != nil && opts.Cache != nil {
			opts.Cache.Unref()
		}
		_ = os.RemoveAll(dir)
		return nil, err
	}

	s := &dualTimelineStore{
		db:            db,
		batch:         db.NewBatch(),
		version:       1,
		dataDir:       dir,
		removeOnClose: true,
		optFeatures:   optFeatures,
	}
	// Enable feature-dependent components
	s.histSession = s.optFeatures && parseDualHistSession()
	s.lastModEnabled = s.optFeatures && parseDualLastModIndex()
	if s.lastModEnabled {
		s.lastMod = make(map[string]uint64, 1024)
	}
	if mb := parseDualHotLatestMB(); s.optFeatures && mb > 0 {
		s.hotLatest = NewHotLatestCache(mb)
		s.hotLatest.SetReturnCopies(false)
	}
	// Initialize value-log for HSS when features enabled (default ON; allow disable)
	if s.optFeatures && parseDualEnableVlog() {
		blobMB := parseDualBlobCacheMB()
		shards := parseDualValueLogShards()
		var blob *BlobCache
		if blobMB > 0 {
			blob = NewBlobCache(blobMB)
		}
		vlogDir := filepath.Join(dir, "vlog")
		vl, err := NewValueLog(vlogDir, shards, blob)
		if err != nil {
			db.Close()
			_ = os.RemoveAll(dir)
			return nil, err
		}
		s.vlog = vl
		s.inlineThresholdHSS = parseDualInlineThresholdHSS()
		s.archivePointersOnly = parseDualArchivePointersOnly()
	}
	return s, nil
}

func (s *dualTimelineStore) Name() string        { return "DualTimeline" }
func (s *dualTimelineStore) SetVersion(h uint64) { s.version = h }

// --- Key/value encoding (match VersionedStore semantics) ---
const (
	versionSuffixSize      = 8
	aliveTombstone    byte = 0
	deadTombstone     byte = 1
)

var (
	prefixLSS = []byte("s/")
	prefixHSS = []byte("h/")
)

func makeVersionedKey(prefix, userKey []byte, version uint64) []byte {
	// [prefix][Enc(userKey)][0x00,0x00][^version]
	enc := encodeUserKey(userKey)
	k := make([]byte, 0, len(prefix)+len(enc)+len(separator)+versionSuffixSize)
	k = append(k, prefix...)
	k = append(k, enc...)
	k = append(k, separator...)
	var vb [versionSuffixSize]byte
	binary.BigEndian.PutUint64(vb[:], ^version)
	k = append(k, vb[:]...)
	return k
}

func parseVersionedKey(k []byte) (prefix byte, userKey []byte, version uint64, ok bool) {
	// Expect: [p]['/'][Enc(userKey)][0x00,0x00][^version]
	if len(k) < 3+len(separator)+versionSuffixSize { // at least "x/" + sep + version suffix
		return 0, nil, 0, false
	}
	p := k[0]
	if k[1] != '/' {
		return 0, nil, 0, false
	}
	// Locate encoded userKey end (just before separator)
	uEnd := len(k) - (len(separator) + versionSuffixSize)
	if uEnd < 2 {
		return 0, nil, 0, false
	}
	// Validate separator
	sepStart := len(k) - (len(separator) + versionSuffixSize)
	if !bytes.Equal(k[sepStart:sepStart+len(separator)], separator) {
		return 0, nil, 0, false
	}
	encUser := k[2:uEnd]
	decoded, derr := decodeUserKey(encUser)
	if derr != nil {
		return 0, nil, 0, false
	}
	inv := binary.BigEndian.Uint64(k[uEnd+len(separator):])
	return p, decoded, ^inv, true
}

func valueWithTombstone(t byte, v []byte) []byte {
	out := make([]byte, 1+len(v))
	out[0] = t
	copy(out[1:], v)
	return out
}

func parseValueWithTombstone(v []byte) (t byte, val []byte) {
	if len(v) == 0 {
		return deadTombstone, nil
	}
	if len(v) == 1 {
		return v[0], nil
	}
	return v[0], v[1:]
}

// --- Encoding helpers (parity with production integration) ---
var (
	separator = []byte{0x00, 0x00}
)

func encodeUserKey(u []byte) []byte {
	// escape 0x00 as 0x00 0xFF and append 0x00 0x00 terminator
	out := make([]byte, 0, len(u)*2+2)
	for _, b := range u {
		if b == 0x00 {
			out = append(out, 0x00, 0xFF)
		} else {
			out = append(out, b)
		}
	}
	return out
}

func encodeUserPrefix(p []byte) []byte {
	// same escaping as key, but no terminator
	out := make([]byte, 0, len(p)*2)
	for _, b := range p {
		if b == 0x00 {
			out = append(out, 0x00, 0xFF)
		} else {
			out = append(out, b)
		}
	}
	return out
}

func decodeUserKey(enc []byte) ([]byte, error) {
	out := make([]byte, 0, len(enc))
	for i := 0; i < len(enc); i++ {
		b := enc[i]
		if b != 0x00 {
			out = append(out, b)
			continue
		}
		// must be 0x00 0xFF
		i++
		if i >= len(enc) {
			return nil, fmt.Errorf("unterminated escape")
		}
		if enc[i] != 0xFF {
			return nil, fmt.Errorf("invalid escape sequence: 0x00 0x%02x", enc[i])
		}
		out = append(out, 0x00)
	}
	return out, nil
}

// --- CanopyStore implementation ---

func (s *dualTimelineStore) Set(key, value []byte) error {
	// Track last-modified height and stage hot-latest publish
	if s.lastModEnabled {
		s.lastMod[string(key)] = s.version
	}
	if s.hotLatest != nil {
		if s.pendingHot == nil {
			s.pendingHot = make(map[string][]byte)
		}
		vv := make([]byte, len(value))
		copy(vv, value)
		s.pendingHot[string(key)] = vv
	}
	// Write to LSS at lssVersion (inline) and HSS at concrete height (pointer-aware)
	if err := s.batch.Set(makeVersionedKey(prefixLSS, key, ^uint64(0)), valueWithTombstone(aliveTombstone, value), nil); err != nil {
		return err
	}
	var hssPayload []byte
	if s.vlog != nil && len(value) > s.inlineThresholdHSS {
		ptr, err := s.vlog.Append(key, value)
		if err != nil {
			return err
		}
		s.pendingVlogSync = true
		hssPayload = dualEncodeLatestPointer(ptr)
	} else if s.optFeatures {
		hssPayload = dualEncodeLatestInline(value)
	} else {
		hssPayload = value
	}
	if err := s.batch.Set(makeVersionedKey(prefixHSS, key, s.version), valueWithTombstone(aliveTombstone, hssPayload), nil); err != nil {
		return err
	}
	return nil
}

func (s *dualTimelineStore) Get(key []byte) ([]byte, error) {
	// Hot-latest cache fast path
	if s.hotLatest != nil {
		if v, ok := s.hotLatest.Get(key); ok {
			return v, nil
		}
	}
// Seek latest in s/
	seek := makeVersionedKey(prefixLSS, key, 0) // ^0 = max, SeekLT to get latest
	lb := append([]byte{}, append(prefixLSS, encodeUserPrefix(key)... )...)
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lb,
		UpperBound: prefixEnd(lb),
		KeyTypes:   pebble.IterKeyTypePointsOnly,
	})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	if !it.SeekLT(seek) {
		return nil, fmt.Errorf("key not found")
	}
	p, u, _, ok := parseVersionedKey(it.Key())
	if !ok || p != 's' || !bytes.Equal(u, key) {
		return nil, fmt.Errorf("key not found")
	}
	t, val := parseValueWithTombstone(it.Value())
	if t == deadTombstone {
		return nil, fmt.Errorf("key not found")
	}
	// LSS stores raw inline values; do not pointer-decode here
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

func (s *dualTimelineStore) Iterator(prefix []byte) (CanopyIterator, error) {
lb := append([]byte{}, append(prefixLSS, encodeUserPrefix(prefix)... )...)
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: lb,
		UpperBound: prefixEnd(lb),
		KeyTypes:   pebble.IterKeyTypePointsOnly,
	})
	if err != nil {
		return nil, err
	}
	it.First()
	return &actualLatestIter{it: it, stripLen: 2, userPrefix: prefix}, nil
}

func (s *dualTimelineStore) HistoricalGet(height uint64, key []byte) ([]byte, error) {
	// If key hasn't changed since 'height', latest is valid
	if s.lastModEnabled {
		if mod, ok := s.lastMod[string(key)]; ok && height >= mod {
			return s.Get(key)
		}
	}
	// If hot cache has a value written at or before height, use it (when opt features on)
	if s.optFeatures && s.hotLatest != nil {
		if ver, v, ok := s.hotLatest.GetWithVersion(key); ok && ver <= height {
			out := make([]byte, len(v))
			copy(out, v)
			return out, nil
		}
	}
seek := makeVersionedKey(prefixHSS, key, height+1) // find <= height
	lb := append([]byte{}, append(prefixHSS, encodeUserPrefix(key)... )...)
	ub := prefixEnd(lb)
	// Iterator: reuse per-height with snapshot if enabled
	var it *pebble.Iterator
	var err error
	if s.histSession {
		if s.histIter == nil || s.histIterHeight != height {
			if s.histIter != nil {
				s.histIter.Close()
				s.histIter = nil
			}
			if s.histSnap != nil {
				s.histSnap.Close()
				s.histSnap = nil
			}
			s.histSnap = s.db.NewSnapshot()
			it, err = s.histSnap.NewIter(&pebble.IterOptions{
				LowerBound:      lb,
				UpperBound:      ub,
				KeyTypes:        pebble.IterKeyTypePointsOnly,
				PointKeyFilters: []pebble.BlockPropertyFilter{newTargetWindowFilter(0, height)},
			})
			if err != nil {
				return nil, err
			}
			s.histIter = it
			s.histIterHeight = height
		} else {
			it = s.histIter
		}
	} else {
		it, err = s.db.NewIter(&pebble.IterOptions{
			LowerBound:      lb,
			UpperBound:      ub,
			KeyTypes:        pebble.IterKeyTypePointsOnly,
			PointKeyFilters: []pebble.BlockPropertyFilter{newTargetWindowFilter(0, height)},
		})
		if err != nil {
			return nil, err
		}
		defer it.Close()
	}
	if !it.SeekLT(seek) {
		if !s.histSession {
			return nil, fmt.Errorf("key not found")
		}
		return nil, fmt.Errorf("key not found")
	}
	p, u, ver, ok := parseVersionedKey(it.Key())
	if !ok || p != 'h' || !bytes.Equal(u, key) || ver > height {
		if !s.histSession {
			return nil, fmt.Errorf("key not found")
		}
		return nil, fmt.Errorf("key not found")
	}
	t, val := parseValueWithTombstone(it.Value())
	if t == deadTombstone {
		if !s.histSession {
			return nil, fmt.Errorf("key not found")
		}
		return nil, fmt.Errorf("key not found")
	}
	if s.optFeatures {
		decoded, derr := dualDecodeLatest(val, s.vlog)
		if derr != nil {
			if !s.histSession {
				return nil, fmt.Errorf("failed to decode historical value: %w", derr)
			}
			return nil, fmt.Errorf("failed to decode historical value: %w", derr)
		}
		out := make([]byte, len(decoded))
		copy(out, decoded)
		return out, nil
	}
	// Legacy mode: values are inline
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

func (s *dualTimelineStore) HistoricalIterator(height uint64, prefix []byte) (CanopyIterator, error) {
	// Iterator with block property filter to prune by version window
	lb := append([]byte{}, append(prefixHSS, encodeUserPrefix(prefix)... )...)
	it, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound:      lb,
		UpperBound:      prefixEnd(lb),
		KeyTypes:        pebble.IterKeyTypePointsOnly,
		PointKeyFilters: []pebble.BlockPropertyFilter{newTargetWindowFilter(0, height)},
	})
	if err != nil {
		return nil, err
	}
	it.First()
	return &actualHistIter{it: it, height: height, stripLen: 2, userPrefix: prefix}, nil
}

func (s *dualTimelineStore) ArchiveIterator(prefix []byte) (CanopyIterator, error) {
	// Iterate all historical versions under h/
lb := append([]byte{}, append(prefixHSS, encodeUserPrefix(prefix)... )...)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lb, UpperBound: prefixEnd(lb), KeyTypes: pebble.IterKeyTypePointsOnly})
	if err != nil {
		return nil, err
	}
	it.First()
	return &actualArchiveIter{it: it, stripLen: 2, zeroCopy: s.optFeatures && dualParseArchiveFast()}, nil
}

func (s *dualTimelineStore) Commit() error {
	// Sync vlog first if needed
	if s.pendingVlogSync && s.vlog != nil {
		if err := s.vlog.SyncPending(); err != nil {
			return err
		}
		s.pendingVlogSync = false
	}
	// Mirror production commit path: apply via DB to avoid batch-internal panics
	if err := s.db.Apply(s.batch, pebble.NoSync); err != nil {
		return err
	}
	// Publish staged hot-latest entries
	if s.hotLatest != nil && len(s.pendingHot) > 0 {
		for k, v := range s.pendingHot {
			s.hotLatest.Set([]byte(k), s.version, v)
			delete(s.pendingHot, k)
		}
	}
	s.batch = s.db.NewBatch()
	return nil
}

func (s *dualTimelineStore) Close() error {
	_ = s.batch.Close()
	if s.histIter != nil {
		s.histIter.Close()
		s.histIter = nil
	}
	if s.histSnap != nil {
		s.histSnap.Close()
		s.histSnap = nil
	}
	if s.vlog != nil {
		_ = s.vlog.Close()
	}
	err := s.db.Close()
	if s.removeOnClose && s.dataDir != "" {
		_ = os.RemoveAll(s.dataDir)
	}
	return err
}

// --- Iterators ---

type actualLatestIter struct {
	it         *pebble.Iterator
	stripLen   int
	userPrefix []byte
	key        []byte
	val        []byte
	valid      bool
	lastUser   []byte
	inited     bool
}

func (ai *actualLatestIter) Valid() bool {
	if !ai.inited {
		ai.advance()
		ai.inited = true
	}
	return ai.valid
}
func (ai *actualLatestIter) Next() {
	if !ai.inited {
		ai.advance()
		ai.inited = true
		return
	}
	ai.advance()
}
func (ai *actualLatestIter) Key() []byte   { return ai.key }
func (ai *actualLatestIter) Value() []byte { return ai.val }
func (ai *actualLatestIter) Close() error  { ai.it.Close(); return nil }

func (ai *actualLatestIter) advance() {
	ai.valid, ai.key, ai.val = false, nil, nil
	for ai.it.Valid() {
		k := ai.it.Key()
		if len(k) < ai.stripLen+versionSuffixSize {
			ai.it.Next()
			continue
		}
		p, u, _, ok := parseVersionedKey(k)
		if !ok || p != 's' {
			ai.it.Next()
			continue
		}
		if len(ai.userPrefix) > 0 && !bytes.HasPrefix(u, ai.userPrefix) {
			ai.it.Next()
			continue
		}
		if bytes.Equal(ai.lastUser, u) {
			ai.it.Next()
			continue
		}
		// First (highest) version for this userKey is current position
		t, v := parseValueWithTombstone(ai.it.Value())
		ai.lastUser = bytes.Clone(u)
// skip to next user key range: s/Enc(u) + 0x00,0x01
	base := make([]byte, 0, 2+len(u)*2+2)
	base = append(base, 's', '/')
	base = append(base, encodeUserKey(u)...)
	next := append(base, 0x00, 0x01)
	ai.it.SeekGE(next)
		if t == deadTombstone {
			continue
		}
		ai.key = bytes.Clone(u)
		ai.val = append([]byte{}, v...)
		ai.valid = true
		return
	}
}

type actualHistIter struct {
	it         *pebble.Iterator
	height     uint64
	stripLen   int
	userPrefix []byte
	key        []byte
	val        []byte
	valid      bool
	inited     bool
	lastUser   []byte
}

func (hi *actualHistIter) Valid() bool {
	if !hi.inited {
		hi.advance()
		hi.inited = true
	}
	return hi.valid
}
func (hi *actualHistIter) Next() {
	if !hi.inited {
		hi.advance()
		hi.inited = true
		return
	}
	hi.advance()
}
func (hi *actualHistIter) Key() []byte   { return hi.key }
func (hi *actualHistIter) Value() []byte { return hi.val }
func (hi *actualHistIter) Close() error  { hi.it.Close(); return nil }

func (hi *actualHistIter) advance() {
	hi.valid, hi.key, hi.val = false, nil, nil
	for hi.it.Valid() {
		k := hi.it.Key()
		if len(k) < hi.stripLen+versionSuffixSize {
			hi.it.Next()
			continue
		}
		p, u, _ /*ver*/, ok := parseVersionedKey(k)
		if !ok || p != 'h' {
			hi.it.Next()
			continue
		}
		if len(hi.userPrefix) > 0 && !bytes.HasPrefix(u, hi.userPrefix) {
			hi.it.Next()
			continue
		}
		if bytes.Equal(hi.lastUser, u) {
			hi.it.Next()
			continue
		}
		// Seek to <= height for this user key
		seek := makeVersionedKey(prefixHSS, u, hi.height+1)
		if hi.it.SeekLT(seek) {
			kk := hi.it.Key()
			pp, uu, ver, ok := parseVersionedKey(kk)
			if ok && pp == 'h' && bytes.Equal(uu, u) && ver <= hi.height {
				t, v := parseValueWithTombstone(hi.it.Value())
				hi.lastUser = bytes.Clone(u)
// move to next userKey: h/Enc(u) + 0x00,0x01
	base := make([]byte, 0, 2+len(u)*2+2)
	base = append(base, 'h', '/')
	base = append(base, encodeUserKey(u)...)
	next := append(base, 0x00, 0x01)
	hi.it.SeekGE(next)
				if t == deadTombstone {
					continue
				}
				hi.key = bytes.Clone(u)
				hi.val = append([]byte{}, v...)
				hi.valid = true
				return
			}
		}
// move to next userKey range if no valid version: h/Enc(u) + 0x00,0x01
	base := make([]byte, 0, 2+len(u)*2+2)
	base = append(base, 'h', '/')
	base = append(base, encodeUserKey(u)...)
	next := append(base, 0x00, 0x01)
	hi.it.SeekGE(next)
	}
}

// dualParseArchiveFast toggles zero-copy returns for archive paths (default ON)
func dualParseArchiveFast() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("ARCHIVE_FAST")))
	// Default ON unless explicitly disabled
	return !(v == "0" || v == "false" || v == "no" || v == "off")
}

type actualArchiveIter struct {
	it       *pebble.Iterator
	stripLen int
	zeroCopy bool
}

func (ai *actualArchiveIter) Valid() bool  { return ai.it.Valid() }
func (ai *actualArchiveIter) Next()        { ai.it.Next() }
func (ai *actualArchiveIter) Close() error { ai.it.Close(); return nil }

func (ai *actualArchiveIter) Key() []byte {
	k := ai.it.Key()
	if len(k) > ai.stripLen {
		out := make([]byte, len(k)-ai.stripLen)
		copy(out, k[ai.stripLen:])
		return out
	}
	out := make([]byte, len(k))
	copy(out, k)
	return out
}

func (ai *actualArchiveIter) Value() []byte {
	if ai.zeroCopy {
		return ai.it.Value()
	}
	v := ai.it.Value()
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

// ---- Block property collector/filter (production parity) ----

const blockPropertyName = "canopy.mvcc.version.range"

type versionedCollector struct{}

// MapPointKey records [version, version+1) per point key using inverted version suffix
func (versionedCollector) MapPointKey(key pebble.InternalKey, _ []byte) (sstable.BlockInterval, error) {
	userKey := key.UserKey
	if len(userKey) < 3+versionSuffixSize { // require at least "x/" + suffix
		return sstable.BlockInterval{}, nil
	}
	_, _, version, ok := parseVersionedKey(userKey)
	if !ok || version == math.MaxUint64 {
		return sstable.BlockInterval{}, nil
	}
	return sstable.BlockInterval{Lower: version, Upper: version + 1}, nil
}

// MapRangeKeys unused
func (versionedCollector) MapRangeKeys(_ sstable.Span) (sstable.BlockInterval, error) {
	return sstable.BlockInterval{}, nil
}

func newVersionedPropertyCollector() pebble.BlockPropertyCollector {
	return sstable.NewBlockIntervalCollector(blockPropertyName, versionedCollector{}, nil)
}

func newTargetWindowFilter(low, high uint64) sstable.BlockPropertyFilter {
	return sstable.NewBlockIntervalFilter(blockPropertyName, low, high+1, nil)
}

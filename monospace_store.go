package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

// internal iterator used by SeekLTPebbleStore
type Iterator interface {
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Close()
}

// MonoSpaceStore implements CanopyStore using SeekLTPebbleStore
type MonoSpaceStore struct {
	store   *SeekLTPebbleStore
	tempDir string
}

// NewMonoSpaceStore creates a new MonoSpace-based store
func NewMonoSpaceStore() (CanopyStore, error) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "canopy-monospace-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Create the underlying store with version 1
	store, err := NewSeekLTPebbleStore(tempDir, 1)
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to create monospace store: %w", err)
	}

	return &MonoSpaceStore{
		store:   store,
		tempDir: tempDir,
	}, nil
}

func (sls *MonoSpaceStore) Name() string {
	return "MonoSpace"
}

func (sls *MonoSpaceStore) SetVersion(height uint64) {
	sls.store.version = height
}

func (sls *MonoSpaceStore) Set(key, value []byte) error {
	return sls.store.Set(key, value)
}

func (sls *MonoSpaceStore) Get(key []byte) ([]byte, error) {
	return sls.store.Get(key)
}

func (sls *MonoSpaceStore) Iterator(prefix []byte) (CanopyIterator, error) {
	iter, err := sls.store.Iterator(prefix)
	if err != nil {
		return nil, err
	}
	return &SeekLTIteratorWrapper{iter: iter}, nil
}

func (sls *MonoSpaceStore) HistoricalGet(height uint64, key []byte) ([]byte, error) {
	return sls.store.HistoricalGet(height, key)
}

func (sls *MonoSpaceStore) HistoricalIterator(height uint64, prefix []byte) (CanopyIterator, error) {
	iter, err := sls.store.HistoricalIterator(height, prefix)
	if err != nil {
		return nil, err
	}
	return &SeekLTIteratorWrapper{iter: iter}, nil
}

func (sls *MonoSpaceStore) ArchiveIterator(prefix []byte) (CanopyIterator, error) {
	iter, err := sls.store.ArchiveIterator(prefix)
	if err != nil {
		return nil, err
	}
	return &SeekLTIteratorWrapper{iter: iter}, nil
}

func (sls *MonoSpaceStore) Commit() error {
	return sls.store.Commit()
}

func (sls *MonoSpaceStore) Close() error {
	err := sls.store.Close()
	if sls.tempDir != "" {
		os.RemoveAll(sls.tempDir)
	}
	return err
}

// SeekLTIteratorWrapper wraps the existing iterator to implement CanopyIterator
type SeekLTIteratorWrapper struct {
	iter Iterator
}

func (siw *SeekLTIteratorWrapper) Valid() bool {
	return siw.iter.Valid()
}

func (siw *SeekLTIteratorWrapper) Next() {
	siw.iter.Next()
}

func (siw *SeekLTIteratorWrapper) Key() []byte {
	return siw.iter.Key()
}

func (siw *SeekLTIteratorWrapper) Value() []byte {
	return siw.iter.Value()
}

func (siw *SeekLTIteratorWrapper) Close() error {
	siw.iter.Close()
	return nil
}

// SeekLTPebbleStore implements the SeekLT (Issue-196 style) versioned store approach
type SeekLTPebbleStore struct {
	db            *pebble.DB
	batch         *pebble.Batch
	version       uint64
	keyBuffer     []byte
	dataDir       string
	removeOnClose bool

	// Hot latest cache (optional)
	hotLatest  *HotLatestCache
	pendingHot map[string][]byte

	// Value-log integration (enabled for production parity)
	inlineThreshold int
	vlog            *ValueLog
	pendingVlogSync bool

	// Historical session (snapshot + cached iterator per height)
	histSession    bool
	histSnap       *pebble.Snapshot
	histIter       *pebble.Iterator
	histIterHeight uint64

	// Last-modified index (in-memory) for no-change-since fast path
	lastModEnabled bool
	lastMod        map[string]uint64

	// Master features toggle
	optFeatures bool

	// Canopy encoding toggle (env-controlled, not tied to opt-features)
	canopyEncoding bool
}

// parseSeekHotLatestMB parses MONOSPACE_HOT_LATEST_MB; default 96MB
func parseSeekHotLatestMB() int {
	v := strings.TrimSpace(os.Getenv("MONOSPACE_HOT_LATEST_MB"))
	if v == "" {
		return 96
	}
	if n, err := strconv.Atoi(v); err == nil && n >= 0 {
		return n
	}
	return 96
}

// parseSeekInlineThreshold parses SEEKLT_INLINE_THRESHOLD; default 2048
func parseSeekInlineThreshold() int {
	base := 2048
	if v := strings.TrimSpace(os.Getenv("MONOSPACE_INLINE_THRESHOLD")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			base = n
		}
	}
	return base
}

// parseSeekBlobCacheMB parses SEEKLT_BLOB_CACHE_MB; default 256MB
func parseSeekBlobCacheMB() int {
	v := strings.TrimSpace(os.Getenv("MONOSPACE_BLOB_CACHE_MB"))
	if v == "" {
		return 256
	}
	if n, err := strconv.Atoi(v); err == nil && n >= 0 {
		return n
	}
	return 256
}

// parseSeekValueLogShards parses SEEKLT_VALUE_LOG_SHARDS; default 8
func parseSeekValueLogShards() int {
	v := strings.TrimSpace(os.Getenv("MONOSPACE_VALUE_LOG_SHARDS"))
	if v == "" {
		return 8
	}
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return n
	}
	return 8
}

// Master feature toggle (default on)
func parseMonospaceOptFeatures() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("MONOSPACE_OPT_FEATURES")))
	if v == "0" || v == "false" || v == "no" || v == "off" {
		return false
	}
	return true
}

// Canopy encoding toggle (default off until fully adopted)
func parseMonospaceCanopyEncoding() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("MONOSPACE_CANOPY_ENCODING")))
	// Default ON; allow explicit disable
	return !(v == "0" || v == "false" || v == "no" || v == "off")
}

// Feature toggles (default on)
func parseMonospaceHistSession() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("MONOSPACE_HIST_SESSION")))
	if v == "0" || v == "false" || v == "no" || v == "off" {
		return false
	}
	return true
}
func parseMonospaceLastModIndex() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("MONOSPACE_LAST_MOD_INDEX")))
	if v == "0" || v == "false" || v == "no" || v == "off" {
		return false
	}
	return true
}

// Small scratch buffer pool for ephemeral seek keys to reduce allocs.
var seekBufPool = sync.Pool{New: func() any { b := make([]byte, 0, 256); return &b }}

func borrowSeekBuf(minCap int) *[]byte {
	p := seekBufPool.Get().(*[]byte)
	if cap(*p) < minCap {
		b := make([]byte, 0, minCap)
		*p = b
	}
	(*p) = (*p)[:0]
	return p
}
func returnSeekBuf(p *[]byte) { seekBufPool.Put(p) }

// makeSeekKey builds [userKey][^version] into a pooled buffer; caller must call release when done.
func (vs *SeekLTPebbleStore) makeSeekKey(userKey []byte, version uint64) ([]byte, func()) {
var need int
if vs.canopyEncoding {
	// worst case doubles zeros + 2 separator bytes + version
	need = len(userKey)*2 + 2 + SeekLTVersionSize
} else {
	need = len(userKey) + SeekLTVersionSize
}
p := borrowSeekBuf(need)
b := (*p)[:0]
if vs.canopyEncoding {
	for _, by := range userKey {
		if by == 0x00 {
			b = append(b, 0x00, 0xFF)
		} else {
			b = append(b, by)
		}
	}
	b = append(b, 0x00, 0x00)
} else {
	b = append(b, userKey...)
}
var vb [SeekLTVersionSize]byte
binary.BigEndian.PutUint64(vb[:], ^version)
b = append(b, vb[:]...)
*p = b
release := func() { *p = (*p)[:0]; returnSeekBuf(p) }
return b, release
}

// makeNextUserKeyBuf builds [userKey][0xFF...8] into a pooled buffer for SeekGE to jump to next user key.
// nextUserKeyBoundBuf returns a seek target just past all versions of userKey
func (vs *SeekLTPebbleStore) nextUserKeyBoundBuf(userKey []byte) ([]byte, func()) {
	if vs.canopyEncoding {
		// Build [Enc(userKey)][0x00 0x01] to skip [Enc(userKey)][0x00 0x00][^version]
		need := len(userKey)*2 + 2
		p := borrowSeekBuf(need)
		b := (*p)[:0]
		for _, by := range userKey {
			if by == 0x00 {
				b = append(b, 0x00, 0xFF)
			} else {
				b = append(b, by)
			}
		}
		b = append(b, 0x00, 0x01)
		*p = b
		release := func() { *p = (*p)[:0]; returnSeekBuf(p) }
		return b, release
	}
	// Legacy: [userKey][0xFF..x8]
	need := len(userKey) + SeekLTVersionSize
	p := borrowSeekBuf(need)
	b := (*p)[:0]
	b = append(b, userKey...)
	for i := 0; i < SeekLTVersionSize; i++ {
		b = append(b, 0xFF)
	}
	*p = b
	release := func() { *p = (*p)[:0]; returnSeekBuf(p) }
	return b, release
}

// NewSeekLTPebbleStore creates the SeekLT Pebble versioned store implementation
func NewSeekLTPebbleStore(path string, version uint64) (*SeekLTPebbleStore, error) {
	createdTemp := false
	if strings.TrimSpace(path) == "" {
		dir, err := os.MkdirTemp("", "bench_monospace_*")
		if err != nil {
			return nil, err
		}
		path = dir
		createdTemp = true
	}

	// Create production-like Pebble options (env-tunable cache)
	cacheMB := 512 // production default to match canopy deployment
	if v := strings.TrimSpace(os.Getenv("PEBBLE_CACHE_MB")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cacheMB = n
		}
	}
	opts := createProductionPebbleOptions(cacheMB)
	// Enable Canopy encoding collector when requested (takes precedence over opt-features)
	useCanopy := parseMonospaceCanopyEncoding()
	if useCanopy {
		opts.BlockPropertyCollectors = append(opts.BlockPropertyCollectors, func() pebble.BlockPropertyCollector { return newCanopyMVCCCollector() })
	} else {
		// Choose block-interval collector only when opt features are enabled; otherwise coarse table-level collector
		if parseMonospaceOptFeatures() {
			opts.BlockPropertyCollectors = append(opts.BlockPropertyCollectors, func() pebble.BlockPropertyCollector { return newMonoBlockCollector() })
		} else {
			opts.BlockPropertyCollectors = append(opts.BlockPropertyCollectors, func() pebble.BlockPropertyCollector { return newInvTSCollector(nil) })
		}
	}

	// MVCC version property collectors disabled for simplicity

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}

	vs := &SeekLTPebbleStore{
		db:            db,
		batch:         db.NewIndexedBatch(),
		version:       version,
		keyBuffer:     make([]byte, 0, 256),
		dataDir:       path,
		removeOnClose: createdTemp,
	}
	// Master features toggle first
	vs.optFeatures = parseMonospaceOptFeatures()
	// Canopy encoding mode
	vs.canopyEncoding = useCanopy

	// Initialize HotLatestCache only when features are enabled
	if vs.optFeatures {
		if mb := parseSeekHotLatestMB(); mb > 0 {
			vs.hotLatest = NewHotLatestCache(mb)
		}
	}

	// Initialize ValueLog when features are enabled (default ON; allow explicit disable)
	if vs.optFeatures {
		vs.inlineThreshold = parseSeekInlineThreshold()
		blobCacheMB := parseSeekBlobCacheMB()
		valueLogShards := parseSeekValueLogShards()
		en := strings.ToLower(strings.TrimSpace(os.Getenv("MONOSPACE_ENABLE_VALUE_LOG")))
		enableVlog := !(en == "0" || en == "false" || en == "no" || en == "off")
		if enableVlog {
			var blobCache *BlobCache
			if blobCacheMB > 0 {
				blobCache = NewBlobCache(blobCacheMB)
			}
			vlogDir := filepath.Join(path, "vlog")
			vl, err := NewValueLog(vlogDir, valueLogShards, blobCache)
			if err != nil {
				_ = db.Close()
				if createdTemp {
					_ = os.RemoveAll(path)
				}
				return nil, fmt.Errorf("failed to create value log: %w", err)
			}
			vs.vlog = vl
		}
	}

	// Enable feature toggles (default on)
	vs.histSession = vs.optFeatures && parseMonospaceHistSession()
	vs.lastModEnabled = vs.optFeatures && parseMonospaceLastModIndex()
	if vs.lastModEnabled {
		vs.lastMod = make(map[string]uint64, 1024)
	}

	return vs, nil
}

// Set stores a key-value pair at the current version (SeekLT approach)
func (vs *SeekLTPebbleStore) Set(key, value []byte) error {
	// Track last-modified version
	if vs.lastModEnabled {
		vs.lastMod[string(key)] = vs.version
	}
	// Stage hot-latest cache publication (post-commit) if enabled
	if vs.hotLatest != nil {
		if vs.pendingHot == nil {
			vs.pendingHot = make(map[string][]byte)
		}
		vv := make([]byte, len(value))
		copy(vv, value)
		vs.pendingHot[string(key)] = vv
	}
	// Encode value, using value-log pointer if above threshold
	var encoded []byte
	if vs.vlog != nil {
		if len(value) > vs.inlineThreshold {
			ptr, err := vs.vlog.Append(key, value)
			if err != nil {
				return fmt.Errorf("failed to append to value log: %w", err)
			}
			vs.pendingVlogSync = true
			encoded = dualEncodeLatestPointer(ptr)
		} else {
			encoded = dualEncodeLatestInline(value)
		}
	} else {
		encoded = value
	}
	k := vs.makeVersionedKey(key, vs.version)
	v := vs.valueWithTombstone(SeekLTAliveTombstone, encoded)
	return vs.batch.Set(k, v, nil)
}

// Delete marks a key as deleted at the current version (SeekLT approach)
func (vs *SeekLTPebbleStore) Delete(key []byte) error {
	k := vs.makeVersionedKey(key, vs.version)
	v := vs.valueWithTombstone(SeekLTDeadTombstone, nil)
	return vs.batch.Set(k, v, nil)
}

// Get retrieves the latest version of a key using the SeekLT approach
func (vs *SeekLTPebbleStore) Get(key []byte) ([]byte, error) {
	// First try hot cache for zero-copy read
	if vs.hotLatest != nil {
		if v, ok := vs.hotLatest.Get(key); ok {
			return v, nil
		}
	}
	value, _, err := vs.get(key)
	return value, err
}

func (vs *SeekLTPebbleStore) get(key []byte) ([]byte, uint64, error) {
// Create versioned key for seeking (use max version to find latest)
	vkey, rel := vs.makeSeekKey(key, 0) // version 0 becomes ^0 = max uint64
	defer rel()

	// Bounds over encoded user-key prefix when canopy encoding is enabled
	var lower, upper []byte
	if vs.canopyEncoding {
encPref := encodeUserPrefixMono(key)
		lower, upper = encPref, prefixEnd(encPref)
	} else {
		lower, upper = key, prefixEnd(key)
	}
	it, err := vs.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
		KeyTypes:   pebble.IterKeyTypePointsOnly,
	})
	if err != nil {
		return nil, 0, err
	}
	defer it.Close()

	// SeekLT to find the latest version <= current version
	if !it.SeekLT(vkey) {
		return nil, 0, fmt.Errorf("key not found")
	}

	// Verify this is the right user key
	userKey, version, err := vs.parseVersionedKey(it.Key())
	if err != nil {
		return nil, 0, err
	}
	if !bytes.Equal(userKey, key) {
		return nil, 0, fmt.Errorf("key not found")
	}

	// Parse value and check tombstone
	tombstone, raw := vs.parseValueWithTombstone(it.Value())
	if tombstone == SeekLTDeadTombstone {
		return nil, 0, fmt.Errorf("key not found")
	}
	// If value-log is disabled, raw is already the actual value (legacy inline); return it directly
	if vs.vlog == nil {
		out := make([]byte, len(raw))
		copy(out, raw)
		return out, version, nil
	}
	// Otherwise decode pointer/inline format
	decoded, derr := dualDecodeLatest(raw, vs.vlog)
	if derr != nil {
		return nil, 0, fmt.Errorf("failed to decode latest value: %w", derr)
	}
	out := make([]byte, len(decoded))
	copy(out, decoded)
	return out, version, nil
}

func (vs *SeekLTPebbleStore) Iterator(prefix []byte) (Iterator, error) {
	// Latest iteration using SeekLT approach
	var lower, upper []byte
	if vs.canopyEncoding {
enc := encodeUserPrefixMono(prefix)
		lower, upper = enc, prefixEnd(enc)
	} else {
		lower, upper = prefix, prefixEnd(prefix)
	}
	it, err := vs.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
		KeyTypes:   pebble.IterKeyTypePointsOnly,
	})
	if err != nil {
		return nil, err
	}
	it.First()

	return &SeekLTIterator{
		iter:        it,
		store:       vs,
		prefix:      prefix,
		initialized: false,
	}, nil
}

// seekltHistoricalIterOptions builds iter options depending on encoding mode
func (vs *SeekLTPebbleStore) seekltHistoricalIterOptions(prefix []byte, height uint64) *pebble.IterOptions {
opts := &pebble.IterOptions{
		KeyTypes: pebble.IterKeyTypePointsOnly,
	}
	// Bounds: encode prefix for canopy mode
	if vs.canopyEncoding {
enc := encodeUserPrefixMono(prefix)
		opts.LowerBound = enc
		opts.UpperBound = prefixEnd(enc)
	} else {
		opts.LowerBound = prefix
		opts.UpperBound = prefixEnd(prefix)
	}
	// Pruning by version window
	if vs.canopyEncoding {
		opts.PointKeyFilters = append(opts.PointKeyFilters, newCanopyVersionFilter(0, height))
	} else if parseMonospaceOptFeatures() {
		opts.PointKeyFilters = append(opts.PointKeyFilters, newMonoBlockFilter(0, height))
	} else {
		opts.PointKeyFilters = append(opts.PointKeyFilters, newInvTSFilter(height))
	}
	return opts
}

func (vs *SeekLTPebbleStore) HistoricalIterator(height uint64, prefix []byte) (Iterator, error) {
	// Historical iteration at specific height using SeekLT
it, err := vs.db.NewIter(vs.seekltHistoricalIterOptions(prefix, height))
	if err != nil {
		return nil, err
	}
	it.First()

	return &SeekLTHistoricalIterator{
		iter:        it,
		store:       vs,
		prefix:      prefix,
		height:      height,
		initialized: false,
	}, nil
}

func (vs *SeekLTPebbleStore) HistoricalGet(height uint64, key []byte) ([]byte, error) {
	// Fast-path: if requesting at/after current version, serve from latest
	if height >= vs.version {
		return vs.Get(key)
	}
	// If key hasn't changed since 'height', latest is valid
	if vs.lastModEnabled {
		if mod, ok := vs.lastMod[string(key)]; ok && height >= mod {
			return vs.Get(key)
		}
	}
	// If hot cache has a value written at or before height, use it (bypass iter) when features are on
	if vs.optFeatures && vs.hotLatest != nil {
		if ver, v, ok := vs.hotLatest.GetWithVersion(key); ok && ver <= height {
			out := make([]byte, len(v))
			copy(out, v)
			return out, nil
		}
	}
	return vs.historicalGetWithCachedIter(height, key)
}

// historicalGetWithCachedIter reuses a per-height iterator across calls to reduce setup overhead
func (vs *SeekLTPebbleStore) historicalGetWithCachedIter(height uint64, key []byte) ([]byte, error) {
	// Ensure iterator for this height
	if vs.histIter == nil || vs.histIterHeight != height {
		// Close previous
		if vs.histIter != nil {
			vs.histIter.Close()
			vs.histIter = nil
		}
		if vs.histSnap != nil {
			vs.histSnap.Close()
			vs.histSnap = nil
		}
		// Build iterator from snapshot if enabled
		var it *pebble.Iterator
		var err error
		if vs.histSession {
			vs.histSnap = vs.db.NewSnapshot()
			// For direct historical Get, we only need version-window pruning; no bounds.
			opts := &pebble.IterOptions{KeyTypes: pebble.IterKeyTypePointsOnly}
			if vs.canopyEncoding {
				opts.PointKeyFilters = []pebble.BlockPropertyFilter{newCanopyVersionFilter(0, height)}
			} else if parseMonospaceOptFeatures() {
				opts.PointKeyFilters = []pebble.BlockPropertyFilter{newMonoBlockFilter(0, height)}
			} else {
				opts.PointKeyFilters = []pebble.BlockPropertyFilter{newInvTSFilter(height)}
			}
			it, err = vs.histSnap.NewIter(opts)
		} else {
			// For direct historical Get, we only need version-window pruning; no bounds.
			opts := &pebble.IterOptions{KeyTypes: pebble.IterKeyTypePointsOnly}
			if vs.canopyEncoding {
				opts.PointKeyFilters = []pebble.BlockPropertyFilter{newCanopyVersionFilter(0, height)}
			} else if parseMonospaceOptFeatures() {
				opts.PointKeyFilters = []pebble.BlockPropertyFilter{newMonoBlockFilter(0, height)}
			} else {
				opts.PointKeyFilters = []pebble.BlockPropertyFilter{newInvTSFilter(height)}
			}
			it, err = vs.db.NewIter(opts)
		}
		if err != nil {
			return nil, err
		}
		vs.histIter = it
		vs.histIterHeight = height
	}
	it := vs.histIter
	// Seek to version just above target to land on <= height
	seekKey, rel := vs.makeSeekKey(key, height+1)
	defer rel()
	if !it.SeekLT(seekKey) {
		return nil, fmt.Errorf("key not found")
	}
	userKey, ver, err := vs.parseVersionedKey(it.Key())
	if err != nil || !bytes.Equal(userKey, key) || ver > height {
		return nil, fmt.Errorf("key not found")
	}
	tombstone, raw := vs.parseValueWithTombstone(it.Value())
	if tombstone == SeekLTDeadTombstone {
		return nil, fmt.Errorf("key not found")
	}
	if vs.vlog == nil {
		out := make([]byte, len(raw))
		copy(out, raw)
		return out, nil
	}
	decoded, derr := dualDecodeLatest(raw, vs.vlog)
	if derr != nil {
		return nil, fmt.Errorf("failed to decode historical value: %w", derr)
	}
	out := make([]byte, len(decoded))
	copy(out, decoded)
	return out, nil
}

func (vs *SeekLTPebbleStore) ArchiveIterator(prefix []byte) (Iterator, error) {
	// Archive iteration (all versions)
	it, err := vs.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixEnd(prefix),
		KeyTypes:   pebble.IterKeyTypePointsOnly,
	})
	if err != nil {
		return nil, err
	}
	it.First()

	return &SeekLTArchiveIterator{it: it, zeroCopy: vs.optFeatures && parseArchiveFast()}, nil
}

func (vs *SeekLTPebbleStore) Commit() error {
	// 1) Sync value log first if we appended any blobs this batch
	if vs.pendingVlogSync && vs.vlog != nil {
		if err := vs.vlog.SyncPending(); err != nil {
			return fmt.Errorf("failed to sync value log: %w", err)
		}
		vs.pendingVlogSync = false
	}
	// 2) Commit Pebble batch
	syncOpt := getSyncOption()
	if err := vs.batch.Commit(&syncOpt); err != nil {
		return err
	}
	// 3) Publish staged hot-latest entries after successful commit
	if vs.hotLatest != nil && len(vs.pendingHot) > 0 {
		for k, v := range vs.pendingHot {
			vs.hotLatest.Set([]byte(k), vs.version, v)
			delete(vs.pendingHot, k)
		}
	}
	// 4) New batch for next ops
	vs.batch = vs.db.NewIndexedBatch()
	return nil
}

func (vs *SeekLTPebbleStore) Close() error {
	vs.batch.Close()
	if vs.histIter != nil {
		vs.histIter.Close()
		vs.histIter = nil
	}
	if vs.histSnap != nil {
		vs.histSnap.Close()
		vs.histSnap = nil
	}
	if vs.vlog != nil {
		_ = vs.vlog.Close()
	}
	err := vs.db.Close()
	if vs.removeOnClose && vs.dataDir != "" {
		os.RemoveAll(vs.dataDir)
	}
	return err
}

// Helper methods for SeekLT key/value encoding

func (vs *SeekLTPebbleStore) makeVersionedKey(userKey []byte, version uint64) []byte {
	vs.keyBuffer = vs.keyBuffer[:0]
	if vs.canopyEncoding {
		// Encode user key with 0x00-escaping and separator, then append ^version
		for _, b := range userKey {
			if b == 0x00 {
				vs.keyBuffer = append(vs.keyBuffer, 0x00, 0xFF)
			} else {
				vs.keyBuffer = append(vs.keyBuffer, b)
			}
		}
		vs.keyBuffer = append(vs.keyBuffer, 0x00, 0x00)
	} else {
		vs.keyBuffer = append(vs.keyBuffer, userKey...)
	}
	var vb [SeekLTVersionSize]byte
	binary.BigEndian.PutUint64(vb[:], ^version)
	vs.keyBuffer = append(vs.keyBuffer, vb[:]...)
	// Return a copy since keyBuffer is reused
	result := make([]byte, len(vs.keyBuffer))
	copy(result, vs.keyBuffer)
	return result
}

func (vs *SeekLTPebbleStore) parseVersionedKey(versionedKey []byte) (userKey []byte, version uint64, err error) {
	if len(versionedKey) < SeekLTVersionSize {
		return nil, 0, fmt.Errorf("key too short")
	}
	if vs.canopyEncoding {
		// Expect [Enc(userKey)][0x00 0x00][^version]
		min := 2 + SeekLTVersionSize
		if len(versionedKey) < min {
			return nil, 0, fmt.Errorf("key too short")
		}
		encEnd := len(versionedKey) - (2 + SeekLTVersionSize)
		enc := versionedKey[:encEnd]
		if !(versionedKey[encEnd] == 0x00 && versionedKey[encEnd+1] == 0x00) {
			return nil, 0, fmt.Errorf("missing separator")
		}
		version = ^binary.BigEndian.Uint64(versionedKey[encEnd+2:])
		// decode user key (reverse 0x00 0xFF -> 0x00)
		buf := make([]byte, 0, len(enc))
		for i := 0; i < len(enc); i++ {
			b := enc[i]
			if b != 0x00 {
				buf = append(buf, b)
				continue
			}
			i++
			if i >= len(enc) || enc[i] != 0xFF {
				return nil, 0, fmt.Errorf("invalid escape")
			}
			buf = append(buf, 0x00)
		}
		return buf, version, nil
	}
	// Legacy layout: [userKey][^version]
	userKeyEnd := len(versionedKey) - SeekLTVersionSize
	userKey = versionedKey[:userKeyEnd]
	version = ^binary.BigEndian.Uint64(versionedKey[userKeyEnd:])
	return
}

func (vs *SeekLTPebbleStore) valueWithTombstone(tombstone byte, value []byte) []byte {
	v := make([]byte, 1+len(value))
	v[0] = tombstone
	if len(value) > 0 {
		copy(v[1:], value)
	}
	return v
}

func (vs *SeekLTPebbleStore) parseValueWithTombstone(v []byte) (tombstone byte, value []byte) {
	if len(v) == 0 {
		return SeekLTDeadTombstone, nil
	}
	if len(v) > 1 {
		value = v[1:]
	}
	return v[0], value
}

// Canopy encoding helpers for bounds
func encodeUserPrefixMono(p []byte) []byte {
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

// SeekLTIterator implements SeekLT iteration with single-pass key deduplication
type SeekLTIterator struct {
	iter        *pebble.Iterator
	store       *SeekLTPebbleStore
	prefix      []byte
	key         []byte
	value       []byte
	isValid     bool
	initialized bool
	lastUserKey []byte
}

func (si *SeekLTIterator) Valid() bool {
	if !si.initialized {
		si.advance()
		si.initialized = true
	}
	return si.isValid
}

func (si *SeekLTIterator) Next() {
	if !si.initialized {
		si.advance()
		si.initialized = true
		return
	}
	si.advance()
}

func (si *SeekLTIterator) Key() []byte   { return si.key }
func (si *SeekLTIterator) Value() []byte { return si.value }
func (si *SeekLTIterator) Close()        { si.iter.Close() }

func (si *SeekLTIterator) advance() {
	si.isValid = false
	si.key = nil
	si.value = nil

	for si.iter.Valid() {
		k := si.iter.Key()
		if len(k) < SeekLTVersionSize {
			si.iter.Next()
			continue
		}

		// Extract user key and version
		userKey, _, err := si.store.parseVersionedKey(k)
		if err != nil {
			si.iter.Next()
			continue
		}

		// Skip if prefix doesn't match
		if len(si.prefix) > 0 && !bytes.HasPrefix(userKey, si.prefix) {
			si.iter.Next()
			continue
		}

		// Skip if we've already processed this user key
		if bytes.Equal(userKey, si.lastUserKey) {
			si.iter.Next()
			continue
		}

		// Parse value and check tombstone
		val := si.iter.Value()
		tombstone, raw := si.store.parseValueWithTombstone(val)

		// Skip to next user key
		si.lastUserKey = bytes.Clone(userKey)
nextKey, rel := si.store.nextUserKeyBoundBuf(userKey)
		si.iter.SeekGE(nextKey)
		rel()

		// Only return if not a tombstone
		if tombstone != SeekLTDeadTombstone {
			decoded, err := dualDecodeLatest(raw, si.store.vlog)
			if err == nil {
				si.key = bytes.Clone(userKey)
				si.value = append([]byte{}, decoded...)
				si.isValid = true
				return
			}
		}
	}
}

// SeekLTHistoricalIterator implements historical iteration at a specific height
type SeekLTHistoricalIterator struct {
	iter        *pebble.Iterator
	store       *SeekLTPebbleStore
	prefix      []byte
	height      uint64
	key         []byte
	value       []byte
	isValid     bool
	initialized bool
	lastUserKey []byte
}

func (hi *SeekLTHistoricalIterator) Valid() bool {
	if !hi.initialized {
		hi.advance()
		hi.initialized = true
	}
	return hi.isValid
}

func (hi *SeekLTHistoricalIterator) Next() {
	if !hi.initialized {
		hi.advance()
		hi.initialized = true
		return
	}
	hi.advance()
}

func (hi *SeekLTHistoricalIterator) Key() []byte   { return hi.key }
func (hi *SeekLTHistoricalIterator) Value() []byte { return hi.value }
func (hi *SeekLTHistoricalIterator) Close()        { hi.iter.Close() }

func (hi *SeekLTHistoricalIterator) advance() {
	hi.isValid = false
	hi.key = nil
	hi.value = nil

	for hi.iter.Valid() {
		k := hi.iter.Key()
		if len(k) < SeekLTVersionSize {
			hi.iter.Next()
			continue
		}

		// Extract user key and version
		userKey, version, err := hi.store.parseVersionedKey(k)
		if err != nil {
			hi.iter.Next()
			continue
		}

		// Skip if prefix doesn't match
		if len(hi.prefix) > 0 && !bytes.HasPrefix(userKey, hi.prefix) {
			hi.iter.Next()
			continue
		}

		// Skip if we've already processed this user key
		if bytes.Equal(userKey, hi.lastUserKey) {
			hi.iter.Next()
			continue
		}

		// Check if this version is suitable (version <= height)
		if version <= hi.height {
			// Parse value and check tombstone
			val := hi.iter.Value()
			tombstone, raw := hi.store.parseValueWithTombstone(val)

			// Skip to next user key
			hi.lastUserKey = bytes.Clone(userKey)
nextKey, rel := hi.store.nextUserKeyBoundBuf(userKey)
			hi.iter.SeekGE(nextKey)
			rel()

			// Only return if not a tombstone
			if tombstone != SeekLTDeadTombstone {
				decoded, err := dualDecodeLatest(raw, hi.store.vlog)
				if err == nil {
					hi.key = bytes.Clone(userKey)
					hi.value = append([]byte{}, decoded...)
					hi.isValid = true
					return
				}
			}
			continue
		}

		// Version too new, skip to next user key
		hi.lastUserKey = bytes.Clone(userKey)
		nextKey := make([]byte, 0, len(userKey)+SeekLTVersionSize)
		nextKey = append(nextKey, userKey...)
		for i := 0; i < SeekLTVersionSize; i++ {
			nextKey = append(nextKey, 0xFF)
		}
		hi.iter.SeekGE(nextKey)
	}
}

// SeekLTArchiveIterator implements archive iteration (all versions)
// parseArchiveFast toggles zero-copy returns for archive paths (default off)
func parseArchiveFast() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("ARCHIVE_FAST")))
	// Default ON unless explicitly disabled
	return !(v == "0" || v == "false" || v == "no" || v == "off")
}

// SeekLTArchiveIterator implements archive iteration (all versions)
type SeekLTArchiveIterator struct {
	it       *pebble.Iterator
	zeroCopy bool
}

func (ai *SeekLTArchiveIterator) Valid() bool { return ai.it.Valid() }
func (ai *SeekLTArchiveIterator) Next()       { ai.it.Next() }

func (ai *SeekLTArchiveIterator) Key() []byte {
	// Return the full versioned key for archive purposes
	key := ai.it.Key()
	k := make([]byte, len(key))
	copy(k, key)
	return k
}

func (ai *SeekLTArchiveIterator) Value() []byte {
	// Return the raw value (including tombstone)
	if ai.zeroCopy {
		return ai.it.Value()
	}
	v := ai.it.Value()
	out := make([]byte, len(v))
	copy(out, v)
	return out
}

func (ai *SeekLTArchiveIterator) Close() { ai.it.Close() }

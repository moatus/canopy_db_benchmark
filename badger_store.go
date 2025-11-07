package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"runtime"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
)

// hasPrefix checks if key has the given prefix
func hasPrefix(key, prefix []byte) bool {
	return bytes.HasPrefix(key, prefix)
}

// BadgerStore implements CanopyStore using Badger database
type BadgerStore struct {
	db          *badger.DB
	version     uint64
	tempDir     string
	changedKeys map[string]bool   // Track keys changed in current version
	pending     map[string][]byte // Staged writes for current version
}

// NewBadgerStore creates a new Badger-based store
func NewBadgerStore() (CanopyStore, error) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "canopy-badger-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Configure Badger to mirror production Canopy (managed mode and options)
	opts := badger.DefaultOptions(tempDir).
		WithNumVersionsToKeep(math.MaxInt64).
		WithLoggingLevel(badger.ERROR).
		WithValueThreshold(1024).
		WithCompression(options.None).
		WithNumMemtables(16).WithMemTableSize(256 << 20).
		WithNumLevelZeroTables(10).WithNumLevelZeroTablesStall(20).
		WithBaseTableSize(128 << 20).WithBaseLevelSize(512 << 20).
		WithNumCompactors(runtime.NumCPU()).
		WithCompactL0OnClose(true).
		WithBypassLockGuard(true).
		WithDetectConflicts(false).
		WithSyncWrites(true)

	db, err := badger.OpenManaged(opts)
	if err != nil {
		os.RemoveAll(tempDir)
		return nil, fmt.Errorf("failed to open badger (managed): %w", err)
	}

	return &BadgerStore{
		db:          db,
		version:     1,
		tempDir:     tempDir,
		changedKeys: make(map[string]bool),
		pending:     make(map[string][]byte),
	}, nil
}

func (bs *BadgerStore) Name() string {
	return "Badger"
}

func (bs *BadgerStore) SetVersion(height uint64) {
	bs.version = height
	// Clear staged state for new version
	bs.changedKeys = make(map[string]bool)
	bs.pending = make(map[string][]byte)
}

func (bs *BadgerStore) Set(key, value []byte) error {
	// Track that this key changed in current version
	bs.changedKeys[string(key)] = true

	// Stage the value; copy to avoid retaining caller's buffer
	v := make([]byte, len(value))
	copy(v, value)
	bs.pending[string(key)] = v
	return nil
}

func (bs *BadgerStore) Get(key []byte) ([]byte, error) {
	var result []byte
	err := bs.db.View(func(txn *badger.Txn) error {
		lssKey := append([]byte("s/"), key...)
		item, err := txn.Get(lssKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			result = make([]byte, len(val))
			copy(result, val)
			return nil
		})
	})
	return result, err
}

func (bs *BadgerStore) Iterator(prefix []byte) (CanopyIterator, error) {
	txn := bs.db.NewTransactionAt(math.MaxUint64, false)
	var pfx []byte
	if len(prefix) > 0 {
		pfx = append([]byte("s/"), prefix...)
	} else {
		pfx = []byte("s/")
	}
	opts := badger.IteratorOptions{Prefix: pfx, PrefetchValues: false}
	iter := txn.NewIterator(opts)
	iter.Rewind()
	return &BadgerIterator{iter: iter, txn: txn, prefix: pfx}, nil
}

func (bs *BadgerStore) HistoricalGet(height uint64, key []byte) ([]byte, error) {
	txn := bs.db.NewTransactionAt(height, false)
	defer txn.Discard()
	hssKey := append([]byte("h/"), key...)
	item, err := txn.Get(hssKey)
	if err != nil {
		return nil, err
	}
	var out []byte
	if err := item.Value(func(val []byte) error {
		out = make([]byte, len(val))
		copy(out, val)
		return nil
	}); err != nil {
		return nil, err
	}
	return out, nil
}

func (bs *BadgerStore) HistoricalIterator(height uint64, prefix []byte) (CanopyIterator, error) {
	txn := bs.db.NewTransactionAt(height, false)
	var pfx []byte
	if len(prefix) > 0 {
		pfx = append([]byte("h/"), prefix...)
	} else {
		pfx = []byte("h/")
	}
	opts := badger.IteratorOptions{Prefix: pfx, PrefetchValues: false}
	iter := txn.NewIterator(opts)
	iter.Rewind()
	return &BadgerIterator{iter: iter, txn: txn, prefix: pfx}, nil
}

func (bs *BadgerStore) ArchiveIterator(prefix []byte) (CanopyIterator, error) {
	txn := bs.db.NewTransactionAt(math.MaxUint64, false)
	var pfx []byte
	if len(prefix) > 0 {
		pfx = append([]byte("h/"), prefix...)
	} else {
		pfx = []byte("h/")
	}
	opts := badger.IteratorOptions{Prefix: pfx, PrefetchValues: false, AllVersions: true}
	iter := txn.NewIterator(opts)
	iter.Rewind()
	return &BadgerIterator{iter: iter, txn: txn, prefix: pfx}, nil
}

func (bs *BadgerStore) Commit() error {
	if len(bs.pending) == 0 {
		return nil
	}
	wb := bs.db.NewWriteBatchAt(bs.version)
	for keyStr, val := range bs.pending {
		lssKey := append([]byte("s/"), []byte(keyStr)...)
		if err := wb.SetEntryAt(&badger.Entry{Key: lssKey, Value: val}, math.MaxUint64); err != nil {
			wb.Cancel()
			return err
		}
		hssKey := append([]byte("h/"), []byte(keyStr)...)
		if err := wb.SetEntryAt(&badger.Entry{Key: hssKey, Value: val}, bs.version); err != nil {
			wb.Cancel()
			return err
		}
	}
	if err := wb.Flush(); err != nil {
		wb.Cancel()
		return err
	}
	bs.pending = make(map[string][]byte)
	bs.changedKeys = make(map[string]bool)
	return nil
}

func (bs *BadgerStore) Close() error {
	err := bs.db.Close()
	if bs.tempDir != "" {
		os.RemoveAll(bs.tempDir)
	}
	return err
}

// BadgerIterator implements CanopyIterator for Badger
type BadgerIterator struct {
	iter   *badger.Iterator
	txn    *badger.Txn
	prefix []byte
}

func (bi *BadgerIterator) Valid() bool {
	return bi.iter.Valid() && hasPrefix(bi.iter.Item().Key(), bi.prefix)
}

func (bi *BadgerIterator) Next() {
	bi.iter.Next()
}

func (bi *BadgerIterator) Key() []byte {
	if !bi.Valid() {
		return nil
	}
	key := bi.iter.Item().Key()
	// Remove prefix to return clean key
	if len(key) > len(bi.prefix) {
		return key[len(bi.prefix):]
	}
	return key
}

func (bi *BadgerIterator) Value() []byte {
	if !bi.Valid() {
		return nil
	}
	var result []byte
	bi.iter.Item().Value(func(val []byte) error {
		result = make([]byte, len(val))
		copy(result, val)
		return nil
	})
	return result
}

func (bi *BadgerIterator) Close() error {
	bi.iter.Close()
	bi.txn.Discard()
	return nil
}

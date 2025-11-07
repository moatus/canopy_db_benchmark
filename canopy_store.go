package main

import (
	"io"
)

// CanopyStore defines the interface that all storage implementations must satisfy
// for Canopy blockchain state storage benchmarking.
type CanopyStore interface {
	// Set stores a key-value pair at the current version
	Set(key, value []byte) error

	// Get retrieves the latest value for a key
	Get(key []byte) ([]byte, error)

	// Iterator returns an iterator over the latest state with optional prefix filtering
	Iterator(prefix []byte) (CanopyIterator, error)

	// HistoricalGet returns the value of a key as of a specific block height (point lookup)
	HistoricalGet(height uint64, key []byte) ([]byte, error)

	// HistoricalIterator returns an iterator over state at a specific block height (scan)
	// Note: kept for completeness; current benchmarks prefer HistoricalGet per key.
	HistoricalIterator(height uint64, prefix []byte) (CanopyIterator, error)

	// ArchiveIterator returns an iterator over all versions of all keys (for exports)
	ArchiveIterator(prefix []byte) (CanopyIterator, error)

	// Commit finalizes all pending writes for the current version
	Commit() error

	// Close releases all resources
	Close() error

	// SetVersion sets the current block height for subsequent operations
	SetVersion(height uint64)

	// Name returns a human-readable name for this store implementation
	Name() string
}

// CanopyIterator defines the interface for iterating over key-value pairs
type CanopyIterator interface {
	// Valid returns true if the iterator is positioned at a valid key-value pair
	Valid() bool

	// Next advances the iterator to the next key-value pair
	Next()

	// Key returns the current key (caller should not modify)
	Key() []byte

	// Value returns the current value (caller should not modify)
	Value() []byte

	// Close releases iterator resources
	io.Closer
}

// StoreFactory is a function that creates a new store instance
type StoreFactory func() (CanopyStore, error)

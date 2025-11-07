package main

import (
	"fmt"
	"testing"
)

// BenchmarkCanopyStorage is the main benchmark function for testing all storage implementations.
// This runs ALL implementations through ALL test types and gives you ONE final summary.
//
// Usage:
//
//	go test -bench=BenchmarkCanopyStorage -benchtime=1x
//	CANOPY_KEYS=10000 CANOPY_VERSIONS=100 go test -bench=BenchmarkCanopyStorage -benchtime=1x
//
// Environment variables:
//
//	CANOPY_KEYS      - Number of keys to test (default: 10000)
//	CANOPY_VERSIONS  - Number of versions to create (default: 50)
//	CANOPY_RUNS      - Number of timing runs for statistics (default: 5)
//	CANOPY_VALUE_SIZE - Size of each value in bytes (default: 1024)
//	CANOPY_UPDATE_RATIO - Fraction of keys updated per version (default: 0.05)
func BenchmarkCanopyStorage(b *testing.B) {
	config := DefaultTestConfig()

	// Show what configuration is being used
	fmt.Printf("Configuration: Keys=%d, Versions=%d, Runs=%d, ValueSize=%d, UpdateRatio=%.3f\n",
		config.NumKeys, config.NumVersions, config.NumRuns, config.ValueSize, config.UpdateRatio)

	suite := NewTestSuite(config)

// Focused implementations: Badger, MonoSpace, DualTimeline
	implementations := map[string]StoreFactory{
		"Badger":       func() (CanopyStore, error) { return NewBadgerStore() },
		"MonoSpace":    func() (CanopyStore, error) { return NewMonoSpaceStore() },
		"DualTimeline": func() (CanopyStore, error) { return NewDualTimeline() },
	}

	suite.RunAllTests(b, implementations)
	suite.PrintResults("Canopy Storage Implementation Comparison")
}

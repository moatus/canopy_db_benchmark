package main

import (
	"encoding/binary"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// Block-interval properties for MonoSpace (SeekLT layout)
// - Legacy (no terminator) layout: [userKey][^version]
// - Canopy layout: [Enc(userKey)][0x00 0x00][^version]
// Both map to [version, version+1) using the last 8 bytes (^version).

const (
	monoBlockPropertyName   = "monospace.invts.interval"
	canopyBlockPropertyName = "canopy.mvcc.version.range"
)

type monoInvTSIntervalMapper struct{}

var _ sstable.IntervalMapper = monoInvTSIntervalMapper{}

func (monoInvTSIntervalMapper) MapPointKey(key pebble.InternalKey, _ []byte) (sstable.BlockInterval, error) {
	uk := key.UserKey
	if len(uk) < 8 {
		return sstable.BlockInterval{}, nil
	}
	inv := binary.BigEndian.Uint64(uk[len(uk)-8:])
	ver := ^inv
	return sstable.BlockInterval{Lower: ver, Upper: ver + 1}, nil
}

func (monoInvTSIntervalMapper) MapRangeKeys(_ sstable.Span) (sstable.BlockInterval, error) {
	return sstable.BlockInterval{}, nil
}

type monoNoSuffixReplacer struct{}

var _ sstable.BlockIntervalSuffixReplacer = monoNoSuffixReplacer{}

func (monoNoSuffixReplacer) ApplySuffixReplacement(i sstable.BlockInterval, _ []byte) (sstable.BlockInterval, error) {
	return i, nil
}

func newMonoBlockCollector() pebble.BlockPropertyCollector {
	return sstable.NewBlockIntervalCollector(monoBlockPropertyName, monoInvTSIntervalMapper{}, monoNoSuffixReplacer{})
}

func newMonoBlockFilter(low, high uint64) pebble.BlockPropertyFilter {
	// Filter expects [low, high) so add 1 to make the upper bound inclusive on heights
	return sstable.NewBlockIntervalFilter(monoBlockPropertyName, low, high+1, monoNoSuffixReplacer{})
}

// Canopy-style version range collector/filter (matches production naming)
func newCanopyMVCCCollector() pebble.BlockPropertyCollector {
	return sstable.NewBlockIntervalCollector(canopyBlockPropertyName, monoInvTSIntervalMapper{}, nil)
}

func newCanopyVersionFilter(low, high uint64) pebble.BlockPropertyFilter {
	return sstable.NewBlockIntervalFilter(canopyBlockPropertyName, low, high+1, nil)
}

package main

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable"
)

const (
	invTSPropertyName = "canopy.invts"
	invTSSuffixLen    = 8
)

// invTSCollector records the minimum and maximum inverted timestamps observed
// while building an SST. It only inspects point keys and optionally restricts
// itself to a specific prefix.
type invTSCollector struct {
	prefix      []byte
	initialized bool
	minInv      uint64
	maxInv      uint64
}

func newInvTSCollector(prefix []byte) pebble.BlockPropertyCollector {
	// Block collectors are invoked per-table so storing the prefix is cheap.
	if len(prefix) > 0 {
		cp := make([]byte, len(prefix))
		copy(cp, prefix)
		prefix = cp
	}
	return &invTSCollector{prefix: prefix}
}

func (c *invTSCollector) Name() string { return invTSPropertyName }

func (c *invTSCollector) AddPointKey(key sstable.InternalKey, _ []byte) error {
	uk := key.UserKey
	if len(uk) < invTSSuffixLen {
		return nil
	}
	if len(c.prefix) > 0 && !bytes.HasPrefix(uk, c.prefix) {
		return nil
	}
	inv := binary.BigEndian.Uint64(uk[len(uk)-invTSSuffixLen:])
	if !c.initialized {
		c.initialized = true
		c.minInv = inv
		c.maxInv = inv
		return nil
	}
	if inv < c.minInv {
		c.minInv = inv
	}
	if inv > c.maxInv {
		c.maxInv = inv
	}
	return nil
}

func (c *invTSCollector) AddRangeKeys(_ sstable.Span) error { return nil }

func (c *invTSCollector) AddCollectedWithSuffixReplacement(_ []byte, _, _ []byte) error {
	return errors.New("invTSCollector does not support suffix replacement")
}

func (c *invTSCollector) SupportsSuffixReplacement() bool { return false }

func (c *invTSCollector) FinishDataBlock(buf []byte) ([]byte, error)  { return buf[:0], nil }
func (c *invTSCollector) AddPrevDataBlockToIndexBlock()               {}
func (c *invTSCollector) FinishIndexBlock(buf []byte) ([]byte, error) { return buf[:0], nil }

func (c *invTSCollector) FinishTable(buf []byte) ([]byte, error) {
	if !c.initialized {
		return nil, nil
	}
	if cap(buf) < 2*invTSSuffixLen {
		buf = make([]byte, 2*invTSSuffixLen)
	} else {
		buf = buf[:2*invTSSuffixLen]
	}
	binary.BigEndian.PutUint64(buf[:invTSSuffixLen], c.minInv)
	binary.BigEndian.PutUint64(buf[invTSSuffixLen:], c.maxInv)
	return buf, nil
}

// invTSFilter skips tables whose inverted timestamp range cannot satisfy the
// requested height.
type invTSFilter struct {
	targetInv uint64
}

func newInvTSFilter(height uint64) pebble.BlockPropertyFilter {
	return &invTSFilter{targetInv: ^height}
}

func (f *invTSFilter) Name() string { return invTSPropertyName }

func (f *invTSFilter) Intersects(prop []byte) (bool, error) {
	if len(prop) < 2*invTSSuffixLen {
		return true, nil
	}
	maxInv := binary.BigEndian.Uint64(prop[invTSSuffixLen:])
	return maxInv >= f.targetInv, nil
}

func (f *invTSFilter) SyntheticSuffixIntersects(prop, _ []byte) (bool, error) {
	return f.Intersects(prop)
}

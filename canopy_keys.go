package main

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
)

// CanopyKeyDistribution defines the realistic distribution of key families in Canopy
type CanopyKeyDistribution struct {
	AccountRatio   float64 // account_ keys (hot path, frequent reads/writes)
	ValidatorRatio float64 // validator_ keys (frequent reads, occasional writes)
	CommitteeRatio float64 // committee_ keys (periodic reads)
	HashRatio      float64 // hash_ and other keys (mostly archival)
}

// DefaultCanopyDistribution returns the realistic key distribution for production Canopy
func DefaultCanopyDistribution() CanopyKeyDistribution {
	return CanopyKeyDistribution{
		AccountRatio:   0.60, // 60% account keys (most active)
		ValidatorRatio: 0.20, // 20% validator keys
		CommitteeRatio: 0.10, // 10% committee keys
		HashRatio:      0.10, // 10% hash/other keys
	}
}

// GenerateCanopyKeys creates a deterministic set of keys matching Canopy's realistic patterns
func GenerateCanopyKeys(count int, seed int64) [][]byte {
	return GenerateCanopyKeysWithDistribution(count, seed, DefaultCanopyDistribution())
}

// GenerateCanopyKeysWithDistribution creates keys with a custom distribution
func GenerateCanopyKeysWithDistribution(count int, seed int64, dist CanopyKeyDistribution) [][]byte {
	// Use deterministic random source for reproducible key generation
	rng := rand.New(rand.NewSource(seed))

	keys := make([][]byte, 0, count)

	// Calculate counts for each key family
	accountCount := int(float64(count) * dist.AccountRatio)
	validatorCount := int(float64(count) * dist.ValidatorRatio)
	committeeCount := int(float64(count) * dist.CommitteeRatio)
	hashCount := count - accountCount - validatorCount - committeeCount

	// Generate account_ keys (most frequent, realistic addresses)
	for i := 0; i < accountCount; i++ {
		addr := generateRealisticAddress(rng)
		key := fmt.Sprintf("account_%s", addr)
		keys = append(keys, []byte(key))
	}

	// Generate validator_ keys (validator public keys and metadata)
	for i := 0; i < validatorCount; i++ {
		valID := generateValidatorID(rng)
		key := fmt.Sprintf("validator_%s", valID)
		keys = append(keys, []byte(key))
	}

	// Generate committee_ keys (committee membership and voting records)
	for i := 0; i < committeeCount; i++ {
		height := rng.Uint64() % 1000000 // realistic block heights
		member := rng.Uint32() % 100     // committee member index
		key := fmt.Sprintf("committee_%d_%d", height, member)
		keys = append(keys, []byte(key))
	}

	// Generate hash_ and other keys (transaction hashes, merkle roots, etc.)
	for i := 0; i < hashCount; i++ {
		hash := generateRealisticHash(rng)
		keyType := []string{"hash_", "merkle_", "tx_", "block_"}[rng.Intn(4)]
		key := fmt.Sprintf("%s%s", keyType, hash)
		keys = append(keys, []byte(key))
	}

	// Shuffle to avoid any ordering bias in benchmarks
	rng.Shuffle(len(keys), func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})

	// Sort for deterministic iteration order (important for consistent benchmarks)
	sort.Slice(keys, func(i, j int) bool {
		return string(keys[i]) < string(keys[j])
	})

	return keys
}

// generateRealisticAddress creates addresses that look like real blockchain addresses
func generateRealisticAddress(rng *rand.Rand) string {
	// Generate 20-byte address (like Ethereum)
	addr := make([]byte, 20)
	rng.Read(addr)
	return fmt.Sprintf("%x", addr)
}

// generateValidatorID creates realistic validator identifiers
func generateValidatorID(rng *rand.Rand) string {
	// Generate 32-byte public key hash
	pubkey := make([]byte, 32)
	rng.Read(pubkey)
	return fmt.Sprintf("%x", pubkey[:16]) // Use first 16 bytes for shorter keys
}

// generateRealisticHash creates realistic hash values
func generateRealisticHash(rng *rand.Rand) string {
	// Generate 32-byte hash
	hash := make([]byte, 32)
	rng.Read(hash)
	return fmt.Sprintf("%x", hash[:16]) // Use first 16 bytes for shorter keys
}

// GetHotKeys returns the subset of keys that should be "hot" (frequently accessed)
// This matches the account_ and validator_ prefixes which are most active in production
func GetHotKeys(keys [][]byte) [][]byte {
	var hotKeys [][]byte
	for _, key := range keys {
		keyStr := string(key)
		if len(keyStr) >= 8 && (keyStr[:8] == "account_" || keyStr[:10] == "validator_") {
			hotKeys = append(hotKeys, key)
		}
	}
	return hotKeys
}

// GetKeysByPrefix returns all keys matching a given prefix
func GetKeysByPrefix(keys [][]byte, prefix string) [][]byte {
	var filtered [][]byte
	prefixBytes := []byte(prefix)
	for _, key := range keys {
		if len(key) >= len(prefixBytes) {
			match := true
			for i, b := range prefixBytes {
				if key[i] != b {
					match = false
					break
				}
			}
			if match {
				filtered = append(filtered, key)
			}
		}
	}
	return filtered
}

// GenerateRealisticValue creates a value of specified size with some realistic structure
func GenerateRealisticValue(size int, seed uint64) []byte {
	if size <= 0 {
		return nil
	}

	// Create deterministic but varied content
	h := sha256.New()
	binary.Write(h, binary.LittleEndian, seed)
	hash := h.Sum(nil)

	value := make([]byte, size)
	hashLen := len(hash)

	// Fill value with repeated hash pattern
	for i := 0; i < size; i++ {
		value[i] = hash[i%hashLen]
	}

	return value
}

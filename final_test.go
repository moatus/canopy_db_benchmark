package main

import (
	"fmt"
	"testing"
)

// TestCanopyKeyGeneration validates the key generation produces realistic patterns
func TestCanopyKeyGeneration(t *testing.T) {
	keys := GenerateCanopyKeys(1000, 12345)

	// Count key families
	accountCount := 0
	validatorCount := 0
	committeeCount := 0
	hashCount := 0

	for _, key := range keys {
		keyStr := string(key)
		switch {
		case len(keyStr) >= 8 && keyStr[:8] == "account_":
			accountCount++
		case len(keyStr) >= 10 && keyStr[:10] == "validator_":
			validatorCount++
		case len(keyStr) >= 10 && keyStr[:10] == "committee_":
			committeeCount++
		case len(keyStr) >= 5 && keyStr[:5] == "hash_":
			hashCount++
		}
	}

	total := len(keys)
	fmt.Printf("Key distribution (n=%d):\n", total)
	fmt.Printf("  account_: %d (%.1f%%)\n", accountCount, float64(accountCount)*100/float64(total))
	fmt.Printf("  validator_: %d (%.1f%%)\n", validatorCount, float64(validatorCount)*100/float64(total))
	fmt.Printf("  committee_: %d (%.1f%%)\n", committeeCount, float64(committeeCount)*100/float64(total))
	fmt.Printf("  hash_: %d (%.1f%%)\n", hashCount, float64(hashCount)*100/float64(total))

	// Validate distribution is roughly correct (60/20/10/10)
	accountPct := float64(accountCount) * 100 / float64(total)
	if accountPct < 50 || accountPct > 70 {
		t.Errorf("Account key percentage %.1f%% is outside expected range 50-70%%", accountPct)
	}

	validatorPct := float64(validatorCount) * 100 / float64(total)
	if validatorPct < 15 || validatorPct > 25 {
		t.Errorf("Validator key percentage %.1f%% is outside expected range 15-25%%", validatorPct)
	}

	// Test hot keys
	hotKeys := GetHotKeys(keys)
	if len(hotKeys) == 0 {
		t.Error("No hot keys found")
	}

	fmt.Printf("  Hot keys: %d (%.1f%%)\n", len(hotKeys), float64(len(hotKeys))*100/float64(total))
}

// TestCanopyResultsFormatting validates the results formatting
func TestCanopyResultsFormatting(t *testing.T) {
	results := NewTestResults()

	// Add some sample results
	results.AddResult(TestResult{
		Implementation: "TestStore",
		TestType:       "Latest-PointGet",
		TimeMin:        1000, // 1μs
		TimeMedian:     1500, // 1.5μs
		TimeMax:        2000, // 2μs
		AllocsPerOp:    10,
		BytesPerOp:     1024,
	})

	results.AddResult(TestResult{
		Implementation: "TestStore",
		TestType:       "Write",
		TimeMin:        1000000, // 1ms
		TimeMedian:     1500000, // 1.5ms
		TimeMax:        2000000, // 2ms
		AllocsPerOp:    100,
		BytesPerOp:     10240,
	})

	fmt.Println("Sample results formatting:")
	results.PrintSummary("Test Results")
}

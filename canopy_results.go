package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// TestResult holds the results of a single test run with timing distribution
type TestResult struct {
	Implementation string
	TestType       string
	TimeMin        time.Duration // fastest run
	TimeMax        time.Duration // slowest run
	TimeMedian     time.Duration // median run
	AllocsPerOp    int64         // median allocs per operation
	BytesPerOp     int64         // median bytes per operation
}

// TestResults manages a collection of test results and provides formatted output
type TestResults struct {
	results []TestResult
}

// NewTestResults creates a new results collector
func NewTestResults() *TestResults {
	return &TestResults{
		results: make([]TestResult, 0),
	}
}

// AddResult adds a test result to the collection
func (tr *TestResults) AddResult(result TestResult) {
	tr.results = append(tr.results, result)
}

// PrintSummary prints results in the SystemLevel format with min/med/max timing
func (tr *TestResults) PrintSummary(title string) {
	if len(tr.results) == 0 {
		return
	}

	fmt.Printf("\n%s\n", title)
	fmt.Printf("%s\n", strings.Repeat("=", len(title)))

	// Group results by test type
	groups := make(map[string][]TestResult)
	for _, result := range tr.results {
		groups[result.TestType] = append(groups[result.TestType], result)
	}

	// Sort test types for consistent output
	testTypes := make([]string, 0, len(groups))
	for testType := range groups {
		testTypes = append(testTypes, testType)
	}
	sort.Strings(testTypes)

	// Define implementation order for consistent display
	implOrder := []string{"Badger", "MonoSpace", "DualTimeline"}
	implIndex := make(map[string]int)
	for i, impl := range implOrder {
		implIndex[impl] = i
	}

	for i, testType := range testTypes {
		if i > 0 {
			fmt.Println()
		}

		fmt.Printf("%s\n", testType)
		fmt.Printf("%s\n", strings.Repeat("-", len(testType)))

		// Sort implementations by predefined order
		results := groups[testType]
		sort.Slice(results, func(i, j int) bool {
			idxI, okI := implIndex[results[i].Implementation]
			idxJ, okJ := implIndex[results[j].Implementation]
			if okI && okJ {
				return idxI < idxJ
			}
			if okI != okJ {
				return okI // known implementations first
			}
			return results[i].Implementation < results[j].Implementation
		})

		// Determine appropriate time unit for this group
		var maxTime time.Duration
		for _, result := range results {
			if result.TimeMax > maxTime {
				maxTime = result.TimeMax
			}
		}

		timeUnit, timeLabel := chooseTimeUnit(maxTime)

		// Print header
		fmt.Printf("%-20s | %-20s | %12s | %12s\n",
			"Variant",
			fmt.Sprintf("Time/Run (%s)", timeLabel),
			"Allocs/Run",
			"Bytes/Run (MB)")
		fmt.Printf("%s\n", strings.Repeat("-", 69))

		// Print results
		for _, result := range results {
			timeRange := formatTimeRange(result.TimeMin, result.TimeMedian, result.TimeMax, timeUnit)
			allocsStr := formatCount(result.AllocsPerOp)
			bytesStr := formatMB(result.BytesPerOp)

			fmt.Printf("%-20s | %-20s | %12s | %12s\n",
				result.Implementation,
				timeRange,
				allocsStr,
				bytesStr)
		}
	}
	fmt.Println()
}

// chooseTimeUnit determines the best unit for displaying times
func chooseTimeUnit(maxTime time.Duration) (string, string) {
	if maxTime >= time.Second {
		return "s", "s"
	} else if maxTime >= time.Millisecond {
		return "ms", "ms"
	} else if maxTime >= time.Microsecond {
		return "μs", "μs"
	} else {
		return "ns", "ns"
	}
}

// formatTimeRange formats min/median/max times in the specified unit
func formatTimeRange(min, median, max time.Duration, unit string) string {
	switch unit {
	case "s":
		return fmt.Sprintf("%.2f/%.2f/%.2f",
			min.Seconds(), median.Seconds(), max.Seconds())
	case "ms":
		minMs := float64(min.Nanoseconds()) / 1e6
		medMs := float64(median.Nanoseconds()) / 1e6
		maxMs := float64(max.Nanoseconds()) / 1e6
		return fmt.Sprintf("%.1f/%.1f/%.1f", minMs, medMs, maxMs)
	case "μs":
		minUs := float64(min.Nanoseconds()) / 1e3
		medUs := float64(median.Nanoseconds()) / 1e3
		maxUs := float64(max.Nanoseconds()) / 1e3
		return fmt.Sprintf("%.1f/%.1f/%.1f", minUs, medUs, maxUs)
	default: // ns
		return fmt.Sprintf("%d/%d/%d",
			min.Nanoseconds(), median.Nanoseconds(), max.Nanoseconds())
	}
}

// formatCount formats integers with thousands separators
func formatCount(n int64) string {
	if n == 0 {
		return "0"
	}

	sign := ""
	if n < 0 {
		sign = "-"
		n = -n
	}

	str := fmt.Sprintf("%d", n)
	if len(str) <= 3 {
		return sign + str
	}

	// Add commas
	var result strings.Builder
	result.Grow(len(str) + len(str)/3)

	for i, digit := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteByte(',')
		}
		result.WriteRune(digit)
	}

	return sign + result.String()
}

// formatMB formats bytes as megabytes with one decimal place
func formatMB(bytes int64) string {
	mb := float64(bytes) / (1024 * 1024)
	return fmt.Sprintf("%.1fMB", mb)
}

// CalculateStats computes min, median, max from a slice of durations
func CalculateStats(times []time.Duration) (min, median, max time.Duration) {
	if len(times) == 0 {
		return 0, 0, 0
	}

	// Sort for median calculation
	sorted := make([]time.Duration, len(times))
	copy(sorted, times)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	min = sorted[0]
	max = sorted[len(sorted)-1]

	// Calculate median
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		median = (sorted[mid-1] + sorted[mid]) / 2
	} else {
		median = sorted[mid]
	}

	return min, median, max
}

// CalculateMedianInt64 computes median from a slice of int64 values
func CalculateMedianInt64(values []int64) int64 {
	if len(values) == 0 {
		return 0
	}

	sorted := make([]int64, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	} else {
		return sorted[mid]
	}
}

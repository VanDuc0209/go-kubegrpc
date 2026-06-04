package retry

import (
	"math"
	"testing"
	"time"

	"pgregory.net/rapid"
)

// TestAttemptBackoffN1 verifies the base case: attempt 1 always returns the
// initial backoff regardless of multiplier or max.
//
// Validates: Requirements 7.4
func TestAttemptBackoffN1(t *testing.T) {
	got := AttemptBackoff(1, 100*time.Millisecond, 2*time.Second, 2.0)
	if got != 100*time.Millisecond {
		t.Fatalf("AttemptBackoff(1, 100ms, 2s, 2.0) = %v; want 100ms", got)
	}
}

// TestAttemptBackoffProperty verifies three properties of AttemptBackoff using
// property-based testing:
//
//  1. The result matches min(maxBackoff, initial * multiplier^(n-1)).
//  2. The sequence for n=1..20 is monotonically non-decreasing.
//  3. Once max is reached, all subsequent values equal maxBackoff.
//
// Validates: Requirements 7.4
func TestAttemptBackoffProperty(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		// Generate random parameters within the documented ranges.
		initialMs := rapid.Int64Range(1, 1000).Draw(rt, "initialMs")      // 1ms..1s
		multiplier := rapid.Float64Range(1.0, 3.0).Draw(rt, "multiplier") // [1.0, 3.0]
		maxMs := rapid.Int64Range(10, 10000).Draw(rt, "maxMs")             // 10ms..10s
		n := rapid.IntRange(1, 20).Draw(rt, "n")                           // [1, 20]

		initial := time.Duration(initialMs) * time.Millisecond
		maxBackoff := time.Duration(maxMs) * time.Millisecond

		got := AttemptBackoff(n, initial, maxBackoff, multiplier)

		// Property 1: result matches the formula min(max, initial * mult^(n-1)).
		expected := formulaBackoff(n, initial, maxBackoff, multiplier)
		if !durationClose(got, expected) {
			rt.Fatalf("n=%d initial=%v max=%v mult=%v: got %v, want %v (formula)",
				n, initial, maxBackoff, multiplier, got, expected)
		}

		// Property 2: the sequence for n=1..20 is monotonically non-decreasing.
		prev := AttemptBackoff(1, initial, maxBackoff, multiplier)
		for i := 2; i <= 20; i++ {
			cur := AttemptBackoff(i, initial, maxBackoff, multiplier)
			if cur < prev {
				rt.Fatalf("sequence not monotone: AttemptBackoff(%d)=%v < AttemptBackoff(%d)=%v (initial=%v max=%v mult=%v)",
					i, cur, i-1, prev, initial, maxBackoff, multiplier)
			}
			prev = cur
		}

		// Property 3: once max is reached, all subsequent values equal maxBackoff.
		hitMax := false
		for i := 1; i <= 20; i++ {
			v := AttemptBackoff(i, initial, maxBackoff, multiplier)
			if v == maxBackoff {
				hitMax = true
			}
			if hitMax && v != maxBackoff {
				rt.Fatalf("value after max not clamped: AttemptBackoff(%d)=%v, max=%v (initial=%v mult=%v)",
					i, v, maxBackoff, initial, multiplier)
			}
		}
	})
}

// formulaBackoff computes the expected backoff using the reference formula
// min(maxBackoff, initial * multiplier^(n-1)).
func formulaBackoff(n int, initial, maxBackoff time.Duration, multiplier float64) time.Duration {
	if n <= 1 {
		if initial > maxBackoff {
			return maxBackoff
		}
		return initial
	}
	exact := float64(initial) * math.Pow(multiplier, float64(n-1))
	if time.Duration(exact) >= maxBackoff {
		return maxBackoff
	}
	return time.Duration(exact)
}

// durationClose returns true when a and b are within 1% relative error or 1ms
// absolute error of each other, whichever is larger. This tolerates the
// iterative floating-point accumulation in the implementation versus the
// single math.Pow call in the reference formula.
func durationClose(a, b time.Duration) bool {
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	// 1ms absolute tolerance
	if diff <= time.Millisecond {
		return true
	}
	// 1% relative tolerance based on the larger value
	larger := a
	if b > a {
		larger = b
	}
	threshold := time.Duration(float64(larger) * 0.01)
	return diff <= threshold
}

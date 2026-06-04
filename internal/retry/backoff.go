package retry

import "time"

// Clock is injectable for tests so time-dependent logic can be driven
// deterministically without real sleeps.
type Clock interface {
	Now() time.Time
	Sleep(d time.Duration)
	After(d time.Duration) <-chan time.Time
}

// realClock delegates to the standard library.
type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) Sleep(d time.Duration)                  { time.Sleep(d) }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// AttemptBackoff returns the sleep duration for the n-th attempt (1-indexed).
//
// Formula: min(MaxBackoff, InitialBackoff * BackoffMultiplier^(n-1))
//
// For n == 1 no prior attempt has been made, so the delay is InitialBackoff.
// The result is clamped to max so callers never wait longer than the configured
// ceiling regardless of how large n is.
//
// Requirements: R7.4
func AttemptBackoff(n int, initial, max time.Duration, multiplier float64) time.Duration {
	if n <= 1 {
		if initial > max {
			return max
		}
		return initial
	}
	dur := float64(initial)
	for i := 1; i < n; i++ {
		dur *= multiplier
		if time.Duration(dur) >= max {
			return max
		}
	}
	if time.Duration(dur) > max {
		return max
	}
	return time.Duration(dur)
}

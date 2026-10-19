package lode

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// These tests pin the backoff schedule itself: the defaults, the cap and
// overflow handling, and how jitter splits the delay. The schedule is not
// observable through WithRetryObserver, so without them any change to the
// arithmetic would go unnoticed.
//
// Known equivalent mutants in jitteredBackoff, left alive on purpose:
//   - `delay > c.maxDelay` → `>=`: equal delays cap to themselves.
//   - `c.jitter <= 0` → `<`: a zero jitter portion yields the same delay.

func TestDefaultRetryConfig_Values(t *testing.T) {
	cfg := defaultRetryConfig()
	if cfg.maxAttempts != 0 {
		t.Errorf("maxAttempts: got %d, want 0 (retries are opt-in)", cfg.maxAttempts)
	}
	if cfg.baseDelay != 10*time.Millisecond {
		t.Errorf("baseDelay: got %v, want 10ms", cfg.baseDelay)
	}
	if cfg.maxDelay != 2*time.Second {
		t.Errorf("maxDelay: got %v, want 2s", cfg.maxDelay)
	}
	if cfg.jitter != 1.0 {
		t.Errorf("jitter: got %v, want 1.0 (full jitter)", cfg.jitter)
	}
	if cfg.observer != nil {
		t.Error("observer: want nil by default")
	}
}

// Full jitter draws uniformly from [0, schedule): the schedule itself is
// never returned, and the cap applies before the draw.
func TestJitteredBackoff_FullJitter_StrictlyBelowSchedule(t *testing.T) {
	cfg := retryConfig{
		baseDelay: 10 * time.Millisecond,
		maxDelay:  2 * time.Second,
		jitter:    1.0,
	}
	for attempt := 1; attempt <= 12; attempt++ {
		schedule := min(cfg.baseDelay<<(attempt-1), cfg.maxDelay) //nolint:gosec // bounded attempt
		for range 32 {
			got := cfg.jitteredBackoff(attempt)
			if got < 0 || got >= schedule {
				t.Fatalf("attempt %d: got %v, want in [0, %v)", attempt, got, schedule)
			}
		}
	}
}

// jitter=0.5 keeps half the schedule deterministic and randomizes the rest:
// attempt 1 at 100ms base lands in [50ms, 100ms).
func TestJitteredBackoff_PartialJitter_SplitsDelay(t *testing.T) {
	cfg := retryConfig{
		baseDelay: 100 * time.Millisecond,
		maxDelay:  time.Second,
		jitter:    0.5,
	}
	for range 64 {
		got := cfg.jitteredBackoff(1)
		if got < 50*time.Millisecond || got >= 100*time.Millisecond {
			t.Fatalf("got %v, want in [50ms, 100ms)", got)
		}
	}
}

// Shifting a 10ms base past 63 bits wraps negative (attempt 41) and then to
// zero (attempt >= 64); both must cap at maxDelay rather than return garbage.
func TestJitteredBackoff_OverflowCapsAtMaxDelay(t *testing.T) {
	cfg := retryConfig{
		baseDelay: 10 * time.Millisecond,
		maxDelay:  2 * time.Second,
		jitter:    0,
	}
	for _, attempt := range []int{41, 64, 200} {
		if got := cfg.jitteredBackoff(attempt); got != cfg.maxDelay {
			t.Errorf("attempt %d: got %v, want cap %v", attempt, got, cfg.maxDelay)
		}
	}
}

// A jitter portion that truncates to zero must skip the random draw
// (rand.Int64N(0) panics) and leave the whole delay deterministic.
func TestJitteredBackoff_SubNanosecondJitterPortion(t *testing.T) {
	cfg := retryConfig{
		baseDelay: time.Nanosecond,
		maxDelay:  time.Second,
		jitter:    0.5,
	}
	if got := cfg.jitteredBackoff(1); got != time.Nanosecond {
		t.Fatalf("got %v, want 1ns", got)
	}
}

// The observer sees each attempt number exactly once, in order, before the
// sleep for that attempt begins.
func TestRetryBackoff_ObserverOncePerAttemptInOrder(t *testing.T) {
	var seen []int
	cfg := retryConfig{
		baseDelay: time.Nanosecond,
		maxDelay:  time.Nanosecond,
		jitter:    0,
		observer:  func(attempt int) { seen = append(seen, attempt) },
	}
	for attempt := 1; attempt <= 3; attempt++ {
		if err := cfg.retryBackoff(t.Context(), attempt); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	if want := []int{1, 2, 3}; !slices.Equal(seen, want) {
		t.Fatalf("observer saw %v, want %v", seen, want)
	}
}

// The observer runs before the wait, so it fires even when the context is
// already canceled and the wait returns immediately with ctx.Err().
func TestRetryBackoff_ObserverFiresBeforeCanceledWait(t *testing.T) {
	calls := 0
	cfg := retryConfig{
		baseDelay: 10 * time.Second,
		maxDelay:  10 * time.Second,
		jitter:    0,
		observer:  func(int) { calls++ },
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := cfg.retryBackoff(ctx, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if calls != 1 {
		t.Fatalf("observer calls: got %d, want 1", calls)
	}
}

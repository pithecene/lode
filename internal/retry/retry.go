// Package retry holds the CAS-conflict retry schedule shared by Lode's
// containers: the configuration the WithRetry* options set, and the jittered
// exponential backoff a commit loop waits between attempts.
package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Config holds retry parameters for CAS conflict retries.
// A zero maxAttempts means no retries (current behavior preserved).
type Config struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
	jitter      float64 // 0.0 = deterministic, 1.0 = full jitter
	observer    func(attempt int)
}

// Default returns a Config with sensible defaults.
// maxAttempts is 0 (no retry) so the default behavior is unchanged.
func Default() Config {
	return Config{
		maxAttempts: 0,
		baseDelay:   10 * time.Millisecond,
		maxDelay:    2 * time.Second,
		jitter:      1.0,
	}
}

// Validation failures of the setters. The option constructors that call the
// setters wrap these with their own name and the caller's invalid-argument
// sentinel, so every container reports the same message.
var (
	errNegativeAttempts = errors.New("n must be non-negative")
	errNonPositiveDelay = errors.New("delay must be positive")
	errJitterRange      = errors.New("jitter must be between 0.0 and 1.0")
	errNilObserver      = errors.New("fn must not be nil")
)

// MaxAttempts is the number of retries after the first attempt.
func (c *Config) MaxAttempts() int { return c.maxAttempts }

// SetMaxAttempts sets the retry count; n must be non-negative.
func (c *Config) SetMaxAttempts(n int) error {
	if n < 0 {
		return errNegativeAttempts
	}
	c.maxAttempts = n
	return nil
}

// SetBaseDelay sets the first backoff delay; d must be positive.
func (c *Config) SetBaseDelay(d time.Duration) error {
	if d <= 0 {
		return errNonPositiveDelay
	}
	c.baseDelay = d
	return nil
}

// SetMaxDelay caps the backoff delay; d must be positive.
func (c *Config) SetMaxDelay(d time.Duration) error {
	if d <= 0 {
		return errNonPositiveDelay
	}
	c.maxDelay = d
	return nil
}

// SetJitter sets the jitter factor; j must be in [0, 1].
func (c *Config) SetJitter(j float64) error {
	if j < 0 || j > 1 {
		return errJitterRange
	}
	c.jitter = j
	return nil
}

// SetObserver registers the per-retry callback; fn must not be nil.
func (c *Config) SetObserver(fn func(attempt int)) error {
	if fn == nil {
		return errNilObserver
	}
	c.observer = fn
	return nil
}

// jitteredBackoff computes the delay for a retry attempt (1-indexed).
// Uses exponential backoff: baseDelay * 2^(attempt-1), capped at maxDelay,
// then applies jitter. With jitter=1.0 (full jitter), the result is
// uniform in [0, delay). With jitter=0.0, the result is deterministic.
func (c *Config) jitteredBackoff(attempt int) time.Duration {
	delay := c.baseDelay << (attempt - 1) //nolint:gosec // intentional bit shift for exponential backoff
	if delay > c.maxDelay || delay <= 0 {
		// Cap at maxDelay. delay <= 0 catches overflow from large attempt values.
		delay = c.maxDelay
	}

	if c.jitter <= 0 {
		return delay
	}

	// Split delay into deterministic and jittered portions.
	// jitter=1.0: entire delay is randomized in [0, delay).
	// jitter=0.5: half deterministic, half random.
	jitterPortion := time.Duration(float64(delay) * c.jitter)
	deterministic := delay - jitterPortion
	if jitterPortion > 0 {
		deterministic += time.Duration(rand.Int64N(int64(jitterPortion)))
	}
	return deterministic
}

// Backoff notifies the retry observer (if any), then sleeps for the
// jittered backoff duration, respecting context cancellation.
// Commit loops call it exactly once per retry, so the observer sees one
// call per retry attempt.
// Returns ctx.Err() if the context is canceled during the wait.
func (c *Config) Backoff(ctx context.Context, attempt int) error {
	if c.observer != nil {
		c.observer(attempt)
	}
	delay := c.jitteredBackoff(attempt)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}

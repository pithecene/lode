package lode

import (
	"context"
	"math/rand/v2"
	"time"
)

// retryConfig holds retry parameters for CAS conflict retries.
// A zero maxAttempts means no retries (current behavior preserved).
type retryConfig struct {
	maxAttempts int
	baseDelay   time.Duration
	maxDelay    time.Duration
	jitter      float64 // 0.0 = deterministic, 1.0 = full jitter
	observer    func(attempt int)
}

// defaultRetryConfig returns a retryConfig with sensible defaults.
// maxAttempts is 0 (no retry) so the default behavior is unchanged.
func defaultRetryConfig() retryConfig {
	return retryConfig{
		maxAttempts: 0,
		baseDelay:   10 * time.Millisecond,
		maxDelay:    2 * time.Second,
		jitter:      1.0,
	}
}

// jitteredBackoff computes the delay for a retry attempt (1-indexed).
// Uses exponential backoff: baseDelay * 2^(attempt-1), capped at maxDelay,
// then applies jitter. With jitter=1.0 (full jitter), the result is
// uniform in [0, delay). With jitter=0.0, the result is deterministic.
func (c *retryConfig) jitteredBackoff(attempt int) time.Duration {
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

// retryBackoff notifies the retry observer (if any), then sleeps for the
// jittered backoff duration, respecting context cancellation.
// Commit loops call it exactly once per retry, so the observer sees one
// call per retry attempt.
// Returns ctx.Err() if the context is canceled during the wait.
func (c *retryConfig) retryBackoff(ctx context.Context, attempt int) error {
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

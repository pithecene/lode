package volume

import (
	"time"

	"github.com/pithecene-io/lode/internal/retry"
	"github.com/pithecene-io/lode/lode"
)

// config holds the resolved configuration for a volume.
type config struct {
	checksum lode.Checksum
	retry    retry.Config
}

// Option configures New. Only the options in this package apply to a
// Volume; lode's dataset options are a separate type and cannot be passed.
type Option interface {
	apply(*config) error
}

// optionFunc adapts a function to Option.
type optionFunc func(*config) error

func (f optionFunc) apply(c *config) error { return f(c) }

// WithChecksum enables block checksums: each staged block records the
// checksum's hash, and the manifest records its algorithm.
// Default: no checksums.
func WithChecksum(c lode.Checksum) Option {
	return optionFunc(func(cfg *config) error {
		cfg.checksum = c
		return nil
	})
}

// retryOption reports a retry.Config setter's validation failure as an
// invalid argument named after the option, worded as lode's WithRetry*
// options word it.
func retryOption(name string, set func(*retry.Config) error) Option {
	return optionFunc(func(cfg *config) error {
		if err := set(&cfg.retry); err != nil {
			return invalidArgument("%s: %v", name, err)
		}
		return nil
	})
}

// WithRetryCount enables automatic CAS retry on snapshot conflict.
// When n > 0, Commit retries up to n times on lode.ErrSnapshotConflict,
// refreshing the parent state between attempts.
// Default: 0 (no retry; lode.ErrSnapshotConflict returned immediately).
func WithRetryCount(n int) Option {
	return retryOption("WithRetryCount", func(r *retry.Config) error { return r.SetMaxAttempts(n) })
}

// WithRetryBaseDelay sets the base delay for exponential backoff on CAS retry.
// The actual delay doubles with each attempt and is jittered.
// Default: 10ms.
func WithRetryBaseDelay(d time.Duration) Option {
	return retryOption("WithRetryBaseDelay", func(r *retry.Config) error { return r.SetBaseDelay(d) })
}

// WithRetryMaxDelay sets the maximum delay for exponential backoff on CAS retry.
// Default: 2s.
func WithRetryMaxDelay(d time.Duration) Option {
	return retryOption("WithRetryMaxDelay", func(r *retry.Config) error { return r.SetMaxDelay(d) })
}

// WithRetryJitter sets the jitter factor for CAS retry backoff.
// 0.0 means no jitter (deterministic delay), 1.0 means full jitter.
// Default: 1.0 (full jitter).
func WithRetryJitter(j float64) Option {
	return retryOption("WithRetryJitter", func(r *retry.Config) error { return r.SetJitter(j) })
}

// WithRetryObserver registers a callback invoked once per CAS retry attempt.
// The callback receives the 1-indexed retry attempt number and is called
// synchronously on the committing goroutine after a snapshot conflict is
// detected, before the backoff sleep. It is never called for the initial
// attempt, so the number of calls equals the number of retries performed.
// The callback must be fast and must not block.
// Has no effect unless WithRetryCount(n) is configured with n > 0.
func WithRetryObserver(fn func(attempt int)) Option {
	return retryOption("WithRetryObserver", func(r *retry.Config) error { return r.SetObserver(fn) })
}

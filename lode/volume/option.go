package volume

import (
	"time"

	"github.com/pithecene-io/lode/internal/retry"
	"github.com/pithecene-io/lode/internal/validate"
	"github.com/pithecene-io/lode/lode"
)

// config holds the resolved configuration for a volume.
type config struct {
	checksum lode.Checksum
	retry    retry.Config
}

// Option configures New. Lode's dataset options are a separate type and do
// not apply to a Volume.
type Option interface {
	apply(*config) error
}

type optionFunc func(*config) error

func (f optionFunc) apply(c *config) error { return f(c) }

// WithChecksum records a checksum for every staged block and the algorithm
// in the manifest. Default: none.
func WithChecksum(c lode.Checksum) Option {
	return optionFunc(func(cfg *config) error {
		cfg.checksum = c
		return nil
	})
}

// retryOption reports a retry.Config setter's validation failure as an
// invalid argument named after the option, as lode's WithRetry* options do.
func retryOption(name string, set func(*retry.Config) error) Option {
	return optionFunc(func(cfg *config) error {
		if err := set(&cfg.retry); err != nil {
			return validate.Argument("%s: %v", name, err)
		}
		return nil
	})
}

// WithRetryCount is lode.WithRetryCount for a Volume's Commit.
func WithRetryCount(n int) Option {
	return retryOption("WithRetryCount", func(r *retry.Config) error { return r.SetMaxAttempts(n) })
}

// WithRetryBaseDelay is lode.WithRetryBaseDelay for a Volume's Commit.
func WithRetryBaseDelay(d time.Duration) Option {
	return retryOption("WithRetryBaseDelay", func(r *retry.Config) error { return r.SetBaseDelay(d) })
}

// WithRetryMaxDelay is lode.WithRetryMaxDelay for a Volume's Commit.
func WithRetryMaxDelay(d time.Duration) Option {
	return retryOption("WithRetryMaxDelay", func(r *retry.Config) error { return r.SetMaxDelay(d) })
}

// WithRetryJitter is lode.WithRetryJitter for a Volume's Commit.
func WithRetryJitter(j float64) Option {
	return retryOption("WithRetryJitter", func(r *retry.Config) error { return r.SetJitter(j) })
}

// WithRetryObserver is lode.WithRetryObserver for a Volume's Commit.
func WithRetryObserver(fn func(attempt int)) Option {
	return retryOption("WithRetryObserver", func(r *retry.Config) error { return r.SetObserver(fn) })
}

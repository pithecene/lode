package retry

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
	cfg := Default()
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

func TestJitteredBackoff(t *testing.T) {
	t.Run("bounds", func(t *testing.T) {
		cfg := Config{
			baseDelay: 10 * time.Millisecond,
			maxDelay:  2 * time.Second,
			jitter:    1.0,
		}

		for attempt := 1; attempt <= 10; attempt++ {
			delay := cfg.jitteredBackoff(attempt)
			if delay < 0 {
				t.Errorf("attempt %d: negative delay %v", attempt, delay)
			}
			if delay > cfg.maxDelay {
				t.Errorf("attempt %d: delay %v exceeds max %v", attempt, delay, cfg.maxDelay)
			}
		}
	})

	t.Run("no jitter deterministic", func(t *testing.T) {
		cfg := Config{
			baseDelay: 10 * time.Millisecond,
			maxDelay:  2 * time.Second,
			jitter:    0.0,
		}

		// With zero jitter, delay should be exactly baseDelay * 2^(attempt-1).
		expected := []time.Duration{
			10 * time.Millisecond,  // attempt 1
			20 * time.Millisecond,  // attempt 2
			40 * time.Millisecond,  // attempt 3
			80 * time.Millisecond,  // attempt 4
			160 * time.Millisecond, // attempt 5
		}
		for i, want := range expected {
			got := cfg.jitteredBackoff(i + 1)
			if got != want {
				t.Errorf("attempt %d: got %v, want %v", i+1, got, want)
			}
		}
	})

	t.Run("capped at max delay", func(t *testing.T) {
		cfg := Config{
			baseDelay: 100 * time.Millisecond,
			maxDelay:  500 * time.Millisecond,
			jitter:    0.0,
		}

		// attempt 1: 100ms, attempt 2: 200ms, attempt 3: 400ms, attempt 4: capped at 500ms
		got := cfg.jitteredBackoff(4)
		if got != 500*time.Millisecond {
			t.Errorf("attempt 4: got %v, want 500ms (capped)", got)
		}
	})

	// Full jitter draws uniformly from [0, schedule): the schedule itself is
	// never returned, and the cap applies before the draw.
	t.Run("full jitter strictly below schedule", func(t *testing.T) {
		cfg := Config{
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
	})

	// jitter=0.5 keeps half the schedule deterministic and randomizes the rest:
	// attempt 1 at 100ms base lands in [50ms, 100ms).
	t.Run("partial jitter splits delay", func(t *testing.T) {
		cfg := Config{
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
	})

	// Shifting a 10ms base past 63 bits wraps negative (attempt 41) and then to
	// zero (attempt >= 64); both must cap at maxDelay rather than return garbage.
	t.Run("overflow caps at max delay", func(t *testing.T) {
		cfg := Config{
			baseDelay: 10 * time.Millisecond,
			maxDelay:  2 * time.Second,
			jitter:    0,
		}
		for _, attempt := range []int{41, 64, 200} {
			if got := cfg.jitteredBackoff(attempt); got != cfg.maxDelay {
				t.Errorf("attempt %d: got %v, want cap %v", attempt, got, cfg.maxDelay)
			}
		}
	})

	// A jitter portion that truncates to zero must skip the random draw
	// (rand.Int64N(0) panics) and leave the whole delay deterministic.
	t.Run("sub nanosecond jitter portion", func(t *testing.T) {
		cfg := Config{
			baseDelay: time.Nanosecond,
			maxDelay:  time.Second,
			jitter:    0.5,
		}
		if got := cfg.jitteredBackoff(1); got != time.Nanosecond {
			t.Fatalf("got %v, want 1ns", got)
		}
	})
}

func TestRetryBackoff(t *testing.T) {
	t.Run("context canceled", func(t *testing.T) {
		cfg := Config{
			baseDelay: 10 * time.Second, // long delay so context cancels first
			maxDelay:  10 * time.Second,
			jitter:    0.0,
		}

		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err := cfg.Backoff(ctx, 1)
		if err == nil {
			t.Fatal("expected context canceled error")
		}
	})

	// The observer sees each attempt number exactly once, in order, before the
	// sleep for that attempt begins.
	t.Run("observer once per attempt in order", func(t *testing.T) {
		var seen []int
		cfg := Config{
			baseDelay: time.Nanosecond,
			maxDelay:  time.Nanosecond,
			jitter:    0,
			observer:  func(attempt int) { seen = append(seen, attempt) },
		}
		for attempt := 1; attempt <= 3; attempt++ {
			if err := cfg.Backoff(t.Context(), attempt); err != nil {
				t.Fatalf("attempt %d: %v", attempt, err)
			}
		}
		if want := []int{1, 2, 3}; !slices.Equal(seen, want) {
			t.Fatalf("observer saw %v, want %v", seen, want)
		}
	})

	// The observer runs before the wait, so it fires even when the context is
	// already canceled and the wait returns immediately with ctx.Err().
	t.Run("observer fires before canceled wait", func(t *testing.T) {
		calls := 0
		cfg := Config{
			baseDelay: 10 * time.Second,
			maxDelay:  10 * time.Second,
			jitter:    0,
			observer:  func(int) { calls++ },
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err := cfg.Backoff(ctx, 1)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, want context.Canceled", err)
		}
		if calls != 1 {
			t.Fatalf("observer calls: got %d, want 1", calls)
		}
	})
}

// The setters are the single validation point for the WithRetry* options of
// every container; each bound is checked at its edge.
func TestConfig_Setters(t *testing.T) {
	cases := []struct {
		name    string
		set     func(*Config) error
		wantErr error
	}{
		{"attempts zero", func(c *Config) error { return c.SetMaxAttempts(0) }, nil},
		{"attempts negative", func(c *Config) error { return c.SetMaxAttempts(-1) }, errNegativeAttempts},
		{"base delay smallest", func(c *Config) error { return c.SetBaseDelay(time.Nanosecond) }, nil},
		{"base delay zero", func(c *Config) error { return c.SetBaseDelay(0) }, errNonPositiveDelay},
		{"max delay smallest", func(c *Config) error { return c.SetMaxDelay(time.Nanosecond) }, nil},
		{"max delay zero", func(c *Config) error { return c.SetMaxDelay(0) }, errNonPositiveDelay},
		{"jitter zero", func(c *Config) error { return c.SetJitter(0) }, nil},
		{"jitter one", func(c *Config) error { return c.SetJitter(1) }, nil},
		{"jitter below", func(c *Config) error { return c.SetJitter(-0.01) }, errJitterRange},
		{"jitter above", func(c *Config) error { return c.SetJitter(1.01) }, errJitterRange},
		{"observer set", func(c *Config) error { return c.SetObserver(func(int) {}) }, nil},
		{"observer nil", func(c *Config) error { return c.SetObserver(nil) }, errNilObserver},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Default()
			if err := tc.set(&cfg); !errors.Is(err, tc.wantErr) {
				t.Fatalf("got %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// A setter that accepts its value stores it; one that rejects leaves the
// config unchanged.
func TestConfig_SettersStore(t *testing.T) {
	cfg := Default()
	observed := 0
	for _, err := range []error{
		cfg.SetMaxAttempts(3),
		cfg.SetBaseDelay(time.Millisecond),
		cfg.SetMaxDelay(time.Second),
		cfg.SetJitter(0.5),
		cfg.SetObserver(func(int) { observed++ }),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if cfg.MaxAttempts() != 3 || cfg.baseDelay != time.Millisecond || cfg.maxDelay != time.Second || cfg.jitter != 0.5 {
		t.Fatalf("setters did not store: %+v", cfg)
	}
	cfg.observer(1)
	if observed != 1 {
		t.Fatal("observer not stored")
	}

	before := cfg
	_ = cfg.SetMaxAttempts(-1)
	_ = cfg.SetBaseDelay(0)
	_ = cfg.SetMaxDelay(0)
	_ = cfg.SetJitter(2)
	if cfg.maxAttempts != before.maxAttempts || cfg.baseDelay != before.baseDelay || cfg.maxDelay != before.maxDelay || cfg.jitter != before.jitter {
		t.Fatalf("rejected value changed the config: %+v", cfg)
	}
}

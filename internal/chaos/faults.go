package chaos

import (
	"context"
	"errors"
	"io"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/pithecene/lode/lode"
)

var (
	// errInjected is a failure the harness injected; a write or read that
	// returns it is expected to fail.
	errInjected = errors.New("chaos: injected failure")
	// errKilled marks a handle whose simulated process has died: every
	// later call fails, and the goroutine continues as a new process with
	// a new handle.
	errKilled = errors.New("chaos: process killed")
)

// Faults describes what a faulty store injects, as probabilities per call.
type Faults struct {
	// Delay is the upper bound of an injected latency, applied with
	// probability PDelay before a call.
	Delay  time.Duration
	PDelay float64
	// PFailBefore fails a call without applying it.
	PFailBefore float64
	// PFailAfter applies a Put, CompareAndSwap or Delete and then reports
	// a failure, as when a response is lost.
	PFailAfter float64
	// PListDrop omits each key from a List result, as a lagging listing
	// does.
	PListDrop float64
	// PKill ends the handle's process before a call, which may fall in the
	// middle of a commit.
	PKill float64
}

// Enabled reports whether any fault is injected.
func (f Faults) Enabled() bool { return f != Faults{} }

// faultLevels are the named fault settings a run can use.
var faultLevels = map[string]Faults{
	"none": {},
	"mild": {Delay: 5 * time.Millisecond, PDelay: 0.2, PFailBefore: 0.01, PFailAfter: 0.01, PListDrop: 0.01, PKill: 0.002},
	"heavy": {
		Delay: 20 * time.Millisecond, PDelay: 0.5, PFailBefore: 0.05, PFailAfter: 0.05, PListDrop: 0.05, PKill: 0.01,
	},
}

// FaultLevelNames lists the named fault settings in a stable order.
func FaultLevelNames() []string { return slices.Sorted(maps.Keys(faultLevels)) }

// storeFor returns the store one handle reaches base through: base itself,
// or, when faults are enabled, a faulty store of its own, seeded from rng so
// a run's fault decisions repeat for the same seed.
func (c Config) storeFor(base lode.Store, rng *rand.Rand) lode.Store {
	if !c.Faults.Enabled() {
		return base
	}
	return newFaultyStore(base, c.Faults, rand.New(rand.NewPCG(rng.Uint64(), rng.Uint64()))) //nolint:gosec // reproducible test randomness, not security
}

// faultyStore injects faults into one handle's calls to a shared store.
// It is safe for concurrent use: a streaming write calls Put from its own
// goroutine.
type faultyStore struct {
	inner  lode.Store
	faults Faults

	mu   sync.Mutex
	rng  *rand.Rand
	dead bool
}

// newFaultyStore wraps inner. It implements ConditionalWriter only when
// inner does, so a commit takes the same path through it as without faults.
func newFaultyStore(inner lode.Store, faults Faults, rng *rand.Rand) lode.Store {
	s := &faultyStore{inner: inner, faults: faults, rng: rng}
	if cw, ok := inner.(lode.ConditionalWriter); ok {
		return &faultyCASStore{faultyStore: s, cas: cw}
	}
	return s
}

// chance draws a uniform number under the lock.
func (s *faultyStore) chance(p float64) bool {
	if p == 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.Float64() < p
}

// before decides what happens ahead of a call: the process may die, the
// call may be delayed, and it may fail without being applied.
func (s *faultyStore) before(ctx context.Context) error {
	s.mu.Lock()
	if !s.dead && s.faults.PKill > 0 && s.rng.Float64() < s.faults.PKill {
		s.dead = true
	}
	dead := s.dead
	var delay time.Duration
	if s.faults.PDelay > 0 && s.rng.Float64() < s.faults.PDelay {
		delay = time.Duration(s.rng.Int64N(int64(s.faults.Delay) + 1))
	}
	s.mu.Unlock()
	if dead {
		return errKilled
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.chance(s.faults.PFailBefore) {
		return errInjected
	}
	return nil
}

// after reports a failure for an applied mutation with probability
// PFailAfter.
func (s *faultyStore) after(err error) error {
	if err == nil && s.chance(s.faults.PFailAfter) {
		return errInjected
	}
	return err
}

func (s *faultyStore) Put(ctx context.Context, path string, r io.Reader) error {
	if err := s.before(ctx); err != nil {
		return err
	}
	return s.after(s.inner.Put(ctx, path, r))
}

func (s *faultyStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := s.before(ctx); err != nil {
		return nil, err
	}
	return s.inner.Get(ctx, path)
}

func (s *faultyStore) Exists(ctx context.Context, path string) (bool, error) {
	if err := s.before(ctx); err != nil {
		return false, err
	}
	return s.inner.Exists(ctx, path)
}

func (s *faultyStore) List(ctx context.Context, prefix string) ([]string, error) {
	if err := s.before(ctx); err != nil {
		return nil, err
	}
	keys, err := s.inner.List(ctx, prefix)
	if err != nil || s.faults.PListDrop == 0 {
		return keys, err
	}
	kept := keys[:0:0]
	for _, k := range keys {
		if !s.chance(s.faults.PListDrop) {
			kept = append(kept, k)
		}
	}
	return kept, nil
}

func (s *faultyStore) Delete(ctx context.Context, path string) error {
	if err := s.before(ctx); err != nil {
		return err
	}
	return s.after(s.inner.Delete(ctx, path))
}

func (s *faultyStore) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	if err := s.before(ctx); err != nil {
		return nil, err
	}
	return s.inner.ReadRange(ctx, path, offset, length)
}

func (s *faultyStore) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	if err := s.before(ctx); err != nil {
		return nil, err
	}
	return s.inner.ReaderAt(ctx, path)
}

// faultyCASStore is a faultyStore over a store that implements
// ConditionalWriter.
type faultyCASStore struct {
	*faultyStore
	cas lode.ConditionalWriter
}

func (s *faultyCASStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	if err := s.before(ctx); err != nil {
		return err
	}
	return s.after(s.cas.CompareAndSwap(ctx, path, expected, replacement))
}

// injected reports whether err comes from an injected fault, and is
// therefore an expected outcome rather than a violation.
func injected(err error) bool {
	return errors.Is(err, errInjected) || errors.Is(err, errKilled)
}

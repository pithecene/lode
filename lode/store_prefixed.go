package lode

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
)

// Prefixed returns a factory whose store keeps every object of the store
// that factory creates under prefix (CONTRACT_STORAGE §Prefixed Stores).
// A key k addresses the object prefix/k, a listing returns keys without
// the prefix, and objects outside the prefix are invisible.
//
// The store applies the key rules of §Put to each key before it adds the
// prefix, so a key such as "../x" is refused with ErrInvalidPath rather
// than reaching outside the prefix. The prefix itself must be non-empty
// and must not escape the root, or the factory returns ErrInvalidArgument
// when it is called, before it calls factory.
//
// The factory may create any RangeStore implementation, such as the
// *s3.Store that Use(s3Store) returns a factory of.
func Prefixed[S RangeStore](factory Factory[S], prefix string) Factory[RangeStore] {
	return func() (RangeStore, error) {
		if factory == nil {
			return nil, validate.Argument("store factory must not be nil")
		}
		p, ok := storepath.Prefix(prefix)
		if !ok || p == "" {
			return nil, validate.Argument("store prefix %q must name a location inside the root", prefix)
		}
		if !strings.HasSuffix(p, "/") {
			p += "/"
		}
		inner, err := factory()
		if err != nil {
			return nil, err
		}
		if validate.Nil(inner) { // a typed nil too, which would panic at first use
			return nil, validate.Argument("store factory returned nil store")
		}
		return &prefixedStore{inner: inner, prefix: p}, nil
	}
}

// prefixedStore maps every key of one store under a prefix of another.
// It is safe for concurrent use when its inner store is.
type prefixedStore struct {
	inner  RangeStore
	prefix string // ends with "/"
}

// key returns the inner key for a key of this store, or ErrInvalidPath.
func (s *prefixedStore) key(k string) (string, error) {
	cleaned, ok := storepath.Key(k)
	if !ok {
		return "", fmt.Errorf("lode: invalid key %q: %w", k, ErrInvalidPath)
	}
	return s.prefix + cleaned, nil
}

func (s *prefixedStore) Put(ctx context.Context, path string, r io.Reader) error {
	k, err := s.key(path)
	if err != nil {
		return err
	}
	return s.inner.Put(ctx, k, r)
}

func (s *prefixedStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	k, err := s.key(path)
	if err != nil {
		return nil, err
	}
	return s.inner.Get(ctx, k)
}

func (s *prefixedStore) Exists(ctx context.Context, path string) (bool, error) {
	k, err := s.key(path)
	if err != nil {
		return false, err
	}
	return s.inner.Exists(ctx, k)
}

func (s *prefixedStore) Delete(ctx context.Context, path string) error {
	k, err := s.key(path)
	if err != nil {
		return err
	}
	return s.inner.Delete(ctx, k)
}

// List lists the inner store under the prefix and returns the keys without
// it. Every key the inner store returns begins with the prefix it was given.
func (s *prefixedStore) List(ctx context.Context, prefix string) ([]string, error) {
	p, ok := storepath.Prefix(prefix)
	if !ok {
		return nil, fmt.Errorf("lode: invalid prefix %q: %w", prefix, ErrInvalidPath)
	}
	keys, err := s.inner.List(ctx, s.prefix+p)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		rel, found := strings.CutPrefix(k, s.prefix)
		if !found {
			return nil, fmt.Errorf("lode: store listed %q outside the prefix %q", k, s.prefix)
		}
		out = append(out, rel)
	}
	return out, nil
}

func (s *prefixedStore) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	k, err := s.key(path)
	if err != nil {
		return nil, err
	}
	return s.inner.ReadRange(ctx, k, offset, length)
}

func (s *prefixedStore) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	k, err := s.key(path)
	if err != nil {
		return nil, err
	}
	return s.inner.ReaderAt(ctx, k)
}

func (s *prefixedStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	k, err := s.key(path)
	if err != nil {
		return err
	}
	return s.inner.CompareAndSwap(ctx, k, expected, replacement)
}

var _ RangeStore = (*prefixedStore)(nil)

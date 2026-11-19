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
// the prefix, and objects outside the prefix are invisible. When the inner
// store implements PrefixLister, the prefixed store preserves it.
//
// The store applies the key rules of §Put to each key before it adds the
// prefix, so a key such as "../x" is refused with ErrInvalidPath rather
// than reaching outside the prefix. The prefix itself must be non-empty
// and must not escape the root, or the factory returns ErrInvalidArgument
// when it is called, before it calls factory.
//
// The factory may create any RangeStore implementation, such as s3.Bucket
// or Use(s3Store).
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
		if validate.Nil(inner) {
			return nil, validate.Argument("store factory returned nil store")
		}
		base := &prefixedStore{inner: inner, prefix: p}
		if prefixes, ok := any(inner).(PrefixLister); ok {
			return &prefixedPrefixStore{prefixedStore: base, prefixes: prefixes}, nil
		}
		return base, nil
	}
}

// prefixedStore maps every key of one store under a prefix of another.
// It is safe for concurrent use when its inner store is.
type prefixedStore struct {
	inner  RangeStore
	prefix string // ends with "/"
}

type prefixedPrefixStore struct {
	*prefixedStore
	prefixes PrefixLister
}

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

func (s *prefixedStore) listTrimmed(prefix, label string, fetch func(string) ([]string, error)) ([]string, error) {
	p, ok := storepath.Prefix(prefix)
	if !ok {
		return nil, fmt.Errorf("lode: invalid prefix %q: %w", prefix, ErrInvalidPath)
	}
	items, err := fetch(s.prefix + p)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		rel, found := strings.CutPrefix(item, s.prefix)
		if !found {
			return nil, fmt.Errorf("lode: store listed %s%q outside the prefix %q", label, item, s.prefix)
		}
		out = append(out, rel)
	}
	return out, nil
}

func (s *prefixedStore) List(ctx context.Context, prefix string) ([]string, error) {
	return s.listTrimmed(prefix, "", func(full string) ([]string, error) {
		return s.inner.List(ctx, full)
	})
}

func (s *prefixedPrefixStore) ListPrefixes(ctx context.Context, prefix string) ([]string, error) {
	return s.listTrimmed(prefix, "prefix ", func(full string) ([]string, error) {
		return s.prefixes.ListPrefixes(ctx, full)
	})
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

var (
	_ RangeStore   = (*prefixedStore)(nil)
	_ RangeStore   = (*prefixedPrefixStore)(nil)
	_ PrefixLister = (*prefixedPrefixStore)(nil)
)

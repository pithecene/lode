package lode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// SingleProcess returns store with a CompareAndSwap that serializes updates
// through a lock held by this process. It is the explicit escape hatch for a
// backend that offers no conditional write.
//
// CAS through the returned store is correct only while every writer to the
// store runs in this process and uses this one returned value. Writers in
// other processes, or a second SingleProcess wrapping the same backend, are
// not serialized and can fork a history.
//
// Reads through the returned store wait for a replacement in progress, so a
// reader in this process observes the old or the new content of a path,
// never its absence between the Delete and the Put.
//
// A store that already implements ConditionalWriter is returned unchanged,
// and a nil store is returned as nil, so that a constructor rejects it.
func SingleProcess(store Store) Store {
	if store == nil {
		return nil
	}
	if _, ok := store.(ConditionalWriter); ok {
		return store
	}
	return &singleProcessStore{Store: store}
}

// singleProcessStore implements CompareAndSwap over a store without one.
// CompareAndSwap holds mu exclusively; the reads hold it shared.
type singleProcessStore struct {
	Store
	mu sync.RWMutex
}

// Get reads path once no replacement is in progress.
func (s *singleProcessStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Store.Get(ctx, path)
}

// Exists reports whether path exists once no replacement is in progress.
func (s *singleProcessStore) Exists(ctx context.Context, path string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Store.Exists(ctx, path)
}

// List lists prefix once no replacement is in progress.
func (s *singleProcessStore) List(ctx context.Context, prefix string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Store.List(ctx, prefix)
}

// ReadRange reads a range of path once no replacement is in progress.
func (s *singleProcessStore) ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Store.ReadRange(ctx, path, offset, length)
}

// ReaderAt opens path once no replacement is in progress.
func (s *singleProcessStore) ReaderAt(ctx context.Context, path string) (io.ReaderAt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Store.ReaderAt(ctx, path)
}

// CompareAndSwap applies the ConditionalWriter semantics under the process
// lock. The replacement is a Delete followed by a Put: if the Put fails, the
// path is left absent, and the next Latest recreates a missing pointer.
func (s *singleProcessStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, exists, err := s.read(ctx, path)
	if err != nil {
		return err
	}
	if (exists && current != expected) || (!exists && expected != "") {
		return ErrSnapshotConflict
	}
	if exists {
		if err := s.Delete(ctx, path); err != nil {
			return fmt.Errorf("lode: single-process cas delete: %w", err)
		}
	}
	if err := s.Put(ctx, path, strings.NewReader(replacement)); err != nil {
		return fmt.Errorf("lode: single-process cas put: %w", err)
	}
	return nil
}

// read returns the content at path and whether it exists. The caller holds
// mu exclusively, so it reads through the wrapped store.
func (s *singleProcessStore) read(ctx context.Context, path string) (string, bool, error) {
	rc, err := s.Store.Get(ctx, path)
	if errors.Is(err, ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("lode: single-process cas read: %w", err)
	}
	defer closer(rc)()
	data, err := io.ReadAll(rc)
	if err != nil {
		return "", false, fmt.Errorf("lode: single-process cas read: %w", err)
	}
	return string(data), true, nil
}

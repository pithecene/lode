package lode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// SingleProcess returns a Store whose CompareAndSwap serializes updates
// through a lock held by this process. It is the explicit escape hatch for a
// backend that offers no conditional write.
//
// CAS through the returned store is correct only while every writer to the
// store runs in this process and uses this one returned value. Writers in
// other processes, or a second SingleProcess wrapping the same backend, are
// not serialized and can fork a history.
//
// A store that already implements ConditionalWriter is returned unchanged.
func SingleProcess(store ObjectStore) Store {
	if s, ok := store.(Store); ok {
		return s
	}
	return &singleProcessStore{ObjectStore: store}
}

// singleProcessStore implements CompareAndSwap over a store without one.
type singleProcessStore struct {
	ObjectStore
	mu sync.Mutex
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

// read returns the content at path and whether it exists.
func (s *singleProcessStore) read(ctx context.Context, path string) (string, bool, error) {
	rc, err := s.Get(ctx, path)
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

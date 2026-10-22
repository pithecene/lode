//go:build unix || windows

package lode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// CompareAndSwap atomically replaces the content at path if and only if
// the current content matches expected. See ConditionalWriter for semantics.
//
// It holds an exclusive lock on a companion .lock file (lockExclusive: flock
// on Unix, LockFileEx on Windows) and writes through a temporary file and a
// rename under the lock.
func (f *fsStore) CompareAndSwap(_ context.Context, path, expected, replacement string) error {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return err
	}

	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("lode: cas mkdir: %w", err)
	}

	// Acquire advisory lock on companion .lock file.
	lockPath := fullPath + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("lode: open lock file: %w", err)
	}
	defer closer(lockFile)()

	unlock, err := lockExclusive(lockFile)
	if err != nil {
		return fmt.Errorf("lode: lock: %w", err)
	}
	defer unlock()

	// Under lock: read current content and compare.
	current, err := os.ReadFile(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("lode: cas read pointer: %w", err)
	}
	fileExists := err == nil

	switch {
	case !fileExists && expected == "":
		// First commit: create via temp-file + rename.
	case !fileExists:
		return ErrSnapshotConflict
	case string(current) == expected:
		// Content matches: replace via temp-file + rename.
	default:
		return ErrSnapshotConflict
	}

	// Atomic write: temp file in same directory, then rename.
	tmp, err := os.CreateTemp(dir, tempPrefix+"cas-*")
	if err != nil {
		return fmt.Errorf("lode: create temp file: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.WriteString(replacement); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("lode: cas write temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("lode: cas close temp: %w", err)
	}

	// replaceFile replaces the pointer even while a reader holds it open,
	// which os.Rename cannot do on Windows.
	if err := replaceFile(tmpName, fullPath); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("lode: cas rename: %w", err)
	}

	return nil
}

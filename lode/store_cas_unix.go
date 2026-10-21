//go:build unix

package lode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// CompareAndSwap atomically replaces the content at path if and only if
// the current content matches expected. See ConditionalWriter for semantics.
//
// Uses flock advisory locking with a companion .lock file and temp-file+rename
// for atomic writes under the lock. Unix-only (syscall.Flock).
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

	if err := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lode: flock: %w", err)
	}
	defer func() { _ = syscall.Flock(int(lockFile.Fd()), syscall.LOCK_UN) }()

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

	if err := os.Rename(tmpName, fullPath); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("lode: cas rename: %w", err)
	}

	return nil
}

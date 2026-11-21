package lode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pithecene/lode/internal/errs"
)

// CompareAndSwap atomically replaces the content at path if and only if
// the current content matches expected. See ConditionalWriter for semantics.
//
// It holds an exclusive lock on a companion .lock file (lockExclusive: flock
// on Unix, LockFileEx on Windows) while honoring ctx cancellation, and writes
// through a temporary file and an atomic rename under the lock.
func (f *fsStore) CompareAndSwap(ctx context.Context, path, expected, replacement string) error {
	fullPath, err := f.safePathForFile(path)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	dir := filepath.Dir(fullPath)
	existing, err := f.prepareDir(dir)
	if err != nil {
		return err
	}

	// Acquire advisory lock on companion .lock file.
	lockPath := fullPath + ".lock"
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("fs: open lock file: %w", err)
	}
	defer closer(lockFile)()

	unlock, err := lockExclusive(ctx, lockFile)
	if err != nil {
		return fmt.Errorf("fs: lock: %w", err)
	}
	defer unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	// Under lock: read current content and compare.
	current, err := os.ReadFile(fullPath)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("fs: cas read pointer: %w", err)
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
		return fmt.Errorf("fs: create temp file: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.WriteString(replacement); err != nil {
		errs.DiscardClose(tmp)
		errs.IgnoreError(os.Remove(tmpName))
		return fmt.Errorf("fs: cas write temp: %w", err)
	}
	if f.sync {
		if err := tmp.Sync(); err != nil {
			errs.DiscardClose(tmp)
			errs.IgnoreError(os.Remove(tmpName))
			return fmt.Errorf("fs: cas sync temp: %w", err)
		}
	}
	if err := tmp.Close(); err != nil {
		errs.IgnoreError(os.Remove(tmpName))
		return fmt.Errorf("fs: cas close temp: %w", err)
	}

	if err := ctx.Err(); err != nil {
		errs.IgnoreError(os.Remove(tmpName))
		return err
	}

	// replaceFile replaces the pointer even while a reader holds it open,
	// which os.Rename cannot do on Windows.
	if err := replaceFile(tmpName, fullPath); err != nil {
		errs.IgnoreError(os.Remove(tmpName))
		return fmt.Errorf("fs: cas rename: %w", err)
	}
	return f.syncPublishedDir(existing, dir)
}

//go:build unix || windows

package lode

import (
	"errors"
	"io"
	"os"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// -----------------------------------------------------------------------------
// CompareAndSwap tests — FS store (Unix and Windows)
// -----------------------------------------------------------------------------

func TestFSStore_CompareAndSwap(t *testing.T) {
	t.Run("create when empty", func(t *testing.T) {
		ctx := t.Context()
		tmpDir, err := os.MkdirTemp("", "lode-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer testutil.RemoveAll(tmpDir)

		store, err := NewFS(tmpDir)
		if err != nil {
			t.Fatal(err)
		}
		cw := store.(ConditionalWriter)

		// Empty expected on new path should create.
		if err := cw.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
			t.Fatalf("CompareAndSwap create failed: %v", err)
		}

		// Verify content was written.
		rc, err := store.Get(ctx, "ptr/latest")
		if err != nil {
			t.Fatalf("Get after CAS create failed: %v", err)
		}
		defer closer(rc)()
		data, _ := io.ReadAll(rc)
		if string(data) != "snap-1" {
			t.Errorf("expected 'snap-1', got %q", string(data))
		}
	})

	t.Run("update match", func(t *testing.T) {
		ctx := t.Context()
		tmpDir, err := os.MkdirTemp("", "lode-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer testutil.RemoveAll(tmpDir)

		store, err := NewFS(tmpDir)
		if err != nil {
			t.Fatal(err)
		}
		cw := store.(ConditionalWriter)

		// Create initial content.
		if err := cw.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
			t.Fatalf("initial create failed: %v", err)
		}

		// Update with matching expected.
		if err := cw.CompareAndSwap(ctx, "ptr/latest", "snap-1", "snap-2"); err != nil {
			t.Fatalf("CompareAndSwap update failed: %v", err)
		}

		// Verify content was updated.
		rc, err := store.Get(ctx, "ptr/latest")
		if err != nil {
			t.Fatalf("Get after CAS update failed: %v", err)
		}
		defer closer(rc)()
		data, _ := io.ReadAll(rc)
		if string(data) != "snap-2" {
			t.Errorf("expected 'snap-2', got %q", string(data))
		}
	})

	t.Run("conflict mismatch", func(t *testing.T) {
		ctx := t.Context()
		tmpDir, err := os.MkdirTemp("", "lode-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer testutil.RemoveAll(tmpDir)

		store, err := NewFS(tmpDir)
		if err != nil {
			t.Fatal(err)
		}
		cw := store.(ConditionalWriter)

		// Create initial content.
		if err := cw.CompareAndSwap(ctx, "ptr/latest", "", "snap-1"); err != nil {
			t.Fatalf("initial create failed: %v", err)
		}

		// Attempt update with wrong expected.
		err = cw.CompareAndSwap(ctx, "ptr/latest", "stale", "snap-2")
		if !errors.Is(err, ErrSnapshotConflict) {
			t.Errorf("expected ErrSnapshotConflict, got: %v", err)
		}

		// Verify content unchanged.
		rc, err := store.Get(ctx, "ptr/latest")
		if err != nil {
			t.Fatalf("Get after conflict: %v", err)
		}
		defer closer(rc)()
		data, _ := io.ReadAll(rc)
		if string(data) != "snap-1" {
			t.Errorf("expected 'snap-1' (unchanged), got %q", string(data))
		}
	})

	t.Run("conflict not exist", func(t *testing.T) {
		ctx := t.Context()
		tmpDir, err := os.MkdirTemp("", "lode-test-*")
		if err != nil {
			t.Fatal(err)
		}
		defer testutil.RemoveAll(tmpDir)

		store, err := NewFS(tmpDir)
		if err != nil {
			t.Fatal(err)
		}
		cw := store.(ConditionalWriter)

		// Non-empty expected on missing path should return conflict.
		err = cw.CompareAndSwap(ctx, "ptr/latest", "snap-0", "snap-1")
		if !errors.Is(err, ErrSnapshotConflict) {
			t.Errorf("expected ErrSnapshotConflict, got: %v", err)
		}
	})
}

// Compile-time interface assertion (Unix-only: fsStore only implements
// ConditionalWriter on Unix where flock is available).
var _ ConditionalWriter = (*fsStore)(nil)

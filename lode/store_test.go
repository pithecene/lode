package lode

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"slices"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// -----------------------------------------------------------------------------
// G1: Immutability tests - Put returns ErrPathExists on overwrite
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// G5: Range read tests
// -----------------------------------------------------------------------------

// Memory store tests

// -----------------------------------------------------------------------------
// List empty prefix tests (empty dataset/storage semantics)
// -----------------------------------------------------------------------------

// storeBackend opens one built-in Store, fresh for each subtest.
type storeBackend struct {
	name string
	open func(t *testing.T) Store
}

// storeBackends are the built-in stores every shared store test runs on.
var storeBackends = []storeBackend{
	{name: "fs", open: openFSStore},
	{name: "memory", open: func(*testing.T) Store { return NewMemory() }},
}

// openFSStore is an FS store over a fresh temporary directory.
func openFSStore(t *testing.T) Store {
	t.Helper()
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { testutil.RemoveAll(tmpDir) })
	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// forEachStore runs test once per built-in store, as a subtest named for it.
func forEachStore(t *testing.T, test func(t *testing.T, store Store)) {
	t.Helper()
	for _, b := range storeBackends {
		t.Run(b.name, func(t *testing.T) { test(t, b.open(t)) })
	}
}

func TestStore_Put(t *testing.T) {
	t.Run("err path exists", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			// First write should succeed
			err := store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("hello")))
			if err != nil {
				t.Fatalf("first Put failed: %v", err)
			}

			// Second write to same path should return ErrPathExists
			err = store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("world")))
			if !errors.Is(err, ErrPathExists) {
				t.Errorf("expected ErrPathExists, got: %v", err)
			}
		})
	})

	t.Run("all or nothing", func(t *testing.T) { forEachStore(t, assertPutAllOrNothing) })
}

func TestStore_Get(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			_, err := store.Get(ctx, "nonexistent.txt")
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("expected ErrNotFound, got: %v", err)
			}
		})
	})

	t.Run("invalid path", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			_, err := store.Get(ctx, "../escape")
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("expected ErrInvalidPath, got: %v", err)
			}
		})
	})
}

func TestStore_Exists(t *testing.T) {
	t.Run("true", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			err := store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("hello")))
			if err != nil {
				t.Fatal(err)
			}

			exists, err := store.Exists(ctx, "test/file.txt")
			if err != nil {
				t.Fatalf("Exists failed: %v", err)
			}
			if !exists {
				t.Error("expected file to exist")
			}
		})
	})

	t.Run("false", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			exists, err := store.Exists(ctx, "nonexistent/file.txt")
			if err != nil {
				t.Fatalf("Exists failed: %v", err)
			}
			if exists {
				t.Error("expected file to not exist")
			}
		})
	})

	t.Run("invalid path", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			_, err := store.Exists(ctx, "../escape")
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("expected ErrInvalidPath, got: %v", err)
			}
		})
	})
}

func TestStore_Delete(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			err := store.Put(ctx, "test/delete.txt", bytes.NewReader([]byte("hello")))
			if err != nil {
				t.Fatal(err)
			}

			err = store.Delete(ctx, "test/delete.txt")
			if err != nil {
				t.Fatalf("Delete failed: %v", err)
			}

			exists, err := store.Exists(ctx, "test/delete.txt")
			if err != nil {
				t.Fatal(err)
			}
			if exists {
				t.Error("expected file to be deleted")
			}
		})
	})

	// Retention deletes objects that readers may still hold open; on Windows
	// that requires the readers to grant delete sharing.
	t.Run("while a reader holds the object", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()
			if err := store.Put(ctx, "test/held.txt", bytes.NewReader([]byte("hello"))); err != nil {
				t.Fatal(err)
			}
			held, err := store.Get(ctx, "test/held.txt")
			if err != nil {
				t.Fatal(err)
			}
			defer closer(held)()

			if err := store.Delete(ctx, "test/held.txt"); err != nil {
				t.Fatalf("Delete with an open reader: %v", err)
			}
			if exists, err := store.Exists(ctx, "test/held.txt"); err != nil || exists {
				t.Errorf("Exists after Delete = %v, %v; want false", exists, err)
			}
		})
	})

	t.Run("not exist", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			err := store.Delete(ctx, "nonexistent.txt")
			if err != nil {
				t.Errorf("Delete nonexistent should not error, got: %v", err)
			}
		})
	})
}

func TestStore_List(t *testing.T) {
	t.Run("non-existent prefix returns empty", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			// List a prefix that doesn't exist - should return empty list, not error
			paths, err := store.List(ctx, "nonexistent/prefix")
			if err != nil {
				t.Fatalf("expected no error for non-existent prefix, got: %v", err)
			}
			if len(paths) != 0 {
				t.Errorf("expected empty list, got: %v", paths)
			}
		})
	})

	t.Run("specific prefix", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			for _, key := range []string{"a/file1.txt", "a/file2.txt", "b/file3.txt"} {
				if err := store.Put(ctx, key, bytes.NewReader([]byte("data"))); err != nil {
					t.Fatal(err)
				}
			}

			paths, err := store.List(ctx, "a/")
			if err != nil {
				t.Fatalf("List failed: %v", err)
			}
			if len(paths) != 2 {
				t.Errorf("expected 2 paths under 'a/', got %d: %v", len(paths), paths)
			}
		})
	})

	t.Run("traversal prefix", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			_, err := store.List(ctx, "../escape")
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("expected ErrInvalidPath for traversal prefix, got: %v", err)
			}
		})
	})

	t.Run("string prefix semantics", func(t *testing.T) { forEachStore(t, assertStringPrefixSemantics) })
}

func TestStore_ReadRange(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			content := []byte("hello world")
			err := store.Put(ctx, "test.txt", bytes.NewReader(content))
			if err != nil {
				t.Fatal(err)
			}

			data, err := store.ReadRange(ctx, "test.txt", 6, 5)
			if err != nil {
				t.Fatalf("ReadRange failed: %v", err)
			}
			if string(data) != "world" {
				t.Errorf("expected 'world', got %q", string(data))
			}
		})
	})

	t.Run("beyond EOF", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			content := []byte("hello")
			err := store.Put(ctx, "test.txt", bytes.NewReader(content))
			if err != nil {
				t.Fatal(err)
			}

			data, err := store.ReadRange(ctx, "test.txt", 3, 100)
			if err != nil {
				t.Fatalf("ReadRange failed: %v", err)
			}
			if string(data) != "lo" {
				t.Errorf("expected 'lo', got %q", string(data))
			}
		})
	})

	t.Run("offset beyond EOF", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			content := []byte("hello")
			err := store.Put(ctx, "test.txt", bytes.NewReader(content))
			if err != nil {
				t.Fatal(err)
			}

			data, err := store.ReadRange(ctx, "test.txt", 100, 10)
			if err != nil {
				t.Fatalf("ReadRange failed: %v", err)
			}
			if len(data) != 0 {
				t.Errorf("expected empty slice, got %d bytes", len(data))
			}
		})
	})

	t.Run("not found", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			_, err := store.ReadRange(ctx, "nonexistent.txt", 0, 10)
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("expected ErrNotFound, got: %v", err)
			}
		})
	})
}

func TestStore_ReadRange_Bounds(t *testing.T) {
	t.Run("negative offset", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			_, err := store.ReadRange(ctx, "test.txt", -1, 10)
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("expected ErrInvalidPath for negative offset, got: %v", err)
			}
		})
	})

	t.Run("negative length", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			_, err := store.ReadRange(ctx, "test.txt", 0, -1)
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("expected ErrInvalidPath for negative length, got: %v", err)
			}
		})
	})

	t.Run("length overflow", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			// This test validates 32-bit platform protection.
			// On 64-bit systems, math.MaxInt == math.MaxInt64, so the overflow check
			// cannot trigger. Skip on 64-bit.
			if math.MaxInt == math.MaxInt64 {
				t.Skip("length overflow check only applies to 32-bit platforms")
			}

			ctx := t.Context()

			_, err := store.ReadRange(ctx, "test.txt", 0, math.MaxInt64)
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("expected ErrInvalidPath for length overflow, got: %v", err)
			}
		})
	})

	t.Run("offset plus length overflow", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			_, err := store.ReadRange(ctx, "test.txt", math.MaxInt64-10, 20)
			if !errors.Is(err, ErrInvalidPath) {
				t.Errorf("expected ErrInvalidPath for offset+length overflow, got: %v", err)
			}
		})
	})

	t.Run("exact bounds", func(t *testing.T) { forEachStore(t, assertReadRangeBounds) })
}

func TestStore_ReaderAt(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			content := []byte("hello world")
			err := store.Put(ctx, "test.txt", bytes.NewReader(content))
			if err != nil {
				t.Fatal(err)
			}

			ra, err := store.ReaderAt(ctx, "test.txt")
			if err != nil {
				t.Fatalf("ReaderAt failed: %v", err)
			}
			// Close if the ReaderAt implements io.Closer (consistency with fsStore pattern)
			if c, ok := ra.(io.Closer); ok {
				defer closer(c)()
			}

			buf := make([]byte, 5)
			n, err := ra.ReadAt(buf, 6)
			if err != nil {
				t.Fatalf("ReadAt failed: %v", err)
			}
			if n != 5 || string(buf) != "world" {
				t.Errorf("expected 'world', got %q", string(buf[:n]))
			}
		})
	})

	t.Run("not found", func(t *testing.T) {
		forEachStore(t, func(t *testing.T, store Store) {
			ctx := t.Context()

			_, err := store.ReaderAt(ctx, "nonexistent.txt")
			if !errors.Is(err, ErrNotFound) {
				t.Errorf("expected ErrNotFound, got: %v", err)
			}
		})
	})
}

// -----------------------------------------------------------------------------
// CompareAndSwap conformance
// -----------------------------------------------------------------------------

// casBackends are the stores whose CompareAndSwap the conformance test
// checks: the built-in stores, and SingleProcess over a store without CAS.
var casBackends = append(slices.Clone(storeBackends), storeBackend{
	name: "single-process",
	open: func(*testing.T) Store { return SingleProcess(testutil.WithoutCAS(NewMemory())) },
})

// TestStore_CompareAndSwap checks the ConditionalWriter semantics table
// (CONTRACT_STORAGE §ConditionalWriter Capability) on every store with CAS.
// The steps run in order on one path; want is the content afterwards, ""
// meaning absent.
func TestStore_CompareAndSwap(t *testing.T) {
	steps := []struct {
		name, expected, replacement string
		wantErr                     error
		want                        string
	}{
		{"missing path, non-empty expected conflicts", "snap-0", "snap-1", ErrSnapshotConflict, ""},
		{"missing path, empty expected creates", "", "snap-1", nil, "snap-1"},
		{"existing path, empty expected conflicts", "", "snap-x", ErrSnapshotConflict, "snap-1"},
		{"matching expected replaces", "snap-1", "snap-2", nil, "snap-2"},
		{"mismatched expected conflicts", "stale", "snap-3", ErrSnapshotConflict, "snap-2"},
	}
	for _, b := range casBackends {
		t.Run(b.name, func(t *testing.T) {
			store := b.open(t)
			for _, s := range steps {
				err := store.CompareAndSwap(t.Context(), "ptr/latest", s.expected, s.replacement)
				if !errors.Is(err, s.wantErr) {
					t.Fatalf("%s: err = %v, want %v", s.name, err, s.wantErr)
				}
				if got := readOrAbsent(t, store, "ptr/latest"); got != s.want {
					t.Fatalf("%s: content = %q, want %q", s.name, got, s.want)
				}
			}
		})
	}
}

// readOrAbsent returns the content at path, or "" when the path is absent.
func readOrAbsent(t *testing.T, store Store, path string) string {
	t.Helper()
	rc, err := store.Get(t.Context(), path)
	if errors.Is(err, ErrNotFound) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	defer closer(rc)()
	data, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// -----------------------------------------------------------------------------
// Compile-time interface assertions
// -----------------------------------------------------------------------------

// The fsStore ConditionalWriter assertion is in store_cas_test.go: fsStore
// implements ConditionalWriter on Unix and Windows.
var _ ConditionalWriter = (*memoryStore)(nil)

package lode

import (
	"bytes"
	"errors"
	"os"
	"testing"

	"github.com/pithecene-io/lode/internal/testutil"
)

// -----------------------------------------------------------------------------
// FS Exists() coverage
// -----------------------------------------------------------------------------

func TestFSStore_Exists_True(t *testing.T) {
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

	err = store.Put(ctx, "test/file.txt", bytes.NewReader([]byte("hello")))
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
}

func TestFSStore_Exists_False(t *testing.T) {
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

	exists, err := store.Exists(ctx, "nonexistent/file.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected file to not exist")
	}
}

func TestFSStore_Exists_InvalidPath(t *testing.T) {
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

	_, err = store.Exists(ctx, "../escape")
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// FS List() coverage
// -----------------------------------------------------------------------------

func TestFSStore_List_SpecificPrefix(t *testing.T) {
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

	// Write files under different prefixes
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
}

func TestFSStore_List_TraversalPrefix(t *testing.T) {
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

	_, err = store.List(ctx, "../escape")
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for traversal prefix, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Path safety — table-driven tests
// -----------------------------------------------------------------------------

func TestFSStore_SafePathForFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	fs := store.(*fsStore)

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"empty path", "", true},
		{"parent traversal", "..", true},
		{"parent traversal prefix", "../secret", true},
		{"leading slash", "/etc/passwd", true},
		{"dot path", ".", true},
		{"double slash normalized", "a//b/c.txt", false},
		{"valid nested", "datasets/test/file.txt", false},
		{"valid single", "file.txt", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := fs.safePathForFile(tt.path)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestFSStore_SafePathForPrefix(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "lode-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer testutil.RemoveAll(tmpDir)

	store, err := NewFS(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	fs := store.(*fsStore)

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"empty prefix", "", false},
		{"parent traversal", "..", true},
		{"parent traversal prefix", "../escape", true},
		{"leading slash", "/absolute", true},
		{"dot prefix", ".", false},
		{"valid prefix", "datasets/test/", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := fs.safePathForPrefix(tt.path)
			if tt.wantErr && err == nil {
				t.Error("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// -----------------------------------------------------------------------------
// FS Get error paths
// -----------------------------------------------------------------------------

func TestFSStore_Get_NotFound(t *testing.T) {
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

	_, err = store.Get(ctx, "nonexistent.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestFSStore_Get_InvalidPath(t *testing.T) {
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

	_, err = store.Get(ctx, "../escape")
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// FS Delete coverage
// -----------------------------------------------------------------------------

func TestFSStore_Delete_Exists(t *testing.T) {
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

	err = store.Put(ctx, "test/delete.txt", bytes.NewReader([]byte("hello")))
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
}

func TestFSStore_Delete_NotExist(t *testing.T) {
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

	// Delete nonexistent should not error
	err = store.Delete(ctx, "nonexistent.txt")
	if err != nil {
		t.Errorf("Delete nonexistent should not error, got: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Memory store — mirror key FS tests
// -----------------------------------------------------------------------------

func TestMemoryStore_Exists_True(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

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
}

func TestMemoryStore_Exists_False(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	exists, err := store.Exists(ctx, "nonexistent/file.txt")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if exists {
		t.Error("expected file to not exist")
	}
}

func TestMemoryStore_Exists_InvalidPath(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	_, err := store.Exists(ctx, "../escape")
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got: %v", err)
	}
}

func TestMemoryStore_List_SpecificPrefix(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

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
}

func TestMemoryStore_List_EmptyPrefix(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	for _, key := range []string{"a/file1.txt", "b/file2.txt"} {
		if err := store.Put(ctx, key, bytes.NewReader([]byte("data"))); err != nil {
			t.Fatal(err)
		}
	}

	paths, err := store.List(ctx, "")
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(paths) != 2 {
		t.Errorf("expected 2 paths for empty prefix, got %d: %v", len(paths), paths)
	}
}

func TestMemoryStore_List_TraversalPrefix(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	_, err := store.List(ctx, "../escape")
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath for traversal prefix, got: %v", err)
	}
}

func TestMemoryStore_Get_NotFound(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	_, err := store.Get(ctx, "nonexistent.txt")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestMemoryStore_Get_InvalidPath(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	_, err := store.Get(ctx, "../escape")
	if !errors.Is(err, ErrInvalidPath) {
		t.Errorf("expected ErrInvalidPath, got: %v", err)
	}
}

func TestMemoryStore_Delete_Exists(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

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
}

func TestMemoryStore_Delete_NotExist(t *testing.T) {
	ctx := t.Context()
	store := NewMemory()

	err := store.Delete(ctx, "nonexistent.txt")
	if err != nil {
		t.Errorf("Delete nonexistent should not error, got: %v", err)
	}
}

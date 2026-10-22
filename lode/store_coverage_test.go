package lode

import (
	"bytes"
	"os"
	"testing"

	"github.com/pithecene/lode/internal/testutil"
)

// -----------------------------------------------------------------------------
// FS Exists() coverage
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// FS List() coverage
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Path safety — table-driven tests
// -----------------------------------------------------------------------------

func TestFSStore_SafePath(t *testing.T) {
	t.Run("for a file", func(t *testing.T) {
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
	})

	t.Run("for a prefix", func(t *testing.T) {
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
	})
}

// -----------------------------------------------------------------------------
// FS Get error paths
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// FS Delete coverage
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// Memory store — mirror key FS tests
// -----------------------------------------------------------------------------

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

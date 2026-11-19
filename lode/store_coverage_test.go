package lode

import (
	"bytes"
	"testing"
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
	store, err := NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fs := store.(*fsStore)
	type pathCase struct {
		name, path string
		wantErr    bool
	}
	for _, group := range []struct {
		name    string
		resolve func(string) (string, error)
		cases   []pathCase
	}{
		{"for a file", fs.safePathForFile, []pathCase{
			{"empty path", "", true},
			{"parent traversal", "..", true},
			{"parent traversal prefix", "../secret", true},
			{"leading slash", "/etc/passwd", true},
			{"dot path", ".", true},
			{"double slash normalized", "a//b/c.txt", false},
			{"valid nested", "datasets/test/file.txt", false},
			{"valid single", "file.txt", false},
		}},
		{"for a prefix", fs.safePathForPrefix, []pathCase{
			{"empty prefix", "", false},
			{"parent traversal", "..", true},
			{"parent traversal prefix", "../escape", true},
			{"leading slash", "/absolute", true},
			{"dot prefix", ".", false},
			{"valid prefix", "datasets/test/", false},
		}},
	} {
		for _, tc := range group.cases {
			t.Run(group.name+"/"+tc.name, func(t *testing.T) {
				if _, err := group.resolve(tc.path); (err != nil) != tc.wantErr {
					t.Errorf("resolve(%q) = %v, want error %t", tc.path, err, tc.wantErr)
				}
			})
		}
	}
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

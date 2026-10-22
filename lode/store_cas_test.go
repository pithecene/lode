//go:build unix || windows

package lode

import (
	"io"
	"strings"
	"testing"
)

// TestFSStore_CompareAndSwap_Replacement covers the replacements that are
// hardest on Windows: replacing a pointer that a reader holds open, which
// needs delete sharing on the reader and a POSIX-semantics rename, and
// replacing one beyond MAX_PATH (260), which needs extended-length paths in
// the Win32 calls that the store makes directly.
func TestFSStore_CompareAndSwap_Replacement(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		holdReader bool
	}{
		{"while a reader holds the pointer", "ptr/latest", true},
		{"at a path longer than MAX_PATH", strings.Repeat("segment-0123456789/", 16) + "latest", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			store, err := NewFS(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			cw := store.(ConditionalWriter)
			if err := cw.CompareAndSwap(ctx, tt.path, "", "snap-1"); err != nil {
				t.Fatal(err)
			}
			if tt.holdReader {
				held, err := store.Get(ctx, tt.path)
				if err != nil {
					t.Fatal(err)
				}
				defer closer(held)()
			}
			if err := cw.CompareAndSwap(ctx, tt.path, "snap-1", "snap-2"); err != nil {
				t.Fatalf("CompareAndSwap replacement: %v", err)
			}
			rc, err := store.Get(ctx, tt.path)
			if err != nil {
				t.Fatal(err)
			}
			defer closer(rc)()
			if data, _ := io.ReadAll(rc); string(data) != "snap-2" {
				t.Errorf("pointer = %q, want snap-2", data)
			}
		})
	}
}

// Compile-time interface assertion (Unix and Windows, where the filesystem
// store has an exclusive file lock: flock or LockFileEx).
var _ ConditionalWriter = (*fsStore)(nil)

package lode

import (
	"bytes"
	"context"
	"sort"
	"testing"
)

// List is a string-prefix match over paths on every store (CONTRACT_STORAGE
// §List): "a/b" matches "a/b/x" and "a/bc". The filesystem store used to treat
// the prefix as a directory, which made partial final tokens list nothing.

func seedPrefixFixture(t *testing.T, store Store) {
	t.Helper()
	ctx := t.Context()
	for _, p := range []string{
		"ds/partitions/day=2024-01-15/segments/1/manifest.json",
		"ds/partitions/day=2024-01-16/segments/2/manifest.json",
		"ds/partitions/day=2024-02-01/segments/3/manifest.json",
		"ds/partitions/day=2024-01-15/segments/1/data/f.jsonl",
		"ds/partitions/day=2024-01-150/segments/4/manifest.json", // sibling that shares the "day=2024-01-15" string prefix
		"other/partitions/day=2024-01-15/segments/9/manifest.json",
	} {
		if err := store.Put(ctx, p, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("put %s: %v", p, err)
		}
	}
}

func listSorted(ctx context.Context, t *testing.T, store Store, prefix string) []string {
	t.Helper()
	paths, err := store.List(ctx, prefix)
	if err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
	sort.Strings(paths)
	return paths
}

func assertStringPrefixSemantics(t *testing.T, store Store) {
	t.Helper()
	ctx := t.Context()
	seedPrefixFixture(t, store)

	// Partial final token: matches the January partitions, excludes February.
	got := listSorted(ctx, t, store, "ds/partitions/day=2024-01")
	want := []string{
		"ds/partitions/day=2024-01-15/segments/1/data/f.jsonl",
		"ds/partitions/day=2024-01-15/segments/1/manifest.json",
		"ds/partitions/day=2024-01-150/segments/4/manifest.json",
		"ds/partitions/day=2024-01-16/segments/2/manifest.json",
	}
	if !equalStrings(got, want) {
		t.Fatalf("partial-token prefix: got %v, want %v", got, want)
	}

	// Trailing slash is part of the prefix: "day=2024-01-15/" excludes the
	// "day=2024-01-150/" sibling.
	got = listSorted(ctx, t, store, "ds/partitions/day=2024-01-15/")
	want = []string{
		"ds/partitions/day=2024-01-15/segments/1/data/f.jsonl",
		"ds/partitions/day=2024-01-15/segments/1/manifest.json",
	}
	if !equalStrings(got, want) {
		t.Fatalf("trailing-slash prefix: got %v, want %v", got, want)
	}

	// Without the trailing slash the sibling is included: string semantics.
	got = listSorted(ctx, t, store, "ds/partitions/day=2024-01-15")
	if len(got) != 3 {
		t.Fatalf("no-slash prefix: got %d paths %v, want 3", len(got), got)
	}

	// A bare root name must not leak into sibling roots ("other/").
	got = listSorted(ctx, t, store, "ds")
	if len(got) != 5 {
		t.Fatalf("prefix \"ds\": got %d paths %v, want 5 under ds/", len(got), got)
	}

	// Nothing matches: empty, not an error.
	got = listSorted(ctx, t, store, "ds/partitions/day=2030")
	if len(got) != 0 {
		t.Fatalf("no-match prefix: got %v, want empty", got)
	}
}

func TestFSStore_List_StringPrefixSemantics(t *testing.T) {
	store, err := NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	assertStringPrefixSemantics(t, store)
}

func TestMemoryStore_List_StringPrefixSemantics(t *testing.T) {
	assertStringPrefixSemantics(t, NewMemory())
}

// TestFSStore_List_RootNormalizingPrefixes verifies prefixes that normalize to
// the store root ("", ".", "a/..") walk the root directly and never its parent.
func TestFSStore_List_RootNormalizingPrefixes(t *testing.T) {
	store, err := NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	seedPrefixFixture(t, store)
	ctx := t.Context()
	for _, prefix := range []string{"", ".", "ds/.."} {
		got := listSorted(ctx, t, store, prefix)
		if len(got) != 6 {
			t.Fatalf("List(%q): got %d paths %v, want all 6", prefix, len(got), got)
		}
	}
}

// TestFSStore_List_TrailingSlashOnRegularFile verifies List("a/") matches
// nothing when "a" is an object, not a directory: "a" does not start with "a/".
func TestFSStore_List_TrailingSlashOnRegularFile(t *testing.T) {
	store, err := NewFS(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	if err := store.Put(ctx, "a", bytes.NewReader([]byte("x"))); err != nil {
		t.Fatal(err)
	}
	if got := listSorted(ctx, t, store, "a/"); len(got) != 0 {
		t.Fatalf("List(\"a/\") over a regular file: got %v, want empty", got)
	}
	if got := listSorted(ctx, t, store, "a"); len(got) != 1 {
		t.Fatalf("List(\"a\"): got %v, want the object itself", got)
	}
}

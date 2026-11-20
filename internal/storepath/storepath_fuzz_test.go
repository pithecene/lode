package storepath

import (
	"strings"
	"testing"
)

// FuzzKey checks that every key Key accepts is canonical: it cannot escape
// the root, carries no backslash, leading or trailing slash, empty or dot
// segment, or reserved temporary leaf name, and normalizing it again changes
// nothing. Adapters rely on this to agree on what a key means.
func FuzzKey(f *testing.F) {
	for _, seed := range []string{"a/b", "/a", "a/../b", "../a", "a\\b", ".", "", "a//b/", "./a", "a/.lode-tmp-x"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, key string) {
		cleaned, ok := Key(key)
		if !ok {
			return
		}
		switch {
		case cleaned != key:
			t.Fatalf("Key(%q) = %q mutated a non-canonical key", key, cleaned)
		case cleaned == "" || cleaned == "." || cleaned == "..":
			t.Fatalf("Key(%q) accepted %q", key, cleaned)
		case strings.ContainsRune(cleaned, '\\'):
			t.Fatalf("Key(%q) = %q contains a backslash", key, cleaned)
		case strings.HasPrefix(cleaned, "/") || strings.HasSuffix(cleaned, "/"):
			t.Fatalf("Key(%q) = %q has a leading or trailing slash", key, cleaned)
		case Reserved(cleaned):
			t.Fatalf("Key(%q) = %q names a reserved temporary file", key, cleaned)
		}
		if again, ok := Key(cleaned); !ok || again != cleaned {
			t.Fatalf("Key is not idempotent: Key(%q) = %q, %v", cleaned, again, ok)
		}
	})
}

// FuzzPrefix checks that an accepted listing prefix is canonical and that
// validating it again changes nothing.
func FuzzPrefix(f *testing.F) {
	for _, seed := range []string{"a/", "a", "/a/", "a/../", "..", "a\\", "", "./"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, prefix string) {
		cleaned, ok := Prefix(prefix)
		if !ok {
			return
		}
		if cleaned != prefix || strings.ContainsRune(cleaned, '\\') || strings.HasPrefix(cleaned, "/") {
			t.Fatalf("Prefix(%q) = %q is not a canonical prefix", prefix, cleaned)
		}
		if again, ok := Prefix(cleaned); !ok || again != cleaned {
			t.Fatalf("Prefix is not idempotent: Prefix(%q) = %q, %v", cleaned, again, ok)
		}
	})
}

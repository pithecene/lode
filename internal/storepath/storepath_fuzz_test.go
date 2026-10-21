package storepath

import (
	"strings"
	"testing"
)

// FuzzKey checks that every key Key accepts is canonical: it cannot escape
// the root, carries no backslash or leading slash, names no reserved
// temporary file, and normalizing it again changes nothing. Adapters rely on this to agree on what a key means.
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
		case cleaned == "" || cleaned == ".":
			t.Fatalf("Key(%q) accepted the root as %q", key, cleaned)
		case strings.ContainsRune(cleaned, '\\'):
			t.Fatalf("Key(%q) = %q contains a backslash", key, cleaned)
		case strings.HasPrefix(cleaned, "/"):
			t.Fatalf("Key(%q) = %q has a leading slash", key, cleaned)
		case escapes(cleaned):
			t.Fatalf("Key(%q) = %q escapes the root", key, cleaned)
		case Reserved(cleaned):
			t.Fatalf("Key(%q) = %q names a reserved temporary file", key, cleaned)
		}
		if again, ok := Key(cleaned); !ok || again != cleaned {
			t.Fatalf("Key is not idempotent: Key(%q) = %q, %v", cleaned, again, ok)
		}
	})
}

// FuzzPrefix checks that an accepted listing prefix cannot escape the root
// and that normalizing it again changes nothing.
func FuzzPrefix(f *testing.F) {
	for _, seed := range []string{"a/", "a", "/a/", "a/../", "..", "a\\", "", "./"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, prefix string) {
		cleaned, ok := Prefix(prefix)
		if !ok {
			return
		}
		if escapes(strings.TrimSuffix(cleaned, "/")) || strings.ContainsRune(cleaned, '\\') || strings.HasPrefix(cleaned, "/") {
			t.Fatalf("Prefix(%q) = %q is not a safe prefix", prefix, cleaned)
		}
		if again, ok := Prefix(cleaned); !ok || again != cleaned {
			t.Fatalf("Prefix is not idempotent: Prefix(%q) = %q, %v", cleaned, again, ok)
		}
	})
}

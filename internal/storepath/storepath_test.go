package storepath

import (
	"math"
	"testing"
)

func TestKey(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"", "", false},
		{".", "", false},
		{"..", "", false},
		{"../secret", "", false},
		{"/", "", false},
		{"datasets/test/file.txt", "datasets/test/file.txt", true},
		{"file.txt", "file.txt", true},
		{"/file.txt", "file.txt", true},
		{"a/./b//c", "a/b/c", true},
		{"a/../b", "b", true}, // resolves inside the root
		{".lode-tmp-x", "", false},
		{"a/.lode-tmp-cas-1", "", false},
		{".lode-tmp-x/a", ".lode-tmp-x/a", true}, // only the final segment is reserved
	}
	for _, c := range cases {
		got, ok := Key(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("Key(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestPrefix(t *testing.T) {
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{"", "", true},
		{".", "", true},
		{"/", "", true},
		{"a/..", "", true},
		{"..", "", false},
		{"../escape", "", false},
		{"datasets/test/", "datasets/test/", true}, // trailing slash preserved
		{"datasets/test", "datasets/test", true},
		{"/day=2026-10", "day=2026-10", true},
	}
	for _, c := range cases {
		got, ok := Prefix(c.in)
		if ok != c.ok || got != c.want {
			t.Errorf("Prefix(%q) = %q, %v; want %q, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestRangeValid(t *testing.T) {
	cases := []struct {
		offset, length int64
		want           bool
	}{
		{0, 0, true},
		{0, MaxRangeLength, true},
		{math.MaxInt64 - 10, 10, true},
		{-1, 1, false},
		{0, -1, false},
		{math.MaxInt64 - 10, 11, false},
	}
	for _, c := range cases {
		if got := RangeValid(c.offset, c.length); got != c.want {
			t.Errorf("RangeValid(%d, %d) = %v, want %v", c.offset, c.length, got, c.want)
		}
	}
}

// A backslash is never a separator and never a key character: on Windows
// filepath would read `..\secret` as a parent reference, on Linux it would be
// an odd file name, and the adapters must agree. Both forms are rejected.
func TestKeyAndPrefix_RejectBackslash(t *testing.T) {
	for _, in := range []string{`..\secret`, `a\b`, `\`, `dir/..\x`} {
		if _, ok := Key(in); ok {
			t.Errorf("Key(%q) accepted", in)
		}
		if _, ok := Prefix(in); ok {
			t.Errorf("Prefix(%q) accepted", in)
		}
	}
}

func TestSegment(t *testing.T) {
	for _, s := range []string{"events", "tenant=acme", "a.b", "..a"} {
		if !Segment(s) {
			t.Errorf("Segment(%q) rejected", s)
		}
	}
	for _, s := range []string{"", ".", "..", "a/b", `a\b`, ReservedPrefix + "x"} {
		if Segment(s) {
			t.Errorf("Segment(%q) accepted", s)
		}
	}
}

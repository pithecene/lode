// Package storepath holds the rules every Store adapter applies to the keys
// and prefixes it is handed, so the fs, memory and S3 stores agree on what a
// valid object key is and what a listing prefix means (CONTRACT_STORAGE §Put,
// §List). Adapters map a rejection to lode.ErrInvalidPath.
package storepath

import (
	"math"
	"path"
	"strings"
)

// Key normalizes an object key: cleaned, slash-separated, no leading slash.
// It rejects the empty key, ".", a backslash anywhere (keys are
// slash-separated on every platform, so a backslash is never a spelling of a
// parent directory that one adapter sees and another does not) and anything
// that would escape the root. It also rejects a key whose final segment
// begins with ReservedPrefix, on every adapter, so that a key behaves alike
// on every backend.
func Key(key string) (string, bool) {
	if key == "" || strings.ContainsRune(key, '\\') {
		return "", false
	}
	cleaned := clean(key)
	if cleaned == "" || cleaned == "." || escapes(cleaned) || Reserved(cleaned) {
		return "", false
	}
	return cleaned, true
}

// ReservedPrefix begins the name of every temporary file the filesystem
// adapter creates while it writes an object (CONTRACT_STORAGE §Put).
const ReservedPrefix = ".lode-tmp-"

// Reserved reports whether the final segment of key begins with
// ReservedPrefix.
func Reserved(key string) bool {
	return strings.HasPrefix(path.Base(key), ReservedPrefix)
}

// Segment reports whether s is one canonical path segment that every
// adapter accepts as a component of a key: not empty, "." or "..", without
// a slash or backslash, and not beginning with ReservedPrefix. A name built
// into a key (a dataset ID, a directory name) must be one, so that cleaning
// the key cannot move it to another name.
func Segment(s string) bool {
	return s != "" && s != "." && s != ".." &&
		!strings.ContainsAny(s, `/\`) && !strings.HasPrefix(s, ReservedPrefix)
}

// Prefix normalizes a listing prefix. The empty prefix (or one that cleans to
// the root) lists everything. A trailing slash is preserved: "a/" and "a" are
// different string prefixes — only the former excludes "a2/x".
func Prefix(prefix string) (string, bool) {
	if prefix == "" {
		return "", true
	}
	if strings.ContainsRune(prefix, '\\') {
		return "", false
	}
	cleaned := clean(prefix)
	if escapes(cleaned) {
		return "", false
	}
	if cleaned == "." || cleaned == "" {
		return "", true
	}
	if strings.HasSuffix(prefix, "/") {
		cleaned += "/"
	}
	return cleaned, true
}

// MaxRangeLength is the largest ReadRange length an adapter accepts: what a
// []byte can address on this platform.
const MaxRangeLength = int64(math.MaxInt)

// RangeValid reports whether a ReadRange request is well-formed: offset and
// length non-negative, length addressable, and offset+length not overflowing.
func RangeValid(offset, length int64) bool {
	return offset >= 0 && length >= 0 && length <= MaxRangeLength && offset <= math.MaxInt64-length
}

func clean(p string) string {
	return strings.TrimPrefix(path.Clean(p), "/")
}

func escapes(cleaned string) bool {
	return cleaned == ".." || strings.HasPrefix(cleaned, "../")
}

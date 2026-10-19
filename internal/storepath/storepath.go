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
// It rejects the empty key, "." and anything that would escape the root.
func Key(key string) (string, bool) {
	if key == "" {
		return "", false
	}
	cleaned := clean(key)
	if cleaned == "" || cleaned == "." || escapes(cleaned) {
		return "", false
	}
	return cleaned, true
}

// Prefix normalizes a listing prefix. The empty prefix (or one that cleans to
// the root) lists everything. A trailing slash is preserved: "a/" and "a" are
// different string prefixes — only the former excludes "a2/x".
func Prefix(prefix string) (string, bool) {
	if prefix == "" {
		return "", true
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

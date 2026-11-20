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

// Key validates an object key as a canonical slash-separated relative path:
// non-empty, not rooted, no trailing slash, no empty ("//"), "." or ".."
// segments, no backslash anywhere, and its final segment not beginning with
// ReservedPrefix (CONTRACT_STORAGE §Put).
func Key(key string) (string, bool) {
	if strings.ContainsRune(key, '\\') || !isCanonicalRelative(key) || Reserved(key) {
		return "", false
	}
	return key, true
}

// ReservedPrefix begins the name of every temporary file the filesystem
// adapter creates while it writes an object (CONTRACT_STORAGE §Put).
const ReservedPrefix = ".lode-tmp-"

// Reserved reports whether the final segment of key begins with
// ReservedPrefix.
func Reserved(key string) bool {
	return strings.HasPrefix(path.Base(key), ReservedPrefix)
}

// SegmentRule states the rule of Segment, for the error that rejects a name.
const SegmentRule = `one path segment: not empty, "." or "..", without "/" or a backslash, and not beginning with "` + ReservedPrefix + `"`

// Segment reports whether s is one canonical path segment that every
// adapter accepts as a component of a key: not empty, "." or "..", without
// a slash or backslash, and not beginning with ReservedPrefix. A name built
// into a key (a dataset ID, a directory name) must be one, so that cleaning
// the key cannot move it to another name.
func Segment(s string) bool {
	return s != "" && s != "." && s != ".." &&
		!strings.ContainsAny(s, `/\`) && !strings.HasPrefix(s, ReservedPrefix)
}

// Prefix validates a listing prefix (CONTRACT_STORAGE §List). The empty prefix
// lists everything; any non-empty prefix must be a canonical slash-separated
// relative path with an optional single trailing slash ("a/" and "a" are
// distinct string prefixes — only the former excludes "a2/x"). Rooted
// prefixes, backslashes, empty segments ("//"), and "." or ".." segments are
// rejected.
func Prefix(prefix string) (string, bool) {
	if prefix == "" {
		return "", true
	}
	if strings.ContainsRune(prefix, '\\') {
		return "", false
	}
	body := strings.TrimSuffix(prefix, "/")
	if !isCanonicalRelative(body) {
		return "", false
	}
	return prefix, true
}

// MaxRangeLength is the largest ReadRange length an adapter accepts: what a
// []byte can address on this platform.
const MaxRangeLength = int64(math.MaxInt)

// RangeValid reports whether a ReadRange request is well-formed: offset and
// length non-negative, length addressable, and offset+length not overflowing.
func RangeValid(offset, length int64) bool {
	return offset >= 0 && length >= 0 && length <= MaxRangeLength && offset <= math.MaxInt64-length
}

// isCanonicalRelative reports whether p is a non-empty, unrooted,
// slash-separated relative path with no trailing slash, empty segment, "." or
// ".." segment.
func isCanonicalRelative(p string) bool {
	if p == "" || p[0] == '/' || p[len(p)-1] == '/' {
		return false
	}
	rem := p
	for {
		seg, tail, found := strings.Cut(rem, "/")
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
		if !found {
			return true
		}
		rem = tail
	}
}

package history

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

// Snapshot IDs have the form <instant>-<nonce> (CONTRACT_CORE §Snapshot ID).
// The instant orders IDs and dates the write; the random nonce makes them
// unique whatever the clock does, so no write can reuse an expired ID.
const (
	idSeparator = "-"
	// NonceBytes is the size of an ID's random nonce.
	NonceBytes = 8
)

// NewID returns a new snapshot ID for a write beginning now. Scan relies on
// IDs ordering by their instant.
func NewID() string {
	var nonce [NonceBytes]byte
	// crypto/rand.Read never returns an error since Go 1.24; it aborts instead.
	_, _ = rand.Read(nonce[:])
	return FormatID(time.Now(), nonce)
}

// FormatID returns the ID of a write beginning at at with the given nonce.
func FormatID(at time.Time, nonce [NonceBytes]byte) string {
	return strconv.FormatInt(at.UnixNano(), 10) + idSeparator + hex.EncodeToString(nonce[:])
}

// Instant is when a snapshot's write began, read from its ID. ok is false
// unless the ID is in the generated form: <instant>-<nonce>, or the legacy
// <instant> alone, where the instant is decimal Unix nanoseconds with no
// sign or leading zero.
func Instant[ID ~string](id ID) (time.Time, bool) {
	instant, nonce, hasNonce := strings.Cut(string(id), idSeparator)
	if hasNonce && !isNonce(nonce) {
		return time.Time{}, false
	}
	n, err := strconv.ParseInt(instant, 10, 64)
	if err != nil || n < 0 || strconv.FormatInt(n, 10) != instant {
		return time.Time{}, false
	}
	return time.Unix(0, n), true
}

func isNonce(s string) bool {
	return len(s) == hex.EncodedLen(NonceBytes) && strings.Trim(s, "0123456789abcdef") == ""
}

// IsGeneratedID reports whether id is in the generated form, new or legacy,
// for any kind of snapshot.
func IsGeneratedID[ID ~string](id ID) bool {
	_, ok := Instant(id)
	return ok
}

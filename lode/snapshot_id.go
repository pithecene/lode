package lode

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
	nonceBytes  = 8
)

// generateID returns a new snapshot ID for a write beginning now.
func generateID() string {
	var nonce [nonceBytes]byte
	// crypto/rand.Read never returns an error since Go 1.24; it aborts instead.
	_, _ = rand.Read(nonce[:])
	return formatSnapshotID(time.Now(), nonce)
}

func formatSnapshotID(at time.Time, nonce [nonceBytes]byte) string {
	return strconv.FormatInt(at.UnixNano(), 10) + idSeparator + hex.EncodeToString(nonce[:])
}

// snapshotInstant is when a snapshot's write began, read from its ID. ok is
// false unless the ID is in the generated form: <instant>-<nonce>, or the
// legacy <instant> alone, where the instant is decimal Unix nanoseconds with
// no sign or leading zero.
func snapshotInstant(id DatasetSnapshotID) (time.Time, bool) {
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
	return len(s) == hex.EncodedLen(nonceBytes) && strings.Trim(s, "0123456789abcdef") == ""
}

// isGeneratedID reports whether id is in the generated form, new or legacy,
// for any kind of snapshot.
func isGeneratedID[ID ~string](id ID) bool {
	_, ok := snapshotInstant(DatasetSnapshotID(id))
	return ok
}

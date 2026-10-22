package history

import (
	"encoding/hex"
	"strconv"
	"strings"
	"testing"
	"time"
)

// FuzzInstant checks that an ID accepted as generated is path-safe and
// round-trips: its instant reformats to the same digits, and with a nonce
// FormatID reproduces the ID exactly. Retention turns only such IDs into
// store paths, so an accepted ID must never carry a separator.
func FuzzInstant(f *testing.F) {
	for _, seed := range []string{
		"1792459022524750970", "1792459022524750970-0123456789abcdef", "0",
		"00123", "-1", "1-0123456789ABCDEF", "1/2", "../1", "1-0123456789abcdef-",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, id string) {
		at, ok := Instant(id)
		if ok != IsGeneratedID(id) {
			t.Fatalf("Instant and IsGeneratedID disagree on %q", id)
		}
		if !ok {
			return
		}
		if strings.ContainsAny(id, "/\\.") {
			t.Fatalf("generated ID %q contains a path character", id)
		}
		instant, nonce, hasNonce := strings.Cut(id, idSeparator)
		if strconv.FormatInt(at.UnixNano(), 10) != instant {
			t.Fatalf("Instant(%q) = %d, which does not reformat to %q", id, at.UnixNano(), instant)
		}
		if !hasNonce {
			return
		}
		var n [NonceBytes]byte
		if _, err := hex.Decode(n[:], []byte(nonce)); err != nil {
			t.Fatalf("accepted nonce %q does not decode: %v", nonce, err)
		}
		if got := FormatID(at, n); got != id {
			t.Fatalf("FormatID round trip of %q = %q", id, got)
		}
	})
}

// FuzzFormatID checks that every ID Lode can generate is accepted as
// generated and yields its instant back.
func FuzzFormatID(f *testing.F) {
	f.Add(int64(1792459022524750970), []byte{1, 2, 3, 4, 5, 6, 7, 8})
	f.Add(int64(0), []byte{})
	f.Fuzz(func(t *testing.T, nanos int64, raw []byte) {
		if nanos < 0 {
			return
		}
		var n [NonceBytes]byte
		copy(n[:], raw)
		id := FormatID(time.Unix(0, nanos), n)
		at, ok := Instant(id)
		if !ok || at.UnixNano() != nanos {
			t.Fatalf("Instant(FormatID(%d)) = %v, %v", nanos, at, ok)
		}
	})
}

package testutil

import (
	"testing"
	"time"

	"github.com/pithecene/lode/internal/history"
)

// AwaitClockPast waits until the wall clock has passed the instant at which
// the write that produced snapshot id began. Tests that rely on a later
// write, cutoff, or generated ID being strictly later than an earlier
// snapshot call it in between: on Windows the wall clock advances in coarse
// steps, so two consecutive operations can otherwise share an instant, and
// the order of their IDs then falls to the random nonce.
func AwaitClockPast(t testing.TB, id string) {
	t.Helper()
	at, ok := history.Instant(id)
	if !ok {
		t.Fatalf("snapshot ID %s carries no instant", id)
	}
	for !time.Now().After(at) {
		time.Sleep(time.Millisecond)
	}
}

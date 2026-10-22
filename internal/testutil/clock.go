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
	// The bound uses elapsed monotonic time, so a wall clock that stops or
	// steps backward fails the test instead of hanging it.
	start := time.Now()
	for !time.Now().After(at) {
		if time.Since(start) > clockWaitLimit {
			t.Fatalf("wall clock did not pass %s within %s", at.Format(time.RFC3339Nano), clockWaitLimit)
		}
		time.Sleep(time.Millisecond)
	}
}

// clockWaitLimit bounds AwaitClockPast. A coarse clock advances within tens
// of milliseconds; a longer wait means the clock is not advancing.
const clockWaitLimit = 10 * time.Second

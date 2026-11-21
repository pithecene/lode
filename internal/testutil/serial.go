package testutil

import "testing"

// Serial marks a test that must not run in parallel with other tests,
// because it changes or depends on process-wide state: the environment, the
// working directory, a package variable, a global hook, or a timing-sensitive
// measurement. It declares the test serial for the testparallel analyzer,
// which requires every top-level test to call exactly one of t.Parallel and
// Serial. The reason is required so that each serial test records the state
// it touches, and a reader can tell when that state no longer applies and the
// test can run in parallel.
func Serial(t testing.TB, reason string) {
	t.Helper()
	_ = reason
}

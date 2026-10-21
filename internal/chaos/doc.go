// Package chaos is a randomized stress harness for Lode. It runs many
// concurrent writers and readers against one dataset on a real store,
// records every commit the library acknowledged, and then checks the
// invariants the contracts promise: a linear history with no forks, no lost
// acknowledged commit, a pointer that names the head, and snapshots whose
// manifests and files agree.
//
// The harness is not part of the regular test suite. TestChaos in this
// package runs it only when -chaos is passed (see `task chaos`). Every run
// is driven by a seed, which is logged: the random choices of a run repeat
// exactly for the same seed, although goroutine scheduling does not.
package chaos

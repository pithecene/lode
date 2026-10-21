package chaos

import (
	"fmt"
	"slices"
	"sync"
)

// Violation is a broken invariant: which check failed and the evidence.
type Violation struct {
	Check  string
	Detail string
}

func (v Violation) String() string { return v.Check + ": " + v.Detail }

// Ledger records what a run's goroutines observed. It is safe for
// concurrent use.
type Ledger struct {
	mu         sync.Mutex
	commits    []Commit
	conflicts  int
	reads      int
	violations []Violation
}

// commit records a commit the library acknowledged.
func (l *Ledger) commit(c Commit) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.commits = append(l.commits, c)
}

// conflict records a commit that exhausted its retries: an expected outcome
// under contention, not a violation.
func (l *Ledger) conflict() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.conflicts++
}

// read records a read whose outcome was checked.
func (l *Ledger) read() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reads++
}

// violate records a broken invariant.
func (l *Ledger) violate(check, format string, args ...any) {
	v := Violation{Check: check, Detail: fmt.Sprintf(format, args...)}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.violations = append(l.violations, v)
}

// Commits returns a copy of the acknowledged commits.
func (l *Ledger) Commits() []Commit {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.commits)
}

// Report summarizes a finished run.
type Report struct {
	Seed       uint64
	Commits    int
	Conflicts  int
	Reads      int
	Snapshots  int
	Violations []Violation
}

func (l *Ledger) report(seed uint64, snapshots int) Report {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Report{
		Seed:       seed,
		Commits:    len(l.commits),
		Conflicts:  l.conflicts,
		Reads:      l.reads,
		Snapshots:  snapshots,
		Violations: slices.Clone(l.violations),
	}
}

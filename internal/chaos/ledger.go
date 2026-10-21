package chaos

import (
	"fmt"
	"slices"
	"sync"

	"github.com/pithecene/lode/lode"
	"github.com/pithecene/lode/lode/volume"
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
	faults     int
	reads      int
	violations []Violation
	// expired lists what Expire reported as expired; failedReads lists
	// snapshots whose reads failed as if expired, to confirm at the end.
	expired     []lode.DatasetSnapshotID
	failedReads []lode.DatasetSnapshotID
	// volumeCommits and blocks record a Volume run's acknowledged commits
	// and the blocks they committed.
	volumeCommits []volume.SnapshotID
	blocks        []Block
	// stalled records that an abandoned commit stalled a Volume run.
	stalled bool
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

// fault records an operation that failed because of an injected fault.
func (l *Ledger) fault() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.faults++
}

// read records a read whose outcome was checked.
func (l *Ledger) read() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reads++
}

// volumeCommit records a Volume commit the library acknowledged.
func (l *Ledger) volumeCommit(id volume.SnapshotID, blocks []Block) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.volumeCommits = append(l.volumeCommits, id)
	l.blocks = append(l.blocks, blocks...)
}

// stall records that the settling Volume commit conflicted with no writer
// left: an abandoned commit stalled the volume (CONTRACT_VOLUME).
func (l *Ledger) stall() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stalled = true
}

// expire records snapshots an Expire report listed as expired.
func (l *Ledger) expire(ids ...lode.DatasetSnapshotID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.expired = append(l.expired, ids...)
}

// failedRead records a read that failed as if its snapshot were expired.
func (l *Ledger) failedRead(id lode.DatasetSnapshotID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failedReads = append(l.failedReads, id)
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

// volumeHistory returns copies of the acknowledged Volume commits and blocks.
func (l *Ledger) volumeHistory() (commits []volume.SnapshotID, blocks []Block) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.volumeCommits), slices.Clone(l.blocks)
}

// retention returns copies of the expired snapshots and of the failed reads.
func (l *Ledger) retention() (expired, failedReads []lode.DatasetSnapshotID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.expired), slices.Clone(l.failedReads)
}

// Report summarizes a finished run.
type Report struct {
	Seed      uint64
	Commits   int
	Conflicts int
	Faults    int
	Reads     int
	Expired   int
	Snapshots int
	// Stalled reports that an abandoned commit stalled a Volume run, so
	// that every later commit conflicted.
	Stalled    bool
	Violations []Violation
}

func (l *Ledger) report(seed uint64, snapshots int) Report {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Report{
		Seed:       seed,
		Commits:    len(l.commits) + len(l.volumeCommits),
		Conflicts:  l.conflicts,
		Faults:     l.faults,
		Reads:      l.reads,
		Expired:    len(l.expired),
		Snapshots:  snapshots,
		Stalled:    l.stalled,
		Violations: slices.Clone(l.violations),
	}
}

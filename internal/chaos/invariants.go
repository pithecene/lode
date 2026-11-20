package chaos

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/pithecene/lode/lode"
)

// verifier holds the evidence a finished Dataset run left: the persisted
// history, a clean handle to read it through, and what the workload
// observed. Each invariant is one of its methods.
type verifier struct {
	ds          chaosDataset
	store       lode.RangeStore
	snapshots   map[lode.SnapshotID]*lode.Snapshot
	chain       *chain[lode.SnapshotID]
	settle      lode.SnapshotID // the settling commit
	head        lode.SnapshotID // what Latest returns
	acked       map[lode.SnapshotID]Commit
	expired     []lode.SnapshotID
	failedReads []lode.SnapshotID
}

func (r *run) newVerifier(ctx context.Context, ds chaosDataset, settle lode.SnapshotID) (*verifier, error) {
	snaps, err := ds.Snapshots(ctx)
	if err != nil {
		return nil, fmt.Errorf("chaos: list snapshots: %w", err)
	}
	head, err := ds.Latest(ctx)
	if err != nil {
		return nil, fmt.Errorf("chaos: resolve the head: %w", err)
	}
	v := &verifier{
		ds:        ds,
		store:     r.store,
		snapshots: make(map[lode.SnapshotID]*lode.Snapshot, len(snaps)),
		chain:     newChain[lode.SnapshotID](),
		settle:    settle,
		head:      head.ID,
		acked:     make(map[lode.SnapshotID]Commit),
	}
	for _, s := range snaps {
		v.snapshots[s.ID] = s
		v.chain.add(s.ID, s.ParentSnapshotID)
	}
	for _, c := range r.ledger.Commits() {
		v.acked[c.ID] = c
	}
	v.expired, v.failedReads = r.ledger.retention()
	return v, nil
}

// check is one invariant over the final history, run by a verifier of type
// V; it returns the evidence of every violation.
type check[V any] struct {
	name string
	run  func(v V, ctx context.Context) []string
}

// runChecks runs every check over v and records each violation.
func runChecks[V any](ctx context.Context, v V, checks []check[V], ledger *Ledger) {
	for _, c := range checks {
		for _, detail := range c.run(v, ctx) {
			ledger.violate(c.name, "%s", detail)
		}
	}
}

// datasetChecks are the invariants the contracts promise for a dataset that
// concurrent writers committed to through a store that supports CAS
// (CONTRACT_WRITE_API §Concurrency, CONTRACT_CORE §Head Pointer and §History
// & Immutability).
var datasetChecks = []check[*verifier]{
	{"acknowledged-commits-persist", (*verifier).acknowledgedPersist},
	{"history-shape", (*verifier).historyShape},
	{"parents-explained", (*verifier).parentsExplained},
	{"contents-match-manifests", (*verifier).contentsMatch},
	{"checksums-verified", (*verifier).checksumsVerified},
	{"anchored-views-read", (*verifier).anchoredViewsRead},
	{"listings-cover-history", (*verifier).listingsCover},
}

// finalGrace is the grace period of the sweep after the run: no write is in
// progress any more, so every orphan is past any positive grace period.
const finalGrace = time.Nanosecond

// verifyDataset settles the head with one clean commit, so that the pointer names
// a committed snapshot, and then runs every check, recording each failure.
func (r *run) verifyDataset(ctx context.Context) int {
	ds, err := r.openDataset(ctx, r.store)
	if err != nil {
		r.ledger.violate("verify", "open: %v", err)
		return 0
	}
	settle, err := ds.writeSettle(ctx)
	if err != nil {
		r.ledger.violate("verify", "settling commit: %v", err)
		return 0
	}
	if r.cfg.Retention {
		if _, err := ds.Sweep(ctx, lode.SweepRequest{Grace: finalGrace}); err != nil {
			r.ledger.violate("verify", "final sweep: %v", err)
		}
	}
	v, err := r.newVerifier(ctx, ds, settle.ID)
	if err != nil {
		r.ledger.violate("verify", "%v", err)
		return 0
	}
	runChecks(ctx, v, r.cfg.checks(), r.ledger)
	return v.chain.size()
}

// checks are the dataset invariants that apply to the run: retention adds
// its own.
func (c Config) checks() []check[*verifier] {
	if !c.Retention {
		return datasetChecks
	}
	return slices.Concat(datasetChecks, retentionChecks)
}

// isExpired reports whether snapshot id reads as expired.
func (v *verifier) isExpired(ctx context.Context, id lode.SnapshotID) bool {
	_, err := v.ds.Snapshot(ctx, id)
	return errors.Is(err, lode.ErrSnapshotExpired)
}

// acknowledgedPersist requires every acknowledged commit to have a
// manifest, unless retention expired it.
func (v *verifier) acknowledgedPersist(ctx context.Context) []string {
	var lost []string
	for id, c := range v.acked {
		if !v.chain.has(id) && !v.isExpired(ctx, id) {
			lost = append(lost, fmt.Sprintf("acknowledged %s (writer %d seq %d) has no manifest and is not expired", id, c.Writer, c.Seq))
		}
	}
	return lost
}

// historyShape requires Latest to return the settling commit, no fork, no
// more chain ends than missing parents account for, and, with no missing
// parent, a head that reaches every snapshot.
func (v *verifier) historyShape(context.Context) []string {
	return append(settled(v.head, v.settle), v.chain.shape(v.head)...)
}

// settled requires the head to be the settling commit, when there is one:
// once it is acknowledged, no other commit is pending.
func settled[ID ~string](head, settle ID) []string {
	if settle == "" || head == settle {
		return nil
	}
	return []string{fmt.Sprintf("Latest returns %s after the settling commit %s", head, settle)}
}

// parentsExplained accepts a parent without a manifest only as an abandoned
// commit, which no writer was told had committed, or as an expired
// snapshot.
func (v *verifier) parentsExplained(ctx context.Context) []string {
	var bad []string
	for _, parent := range v.chain.missingParents() {
		if _, acked := v.acked[parent]; acked && !v.isExpired(ctx, parent) {
			bad = append(bad, fmt.Sprintf("acknowledged %s is a parent but has no manifest and is not expired", parent))
		}
	}
	return bad
}

// contentsMatch reads every snapshot: its units must match its manifest's
// row count, its objects must record CRC32C checksums (and Parquet statistics
// when encoded as Parquet), and an acknowledged commit must hold exactly its
// writer's payload.
func (v *verifier) contentsMatch(ctx context.Context) []string {
	var bad []string
	for _, id := range v.chain.ids() {
		snap := v.snapshots[id]
		bad = append(bad, verifyManifestObjects(snap)...)
		bad = append(bad, v.verifySnapshotContents(ctx, snap)...)
	}
	return bad
}

func verifyManifestObjects(snap *lode.Snapshot) []string {
	var bad []string
	if snap.ChecksumAlgorithm != "crc32c" {
		bad = append(bad, fmt.Sprintf("%s: checksum_algorithm = %q, want crc32c", snap.ID, snap.ChecksumAlgorithm))
	}
	var statRows int64
	for i, obj := range snap.Objects {
		if obj.Checksum == "" {
			bad = append(bad, fmt.Sprintf("%s: objects[%d] (%s) missing checksum", snap.ID, i, obj.Path))
		}
		if snap.Codec == "parquet" {
			if obj.Stats == nil {
				bad = append(bad, fmt.Sprintf("%s: parquet objects[%d] (%s) missing stats", snap.ID, i, obj.Path))
			} else {
				statRows += obj.Stats.RowCount
			}
		}
	}
	if snap.Codec == "parquet" && len(bad) == 0 && statRows != snap.RowCount {
		bad = append(bad, fmt.Sprintf("%s: parquet object stats sum to %d rows, manifest records %d", snap.ID, statRows, snap.RowCount))
	}
	return bad
}

func (v *verifier) verifySnapshotContents(ctx context.Context, snap *lode.Snapshot) []string {
	units, err := v.ds.readUnits(ctx, snap.ID)
	if err != nil {
		return []string{fmt.Sprintf("%s: Read: %v", snap.ID, err)}
	}
	if units != snap.RowCount {
		return []string{fmt.Sprintf("%s: %d units, manifest records %d", snap.ID, units, snap.RowCount)}
	}
	c, acked := v.acked[snap.ID]
	if !acked {
		return nil
	}
	ok, err := v.ds.checkCommit(ctx, c)
	if err != nil || !ok {
		return []string{fmt.Sprintf("%s: payload read does not match writer %d seq %d (%v)", snap.ID, c.Writer, c.Seq, err)}
	}
	return nil
}

// checksumsVerified confirms that read-time CRC32C verification rejects a
// mismatched digest with lode.ErrChecksumMismatch.
func (v *verifier) checksumsVerified(ctx context.Context) []string {
	headSnap, ok := v.snapshots[v.head]
	if !ok || len(headSnap.Objects) == 0 || headSnap.Objects[0].Checksum == "" {
		return nil
	}
	orig := headSnap.Objects[0].Checksum
	tampered := tamperChecksumStore{
		ObjectStore: v.store,
		oldDigest:   orig,
		newDigest:   flipHex(orig),
	}
	err := v.ds.readWithStore(ctx, tampered, v.head)
	if !errors.Is(err, lode.ErrChecksumMismatch) {
		return []string{fmt.Sprintf("reading %s with a tampered CRC32C digest returned %v, want ErrChecksumMismatch", v.head, err)}
	}
	return nil
}

func flipHex(digest string) string {
	if strings.HasPrefix(digest, "0") {
		return "f" + digest[1:]
	}
	return "0" + digest[1:]
}

type tamperChecksumStore struct {
	lode.ObjectStore
	oldDigest, newDigest string
}

func (s tamperChecksumStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	rc, err := s.ObjectStore.Get(ctx, path)
	if err != nil || !strings.HasSuffix(path, "/manifest.json") {
		return rc, err
	}
	data, readErr := io.ReadAll(rc)
	_ = rc.Close()
	if readErr != nil {
		return nil, readErr
	}
	mutated := strings.ReplaceAll(string(data), s.oldDigest, s.newDigest)
	return io.NopCloser(strings.NewReader(mutated)), nil
}

// anchoredViewsRead opens up to 16 surviving acknowledged commits via ReadRoot
// with WithAnchor(id), verifying payload equality, one removal fence check per operation,
// and single manifest Get across Open + Snapshot(id) + Read(id).
func (v *verifier) anchoredViewsRead(ctx context.Context) []string {
	var commits []Commit
	for _, id := range v.chain.ids() {
		if c, ok := v.acked[id]; ok {
			commits = append(commits, c)
		}
	}
	var bad []string
	for _, c := range sampleCommits(commits, 16) {
		probe := &anchorProbeStore{ObjectStore: v.store}
		ok, err := v.ds.checkAnchored(ctx, probe, c)
		switch {
		case err != nil || !ok:
			bad = append(bad, fmt.Sprintf("%s: WithAnchor read failed or mismatched (%v)", c.ID, err))
		case probe.pointerReads != 3:
			bad = append(bad, fmt.Sprintf("%s: WithAnchor checked the removal fence %d times across Open+Snapshot+Read, want 3", c.ID, probe.pointerReads))
		case probe.manifestHits != 1:
			bad = append(bad, fmt.Sprintf("%s: WithAnchor loaded manifest %d times across Open+Snapshot+Read, want 1", c.ID, probe.manifestHits))
		}
	}
	return bad
}

func sampleCommits(commits []Commit, limit int) []Commit {
	if len(commits) <= limit {
		return commits
	}
	out := make([]Commit, 0, limit)
	step := len(commits) / limit
	for i := 0; i < len(commits) && len(out) < limit-1; i += step {
		out = append(out, commits[i])
	}
	return append(out, commits[len(commits)-1])
}

type anchorProbeStore struct {
	lode.ObjectStore
	pointerReads int
	manifestHits int
}

func (s *anchorProbeStore) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	if strings.HasSuffix(path, "/latest") {
		s.pointerReads++
	}
	rc, err := s.ObjectStore.Get(ctx, path)
	if err == nil && strings.HasSuffix(path, "/manifest.json") {
		s.manifestHits++
	}
	return rc, err
}

func (s *anchorProbeStore) Exists(ctx context.Context, path string) (bool, error) {
	if strings.HasSuffix(path, "/latest") {
		s.pointerReads++
	}
	return s.ObjectStore.Exists(ctx, path)
}

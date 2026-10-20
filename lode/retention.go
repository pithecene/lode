package lode

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Retention removes snapshots without ever removing what a manifest still
// describes (CONTRACT_RETENTION.md). Expiry writes a tombstone, then deletes
// manifests, then data, so a crash at any point leaves either a readable
// snapshot or garbage that a later run reclaims.

// Expire removes the selected snapshots, oldest first (CONTRACT_RETENTION
// §Snapshot Expiry). The report is never nil and records every outcome
// reached before an error.
func (d *dataset) Expire(ctx context.Context, req ExpireRequest) (*ExpireReport, error) {
	report := &ExpireReport{}
	if err := d.validateExpire(req); err != nil {
		return report, err
	}
	sel := newSelection(req)
	candidates, err := d.expiryCandidates(ctx, sel)
	if err != nil {
		return report, err
	}
	head, err := newHeadGuard(ctx, &d.history)
	if err != nil {
		return report, err
	}
	run := expiryRun{ds: d, sel: sel, head: head, dryRun: req.DryRun}
	for _, c := range candidates {
		outcome, err := run.expire(ctx, c)
		report.record(outcome)
		if err != nil {
			return report, err
		}
	}
	return report, nil
}

// validateExpire checks the request before any store call. A request with
// no selector is refused: for a destructive operation the empty
// intersection is never the universe. Malformed is checked before
// unsupported, as for PartitionListOptions.Prefix.
func (d *dataset) validateExpire(req ExpireRequest) error {
	checks := []struct {
		fails bool
		err   func() error
	}{
		{len(req.IDs) == 0 && req.Partition == "" && req.Before.IsZero(), func() error {
			return invalidArgument("Expire requires at least one of IDs, Partition or Before")
		}},
		{!validPartitionPrefix(req.Partition), func() error {
			return fmt.Errorf("lode: Expire partition %q: %w", req.Partition, ErrInvalidPath)
		}},
		{req.Partition != "" && !d.layout.supportsPartitions(), func() error {
			return fmt.Errorf("lode: Expire partition %q: %w", req.Partition, ErrPartitionPrefixNotSupported)
		}},
	}
	for _, c := range checks {
		if c.fails {
			return c.err()
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// Selection
// -----------------------------------------------------------------------------

// selection is a validated ExpireRequest as data: which snapshot IDs match,
// and which partition, if any, bounds the listing.
type selection struct {
	partition string // without a trailing "/"; "" when unset
	matches   []func(DatasetSnapshotID) bool
}

func newSelection(req ExpireRequest) selection {
	sel := selection{partition: strings.TrimSuffix(req.Partition, "/")}
	if len(req.IDs) > 0 {
		ids := make(map[DatasetSnapshotID]bool, len(req.IDs))
		for _, id := range req.IDs {
			ids[id] = true
		}
		sel.matches = append(sel.matches, func(id DatasetSnapshotID) bool { return ids[id] })
	}
	if !req.Before.IsZero() {
		sel.matches = append(sel.matches, func(id DatasetSnapshotID) bool { return startedBefore(id, req.Before) })
	}
	return sel
}

// selects reports whether every set selector matches id. Partition is not
// among them: the listing prefix applies it.
func (s selection) selects(id DatasetSnapshotID) bool {
	for _, match := range s.matches {
		if !match(id) {
			return false
		}
	}
	return true
}

// listPrefix is what to list: the partition subtree when one is set (the
// trailing "/" makes the match whole-segment), else every snapshot.
func (s selection) listPrefix(l layout, dataset DatasetID) string {
	if s.partition == "" {
		return l.snapshotsPrefix(dataset)
	}
	return l.partitionsPrefix(dataset, s.partition+"/")
}

// spansOutside reports whether a snapshot selected by partition also has
// files outside it (CONTRACT_RETENTION §Mixed snapshots).
func (s selection) spansOutside(l layout, m *Manifest) bool {
	if s.partition == "" {
		return false
	}
	return slices.ContainsFunc(m.Files, func(f FileRef) bool {
		return !withinPartition(l.extractPartitionPath(f.Path), s.partition)
	})
}

// snapshotInstant is when a snapshot's write began, read from its ID
// (decimal Unix nanoseconds, generateID). ok is false for any other ID.
func snapshotInstant(id DatasetSnapshotID) (time.Time, bool) {
	n, err := strconv.ParseInt(string(id), 10, 64)
	if err != nil || n < 0 {
		return time.Time{}, false
	}
	return time.Unix(0, n), true
}

func startedBefore(id DatasetSnapshotID, cutoff time.Time) bool {
	t, ok := snapshotInstant(id)
	return ok && t.Before(cutoff)
}

// expiryCandidate is a selected snapshot with the manifest paths the
// listing found for it, sorted.
type expiryCandidate struct {
	id    DatasetSnapshotID
	paths []string
}

// expiryCandidates lists the snapshots the selection names, oldest first.
func (d *dataset) expiryCandidates(ctx context.Context, sel selection) ([]expiryCandidate, error) {
	keys, err := d.store.List(ctx, sel.listPrefix(d.layout, d.id))
	if err != nil {
		return nil, fmt.Errorf("lode: failed to list snapshots: %w", err)
	}
	return groupManifests(keys, func(p string) (DatasetSnapshotID, bool) {
		sp, ok := d.layout.parse(p)
		ok = ok && sp.kind == manifestLeaf && sp.dataset == d.id && sel.selects(sp.snapshot)
		return sp.snapshot, ok
	}), nil
}

// groupManifests groups the keys owner accepts by snapshot, oldest first.
func groupManifests(keys []string, owner func(string) (DatasetSnapshotID, bool)) []expiryCandidate {
	byID := make(map[DatasetSnapshotID][]string)
	for _, k := range keys {
		if id, ok := owner(k); ok {
			byID[id] = append(byID[id], k)
		}
	}
	out := make([]expiryCandidate, 0, len(byID))
	for id, paths := range byID {
		slices.Sort(paths)
		out = append(out, expiryCandidate{id: id, paths: paths})
	}
	slices.SortFunc(out, func(a, b expiryCandidate) int { return strings.Compare(string(a.id), string(b.id)) })
	return out
}

// -----------------------------------------------------------------------------
// The head
// -----------------------------------------------------------------------------

// headGuard protects every snapshot observed as the head during one run,
// re-resolving before each irreversible step (CONTRACT_RETENTION §Head
// protection). A head stays protected after a newer commit supersedes it:
// the run's report promises the head it saw was kept.
type headGuard struct {
	history   *history[DatasetSnapshotID, Manifest]
	current   DatasetSnapshotID
	protected map[DatasetSnapshotID]bool
}

func newHeadGuard(ctx context.Context, h *history[DatasetSnapshotID, Manifest]) (*headGuard, error) {
	g := &headGuard{history: h, protected: make(map[DatasetSnapshotID]bool)}
	return g, g.refresh(ctx)
}

func (g *headGuard) refresh(ctx context.Context) error {
	id, err := g.history.CommittedHead(ctx, g.current)
	if err != nil {
		return err
	}
	g.current = id
	g.protected[id] = true
	return nil
}

func (g *headGuard) is(id DatasetSnapshotID) bool { return g.protected[id] }

// -----------------------------------------------------------------------------
// Expiring one snapshot
// -----------------------------------------------------------------------------

// expiryVerdict is what Expire decided for one candidate.
type expiryVerdict int

const (
	verdictVanished expiryVerdict = iota // every manifest already gone
	verdictRetained                      // the head
	verdictMixed                         // files outside the selected partition
	verdictExpired                       // expired, or would be on a dry run
)

// expiryOutcome is one candidate's verdict and the files it touched.
type expiryOutcome struct {
	id      DatasetSnapshotID
	verdict expiryVerdict
	deleted []FileRef // data objects deleted, or that would be on a dry run
	skipped []string  // manifest paths that are not this snapshot's objects
}

// recordVerdict says where each verdict lands in the report.
var recordVerdict = map[expiryVerdict]func(*ExpireReport, DatasetSnapshotID){
	verdictRetained: func(r *ExpireReport, id DatasetSnapshotID) { r.RetainedHead = id },
	verdictMixed:    func(r *ExpireReport, id DatasetSnapshotID) { r.Mixed = append(r.Mixed, id) },
	verdictExpired:  func(r *ExpireReport, id DatasetSnapshotID) { r.Expired = append(r.Expired, id) },
}

func (r *ExpireReport) record(o expiryOutcome) {
	if rec, ok := recordVerdict[o.verdict]; ok {
		rec(r, o.id)
	}
	r.Objects += len(o.deleted)
	r.Bytes += totalBytes(o.deleted)
	r.Skipped = append(r.Skipped, o.skipped...)
}

// expiryRun is one Expire call's fixed context.
type expiryRun struct {
	ds     *dataset
	sel    selection
	head   *headGuard
	dryRun bool
}

// expire decides one candidate and, unless this is a dry run, carries the
// decision out.
func (r expiryRun) expire(ctx context.Context, c expiryCandidate) (expiryOutcome, error) {
	out := expiryOutcome{id: c.id}
	if r.head.is(c.id) {
		out.verdict = verdictRetained
		return out, nil
	}
	m, found, err := r.ds.loadCandidate(ctx, c)
	if err != nil || !found {
		return out, err
	}
	if r.sel.spansOutside(r.ds.layout, m) {
		out.verdict = verdictMixed
		return out, nil
	}
	owned, skipped := r.ds.splitFiles(c.id, m)
	out.skipped = skipped
	// Recheck the head immediately before the irreversible steps; a dry run
	// rechecks too, so its report is the one a real run would produce.
	if err := r.head.refresh(ctx); err != nil {
		return out, err
	}
	if r.head.is(c.id) {
		out.verdict = verdictRetained
		return out, nil
	}
	if r.dryRun {
		// A real run would fail on a tombstone recording another snapshot.
		if err := r.ds.tombstones().check(ctx, c.id); err != nil {
			return out, err
		}
		out.verdict, out.deleted = verdictExpired, owned
		return out, nil
	}
	return r.ds.removeSnapshot(ctx, c, m, owned, out)
}

// loadCandidate loads the snapshot's manifest: the canonical copy, else any
// partition copy the listing found (pre-canonical data, or an expiry that
// stopped after its canonical delete). found is false when every copy is
// gone: another expiry finished it.
func (d *dataset) loadCandidate(ctx context.Context, c expiryCandidate) (m *Manifest, found bool, err error) {
	paths := append([]string{d.layout.manifestPath(d.id, c.id)}, c.paths...)
	for _, p := range paths {
		s, ok, err := d.history.loadListed(ctx, manifestEntry[DatasetSnapshotID]{c.id, p})
		if err != nil {
			return nil, false, fmt.Errorf("lode: failed to load manifest %s: %w", p, err)
		}
		if ok {
			return s.Manifest, true, nil
		}
	}
	return nil, false, nil
}

// splitFiles separates the data objects Expire may delete from every other
// path a manifest lists, which is reported and left in place.
func (d *dataset) splitFiles(id DatasetSnapshotID, m *Manifest) (owned []FileRef, skipped []string) {
	for _, f := range m.Files {
		if d.layout.isObjectOf(d.id, id, dataLeaf, f.Path) {
			owned = append(owned, f)
			continue
		}
		skipped = append(skipped, f.Path)
	}
	return owned, skipped
}

// removeSnapshot runs steps 1–4: tombstone, canonical manifest, partition
// manifests, data. The snapshot is expired once its canonical manifest is
// gone, so the verdict is set then, even if a later step fails.
func (d *dataset) removeSnapshot(ctx context.Context, c expiryCandidate, m *Manifest, owned []FileRef, out expiryOutcome) (expiryOutcome, error) {
	if err := d.tombstones().record(ctx, newTombstone(c.id, m.Files)); err != nil {
		return out, err
	}
	manifests := d.allManifestPaths(c, m)
	if err := deleteObject(ctx, d.store, manifests[0]); err != nil {
		return out, err
	}
	out.verdict = verdictExpired
	if err := deleteObjects(ctx, d.store, manifests[1:]); err != nil {
		return out, err
	}
	for _, f := range owned {
		if err := deleteObject(ctx, d.store, f.Path); err != nil {
			return out, err
		}
		out.deleted = append(out.deleted, f)
	}
	return out, nil
}

// allManifestPaths is every manifest path of the snapshot, canonical first:
// the ones commit wrote for its files' partitions and any the listing found.
// A path derived from a malformed FileRef is dropped unless it is a
// canonical manifest path of this snapshot.
func (d *dataset) allManifestPaths(c expiryCandidate, m *Manifest) []string {
	partitions := make([]string, 0, len(m.Files))
	for _, f := range m.Files {
		partitions = append(partitions, d.layout.extractPartitionPath(f.Path))
	}
	paths := slices.DeleteFunc(d.manifestPaths(c.id, partitions), func(p string) bool {
		return !d.layout.isObjectOf(d.id, c.id, manifestLeaf, p)
	})
	for _, p := range c.paths {
		if !slices.Contains(paths, p) {
			paths = append(paths, p)
		}
	}
	return paths
}

// -----------------------------------------------------------------------------
// Deletion
// -----------------------------------------------------------------------------

// deleteObject deletes p. A path already gone is success, so every
// retention step can be repeated.
func deleteObject(ctx context.Context, store Store, p string) error {
	if err := store.Delete(ctx, p); err != nil && !errors.Is(err, ErrNotFound) {
		return fmt.Errorf("lode: failed to delete %s: %w", p, err)
	}
	return nil
}

func deleteObjects(ctx context.Context, store Store, paths []string) error {
	for _, p := range paths {
		if err := deleteObject(ctx, store, p); err != nil {
			return err
		}
	}
	return nil
}

func totalBytes(files []FileRef) int64 {
	var n int64
	for _, f := range files {
		n += f.SizeBytes
	}
	return n
}

package lode

import (
	"context"
	"errors"
	"fmt"
	"slices"
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
	listing, err := d.expiryListing(ctx, sel)
	if err != nil {
		return report, err
	}
	head, err := newHeadGuard(ctx, &d.history)
	if err != nil {
		return report, err
	}
	run := expiryRun{ds: d, sel: sel, head: head, listed: listing.listed, dryRun: req.DryRun}
	for _, c := range listing.candidates {
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
// which partition, if any, bounds the listing, and which IDs may be resumed
// from their tombstone alone.
type selection struct {
	partition string // without a trailing "/"; "" when unset
	matches   []func(DatasetSnapshotID) bool
	// byTombstone are the requested IDs a tombstone alone can select:
	// Partition and Before select from manifests only.
	byTombstone []DatasetSnapshotID
}

func newSelection(req ExpireRequest) selection {
	sel := selection{partition: strings.TrimSuffix(req.Partition, "/")}
	if req.Partition == "" && req.Before.IsZero() {
		sel.byTombstone = slices.Compact(slices.Sorted(slices.Values(req.IDs))) // each once
	}
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

func startedBefore(id DatasetSnapshotID, cutoff time.Time) bool {
	t, ok := snapshotInstant(id)
	return ok && t.Before(cutoff)
}

// snapshotKeys is one snapshot's keys of one kind from a listing, sorted.
type snapshotKeys struct {
	id    DatasetSnapshotID
	paths []string
}

// expiryCandidate is a selected snapshot: the manifest paths the listing
// found and, for one selected by its tombstone alone, that tombstone.
type expiryCandidate struct {
	snapshotKeys
	recorded *tombstone
}

// expiryListing is what Expire's one listing found: the candidates, oldest
// first, and every listed key, which meters what the run deletes.
type expiryListing struct {
	candidates []expiryCandidate
	listed     map[string]bool
}

func (d *dataset) expiryListing(ctx context.Context, sel selection) (expiryListing, error) {
	keys, err := d.store.List(ctx, sel.listPrefix(d.layout, d.id))
	if err != nil {
		return expiryListing{}, fmt.Errorf("lode: failed to list snapshots: %w", err)
	}
	listing := expiryListing{listed: make(map[string]bool, len(keys))}
	for _, k := range keys {
		listing.listed[k] = true
	}
	withManifest := make(map[DatasetSnapshotID]bool)
	for _, g := range groupBySnapshot(keys, d.objectsOfKind(manifestLeaf, sel.selects)) {
		withManifest[g.id] = true
		listing.candidates = append(listing.candidates, expiryCandidate{snapshotKeys: g})
	}
	resumed, err := d.tombstoneCandidates(ctx, sel, withManifest)
	if err != nil {
		return expiryListing{}, err
	}
	listing.candidates = append(listing.candidates, resumed...)
	slices.SortFunc(listing.candidates, func(a, b expiryCandidate) int { return strings.Compare(string(a.id), string(b.id)) })
	return listing, nil
}

// tombstoneCandidates are the requested IDs with no manifest left whose
// expiry a tombstone records: the request completes them from it.
func (d *dataset) tombstoneCandidates(ctx context.Context, sel selection, withManifest map[DatasetSnapshotID]bool) ([]expiryCandidate, error) {
	var out []expiryCandidate
	for _, id := range sel.byTombstone {
		if withManifest[id] {
			continue
		}
		t, found, err := d.tombstones().lookup(ctx, id)
		if err != nil {
			return nil, err
		}
		if found {
			out = append(out, expiryCandidate{snapshotKeys: snapshotKeys{id: id}, recorded: &t})
		}
	}
	return out, nil
}

// objectsOfKind accepts keys that are, in canonical form, this dataset's
// objects of the given kind and belong to a snapshot keep accepts. A
// listing may return a raw key that a store would clean into another path;
// such a key is outside retention's scope.
func (d *dataset) objectsOfKind(kind pathKind, keep func(DatasetSnapshotID) bool) func(string) (DatasetSnapshotID, bool) {
	return func(p string) (DatasetSnapshotID, bool) {
		sp, ok := d.layout.parse(p)
		return sp.snapshot, ok && d.layout.isObjectOf(d.id, sp.snapshot, kind, p) && keep(sp.snapshot)
	}
}

// groupBySnapshot groups the keys owner accepts by snapshot, oldest first.
func groupBySnapshot(keys []string, owner func(string) (DatasetSnapshotID, bool)) []snapshotKeys {
	byID := make(map[DatasetSnapshotID][]string)
	for _, k := range keys {
		if id, ok := owner(k); ok {
			byID[id] = append(byID[id], k)
		}
	}
	out := make([]snapshotKeys, 0, len(byID))
	for id, paths := range byID {
		slices.Sort(paths)
		out = append(out, snapshotKeys{id: id, paths: paths})
	}
	slices.SortFunc(out, func(a, b snapshotKeys) int { return strings.Compare(string(a.id), string(b.id)) })
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
	verdictVanished expiryVerdict = iota // neither manifest nor tombstone left
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
	listed map[string]bool
	dryRun bool
}

// expiryPlan is what expiring one snapshot works from: its tombstone, the
// one already recorded or the one this run would record from the manifest.
type expiryPlan struct {
	tomb     tombstone
	recorded bool      // an earlier or concurrent run recorded it
	manifest *Manifest // nil when no manifest is left
}

// expire decides one candidate and, unless this is a dry run, carries the
// decision out.
func (r expiryRun) expire(ctx context.Context, c expiryCandidate) (expiryOutcome, error) {
	out := expiryOutcome{id: c.id}
	if r.head.is(c.id) {
		return r.retain(ctx, out)
	}
	plan, found, err := r.plan(ctx, c)
	if err != nil || !found {
		return out, err
	}
	if plan.manifest != nil && r.sel.spansOutside(r.ds.layout, plan.manifest) {
		out.verdict = verdictMixed
		return out, nil
	}
	owned, skipped := r.ds.splitFiles(c.id, plan.tomb.Files)
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
		out.verdict, out.deleted = verdictExpired, r.present(owned)
		return out, nil
	}
	return r.remove(ctx, c, plan, owned, out)
}

// retain keeps a snapshot the head guard protects. The pointer is still
// rechecked, as before any snapshot's step 1: a commit in flight stops the
// run with ErrHeadUncommitted here too.
func (r expiryRun) retain(ctx context.Context, out expiryOutcome) (expiryOutcome, error) {
	if err := r.head.refresh(ctx); err != nil {
		return out, err
	}
	out.verdict = verdictRetained
	return out, nil
}

// plan finds the tombstone to work from. found is false when the snapshot
// has neither a manifest nor a tombstone left: another run finished it.
func (r expiryRun) plan(ctx context.Context, c expiryCandidate) (plan expiryPlan, found bool, err error) {
	// A candidate selected by its tombstone is checked for a manifest too:
	// one may have appeared since the listing.
	m, hasManifest, err := r.ds.loadCandidate(ctx, c)
	if err != nil {
		return plan, false, err
	}
	t, recorded, err := r.tombstoneFor(ctx, c)
	if err != nil {
		return plan, false, err
	}
	if !recorded && !hasManifest {
		return plan, false, nil
	}
	if !recorded {
		return expiryPlan{tomb: newTombstone(c.id, m.Files), manifest: m}, true, nil
	}
	// A tombstone left by an earlier snapshot with the same ID must never
	// steer deletes of this one, even when no copy was found above: every
	// path this expiry would delete is checked.
	matches, err := r.ds.copiesMatch(ctx, c.id, r.ds.allManifestPaths(c, t.Files), t.Files)
	if err != nil {
		return plan, false, err
	}
	if !matches {
		return plan, false, fmt.Errorf("lode: tombstone for %s records a different inventory than its manifest", c.id)
	}
	return expiryPlan{tomb: t, recorded: true, manifest: m}, true, nil
}

// tombstoneFor is the candidate's recorded tombstone: the one selection
// already read, or a lookup.
func (r expiryRun) tombstoneFor(ctx context.Context, c expiryCandidate) (tombstone, bool, error) {
	if c.recorded != nil {
		return *c.recorded, true, nil
	}
	return r.ds.tombstones().lookup(ctx, c.id)
}

// sameInventory reports whether two inventories list the same paths with
// the same sizes, in order.
func sameInventory(a, b []FileRef) bool {
	return slices.EqualFunc(a, b, func(x, y FileRef) bool { return x.Path == y.Path && x.SizeBytes == y.SizeBytes })
}

// present meters files: those the run's listing showed, so an object an
// earlier interrupted run already deleted is not counted twice.
func (r expiryRun) present(files []FileRef) []FileRef {
	return slices.DeleteFunc(slices.Clone(files), func(f FileRef) bool { return !r.listed[f.Path] })
}

// remove runs steps 1–4 from the plan: tombstone (unless recorded already),
// canonical manifest, partition manifests, data. The snapshot is expired
// once its canonical manifest is gone, so the verdict is set then, even if
// a later step fails.
func (r expiryRun) remove(ctx context.Context, c expiryCandidate, plan expiryPlan, owned []FileRef, out expiryOutcome) (expiryOutcome, error) {
	d := r.ds
	if !plan.recorded {
		if err := d.tombstones().record(ctx, plan.tomb); err != nil {
			return out, err
		}
	}
	manifests := d.allManifestPaths(c, plan.tomb.Files)
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
		if r.listed[f.Path] {
			out.deleted = append(out.deleted, f)
		}
	}
	return out, nil
}

// loadCandidate loads the snapshot's manifest: the canonical copy, else any
// partition copy the listing found (pre-canonical data, or an expiry that
// stopped after its canonical delete). found is false when every copy is
// gone: another expiry finished it.
func (d *dataset) loadCandidate(ctx context.Context, c expiryCandidate) (m *Manifest, found bool, err error) {
	for _, p := range d.manifestCopies(c.id, c.paths) {
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

// copiesMatch reports whether every manifest copy at paths, and the canonical
// one, records files where it still loads. Callers pass every path their
// expiry would delete. Commit writes one serialization to every path, so a
// copy that differs belongs to another snapshot with the same ID.
func (d *dataset) copiesMatch(ctx context.Context, id DatasetSnapshotID, listed []string, files []FileRef) (bool, error) {
	for _, p := range d.manifestCopies(id, listed) {
		s, ok, err := d.history.loadListed(ctx, manifestEntry[DatasetSnapshotID]{id, p})
		if err != nil {
			return false, fmt.Errorf("lode: failed to load manifest %s: %w", p, err)
		}
		if ok && !sameInventory(s.Manifest.Files, files) {
			return false, nil
		}
	}
	return true, nil
}

// manifestCopies is where snapshot id's manifest may be: the canonical path
// first, then every other copy the listing found.
func (d *dataset) manifestCopies(id DatasetSnapshotID, listed []string) []string {
	canonical := d.layout.manifestPath(d.id, id)
	others := slices.DeleteFunc(slices.Clone(listed), func(p string) bool { return p == canonical })
	return append([]string{canonical}, others...)
}

// splitFiles separates the data objects Expire may delete from every other
// path an inventory lists, which is reported and left in place.
func (d *dataset) splitFiles(id DatasetSnapshotID, files []FileRef) (owned []FileRef, skipped []string) {
	for _, f := range files {
		if d.layout.isObjectOf(d.id, id, dataLeaf, f.Path) {
			owned = append(owned, f)
			continue
		}
		skipped = append(skipped, f.Path)
	}
	return owned, skipped
}

// allManifestPaths is every manifest path of the snapshot, canonical first:
// the ones commit wrote for its inventory's partitions and any the listing
// found. A path derived from a malformed FileRef is dropped unless it is a
// canonical manifest path of this snapshot.
func (d *dataset) allManifestPaths(c expiryCandidate, files []FileRef) []string {
	partitions := make([]string, 0, len(files))
	for _, f := range files {
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

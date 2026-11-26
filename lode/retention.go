package lode

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/validate"
)

// Retention removes snapshots without ever removing what a manifest still
// describes (CONTRACT_RETENTION.md). Expiry writes a tombstone, then deletes
// manifests, then data, so a crash at any point leaves either a readable
// snapshot or garbage that a later run reclaims.

// Expire removes the selected snapshots, oldest first (CONTRACT_RETENTION
// §Snapshot Expiry). The report is never nil and records every outcome
// reached before an error.
func (d *writer) Expire(ctx context.Context, req ExpireRequest) (*ExpireReport, error) {
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
	if err == nil && head.current == "" {
		err = d.refuseHeadless(ctx)
	}
	if err != nil {
		return report, err
	}
	run := expiryRun{ds: d.dataset, sel: sel, head: head, listed: listing.listed, dryRun: req.DryRun}
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
			return validate.Argument("Expire requires at least one of IDs, Partition or Before")
		}},
		{!validPartitionPrefix(req.Partition), func() error {
			return fmt.Errorf("lode: Expire partition %q: %w", req.Partition, ErrInvalidPath)
		}},
		{req.Partition != "" && !d.layout.supportsPartitions(), func() error {
			return fmt.Errorf("lode: Expire partition %q: %w", req.Partition, ErrNotPartitioned)
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
	matches   []func(SnapshotID) bool
	// byTombstone are the requested IDs a tombstone alone can select:
	// Partition and Before select from manifests only.
	byTombstone []SnapshotID
}

func newSelection(req ExpireRequest) selection {
	sel := selection{partition: strings.TrimSuffix(req.Partition, "/")}
	if req.Partition == "" && req.Before.IsZero() {
		sel.byTombstone = slices.Compact(slices.Sorted(slices.Values(req.IDs))) // each once
	}
	if len(req.IDs) > 0 {
		ids := make(map[SnapshotID]struct{}, len(req.IDs))
		for _, id := range req.IDs {
			ids[id] = struct{}{}
		}
		sel.matches = append(sel.matches, func(id SnapshotID) bool {
			_, ok := ids[id]
			return ok
		})
	}
	if !req.Before.IsZero() {
		sel.matches = append(sel.matches, func(id SnapshotID) bool { return startedBefore(id, req.Before) })
	}
	return sel
}

// selects reports whether every set selector matches id. Partition is not
// among them: the listing prefix applies it.
func (s selection) selects(id SnapshotID) bool {
	for _, match := range s.matches {
		if !match(id) {
			return false
		}
	}
	return true
}

// listPrefix is what to list: the partition subtree when one is set (the
// trailing "/" makes the match whole-segment), else every snapshot.
func (s selection) listPrefix(l *pathLayout, dataset DatasetID) string {
	if s.partition == "" {
		return l.snapshotsPrefix(dataset)
	}
	return l.partitionsPrefix(dataset, s.partition+"/")
}

// spansOutside reports whether a snapshot selected by partition also has
// data objects outside it (CONTRACT_RETENTION §Mixed Snapshots). Callers pass
// only the snapshot's own data objects: a foreign path is never deleted, so
// it never decides.
func (s selection) spansOutside(l *pathLayout, owned []ObjectRef) bool {
	if s.partition == "" {
		return false
	}
	return slices.ContainsFunc(owned, func(obj ObjectRef) bool {
		return !withinPartition(l.extractPartitionPath(obj.Path), s.partition)
	})
}

func startedBefore(id SnapshotID, cutoff time.Time) bool {
	t, ok := history.Instant(id)
	return ok && t.Before(cutoff)
}

// snapshotKeys is one snapshot's keys of one kind from a listing, sorted.
type snapshotKeys struct {
	id    SnapshotID
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
	listed     map[string]struct{}
}

func (d *dataset) expiryListing(ctx context.Context, sel selection) (expiryListing, error) {
	keys, err := d.store.List(ctx, sel.listPrefix(d.layout, d.id))
	if err != nil {
		return expiryListing{}, fmt.Errorf("failed to list snapshots: %w", err)
	}
	listing := expiryListing{listed: make(map[string]struct{}, len(keys))}
	for _, k := range keys {
		listing.listed[k] = struct{}{}
	}
	withManifest := make(map[SnapshotID]struct{})
	for _, g := range groupBySnapshot(keys, d.objectsOfKind(manifestLeaf, sel.selects)) {
		withManifest[g.id] = struct{}{}
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
func (d *dataset) tombstoneCandidates(ctx context.Context, sel selection, withManifest map[SnapshotID]struct{}) ([]expiryCandidate, error) {
	var out []expiryCandidate
	for _, id := range sel.byTombstone {
		if _, ok := withManifest[id]; ok {
			continue
		}
		t, err := d.tombstones().lookup(ctx, id)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, expiryCandidate{snapshotKeys: snapshotKeys{id: id}, recorded: &t})
	}
	return out, nil
}

// objectsOfKind accepts keys that are, in canonical form, this dataset's
// objects of the given kind and belong to a snapshot keep accepts. A
// listing may return a raw key that a store would clean into another path;
// such a key is outside retention's scope.
func (d *dataset) objectsOfKind(kind pathKind, keep func(SnapshotID) bool) func(string) (SnapshotID, bool) {
	return func(p string) (SnapshotID, bool) {
		sp, ok := d.layout.parse(p)
		return sp.snapshot, ok && d.layout.isObjectOf(d.id, sp.snapshot, kind, p) && keep(sp.snapshot)
	}
}

// groupBySnapshot groups the keys owner accepts by snapshot, oldest first.
func groupBySnapshot(keys []string, owner func(string) (SnapshotID, bool)) []snapshotKeys {
	byID := make(map[SnapshotID][]string)
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
	history   *history.History[SnapshotID, Manifest]
	current   SnapshotID
	protected map[SnapshotID]struct{}
}

// refuseHeadless is for a dataset with neither a pointer nor a manifest: a
// tombstone there means snapshots existed, so the head is unknown, not
// absent (CONTRACT_RETENTION §Head Protection).
func (d *dataset) refuseHeadless(ctx context.Context) error {
	keys, err := d.tombstones().list(ctx)
	if err != nil || len(keys) == 0 {
		return err
	}
	return fmt.Errorf("lode: latest pointer is absent beside tombstones: %w", ErrHeadUncommitted)
}

func newHeadGuard(ctx context.Context, h *history.History[SnapshotID, Manifest]) (*headGuard, error) {
	g := &headGuard{history: h, protected: make(map[SnapshotID]struct{})}
	return g, g.refresh(ctx)
}

func (g *headGuard) refresh(ctx context.Context) error {
	id, err := g.history.CommittedHead(ctx, g.current)
	if err != nil {
		return err
	}
	g.current = id
	g.protected[id] = struct{}{}
	return nil
}

func (g *headGuard) is(id SnapshotID) bool {
	_, ok := g.protected[id]
	return ok
}

// -----------------------------------------------------------------------------
// Expiring one snapshot
// -----------------------------------------------------------------------------

// expiryVerdict is what Expire decided for one candidate.
type expiryVerdict int

const (
	verdictVanished expiryVerdict = iota // neither manifest nor tombstone left
	verdictRetained                      // the head
	verdictMixed                         // objects outside the selected partition
	verdictExpired                       // expired, or would be on a dry run
)

// expiryOutcome is one candidate's verdict and the objects it touched.
type expiryOutcome struct {
	id      SnapshotID
	verdict expiryVerdict
	deleted []ObjectRef // data objects deleted, or that would be on a dry run
	skipped []string    // manifest paths that are not this snapshot's objects
}

// recordVerdict says where each verdict lands in the report.
var recordVerdict = map[expiryVerdict]func(*ExpireReport, SnapshotID){
	verdictRetained: func(r *ExpireReport, id SnapshotID) { r.RetainedHead = id },
	verdictMixed:    func(r *ExpireReport, id SnapshotID) { r.Mixed = append(r.Mixed, id) },
	verdictExpired:  func(r *ExpireReport, id SnapshotID) { r.Expired = append(r.Expired, id) },
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
	listed map[string]struct{}
	dryRun bool
}

// expiryPlan is what expiring one snapshot works from: its tombstone, the
// one already recorded or the one this run would record from the manifest.
type expiryPlan struct {
	tomb     tombstone
	recorded bool // an earlier or concurrent run recorded it
	// loaded is the manifest copy that a new tombstone was built from, keyed by
	// its path, so the check of every copy does not read that path again. A
	// recorded tombstone leaves it nil.
	loaded map[string]*Manifest
}

// expire decides one candidate and, unless this is a dry run, carries the
// decision out.
func (r expiryRun) expire(ctx context.Context, c expiryCandidate) (expiryOutcome, error) {
	out := expiryOutcome{id: c.id}
	if r.head.is(c.id) {
		return r.retain(ctx, out)
	}
	plan, err := r.plan(ctx, c)
	if errors.Is(err, errSnapshotGone) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	split := r.ds.splitObjects(c.id, plan.tomb.Objects)
	// A tombstone's inventory is the whole snapshot, so a resumed expiry is
	// checked against the partition too.
	if r.sel.spansOutside(r.ds.layout, split.owned) {
		out.verdict = verdictMixed
		return out, nil
	}
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
		out = out.expired(split)
		out.deleted = r.present(split.owned)
		return out, nil
	}
	return r.remove(ctx, c, plan, split, out)
}

// retain keeps a snapshot the head guard protects. The pointer is still
// rechecked, as before any snapshot's step 1: a pending commit stops the
// run with ErrHeadUncommitted here too.
func (r expiryRun) retain(ctx context.Context, out expiryOutcome) (expiryOutcome, error) {
	if err := r.head.refresh(ctx); err != nil {
		return out, err
	}
	out.verdict = verdictRetained
	return out, nil
}

// errSnapshotGone is a candidate with neither a manifest nor a tombstone
// left: another run finished it, so this run has nothing to do.
var errSnapshotGone = errors.New("snapshot already gone")

// plan finds the tombstone to work from, the one recorded or the one this
// run would record from the manifest, and checks it against every manifest
// copy this expiry would delete before anything is written.
func (r expiryRun) plan(ctx context.Context, c expiryCandidate) (expiryPlan, error) {
	// Checked before anything else, so a dry run fails where the real run
	// would when it came to record the tombstone.
	if !history.IsGeneratedID(c.id) {
		return expiryPlan{}, errUntombstonable(c.id)
	}
	plan, err := r.tombstonePlan(ctx, c)
	if err != nil {
		return plan, err
	}
	// A copy the tombstone does not describe (malformed, or of an earlier
	// snapshot with the same ID) must never be deleted under it; every path
	// this expiry would delete is checked, listed or not.
	matches, err := r.ds.describesEveryCopy(ctx, plan.tomb, r.ds.allManifestPaths(c.id, c.paths, plan.tomb.Objects), plan.loaded)
	if err != nil {
		return expiryPlan{}, err
	}
	if !matches {
		return expiryPlan{}, fmt.Errorf("lode: tombstone for %s records another snapshot than its manifest", c.id)
	}
	return plan, nil
}

// tombstonePlan picks the tombstone: the recorded one, else a new one from
// the manifest. errSnapshotGone means neither is left.
func (r expiryRun) tombstonePlan(ctx context.Context, c expiryCandidate) (expiryPlan, error) {
	// A candidate selected by its tombstone is checked for a manifest too:
	// one may have appeared since the listing.
	m, path, err := r.ds.loadCandidate(ctx, c)
	hasManifest := err == nil
	var loaded map[string]*Manifest
	if hasManifest {
		loaded = map[string]*Manifest{path: m}
	}
	if err != nil && !errors.Is(err, ErrNotFound) {
		return expiryPlan{}, err
	}
	t, err := r.tombstoneFor(ctx, c)
	switch {
	case errors.Is(err, ErrNotFound) && !hasManifest:
		return expiryPlan{}, errSnapshotGone
	case (errors.Is(err, ErrNotFound) || errors.Is(err, errEmptyTombstone)) && hasManifest:
		// None recorded yet; an empty placeholder is replaced by the write.
		return expiryPlan{tomb: newTombstone(c.id, m), loaded: loaded}, nil
	case err != nil:
		return expiryPlan{}, err
	}
	// A recorded tombstone was not built from the loaded copy, and its
	// existence lets another run delete that copy and a writer reuse the ID,
	// so the check of every copy reads each path again.
	return expiryPlan{tomb: t, recorded: true}, nil
}

// tombstoneFor is the candidate's recorded tombstone: the one selection
// already read, or a lookup. ErrNotFound means none is recorded.
func (r expiryRun) tombstoneFor(ctx context.Context, c expiryCandidate) (tombstone, error) {
	if c.recorded != nil {
		return *c.recorded, nil
	}
	return r.ds.tombstones().lookup(ctx, c.id)
}

// present meters objects: those the run's listing showed, so an object an
// earlier interrupted run already deleted is not counted twice.
func (r expiryRun) present(objects []ObjectRef) []ObjectRef {
	return slices.DeleteFunc(slices.Clone(objects), func(obj ObjectRef) bool {
		_, ok := r.listed[obj.Path]
		return !ok
	})
}

// remove runs steps 1–4 from the plan: tombstone (unless recorded already),
// canonical manifest, partition manifests, data. The snapshot is expired
// once its canonical manifest is gone, so the verdict is set then, even if
// a later step fails.
func (r expiryRun) remove(ctx context.Context, c expiryCandidate, plan expiryPlan, split inventorySplit, out expiryOutcome) (expiryOutcome, error) {
	d := r.ds
	manifests := d.allManifestPaths(c.id, c.paths, plan.tomb.Objects)
	if !plan.recorded {
		won, err := d.tombstones().record(ctx, plan.tomb)
		if err != nil {
			return out, err
		}
		// Another run recorded the tombstone, and may have finished its expiry
		// since this plan checked a copy it had loaded, so every copy is read
		// again before any is deleted.
		if !won && plan.loaded != nil {
			matches, err := d.describesEveryCopy(ctx, plan.tomb, manifests, nil)
			if err != nil {
				return out, err
			}
			if !matches {
				return out, fmt.Errorf("lode: tombstone for %s records another snapshot than its manifest", c.id)
			}
		}
	}
	if err := deleteObject(ctx, d.store, manifests[0]); err != nil {
		return out, err
	}
	out = out.expired(split)
	if err := deleteObjects(ctx, d.store, manifests[1:]); err != nil {
		return out, err
	}
	for _, obj := range split.owned {
		if err := deleteObject(ctx, d.store, obj.Path); err != nil {
			return out, err
		}
		if _, ok := r.listed[obj.Path]; ok {
			out.deleted = append(out.deleted, obj)
		}
	}
	return out, nil
}

// loadCandidate loads the snapshot's manifest: the canonical copy, else any
// partition copy the listing found (pre-canonical data, or an expiry that
// stopped after its canonical delete). ErrNotFound means every copy is gone.
func (d *dataset) loadCandidate(ctx context.Context, c expiryCandidate) (*Manifest, string, error) {
	for _, p := range d.manifestCopies(c.id, c.paths) {
		m, err := d.loadManifestAt(ctx, c.id, p)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		return m, p, err
	}
	return nil, "", fmt.Errorf("lode: snapshot %s has no manifest: %w", c.id, ErrNotFound)
}

// describesEveryCopy reports whether t describes every manifest copy of its
// snapshot that still loads at paths or the canonical path. Callers pass
// every path their expiry would delete. Commit writes one serialization to
// every path, so a copy t does not describe belongs to another snapshot
// with the same ID.
func (d *dataset) describesEveryCopy(ctx context.Context, t tombstone, paths []string, loaded map[string]*Manifest) (bool, error) {
	copies, err := d.loadCopies(ctx, t.SnapshotID, paths, loaded)
	if err != nil {
		return false, err
	}
	return !slices.ContainsFunc(copies, func(m *Manifest) bool { return !t.describes(m) }), nil
}

// loadCopies loads every manifest copy of snapshot id that is present at
// paths or the canonical path, whether or not a listing showed it. A copy in
// loaded, which the caller already loaded and validated, is used as it is
// rather than read again.
func (d *dataset) loadCopies(ctx context.Context, id SnapshotID, paths []string, loaded map[string]*Manifest) ([]*Manifest, error) {
	var copies []*Manifest
	for _, p := range d.manifestCopies(id, paths) {
		if m, ok := loaded[p]; ok {
			copies = append(copies, m)
			continue
		}
		m, err := d.loadManifestAt(ctx, id, p)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		copies = append(copies, m)
	}
	return copies, nil
}

// loadManifestAt loads and validates the manifest copy at p. ErrNotFound
// means it is absent. An invalid or misplaced manifest is an error: nothing
// is expired or recorded from it.
func (d *dataset) loadManifestAt(ctx context.Context, id SnapshotID, p string) (*Manifest, error) {
	s, err := d.history.LoadAt(ctx, id, p)
	if errors.Is(err, ErrNotFound) {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest %s: %w", p, err)
	}
	return s.Manifest, nil
}

// manifestCopies is where snapshot id's manifest may be: the canonical path
// first, then every other copy the listing found.
func (d *dataset) manifestCopies(id SnapshotID, listed []string) []string {
	canonical := d.layout.manifestPath(d.id, id)
	others := slices.DeleteFunc(slices.Clone(listed), func(p string) bool { return p == canonical })
	return append([]string{canonical}, others...)
}

// inventorySplit is an inventory divided into the data objects Expire may
// delete and every other path, which is reported and left in place.
type inventorySplit struct {
	owned   []ObjectRef
	skipped []string
}

func (d *dataset) splitObjects(id SnapshotID, objects []ObjectRef) inventorySplit {
	var split inventorySplit
	seen := make(map[string]struct{}, len(objects))
	for _, obj := range objects {
		if _, dup := seen[obj.Path]; dup {
			continue // one object, counted once however often it is listed
		}
		seen[obj.Path] = struct{}{}
		if d.layout.isObjectOf(d.id, id, dataLeaf, obj.Path) {
			split.owned = append(split.owned, obj)
			continue
		}
		split.skipped = append(split.skipped, obj.Path)
	}
	return split
}

// expired marks out expired: only then does it report the snapshot's
// skipped paths, so a run that stops earlier never claims them.
func (out expiryOutcome) expired(split inventorySplit) expiryOutcome {
	out.verdict, out.skipped = verdictExpired, split.skipped
	return out
}

// allManifestPaths is every manifest path of the snapshot, canonical first:
// the ones commit wrote for its inventory's partitions and any a listing
// found. A path derived from a malformed ObjectRef is dropped unless it is a
// canonical manifest path of this snapshot.
func (d *dataset) allManifestPaths(id SnapshotID, listed []string, objects []ObjectRef) []string {
	partitions := make([]string, 0, len(objects))
	for _, obj := range objects {
		partitions = append(partitions, d.layout.extractPartitionPath(obj.Path))
	}
	paths := slices.DeleteFunc(d.manifestPaths(id, partitions), func(p string) bool {
		return !d.layout.isObjectOf(d.id, id, manifestLeaf, p)
	})
	for _, p := range listed {
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
		return fmt.Errorf("failed to delete %s: %w", p, err)
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

func totalBytes(objects []ObjectRef) int64 {
	var n int64
	for _, obj := range objects {
		n += obj.SizeBytes
	}
	return n
}

package lode

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

// Sweep completes interrupted expiries and reclaims never-committed data
// (CONTRACT_RETENTION §Unreferenced-Object Sweep). It works from two
// listings taken up front; the report is never nil.
func (d *dataset) Sweep(ctx context.Context, req SweepRequest) (*SweepReport, error) {
	report := &SweepReport{}
	if req.Grace <= 0 {
		return report, invalidArgument("Sweep grace must be positive, got %s", req.Grace)
	}
	inv, err := d.sweepInventory(ctx)
	report.Unparseable = inv.invalidTombstones // examined before any later failure
	if err != nil {
		return report, err
	}
	run := sweepRun{
		ds: d, inv: inv, dryRun: req.DryRun, cutoff: time.Now().Add(-req.Grace),
		settled: make(map[DatasetSnapshotID]tombstone), cleared: make(map[DatasetSnapshotID]bool),
		held: d.rejectedTombstoneIDs(inv.invalidTombstones),
	}
	if err := run.settle(ctx, report); err != nil {
		return report, err
	}
	return report, run.reclaim(ctx, report)
}

// -----------------------------------------------------------------------------
// Inventory
// -----------------------------------------------------------------------------

// sweepInventory is what a sweep acts on, gathered before any delete.
type sweepInventory struct {
	tombstones        []tombstone // valid tombstones, oldest first
	invalidTombstones []string
	manifests         map[DatasetSnapshotID][]string // every manifest key, by snapshot
	objects           []snapshotKeys                 // every data object key, by snapshot
	dataOf            map[DatasetSnapshotID][]string // the same keys, looked up by snapshot
	holds             func(DatasetSnapshotID) bool   // tombstoned snapshots never settled this run
}

func (d *dataset) sweepInventory(ctx context.Context) (sweepInventory, error) {
	var inv sweepInventory
	var err error
	if inv.tombstones, inv.invalidTombstones, err = d.inventoryTombstones(ctx); err != nil {
		return inv, err
	}
	keys, err := d.store.List(ctx, d.layout.snapshotsPrefix(d.id))
	if err != nil {
		return inv, fmt.Errorf("lode: failed to list snapshots: %w", err)
	}
	all := func(DatasetSnapshotID) bool { return true }
	inv.manifests = make(map[DatasetSnapshotID][]string)
	for _, g := range groupBySnapshot(keys, d.objectsOfKind(manifestLeaf, all)) {
		inv.manifests[g.id] = g.paths
	}
	inv.objects = groupBySnapshot(keys, d.objectsOfKind(dataLeaf, all))
	inv.dataOf = make(map[DatasetSnapshotID][]string, len(inv.objects))
	for _, g := range inv.objects {
		inv.dataOf[g.id] = g.paths
	}
	inv.holds, err = d.sweepGuard(ctx, inv)
	return inv, err
}

// inventoryTombstones splits the tombstones into those validly recorded,
// oldest first, and the keys of those that are not.
func (d *dataset) inventoryTombstones(ctx context.Context) (valid []tombstone, invalid []string, err error) {
	ts := d.tombstones()
	keys, err := ts.list(ctx)
	if err != nil {
		return nil, nil, err
	}
	slices.Sort(keys)
	for _, k := range keys {
		t, err := ts.recorded(ctx, k)
		if errors.Is(err, errInvalidTombstone) {
			invalid = append(invalid, k)
			continue
		}
		if err != nil {
			return valid, invalid, err // what was examined is still reported
		}
		valid = append(valid, t)
	}
	return valid, invalid, nil
}

// rejectedTombstoneIDs is the snapshots whose tombstone failed validation,
// named by key. They are held: a tombstone Lode cannot trust says nothing
// about whether the snapshot's objects are garbage.
func (d *dataset) rejectedTombstoneIDs(keys []string) map[DatasetSnapshotID]bool {
	prefix := d.layout.tombstonesPrefix(d.id)
	held := make(map[DatasetSnapshotID]bool, len(keys))
	for _, k := range keys {
		held[DatasetSnapshotID(strings.TrimPrefix(k, prefix))] = true
	}
	return held
}

// sweepGuard decides which tombstoned snapshots this sweep must not settle
// (CONTRACT_RETENTION §Head protection). Only the pointer names the head:
//   - a pointer naming a committed snapshot holds that one;
//   - a pointer naming a snapshot with no manifest means a commit is in
//     flight and the head is unknown, so none is settled this run (orphans
//     past grace still are);
//   - with no pointer, a dataset showing a manifest or a tombstone is
//     refused with ErrHeadUncommitted, and one showing neither has no head.
//
// Lode never tombstones a head, so this only keeps a misplaced object under
// expired/ from removing one.
func (d *dataset) sweepGuard(ctx context.Context, inv sweepInventory) (func(DatasetSnapshotID) bool, error) {
	head, err := d.history.readPointer(ctx)
	if errors.Is(err, ErrNotFound) {
		if len(inv.manifests) > 0 || len(inv.tombstones) > 0 || len(inv.invalidTombstones) > 0 {
			return nil, fmt.Errorf("lode: latest pointer is absent: %w", ErrHeadUncommitted)
		}
		return holdsNone, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lode: failed to resolve head: %w", err)
	}
	// One pointer read decides: the value just read is the one confirmed.
	err = d.history.confirmCommitted(ctx, head)
	if errors.Is(err, ErrHeadUncommitted) {
		return holdsAll, nil
	}
	if err != nil {
		return nil, err
	}
	return func(id DatasetSnapshotID) bool { return id == head }, nil
}

func holdsNone(DatasetSnapshotID) bool { return false }
func holdsAll(DatasetSnapshotID) bool  { return true }

// -----------------------------------------------------------------------------
// Settling
// -----------------------------------------------------------------------------

// sweepRun is one Sweep call's state: the inventory and what it settled.
type sweepRun struct {
	ds      *dataset
	inv     sweepInventory
	dryRun  bool
	cutoff  time.Time                       // an orphan whose write began before this is reclaimed
	settled map[DatasetSnapshotID]tombstone // settled this run or earlier, with its inventory
	cleared map[DatasetSnapshotID]bool      // settled from what this run found, unlisted objects included
	held    map[DatasetSnapshotID]bool      // protected, or its tombstone rejected: never touched
}

// settle removes the remaining manifests of every tombstoned snapshot,
// canonical first (§Order of operations, steps 2–3). A snapshot is settled
// only once all of them are gone; a dry run settles virtually.
func (r sweepRun) settle(ctx context.Context, report *SweepReport) error {
	for _, t := range r.inv.tombstones {
		if err := r.settleOne(ctx, t, report); err != nil {
			return err
		}
	}
	return nil
}

// settleOne settles one tombstoned snapshot from the manifest copies that
// are present, not only those the listing showed: a listing may omit one.
// A snapshot with nothing left in the listing has nothing to remove and
// costs no store call. A copy its tombstone does not describe means the
// tombstone belongs to another snapshot: it is held and reported, protected
// or not. A protected snapshot is held. Settled reports the snapshots whose
// manifests this run removed.
func (r sweepRun) settleOne(ctx context.Context, t tombstone, report *SweepReport) error {
	id := t.SnapshotID
	if r.nothingLeft(id) {
		r.settled[id] = t
		return nil
	}
	paths := r.ds.allManifestPaths(id, r.inv.manifests[id], append(slices.Clone(t.Files), refsTo(r.inv.dataOf[id])...))
	copies, err := r.ds.loadCopies(ctx, id, paths)
	if err != nil {
		return err
	}
	if slices.ContainsFunc(copies, func(m *Manifest) bool { return !t.describes(m) }) {
		report.Unparseable = append(report.Unparseable, r.ds.layout.tombstonePath(r.ds.id, id))
		r.held[id] = true
		return nil
	}
	if r.inv.holds(id) {
		r.held[id] = true
		return nil
	}
	if len(copies) > 0 && !r.dryRun {
		if err := deleteObjects(ctx, r.ds.store, paths); err != nil {
			return err
		}
	}
	r.settled[id], r.cleared[id] = t, true
	if len(copies) > 0 {
		report.Settled = append(report.Settled, id)
	}
	return nil
}

// nothingLeft reports whether the listing showed neither a manifest nor a
// data object of the snapshot.
func (r sweepRun) nothingLeft(id DatasetSnapshotID) bool {
	_, manifests := r.inv.manifests[id]
	return !manifests && len(r.inv.dataOf[id]) == 0
}

// refsTo wraps object paths as FileRefs, from which manifest paths derive.
func refsTo(paths []string) []FileRef {
	refs := make([]FileRef, 0, len(paths))
	for _, p := range paths {
		refs = append(refs, FileRef{Path: p})
	}
	return refs
}

// anyManifest reports whether a manifest of snapshot id is present at paths
// or the canonical path, whether or not a listing showed it.
func (d *dataset) anyManifest(ctx context.Context, id DatasetSnapshotID, paths []string) (bool, error) {
	for _, p := range d.manifestCopies(id, paths) {
		present, err := d.store.Exists(ctx, p)
		if err != nil {
			return false, fmt.Errorf("lode: failed to check manifest %s: %w", p, err)
		}
		if present {
			return true, nil
		}
	}
	return false, nil
}

// -----------------------------------------------------------------------------
// Reclaiming
// -----------------------------------------------------------------------------

// objectClass is what a sweep does with one snapshot's data objects.
type objectClass int

const (
	classKept        objectClass = iota // live, held, or within the grace period
	classSettled                        // its snapshot was settled: reclaimed regardless of grace
	classOrphan                         // never committed and older than grace: reclaimed
	classUnparseable                    // never committed and its ID carries no instant: reported
)

// objectAction is what a sweep does with one group of objects.
type objectAction struct{ reclaim, orphan, report bool }

// objectActions is what each class does, as data.
var objectActions = map[objectClass]objectAction{
	classSettled:     {reclaim: true},
	classOrphan:      {reclaim: true, orphan: true},
	classUnparseable: {report: true},
}

// classify decides by settlement, liveness and age. A held snapshot (one
// the head guard held back, or whose tombstone was rejected) keeps its
// objects; otherwise the guard is not
// consulted, so a crashed commit's data, which the pointer may still name,
// ages out like any other orphan.
func (r sweepRun) classify(id DatasetSnapshotID) objectClass {
	if r.held[id] {
		return classKept
	}
	if _, settled := r.settled[id]; settled {
		return classSettled
	}
	if _, live := r.inv.manifests[id]; live {
		return classKept
	}
	began, ok := snapshotInstant(id)
	if !ok {
		return classUnparseable
	}
	if began.Before(r.cutoff) {
		return classOrphan
	}
	return classKept
}

// reclaim applies each snapshot's class to its data objects (§Definition,
// step 2). Reclaim is per snapshot; a settled snapshot's inventory decides
// only how each object is counted (§Accounting).
func (r sweepRun) reclaim(ctx context.Context, report *SweepReport) error {
	for _, g := range r.inv.objects {
		act, err := r.actionFor(ctx, g)
		if err != nil {
			return err
		}
		if act.report {
			report.Unparseable = append(report.Unparseable, g.paths...)
		}
		if !act.reclaim {
			continue
		}
		sizes := recordedSizes(r.settled[g.id])
		for _, p := range g.paths {
			if err := r.delete(ctx, p); err != nil {
				return err
			}
			report.count(p, sizes, act.orphan)
		}
	}
	return r.reclaimUnlisted(ctx)
}

// reclaimUnlisted deletes the inventory objects of snapshots this run
// settled that its listing did not show, so a lagging listing leaves no
// expired data behind. They are not counted: counts follow the listing
// (§Accounting).
func (r sweepRun) reclaimUnlisted(ctx context.Context) error {
	for _, id := range slices.Sorted(maps.Keys(r.cleared)) {
		for _, p := range r.unlisted(id) {
			if err := r.delete(ctx, p); err != nil {
				return err
			}
		}
	}
	return nil
}

// unlisted is the snapshot's own inventory objects its listing did not show.
func (r sweepRun) unlisted(id DatasetSnapshotID) []string {
	listed := r.inv.dataOf[id]
	var out []string
	for _, f := range r.ds.splitFiles(id, r.settled[id].Files).owned {
		if !slices.Contains(listed, f.Path) {
			out = append(out, f.Path)
		}
	}
	return out
}

// actionFor is what to do with one group. An orphan is reclaimed only once
// no manifest of its snapshot is found at a path its objects derive: a
// listing may omit one, and a manifest still describes its objects.
func (r sweepRun) actionFor(ctx context.Context, g snapshotKeys) (objectAction, error) {
	act := objectActions[r.classify(g.id)]
	if !act.orphan {
		return act, nil
	}
	live, err := r.ds.anyManifest(ctx, g.id, r.ds.allManifestPaths(g.id, nil, refsTo(g.paths)))
	if err != nil || live {
		return objectAction{}, err
	}
	return act, nil
}

func (r sweepRun) delete(ctx context.Context, p string) error {
	if r.dryRun {
		return nil
	}
	return deleteObject(ctx, r.ds.store, p)
}

// recordedSizes maps each inventory path to its recorded size; empty for a
// snapshot with no tombstone.
func recordedSizes(t tombstone) map[string]int64 {
	sizes := make(map[string]int64, len(t.Files))
	for _, f := range t.Files {
		sizes[f.Path] = f.SizeBytes
	}
	return sizes
}

// count meters one deleted object: its recorded size when an inventory
// lists it, otherwise as unsized and, for a never-committed snapshot, as an
// orphan.
func (r *SweepReport) count(p string, sizes map[string]int64, orphan bool) {
	r.Objects++
	if size, ok := sizes[p]; ok {
		r.Bytes += size
		return
	}
	r.Unsized++
	if orphan {
		r.Orphans++
	}
}

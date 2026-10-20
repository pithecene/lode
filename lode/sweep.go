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
	if err != nil {
		return report, err
	}
	report.Unparseable = inv.invalidTombstones
	run := sweepRun{
		ds: d, inv: inv, dryRun: req.DryRun, cutoff: time.Now().Add(-req.Grace),
		settled: make(map[DatasetSnapshotID]tombstone), held: d.rejectedTombstoneIDs(inv.invalidTombstones),
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
	protected         map[DatasetSnapshotID]bool     // never settled
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
	inv.protected, err = d.sweepProtected(ctx, inv.manifests)
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
		t, ok, err := ts.recorded(ctx, k)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			valid = append(valid, t)
			continue
		}
		invalid = append(invalid, k)
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

// sweepProtected is the head guard of a sweep: the snapshot the pointer
// names and, when the pointer is absent or names a snapshot without a
// manifest, the one a scan would return. Lode never tombstones a head, so
// this only keeps a misplaced object under expired/ from removing one.
func (d *dataset) sweepProtected(ctx context.Context, manifests map[DatasetSnapshotID][]string) (map[DatasetSnapshotID]bool, error) {
	id, err := d.history.readPointer(ctx)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return nil, fmt.Errorf("lode: failed to resolve head: %w", err)
	}
	protected := map[DatasetSnapshotID]bool{id: true}
	if _, committed := manifests[id]; !committed && len(manifests) > 0 {
		protected[slices.Max(slices.Collect(maps.Keys(manifests)))] = true
	}
	return protected, nil
}

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

// settleOne settles one tombstoned snapshot. One with a manifest copy its
// tombstone does not describe is held and reported, protected or not,
// because that tombstone belongs to another snapshot; a protected one is
// held; one with no manifest left was settled by an earlier run and is not
// reported.
func (r sweepRun) settleOne(ctx context.Context, t tombstone, report *SweepReport) error {
	id := t.SnapshotID
	listed, pending := r.inv.manifests[id]
	contradicted, err := r.contradicted(ctx, t, listed, pending)
	if err != nil {
		return err
	}
	if contradicted {
		report.Unparseable = append(report.Unparseable, r.ds.layout.tombstonePath(r.ds.id, id))
	}
	if contradicted || r.inv.protected[id] {
		r.held[id] = true
		return nil
	}
	if !pending {
		r.settled[id] = t
		return nil
	}
	if !r.dryRun {
		if err := deleteObjects(ctx, r.ds.store, r.ds.allManifestPaths(id, listed, t.Files)); err != nil {
			return err
		}
	}
	r.settled[id] = t
	report.Settled = append(report.Settled, id)
	return nil
}

// contradicted reports whether a manifest copy of t's snapshot that is
// still present records another inventory. With none listed, there is
// nothing to contradict it.
func (r sweepRun) contradicted(ctx context.Context, t tombstone, listed []string, pending bool) (bool, error) {
	if !pending {
		return false, nil
	}
	matches, err := r.ds.copiesMatch(ctx, t.SnapshotID, r.ds.allManifestPaths(t.SnapshotID, listed, t.Files), t.Files)
	return !matches, err
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

// objectActions is what each class does, as data.
var objectActions = map[objectClass]struct{ reclaim, orphan, report bool }{
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
		act := objectActions[r.classify(g.id)]
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
	return nil
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

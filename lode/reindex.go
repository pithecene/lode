package lode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/pithecene/lode/internal/history"
)

// The partition manifests of a Hive snapshot are an index of its canonical
// manifest, persisted after the commit is complete (CONTRACT_CORE §Head
// Pointer). A writer that terminates first leaves a committed snapshot that
// a listing filtered by partition omits. Sweep restores the index from the
// canonical manifest (CONTRACT_RETENTION §Unreferenced-Object Sweep, step 3).

// reindex persists the missing partition manifests of every committed
// snapshot that may be reindexed, oldest first.
func (r sweepRun) reindex(ctx context.Context, report *SweepReport) error {
	if !r.ds.layout.supportsPartitions() {
		return nil
	}
	for _, id := range slices.Sorted(maps.Keys(r.inv.manifests)) {
		if !r.mayReindex(id) {
			continue
		}
		// A snapshot is reported even when its repair then fails: the copies
		// it created remain, and a report describes the work completed.
		reindexed, err := r.reindexOne(ctx, id)
		if reindexed {
			report.Reindexed = append(report.Reindexed, id)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// mayReindex reports whether snapshot id is committed, untouched by
// retention, past the grace period, and missing a partition manifest that
// its listed data implies. Within the grace period its writer may still be
// persisting the index, which a copy persisted here would make fail with
// ErrPathExists (CONTRACT_RETENTION §Grace Period).
func (r sweepRun) mayReindex(id DatasetSnapshotID) bool {
	_, settled := r.settled[id]
	if settled || r.held[id] {
		return false
	}
	if !slices.Contains(r.inv.manifests[id], r.ds.layout.manifestPath(r.ds.id, id)) {
		return false
	}
	began, ok := history.Instant(id)
	return ok && began.Before(r.cutoff) && r.indexGap(id)
}

// indexGap reports whether a listed data object of snapshot id lies in a
// partition without a listed manifest of the snapshot. It decides from the
// listing alone, so a complete index costs no store call.
func (r sweepRun) indexGap(id DatasetSnapshotID) bool {
	indexed := make(map[string]bool, len(r.inv.manifests[id]))
	for _, p := range r.inv.manifests[id] {
		indexed[r.ds.layout.extractPartitionPath(p)] = true
	}
	return slices.ContainsFunc(r.inv.dataOf[id], func(p string) bool {
		partition := r.ds.layout.extractPartitionPath(p)
		return partition != "" && !indexed[partition]
	})
}

// reindexOne copies the canonical manifest of snapshot id, unchanged, to
// every partition path its files derive that the listing does not show. The
// canonical manifest, not the listing, decides the partitions. A copy
// persisted while an expiry recorded the snapshot is removed again, so the
// repair never restores a manifest that the expiry deleted. It reports
// whether copies created by this run remain, including when it fails.
func (r sweepRun) reindexOne(ctx context.Context, id DatasetSnapshotID) (bool, error) {
	canonical := r.ds.layout.manifestPath(r.ds.id, id)
	data, m, err := r.ds.readCanonical(ctx, id, canonical)
	if errors.Is(err, ErrNotFound) {
		return false, nil // expired since the listing
	}
	if err != nil {
		return false, err
	}
	// The tombstone check runs even after a failed write: a copy created
	// before the failure must not outlive an expiry either.
	// Only the snapshot's own data objects name its partitions: a FileRef
	// outside them (§Mixed Snapshots) never places a copy in this dataset.
	owned := r.ds.splitFiles(id, m.Files).owned
	persisted, putErr := r.persistMissing(ctx, id, r.ds.allManifestPaths(id, nil, owned), data)
	if len(persisted) == 0 || r.dryRun {
		return len(persisted) > 0, putErr
	}
	kept, err := r.keepUnlessExpired(ctx, id, persisted)
	return kept, errors.Join(putErr, err)
}

// persistMissing creates a copy of data at each partition path of snapshot
// id that the listing does not show, and returns the paths it created,
// those created before a failure included.
func (r sweepRun) persistMissing(ctx context.Context, id DatasetSnapshotID, paths []string, data []byte) ([]string, error) {
	canonical := r.ds.layout.manifestPath(r.ds.id, id)
	var persisted []string
	for _, p := range paths {
		if p == canonical || slices.Contains(r.inv.manifests[id], p) {
			continue
		}
		created, err := r.persistCopy(ctx, p, data)
		if err != nil {
			return persisted, err
		}
		if created {
			persisted = append(persisted, p)
		}
	}
	return persisted, nil
}

// persistCopy creates the manifest copy at p and reports whether this run
// created it; a dry run reports whether it would. A copy already present,
// which a lagging listing omitted, is left as it is.
func (r sweepRun) persistCopy(ctx context.Context, p string, data []byte) (bool, error) {
	if r.dryRun {
		present, err := r.ds.store.Exists(ctx, p)
		if err != nil {
			return false, fmt.Errorf("failed to check manifest %s: %w", p, err)
		}
		return !present, nil
	}
	err := r.ds.store.Put(ctx, p, bytes.NewReader(data))
	if errors.Is(err, ErrPathExists) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("failed to persist manifest %s: %w", p, err)
	}
	return true, nil
}

// keepUnlessExpired keeps the copies persisted for snapshot id unless a
// tombstone now records the snapshot. An expiry writes its tombstone before
// it deletes any manifest (CONTRACT_RETENTION §Order of Operations), so a
// copy persisted after the expiry deleted the partition manifests is seen
// here and deleted. It reports whether copies may remain: when the check
// or a deletion fails, they are reported with the error.
func (r sweepRun) keepUnlessExpired(ctx context.Context, id DatasetSnapshotID, persisted []string) (bool, error) {
	absent, err := r.ds.tombstones().absent(ctx, id)
	if err != nil || absent {
		return true, err
	}
	if err := deleteObjects(ctx, r.ds.store, persisted); err != nil {
		return true, err
	}
	return false, nil
}

// readCanonical reads the canonical manifest of snapshot id at p and
// returns its bytes, to copy unchanged, with the validated manifest they
// encode.
func (d *dataset) readCanonical(ctx context.Context, id DatasetSnapshotID, p string) ([]byte, *Manifest, error) {
	rc, err := d.store.Get(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	defer closer(rc)()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, nil, fmt.Errorf("lode: failed to read manifest %s: %w", p, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, nil, fmt.Errorf("lode: failed to decode manifest %s: %w", p, err)
	}
	if err := d.validateCopy(&m, id); err != nil {
		return nil, nil, fmt.Errorf("failed to load manifest %s: %w", p, err)
	}
	return data, &m, nil
}

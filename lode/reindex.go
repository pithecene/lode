package lode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/pithecene/lode/internal/history"
)

const reindexCleanupTimeout = 10 * time.Second

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
func (r sweepRun) mayReindex(id SnapshotID) bool {
	_, settled := r.settled[id]
	_, held := r.held[id]
	if settled || held {
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
func (r sweepRun) indexGap(id SnapshotID) bool {
	indexed := make(map[string]struct{}, len(r.inv.manifests[id]))
	for _, p := range r.inv.manifests[id] {
		indexed[r.ds.layout.extractPartitionPath(p)] = struct{}{}
	}
	return slices.ContainsFunc(r.inv.dataOf[id], func(p string) bool {
		partition := r.ds.layout.extractPartitionPath(p)
		if partition == "" {
			return false
		}
		_, ok := indexed[partition]
		return !ok
	})
}

// reindexOne copies the canonical manifest of snapshot id, unchanged, to
// every partition path its objects derive that the listing does not show. The
// canonical manifest, not the listing, decides the partitions. A copy
// persisted while an expiry recorded the snapshot is removed again, so the
// repair never restores a manifest that the expiry deleted. It reports
// whether copies created by this run remain, including when it fails.
func (r sweepRun) reindexOne(ctx context.Context, id SnapshotID) (bool, error) {
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
	// Only the snapshot's own data objects name its partitions: an ObjectRef
	// outside them (§Mixed Snapshots) never places a copy in this dataset.
	owned := r.ds.splitObjects(id, m.Objects).owned
	persisted, uncertain, putErr := r.persistMissing(ctx, id, r.ds.allManifestPaths(id, nil, owned), data)
	reindexed := len(persisted) > 0
	if uncertain != "" {
		persisted = append(persisted, uncertain)
	}
	if len(persisted) == 0 || r.dryRun {
		return reindexed, putErr
	}
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reindexCleanupTimeout)
	defer cancel()
	kept, err := r.keepUnlessExpired(cleanupCtx, id, persisted)
	if !kept || reindexed {
		return kept, errors.Join(putErr, err)
	}
	// An ambiguous Put alone does not establish that the repair wrote a copy.
	present, existsErr := r.ds.store.Exists(cleanupCtx, uncertain)
	if existsErr != nil {
		existsErr = fmt.Errorf("failed to verify repaired manifest %s: %w", uncertain, existsErr)
	}
	return present, errors.Join(putErr, err, existsErr)
}

// persistMissing creates a copy of data at each partition path of snapshot
// id that the listing does not show. It returns confirmed copies (hypothetical
// in a dry run) and the path of a failed Put that may have reached storage.
func (r sweepRun) persistMissing(ctx context.Context, id SnapshotID, paths []string, data []byte) ([]string, string, error) {
	canonical := r.ds.layout.manifestPath(r.ds.id, id)
	var persisted []string
	for _, p := range paths {
		if p == canonical || slices.Contains(r.inv.manifests[id], p) {
			continue
		}
		mayHavePersisted, putErr := r.persistCopy(ctx, p, data)
		if putErr != nil {
			if mayHavePersisted {
				return persisted, p, putErr
			}
			return persisted, "", putErr
		}
		if mayHavePersisted {
			persisted = append(persisted, p)
		}
	}
	return persisted, "", nil
}

// persistCopy creates the manifest copy at p and reports whether this run
// may have created it. A dry run reports whether it would. A copy already
// present, which a lagging listing omitted, is left as it is.
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
		return true, fmt.Errorf("failed to persist manifest %s: %w", p, err)
	}
	return true, nil
}

// keepUnlessExpired keeps the copies persisted for snapshot id unless the
// dataset was removed or a tombstone now records the snapshot. Both RemoveDataset
// and Expire record their marker before deleting manifests, so a copy persisted
// after their deletion pass is seen here and deleted. It reports whether copies
// may remain, including after a failed check or deletion.
func (r sweepRun) keepUnlessExpired(ctx context.Context, id SnapshotID, persisted []string) (bool, error) {
	if err := r.ds.history.CheckLive(ctx); err != nil {
		if errors.Is(err, ErrRemoved) {
			if delErr := deleteObjects(ctx, r.ds.store, persisted); delErr != nil {
				return true, errors.Join(err, delErr)
			}
			return false, err
		}
		return true, err
	}
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
func (d *dataset) readCanonical(ctx context.Context, id SnapshotID, p string) ([]byte, *Manifest, error) {
	rc, err := d.store.Get(ctx, p)
	if err != nil {
		return nil, nil, err
	}
	defer closer(rc)()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, nil, fmt.Errorf("lode: failed to read manifest %s: %w", p, err)
	}
	m, err := newDatasetKind(d.id, d.layout, d.store).Decode(id, bytes.NewReader(data))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load manifest %s: %w", p, err)
	}
	return data, m, nil
}

package lode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/pithecene/lode/internal/errs"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
)

// -----------------------------------------------------------------------------
// Listing (CONTRACT_READ_API.md)
// -----------------------------------------------------------------------------

// listDatasets returns the IDs of committed datasets under datasets/. When the
// store implements PrefixLister, discovery uses shallow prefix enumeration plus
// per-dataset pointer/manifest checks (O(D) instead of scanning every object).
// Uncommitted and removed datasets are invisible, and an empty store returns an
// empty slice.
func listDatasets(ctx context.Context, store ObjectStore, flat bool, opts DatasetListOptions) ([]DatasetID, error) {
	if flat {
		return nil, ErrDatasetsNotModeled
	}
	if lister, ok := store.(PrefixLister); ok {
		return listDatasetsByPrefix(ctx, store, lister, opts)
	}
	return listDatasetsByScan(ctx, store, opts)
}

func listDatasetsByPrefix(ctx context.Context, store ObjectStore, lister PrefixLister, opts DatasetListOptions) ([]DatasetID, error) {
	prefixes, err := lister.ListPrefixes(ctx, datasetsDir+"/")
	if err != nil {
		return nil, err
	}
	unpart := newPathGrammar(false, false)
	hive := newPathGrammar(false, true)
	datasets := make([]DatasetID, 0, len(prefixes))
	for _, p := range prefixes {
		rel, ok := strings.CutPrefix(p, datasetsDir+"/")
		if !ok {
			continue
		}
		seg, ok := strings.CutSuffix(rel, "/")
		if !ok || !storepath.Segment(seg) {
			continue
		}
		id := DatasetID(seg)
		committed, err := datasetHasCommittedManifest(ctx, store, id, unpart, hive)
		if err != nil {
			return nil, err
		}
		if committed {
			datasets = append(datasets, id)
		}
	}
	sort.Slice(datasets, func(i, j int) bool { return datasets[i] < datasets[j] })
	return cursorWindow(datasets, func(id DatasetID) bool { return id > opts.StartAfter }, opts.Limit), nil
}

func datasetHasCommittedManifest(ctx context.Context, store ObjectStore, id DatasetID, unpart, hive pathGrammar) (bool, error) {
	pointer := join(datasetsDir, string(id), latestFile)
	rc, err := store.Get(ctx, pointer)
	if err == nil {
		data, readErr := io.ReadAll(rc)
		errs.DiscardClose(rc)
		if readErr != nil {
			return false, fmt.Errorf("lode: failed to read latest pointer: %w", readErr)
		}
		target := strings.TrimSpace(string(data))
		if target == history.RemovedPointer {
			return false, nil
		}
		snapID := SnapshotID(target)
		if history.IsGeneratedID(snapID) {
			for _, g := range []pathGrammar{unpart, hive} {
				ok, exErr := store.Exists(ctx, g.objectPath(id, "", snapID, "", manifestFile))
				if exErr != nil {
					return false, exErr
				}
				if ok {
					return true, nil
				}
			}
		}
	} else if !errors.Is(err, ErrNotFound) {
		return false, err
	}

	// Fallback for pointerless pre-v0.10 datasets or datasets with a pending
	// commit on top of an already-committed history.
	keys, err := store.List(ctx, datasetsDir+"/"+string(id)+"/")
	if err != nil {
		return false, err
	}
	for _, k := range keys {
		for _, g := range []pathGrammar{unpart, hive} {
			if sp, ok := g.parse(k); ok && sp.kind == manifestLeaf && sp.dataset == id {
				return true, nil
			}
		}
	}
	return false, nil
}

func listDatasetsByScan(ctx context.Context, store ObjectStore, opts DatasetListOptions) ([]DatasetID, error) {
	paths, err := store.List(ctx, datasetsDir+"/")
	if err != nil {
		return nil, err
	}
	grammars := []pathGrammar{newPathGrammar(false, false), newPathGrammar(false, true)}
	seen := make(map[DatasetID]struct{})
	datasets := []DatasetID{}
	for _, p := range paths {
		for _, g := range grammars {
			sp, ok := g.parse(p)
			if !ok || sp.kind != manifestLeaf {
				continue
			}
			if _, dup := seen[sp.dataset]; dup {
				continue
			}
			removed, err := isPointerRemoved(ctx, store, join(datasetsDir, string(sp.dataset), latestFile))
			if err != nil {
				return nil, err
			}
			seen[sp.dataset] = struct{}{}
			if !removed {
				datasets = append(datasets, sp.dataset)
			}
		}
	}
	sort.Slice(datasets, func(i, j int) bool { return datasets[i] < datasets[j] })
	return cursorWindow(datasets, func(id DatasetID) bool { return id > opts.StartAfter }, opts.Limit), nil
}

func isPointerRemoved(ctx context.Context, store ObjectStore, pointer string) (bool, error) {
	rc, err := store.Get(ctx, pointer)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer closer(rc)()
	data, err := io.ReadAll(rc)
	if err != nil {
		return false, fmt.Errorf("lode: failed to read latest pointer: %w", err)
	}
	return strings.TrimSpace(string(data)) == history.RemovedPointer, nil
}

// ListPartitions returns the canonical partition paths present in the dataset's
// committed snapshots, sorted lexicographically. On Hive layouts, partition
// paths are derived directly from listed partition-manifest keys (0 manifest Gets when
// all snapshots are indexed); canonical-only snapshots whose partition index
// was not yet persisted are loaded individually.
func (d *dataset) ListPartitions(ctx context.Context, opts PartitionListOptions) ([]PartitionPath, error) {
	if !validPartitionPrefix(opts.Prefix) {
		return nil, ErrInvalidPath
	}
	if opts.Prefix != "" && !d.layout.supportsPartitions() {
		return nil, ErrNotPartitioned
	}

	if err := d.history.CheckLive(ctx); err != nil {
		return nil, err
	}
	prefix := d.layout.snapshotsPrefixForPartition(d.id, "")
	if opts.Prefix != "" {
		prefix = d.layout.partitionsPrefix(d.id, opts.Prefix)
	}
	paths, err := d.store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	expFilter, err := d.newExpiryFilter(ctx, paths, prefix)
	if err != nil {
		return nil, err
	}

	anyManifest := false
	seenPartitions := make(map[string]struct{})
	indexedSnapshots := make(map[SnapshotID]struct{})
	type snapPart struct {
		id   SnapshotID
		part string
	}
	indexedParts := make(map[SnapshotID]map[string]struct{})
	var dataPairs []snapPart
	var canonical []listedManifest
	partitions := []PartitionPath{}

	for _, p := range paths {
		sp, ok := d.layout.parse(p)
		if !ok {
			continue
		}
		if sp.kind == dataLeaf {
			if sp.snapshot != "" && sp.partition != "" {
				dataPairs = append(dataPairs, snapPart{id: sp.snapshot, part: sp.partition})
			}
			continue
		}
		if sp.kind != manifestLeaf {
			continue
		}
		skip, err := expFilter.skipSnapshot(ctx, sp.snapshot)
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		anyManifest = true
		if sp.snapshot == "" || !d.layout.supportsPartitions() {
			continue
		}
		if sp.partition == "" {
			canonical = append(canonical, listedManifest{id: sp.snapshot, path: p})
			continue
		}
		indexedSnapshots[sp.snapshot] = struct{}{}
		addIndexedPartition(indexedParts, sp.snapshot, sp.partition)
		if _, dup := seenPartitions[sp.partition]; !dup && strings.HasPrefix(sp.partition, opts.Prefix) {
			partPath, err := listedPartitionPath(sp.partition, p)
			if err != nil {
				return nil, err
			}
			seenPartitions[sp.partition] = struct{}{}
			partitions = append(partitions, partPath)
		}
	}

	if !anyManifest {
		if opts.Prefix == "" {
			return nil, ErrNotFound
		}
		return partitions, nil
	}
	if !d.layout.supportsPartitions() {
		return partitions, nil
	}

	partialSnapshots := make(map[SnapshotID]struct{})
	for _, dp := range dataPairs {
		if _, indexed := indexedParts[dp.id][dp.part]; !indexed {
			partialSnapshots[dp.id] = struct{}{}
		}
	}

	partitions, err = d.collectUnindexedPartitions(ctx, canonical, indexedSnapshots, partialSnapshots, seenPartitions, opts.Prefix, partitions)
	if err != nil {
		return nil, err
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].path < partitions[j].path })
	return cursorWindow(partitions, func(p PartitionPath) bool { return p.path > opts.StartAfter.path }, opts.Limit), nil
}

func addIndexedPartition(indexedParts map[SnapshotID]map[string]struct{}, id SnapshotID, part string) {
	if indexedParts[id] == nil {
		indexedParts[id] = make(map[string]struct{})
	}
	indexedParts[id][part] = struct{}{}
}

// collectUnindexedPartitions loads only canonical manifests whose partition
// index is absent or incomplete in the store listing.
func (d *dataset) collectUnindexedPartitions(
	ctx context.Context,
	canonical []listedManifest,
	indexed, partial map[SnapshotID]struct{},
	seen map[string]struct{},
	prefix string,
	partitions []PartitionPath,
) ([]PartitionPath, error) {
	for _, m := range canonical {
		_, isIndexed := indexed[m.id]
		_, isPartial := partial[m.id]
		if isIndexed && !isPartial {
			continue
		}
		manifest, err := d.loadListed(ctx, m)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, obj := range manifest.Objects {
			part := d.layout.extractPartitionPath(obj.Path)
			if part == "" || !strings.HasPrefix(part, prefix) {
				continue
			}
			if _, dup := seen[part]; dup {
				continue
			}
			partPath, err := listedPartitionPath(part, m.path)
			if err != nil {
				return nil, err
			}
			seen[part] = struct{}{}
			partitions = append(partitions, partPath)
		}
	}
	return partitions, nil
}

// Manifests yields the dataset's committed manifests, within partition when
// non-zero, in (Partition, ID) order after opts.StartAfter and up to
// opts.Limit. Each manifest in the window is loaded and validated once on
// demand. Yields ErrNotFound if the dataset holds no manifests.
func (d *dataset) Manifests(ctx context.Context, partition PartitionPath, opts ManifestListOptions) iter.Seq2[LoadedManifest, error] {
	return func(yield func(LoadedManifest, error) bool) {
		if err := d.history.CheckLive(ctx); err != nil {
			yield(LoadedManifest{}, err)
			return
		}
		listing, err := d.listManifests(ctx, d.layout.snapshotsPrefixForPartition(d.id, partition.path))
		if err != nil {
			yield(LoadedManifest{}, err)
			return
		}
		if !listing.any {
			yield(LoadedManifest{}, ErrNotFound)
			return
		}
		if partition.path != "" && !d.layout.supportsPartitions() {
			return
		}
		d.yieldLoaded(ctx, inRefOrder(listing.manifests), listing.tombstoned, opts, yield)
	}
}

func (d *dataset) yieldLoaded(ctx context.Context, listed []listedManifest, tombstoned map[SnapshotID]struct{}, opts ManifestListOptions, yield func(LoadedManifest, error) bool) {
	// listed is in ref order, so the manifests after the cursor are a suffix.
	window := listed[sort.Search(len(listed), func(i int) bool {
		return manifestRefLess(opts.StartAfter, ManifestRef{ID: listed[i].id, Partition: PartitionPath{path: listed[i].partition}})
	}):]
	if opts.Prefetch > 0 {
		d.yieldPrefetched(ctx, window, tombstoned, opts, yield)
		return
	}
	yielded := 0
	for _, m := range window {
		if opts.Limit > 0 && yielded == opts.Limit {
			return
		}
		r := d.loadWindowed(ctx, m, tombstoned)
		if r.skip {
			continue
		}
		if !yield(r.loaded, r.err) || r.err != nil {
			return
		}
		yielded++
	}
}

// windowLoad is the outcome of loading one listed manifest: a manifest, an
// error that stops the listing, or a skip for a snapshot that went away.
type windowLoad struct {
	loaded LoadedManifest
	err    error
	skip   bool
}

// loadWindowed loads one listed manifest, skipping a snapshot that an expiry
// or a removal took after the listing.
func (d *dataset) loadWindowed(ctx context.Context, m listedManifest, tombstoned map[SnapshotID]struct{}) windowLoad {
	if _, isTombstoned := tombstoned[m.id]; m.partition != "" && isTombstoned {
		live, err := newDatasetKind(d.id, d.layout, d.store).NotExpired(ctx, m.id)
		if err != nil {
			return windowLoad{err: err}
		}
		if !live {
			return windowLoad{skip: true}
		}
	}
	loaded, err := d.loadRef(ctx, m)
	if errors.Is(err, ErrNotFound) {
		return windowLoad{skip: true}
	}
	return windowLoad{loaded: loaded, err: err}
}

// yieldPrefetched yields the window in order while up to opts.Prefetch loads
// run ahead of the caller. A load starts only while the manifests yielded and
// in flight stay within opts.Limit, so no manifest beyond the limit is read.
// A caller that cancels its context receives the context's error at its next
// advance, in place of a manifest already loaded. Every load has finished when
// it returns.
func (d *dataset) yieldPrefetched(ctx context.Context, window []listedManifest, tombstoned map[SnapshotID]struct{}, opts ManifestListOptions, yield func(LoadedManifest, error) bool) {
	caller := ctx
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	var pending []chan windowLoad
	next, yielded := 0, 0
	for {
		for next < len(window) && len(pending) < opts.Prefetch && (opts.Limit <= 0 || yielded+len(pending) < opts.Limit) {
			done := make(chan windowLoad, 1)
			m := window[next]
			wg.Go(func() { done <- d.loadWindowed(ctx, m, tombstoned) })
			pending = append(pending, done)
			next++
		}
		if len(pending) == 0 {
			return
		}
		r := <-pending[0]
		pending = pending[1:]
		if err := caller.Err(); err != nil {
			yield(LoadedManifest{}, err)
			return
		}
		if r.skip {
			continue
		}
		if !yield(r.loaded, r.err) || r.err != nil {
			return
		}
		yielded++
		// The caller has advanced: a cancellation it made since the last
		// manifest ends the listing here, also after the window's last one.
		if err := caller.Err(); err != nil {
			yield(LoadedManifest{}, err)
			return
		}
	}
}

func (d *dataset) loadRef(ctx context.Context, m listedManifest) (LoadedManifest, error) {
	manifest, err := d.loadListed(ctx, m)
	if err != nil {
		return LoadedManifest{}, err
	}
	path, err := listedPartitionPath(m.partition, m.path)
	if err != nil {
		return LoadedManifest{}, err
	}
	return LoadedManifest{Ref: ManifestRef{ID: m.id, Partition: path}, Manifest: manifest}, nil
}

func inRefOrder(listed []listedManifest) []listedManifest {
	ordered := slices.Clone(listed)
	sort.SliceStable(ordered, func(i, j int) bool {
		return manifestRefLess(
			ManifestRef{ID: ordered[i].id, Partition: PartitionPath{path: ordered[i].partition}},
			ManifestRef{ID: ordered[j].id, Partition: PartitionPath{path: ordered[j].partition}},
		)
	})
	return ordered
}

type listedManifest struct {
	id        SnapshotID
	partition string
	path      string
}

type manifestListing struct {
	manifests  []listedManifest
	tombstoned map[SnapshotID]struct{}
	any        bool
}

func (d *dataset) listManifests(ctx context.Context, prefix string) (manifestListing, error) {
	paths, err := d.store.List(ctx, prefix)
	if err != nil {
		return manifestListing{}, err
	}
	expFilter, err := d.newExpiryFilter(ctx, paths, prefix)
	if err != nil {
		return manifestListing{}, err
	}

	listing := manifestListing{tombstoned: expFilter.tombstoned}
	var indexed, canonical []listedManifest
	for _, p := range paths {
		sp, ok := d.layout.manifest(p)
		if !ok {
			continue
		}
		skip, err := expFilter.skipSnapshot(ctx, sp.snapshot)
		if err != nil {
			return manifestListing{}, err
		}
		if skip {
			continue
		}
		listing.any = true
		if sp.snapshot == "" {
			continue
		}
		m := listedManifest{id: sp.snapshot, partition: sp.partition, path: p}
		if d.layout.supportsPartitions() && sp.partition == "" {
			canonical = append(canonical, m)
			continue
		}
		indexed = append(indexed, m)
	}
	sortListed(indexed)

	seen := make(map[SnapshotID]struct{}, len(indexed)+len(canonical))
	for _, m := range slices.Concat(indexed, canonical) {
		if _, dup := seen[m.id]; !dup {
			seen[m.id] = struct{}{}
			listing.manifests = append(listing.manifests, m)
		}
	}
	sortListed(listing.manifests)
	return listing, nil
}

type expiryFilter struct {
	ds         *dataset
	tombstoned map[SnapshotID]struct{}
	cache      map[SnapshotID]*bool
}

func (d *dataset) newExpiryFilter(ctx context.Context, listedPaths []string, prefix string) (expiryFilter, error) {
	f := expiryFilter{ds: d}
	if !d.layout.supportsPartitions() {
		return f, nil
	}
	tombPrefix := d.layout.tombstonesPrefix(d.id)
	var keys []string
	if prefix == d.layout.datasetPrefix(d.id) {
		for _, p := range listedPaths {
			if isTombstoneName(p, tombPrefix) {
				keys = append(keys, p)
			}
		}
	} else {
		var err error
		keys, err = d.tombstones().list(ctx)
		if err != nil {
			return f, err
		}
	}
	if len(keys) == 0 {
		return f, nil
	}
	f.tombstoned = make(map[SnapshotID]struct{}, len(keys))
	for _, k := range keys {
		f.tombstoned[SnapshotID(strings.TrimPrefix(k, tombPrefix))] = struct{}{}
	}
	f.cache = make(map[SnapshotID]*bool, len(keys))
	return f, nil
}

func (f expiryFilter) skipSnapshot(ctx context.Context, id SnapshotID) (bool, error) {
	if id == "" {
		return false, nil
	}
	if _, ok := f.tombstoned[id]; !ok {
		return false, nil
	}
	if cached, ok := f.cache[id]; ok {
		return *cached, nil
	}
	live, err := newDatasetKind(f.ds.id, f.ds.layout, f.ds.store).NotExpired(ctx, id)
	if err != nil {
		return false, err
	}
	expired := !live
	f.cache[id] = &expired
	return expired, nil
}

func sortListed(ms []listedManifest) {
	sort.Slice(ms, func(i, j int) bool {
		if ms[i].partition != ms[j].partition {
			return ms[i].partition < ms[j].partition
		}
		return ms[i].path < ms[j].path
	})
}

func validPartitionPrefix(prefix string) bool {
	return !strings.HasPrefix(prefix, "/") && validPartitionPath(strings.TrimSuffix(prefix, "/"))
}

// ParsePartitionPath parses a canonical relative partition path. The empty
// string parses to the zero value.
func ParsePartitionPath(s string) (PartitionPath, error) {
	if !validPartitionPath(s) {
		return PartitionPath{}, fmt.Errorf("lode: partition path %q: %w", s, ErrInvalidPath)
	}
	return PartitionPath{path: s}, nil
}

// String returns the path as it appears in storage keys, or "" for the zero
// value.
func (p PartitionPath) String() string { return p.path }

// MarshalText encodes the path as its string.
func (p PartitionPath) MarshalText() ([]byte, error) { return []byte(p.path), nil }

// UnmarshalText parses text as ParsePartitionPath does.
func (p *PartitionPath) UnmarshalText(text []byte) error {
	parsed, err := ParsePartitionPath(string(text))
	if err != nil {
		return err
	}
	*p = parsed
	return nil
}

func listedPartitionPath(p, source string) (PartitionPath, error) {
	if !validPartitionPath(p) {
		return PartitionPath{}, fmt.Errorf("lode: manifest %s names partition %q, which is not a canonical path: %w", source, p, ErrManifestInvalid)
	}
	return PartitionPath{path: p}, nil
}

func validPartitionPath(path string) bool {
	if path == "" {
		return true
	}
	if strings.ContainsRune(path, '\\') {
		return false
	}
	for seg := range strings.SplitSeq(path, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

func manifestRefLess(a, b ManifestRef) bool {
	if a.Partition != b.Partition {
		return a.Partition.path < b.Partition.path
	}
	return a.ID < b.ID
}

func cursorWindow[T any](items []T, after func(T) bool, limit int) []T {
	start := 0
	for start < len(items) && !after(items[start]) {
		start++
	}
	items = items[start:]
	if limit > 0 && len(items) > limit {
		items = items[:limit]
	}
	return items
}

// OpenObject returns a reader for the raw stored bytes of a data object in this
// dataset without decompressing or verifying checksums. The caller closes the
// returned reader.
func (d *dataset) OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error) {
	if err := d.checkObject(obj); err != nil {
		return nil, err
	}
	return d.store.Get(ctx, obj.Path)
}

// OpenDecompressed returns a streaming reader for the decompressed payload of
// obj using m's recorded compressor and checksum settings. When m records a
// checksum algorithm, reading to io.EOF verifies the stored digest and returns
// ErrChecksumMismatch on corruption. The caller closes the returned reader.
func (d *dataset) OpenDecompressed(ctx context.Context, m *Manifest, obj ObjectRef) (io.ReadCloser, error) {
	if err := validateManifest(m); err != nil {
		return nil, err
	}
	if m.DatasetID != d.id {
		return nil, &validate.ManifestError{
			Field:   "dataset_id",
			Message: fmt.Sprintf("got %q, want %q", m.DatasetID, d.id),
		}
	}
	if err := d.checkObject(obj); err != nil {
		return nil, err
	}
	if !d.layout.isObjectOf(d.id, m.SnapshotID, dataLeaf, obj.Path) {
		return nil, validate.Argument("object %q does not belong to snapshot %s of dataset %s", obj.Path, m.SnapshotID, d.id)
	}
	for _, recorded := range m.Objects {
		if recorded.Path == obj.Path {
			return d.openObjectStream(ctx, m, recorded)
		}
	}
	return nil, validate.Argument("object %q is absent from the inventory of snapshot %s", obj.Path, m.SnapshotID)
}

// ReaderAt returns an io.ReaderAt for random access to the raw stored bytes of
// a data object in this dataset. Returns ErrRangeReadNotSupported if the store
// does not implement RangeReader.
func (d *dataset) ReaderAt(ctx context.Context, obj ObjectRef) (io.ReaderAt, error) {
	if err := d.checkObject(obj); err != nil {
		return nil, err
	}
	ranges, ok := d.store.(RangeReader)
	if !ok {
		return nil, ErrRangeReadNotSupported
	}
	return ranges.ReaderAt(ctx, obj.Path)
}

func (d *dataset) checkObject(obj ObjectRef) error {
	if key, ok := storepath.Key(obj.Path); !ok || key != obj.Path {
		return validate.Argument("object %q is not a canonical data object key of dataset %s", obj.Path, d.id)
	}
	sp, ok := d.layout.parse(obj.Path)
	if !ok || sp.dataset != d.id || sp.kind != dataLeaf || sp.snapshot == "" || !validPartitionPath(sp.partition) {
		return validate.Argument("object %q is not a data object of dataset %s", obj.Path, d.id)
	}
	return nil
}

func (d *dataset) loadListed(ctx context.Context, m listedManifest) (*Manifest, error) {
	s, err := d.history.LoadAt(ctx, m.id, m.path)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest %s: %w", m.path, err)
	}
	return s.Manifest, nil
}

// -----------------------------------------------------------------------------
// Manifest Validation
// -----------------------------------------------------------------------------

func validateManifest(m *Manifest) error {
	if m == nil {
		return &validate.ManifestError{Field: "manifest", Message: "is nil"}
	}
	if m.SchemaName == "" {
		return &validate.ManifestError{Field: "schema_name", Message: "is required"}
	}
	if m.FormatVersion == "" {
		return &validate.ManifestError{Field: "format_version", Message: "is required"}
	}
	if m.DatasetID == "" {
		return &validate.ManifestError{Field: "dataset_id", Message: "is required"}
	}
	if m.SnapshotID == "" {
		return &validate.ManifestError{Field: "snapshot_id", Message: "is required"}
	}
	if m.ParentSnapshotID == m.SnapshotID {
		return &validate.ManifestError{Field: "parent_snapshot_id", Message: "must differ from snapshot_id"}
	}
	if m.CreatedAt.IsZero() {
		return &validate.ManifestError{Field: "created_at", Message: "is required"}
	}
	if !m.Metadata.Valid() {
		return &validate.ManifestError{Field: "metadata", Message: "must be a JSON object (use {} for no metadata)"}
	}
	if m.Objects == nil {
		return &validate.ManifestError{Field: "objects", Message: "must not be nil (use empty slice for no objects)"}
	}
	if m.RowCount < 0 {
		return &validate.ManifestError{Field: "row_count", Message: "must be non-negative"}
	}
	if m.Compressor == "" {
		return &validate.ManifestError{Field: "compressor", Message: "is required"}
	}
	if m.Partitioner == "" {
		return &validate.ManifestError{Field: "partitioner", Message: "is required"}
	}
	if err := checkLegacyPaths(m); err != nil {
		return err
	}
	return validateObjects(m.Objects)
}

func validateObjects(objects []ObjectRef) error {
	for i, obj := range objects {
		if obj.Path == "" {
			return &validate.ManifestError{Field: fmt.Sprintf("objects[%d].path", i), Message: "is required"}
		}
		if obj.SizeBytes < 0 {
			return &validate.ManifestError{Field: fmt.Sprintf("objects[%d].size_bytes", i), Message: "must be non-negative"}
		}
	}
	return nil
}

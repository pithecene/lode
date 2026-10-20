package lode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

// -----------------------------------------------------------------------------
// Reader Configuration
// -----------------------------------------------------------------------------

// readerConfig holds the resolved configuration for a reader.
type readerConfig struct {
	layout layout
}

// -----------------------------------------------------------------------------
// Reader Implementation
// -----------------------------------------------------------------------------

// reader implements the DatasetReader interface.
type reader struct {
	store  Store
	layout layout
}

// NewDatasetReader creates a DatasetReader with documented defaults.
//
// Default behavior:
//   - Layout: NewDefaultLayout()
//
// Use option functions to override defaults:
//   - WithLayout(l) to use a different layout
func NewDatasetReader(factory StoreFactory, opts ...Option) (DatasetReader, error) {
	if factory == nil {
		return nil, invalidArgument("store factory is required")
	}

	cfg := &readerConfig{
		layout: NewDefaultLayout(),
	}

	for _, opt := range opts {
		if err := opt.applyReader(cfg); err != nil {
			return nil, err // option errors carry the lode: prefix
		}
	}

	if cfg.layout == nil {
		return nil, invalidArgument("layout must not be nil")
	}

	// Arguments are checked before any store work (CONTRACT_ERRORS §5).
	store, err := factory()
	if err != nil {
		return nil, fmt.Errorf("lode: store factory failed: %w", err)
	}
	if store == nil {
		return nil, invalidArgument("store factory returned nil store")
	}

	return &reader{
		store:  store,
		layout: cfg.layout,
	}, nil
}

func (r *reader) ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error) {
	if !r.layout.supportsDatasetEnumeration() {
		return nil, ErrDatasetsNotModeled
	}

	paths, err := r.store.List(ctx, r.layout.datasetsPrefix())
	if err != nil {
		return nil, err
	}

	seen := make(map[DatasetID]bool)
	var datasets []DatasetID
	for _, p := range paths {
		if id := r.layout.parseDatasetID(p); id != "" && !seen[id] {
			seen[id] = true
			datasets = append(datasets, id)
		}
	}

	// Contract: empty list means storage truly empty, not "no manifests"
	if len(datasets) == 0 && len(paths) > 0 {
		return nil, ErrNoManifests
	}

	sort.Slice(datasets, func(i, j int) bool { return datasets[i] < datasets[j] })
	return cursorWindow(datasets, func(id DatasetID) bool { return id > opts.StartAfter }, opts.Limit), nil
}

func (r *reader) ListPartitions(ctx context.Context, dataset DatasetID, opts PartitionListOptions) ([]PartitionRef, error) {
	// Malformed before unsupported: a prefix that could escape the dataset is
	// rejected as such regardless of layout (CONTRACT_READ_API §Partition Prefix).
	if !validPartitionPrefix(opts.Prefix) {
		return nil, ErrInvalidPath
	}
	if opts.Prefix != "" && !r.layout.supportsPartitions() {
		return nil, ErrPartitionPrefixNotSupported
	}

	// Partition-first layouts put every segment under its partition, so a
	// partition prefix is a storage prefix (CONTRACT_LAYOUT §Hive-style).
	prefix := r.layout.snapshotsPrefixForPartition(dataset, "")
	if opts.Prefix != "" {
		prefix = r.layout.partitionsPrefix(dataset, opts.Prefix)
	}
	listing, err := r.listManifests(ctx, prefix)
	if err != nil {
		return nil, err
	}
	// With a prefix, an empty subtree is a valid answer, not a missing dataset.
	if !listing.any && opts.Prefix == "" {
		return nil, ErrNotFound
	}

	// Each snapshot's manifest is loaded once; its files name the partitions.
	// A snapshot found under the prefix may also carry files for partitions
	// outside it; those are not part of this listing.
	seen := make(map[string]bool)
	var partitions []PartitionRef
	for _, m := range listing.manifests {
		manifest, ok, err := r.loadListed(ctx, m)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		for _, f := range manifest.Files {
			p := r.layout.extractPartitionPath(f.Path)
			if p == "" || seen[p] || !strings.HasPrefix(p, opts.Prefix) {
				continue
			}
			seen[p] = true
			partitions = append(partitions, PartitionRef{Path: p})
		}
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].Path < partitions[j].Path })
	return cursorWindow(partitions, func(p PartitionRef) bool { return p.Path > opts.StartAfter }, opts.Limit), nil
}

func (r *reader) ListManifests(ctx context.Context, dataset DatasetID, partition string, opts ManifestListOptions) ([]ManifestRef, error) {
	listing, err := r.listManifests(ctx, r.layout.snapshotsPrefixForPartition(dataset, partition))
	if err != nil {
		return nil, err
	}
	if !listing.any {
		return nil, ErrNotFound
	}

	// Validate each snapshot's manifest once (CONTRACT_READ_API) — the
	// smallest-partition one, which the listing order guarantees comes first.
	refs := make([]ManifestRef, 0, len(listing.manifests))
	for _, m := range listing.manifests {
		_, ok, err := r.loadListed(ctx, m)
		if err != nil {
			return nil, err
		}
		if ok {
			refs = append(refs, ManifestRef{ID: m.id, Partition: m.partition})
		}
	}
	// A layout without partitions has no snapshot in any named partition: the
	// selector matches nothing (the dataset itself was found and validated).
	if partition != "" && !r.layout.supportsPartitions() {
		refs = refs[:0]
	}

	sort.Slice(refs, func(i, j int) bool { return manifestRefLess(refs[i], refs[j]) })
	return cursorWindow(refs, func(ref ManifestRef) bool { return manifestRefLess(opts.StartAfter, ref) }, opts.Limit), nil
}

// listedManifest is one snapshot's manifest as found by listing.
type listedManifest struct {
	id        DatasetSnapshotID
	partition string
	path      string
}

// manifestListing is the result of enumerating manifests under a prefix.
type manifestListing struct {
	// manifests holds one entry per snapshot, in (partition, path) order; the
	// entry is the snapshot's lexicographically smallest partition manifest.
	manifests []listedManifest
	// any reports whether the prefix held any manifest at all, including the
	// canonical ones excluded from manifests; it distinguishes an empty
	// dataset from a missing one.
	any bool
}

// listManifests enumerates manifests under prefix. Store.List order is
// unspecified (CONTRACT_STORAGE §List), so entries are sorted by (partition,
// path): path order alone is not partition order when one partition name
// prefixes another ("day=a-b/…" sorts before "day=a/…"). Canonical manifests
// on partition-aware layouts exist only for O(1) lookup by ID and are not
// listed.
func (r *reader) listManifests(ctx context.Context, prefix string) (manifestListing, error) {
	paths, err := r.store.List(ctx, prefix)
	if err != nil {
		return manifestListing{}, err
	}

	var listing manifestListing
	var all []listedManifest
	for _, p := range paths {
		if !r.layout.isManifest(p) {
			continue
		}
		listing.any = true
		id := r.layout.parseSnapshotID(p)
		partition := r.layout.parsePartitionFromManifest(p)
		if id == "" || (r.layout.supportsPartitions() && partition == "") {
			continue
		}
		all = append(all, listedManifest{id: id, partition: partition, path: p})
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].partition != all[j].partition {
			return all[i].partition < all[j].partition
		}
		return all[i].path < all[j].path
	})

	seen := make(map[DatasetSnapshotID]bool, len(all))
	for _, m := range all {
		if !seen[m.id] {
			seen[m.id] = true
			listing.manifests = append(listing.manifests, m)
		}
	}
	return listing, nil
}

// validPartitionPrefix reports whether a PartitionListOptions.Prefix is a
// canonical relative partition path fragment: no leading "/", no empty, "."
// or ".." segments. The prefix is concatenated under the dataset's partitions
// directory, so anything else could escape to another dataset's subtree.
// Empty is valid (no filter); a trailing "/" is valid (names a partition
// boundary).
func validPartitionPrefix(prefix string) bool {
	if prefix == "" {
		return true
	}
	// Backslash is a separator for the filesystem store on Windows, so it
	// could smuggle ".." past a "/"-only split; partition paths never contain it.
	if strings.HasPrefix(prefix, "/") || strings.ContainsRune(prefix, '\\') {
		return false
	}
	for seg := range strings.SplitSeq(strings.TrimSuffix(prefix, "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			return false
		}
	}
	return true
}

// manifestRefLess orders ManifestRefs by (Partition, ID), the order
// ManifestListOptions promises and ManifestListOptions.StartAfter resumes in.
func manifestRefLess(a, b ManifestRef) bool {
	if a.Partition != b.Partition {
		return a.Partition < b.Partition
	}
	return a.ID < b.ID
}

// cursorWindow returns the items strictly after a cursor, capped at limit
// (zero means no cap). items must already be sorted in the order that
// after() tests against; the cursor position is the first item for which
// after() holds.
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

func (r *reader) GetManifest(ctx context.Context, dataset DatasetID, ref ManifestRef) (*Manifest, error) {
	manifestPath := r.layout.manifestPathInPartition(dataset, ref.ID, ref.Partition)
	return r.loadManifest(ctx, manifestPath)
}

func (r *reader) OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error) {
	return r.store.Get(ctx, obj.Path)
}

func (r *reader) ReaderAt(ctx context.Context, obj ObjectRef) (io.ReaderAt, error) {
	return r.store.ReaderAt(ctx, obj.Path)
}

// loadListed loads and validates a manifest a listing returned. ok is false
// when it was deleted after the listing: an expiry removed it, which
// enumeration skips rather than reports (CONTRACT_RETENTION §Lineage).
func (r *reader) loadListed(ctx context.Context, m listedManifest) (manifest *Manifest, ok bool, err error) {
	manifest, err = r.loadManifest(ctx, m.path)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to load manifest %s: %w", m.path, err)
	}
	return manifest, true, nil
}

func (r *reader) loadManifest(ctx context.Context, manifestPath string) (*Manifest, error) {
	rc, err := r.store.Get(ctx, manifestPath)
	if err != nil {
		return nil, err
	}
	defer closer(rc)()

	var manifest Manifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("failed to decode manifest: %w", err)
	}

	if err := validateManifest(&manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

// -----------------------------------------------------------------------------
// Manifest Validation
// -----------------------------------------------------------------------------

// manifestValidationError provides details about manifest validation failures.
type manifestValidationError struct {
	Field   string
	Message string
}

func (e *manifestValidationError) Error() string {
	return fmt.Sprintf("invalid manifest: %s: %s", e.Field, e.Message)
}

func (e *manifestValidationError) Unwrap() error {
	return ErrManifestInvalid
}

// validateManifest checks that a manifest contains all required fields
// per CONTRACT_CORE.md and CONTRACT_READ_API.md.
func validateManifest(m *Manifest) error {
	if m == nil {
		return &manifestValidationError{Field: "manifest", Message: "is nil"}
	}

	// Codec is optional: empty is valid for raw blob storage.
	if err := firstInvalid([]fieldCheck{
		{m.SchemaName == "", "schema_name", "is required"},
		{m.FormatVersion == "", "format_version", "is required"},
		{m.DatasetID == "", "dataset_id", "is required"},
		{m.SnapshotID == "", "snapshot_id", "is required"},
		{m.CreatedAt.IsZero(), "created_at", "is required"},
		{m.Metadata == nil, "metadata", "must not be nil (use empty map for no metadata)"},
		{m.Files == nil, "files", "must not be nil (use empty slice for no files)"},
		{m.RowCount < 0, "row_count", "must be non-negative"},
		{m.Compressor == "", "compressor", "is required"},
		{m.Partitioner == "", "partitioner", "is required"},
	}); err != nil {
		return err
	}
	return validateFiles(m.Files)
}

// validateFiles checks every FileRef of an inventory: a manifest's, or the
// copy a retention tombstone records (CONTRACT_ERRORS §File Validation).
func validateFiles(files []FileRef) error {
	for i, f := range files {
		if err := firstInvalid([]fieldCheck{
			{f.Path == "", fmt.Sprintf("files[%d].path", i), "is required"},
			{f.SizeBytes < 0, fmt.Sprintf("files[%d].size_bytes", i), "must be non-negative"},
		}); err != nil {
			return err
		}
	}
	return nil
}

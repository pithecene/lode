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
		return nil, errors.New("lode: store factory is required")
	}

	store, err := factory()
	if err != nil {
		return nil, fmt.Errorf("lode: store factory failed: %w", err)
	}
	if store == nil {
		return nil, errors.New("lode: store factory returned nil store")
	}

	cfg := &readerConfig{
		layout: NewDefaultLayout(),
	}

	for _, opt := range opts {
		if err := opt.applyReader(cfg); err != nil {
			return nil, fmt.Errorf("lode: %w", err)
		}
	}

	if cfg.layout == nil {
		return nil, errors.New("lode: layout must not be nil")
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
		if !r.layout.isManifest(p) {
			continue
		}

		datasetID := r.layout.parseDatasetID(p)
		if datasetID == "" || seen[datasetID] {
			continue
		}
		seen[datasetID] = true
		datasets = append(datasets, datasetID)

		if opts.Limit > 0 && len(datasets) >= opts.Limit {
			break
		}
	}

	// Contract: empty list means storage truly empty, not "no manifests"
	if len(datasets) == 0 && len(paths) > 0 {
		return nil, ErrNoManifests
	}

	return datasets, nil
}

func (r *reader) ListPartitions(ctx context.Context, dataset DatasetID, opts PartitionListOptions) ([]PartitionRef, error) {
	if opts.Prefix != "" && !r.layout.supportsPartitions() {
		return nil, ErrPartitionPrefixNotSupported
	}

	// Single-pass: list paths, load each manifest once, extract partitions.
	// Eliminates the double-deserialization of ListManifests + GetManifest (CX-3).
	prefix := r.layout.segmentsPrefixForPartition(dataset, "")
	if opts.Prefix != "" {
		// Partition-first layouts put every segment under its partition, so a
		// partition prefix is a storage prefix (CONTRACT_LAYOUT §Hive-style).
		prefix = r.layout.partitionsPrefix(dataset, opts.Prefix)
	}
	paths, err := r.store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	// Store.List ordering is unspecified (CONTRACT_STORAGE §List); sort so
	// the result is deterministic regardless of backend.
	sort.Strings(paths)

	seenSnap := make(map[DatasetSnapshotID]bool)
	seenPart := make(map[string]bool)
	var partitions []PartitionRef
	hasAnyManifest := false

	for _, p := range paths {
		if !r.layout.isManifest(p) {
			continue
		}
		hasAnyManifest = true

		snapshotID := r.layout.parseSegmentID(p)
		if snapshotID == "" || seenSnap[snapshotID] {
			continue
		}

		manifestPartition := r.layout.parsePartitionFromManifest(p)

		// Skip canonical manifests in partition-aware layouts (same logic as ListManifests).
		if r.layout.supportsPartitions() && manifestPartition == "" {
			continue
		}

		seenSnap[snapshotID] = true

		// Load each manifest once — the only Get per manifest.
		manifest, err := r.loadManifest(ctx, p)
		if err != nil {
			return nil, fmt.Errorf("failed to load manifest %s: %w", p, err)
		}

		for _, f := range manifest.Files {
			partPath := r.layout.extractPartitionPath(f.Path)
			// A snapshot found under the prefix may also carry files for
			// partitions outside it; those are not part of this listing.
			if partPath == "" || seenPart[partPath] || !strings.HasPrefix(partPath, opts.Prefix) {
				continue
			}
			seenPart[partPath] = true
			partitions = append(partitions, PartitionRef{Path: partPath})
		}
	}

	// With a prefix, an empty subtree is a valid answer, not a missing dataset.
	if !hasAnyManifest && opts.Prefix == "" {
		return nil, ErrNotFound
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].Path < partitions[j].Path })
	return cursorWindow(partitions, func(p PartitionRef) bool { return p.Path > opts.StartAfter }, opts.Limit), nil
}

func (r *reader) ListManifests(ctx context.Context, dataset DatasetID, partition string, opts ManifestListOptions) ([]ManifestRef, error) {
	prefix := r.layout.segmentsPrefixForPartition(dataset, partition)
	paths, err := r.store.List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	// Store.List ordering is unspecified (CONTRACT_STORAGE §List); sort so
	// each snapshot resolves to its lexicographically smallest partition
	// manifest regardless of backend.
	sort.Strings(paths)

	var refs []ManifestRef
	seen := make(map[DatasetSnapshotID]bool)
	hasAnyManifest := false

	for _, p := range paths {
		if !r.layout.isManifest(p) {
			continue
		}
		hasAnyManifest = true

		snapshotID := r.layout.parseSegmentID(p)
		if snapshotID == "" || seen[snapshotID] {
			continue
		}

		manifestPartition := r.layout.parsePartitionFromManifest(p)

		// Skip canonical manifests in partition-aware layouts. These exist
		// only for O(1) Snapshot(ctx, id) lookups and must not appear in
		// listing results (their empty Partition field is nondeterministic
		// relative to partition-specific manifests in store listing order).
		if r.layout.supportsPartitions() && manifestPartition == "" {
			continue
		}

		// Always validate manifest per CONTRACT_READ_API.md
		manifest, err := r.loadManifest(ctx, p)
		if err != nil {
			return nil, fmt.Errorf("failed to load manifest %s: %w", p, err)
		}

		// Apply partition filter if specified
		if partition != "" && manifestPartition == "" {
			if !r.manifestContainsPartition(manifest, partition) {
				continue
			}
		}

		seen[snapshotID] = true
		refs = append(refs, ManifestRef{
			ID:        snapshotID,
			Partition: manifestPartition,
		})
	}

	if !hasAnyManifest {
		return nil, ErrNotFound
	}

	sort.Slice(refs, func(i, j int) bool { return manifestRefLess(refs[i], refs[j]) })
	return cursorWindow(refs, func(ref ManifestRef) bool { return manifestRefLess(opts.StartAfter, ref) }, opts.Limit), nil
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

func (r *reader) manifestContainsPartition(m *Manifest, partition string) bool {
	for _, f := range m.Files {
		partPath := r.layout.extractPartitionPath(f.Path)
		if partPath == partition || strings.HasPrefix(partPath, partition+"/") {
			return true
		}
	}
	return false
}

// -----------------------------------------------------------------------------
// Manifest Validation
// -----------------------------------------------------------------------------

// ErrManifestInvalid indicates a manifest failed validation.
var ErrManifestInvalid = errors.New("invalid manifest")

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

	if m.SchemaName == "" {
		return &manifestValidationError{Field: "schema_name", Message: "is required"}
	}
	if m.FormatVersion == "" {
		return &manifestValidationError{Field: "format_version", Message: "is required"}
	}
	if m.DatasetID == "" {
		return &manifestValidationError{Field: "dataset_id", Message: "is required"}
	}
	if m.SnapshotID == "" {
		return &manifestValidationError{Field: "snapshot_id", Message: "is required"}
	}
	if m.CreatedAt.IsZero() {
		return &manifestValidationError{Field: "created_at", Message: "is required"}
	}
	if m.Metadata == nil {
		return &manifestValidationError{Field: "metadata", Message: "must not be nil (use empty map for no metadata)"}
	}
	if m.Files == nil {
		return &manifestValidationError{Field: "files", Message: "must not be nil (use empty slice for no files)"}
	}
	if m.RowCount < 0 {
		return &manifestValidationError{Field: "row_count", Message: "must be non-negative"}
	}
	// Codec is optional (empty string is valid for raw blob storage)
	if m.Compressor == "" {
		return &manifestValidationError{Field: "compressor", Message: "is required"}
	}
	if m.Partitioner == "" {
		return &manifestValidationError{Field: "partitioner", Message: "is required"}
	}

	for i, f := range m.Files {
		if f.Path == "" {
			return &manifestValidationError{
				Field:   fmt.Sprintf("files[%d].path", i),
				Message: "is required",
			}
		}
		if f.SizeBytes < 0 {
			return &manifestValidationError{
				Field:   fmt.Sprintf("files[%d].size_bytes", i),
				Message: "must be non-negative",
			}
		}
	}

	return nil
}

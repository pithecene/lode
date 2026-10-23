package lode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/pithecene/lode/internal/validate"
)

// -----------------------------------------------------------------------------
// Listing (CONTRACT_READ_API.md)
// -----------------------------------------------------------------------------

// listDatasets returns the IDs of the datasets under a root's datasets
// directory: those with a manifest of either grammar, unpartitioned or
// partitioned, under the root's names.
func listDatasets(ctx context.Context, store ObjectStore, names ManifestPaths, opts DatasetListOptions) ([]DatasetID, error) {
	if names.Flat {
		return nil, ErrDatasetsNotModeled
	}
	paths, err := store.List(ctx, names.Datasets+"/")
	if err != nil {
		return nil, err
	}

	grammars := []pathGrammar{names.grammar(false), names.grammar(true)}
	seen := make(map[DatasetID]bool)
	var datasets []DatasetID
	for _, p := range paths {
		for _, g := range grammars {
			if sp, ok := g.parse(p); ok && sp.kind == manifestLeaf && !seen[sp.dataset] {
				seen[sp.dataset] = true
				datasets = append(datasets, sp.dataset)
			}
		}
	}

	// Contract: empty list means storage truly empty, not "no manifests"
	if len(datasets) == 0 && len(paths) > 0 {
		return nil, ErrNoManifests
	}

	sort.Slice(datasets, func(i, j int) bool { return datasets[i] < datasets[j] })
	return cursorWindow(datasets, func(id DatasetID) bool { return id > opts.StartAfter }, opts.Limit), nil
}

// ListPartitions returns the partition paths that the dataset's committed
// manifests name.
func (d *dataset) ListPartitions(ctx context.Context, opts PartitionListOptions) ([]PartitionRef, error) {
	// Malformed before unsupported: a prefix that could escape the dataset is
	// rejected as such regardless of layout (CONTRACT_READ_API §Partition Prefix).
	if !validPartitionPrefix(opts.Prefix) {
		return nil, ErrInvalidPath
	}
	if opts.Prefix != "" && !d.layout.supportsPartitions() {
		return nil, ErrPartitionPrefixNotSupported
	}

	// Partition-first layouts put every segment under its partition, so a
	// partition prefix is a storage prefix (CONTRACT_LAYOUT §Layout-Specific Requirements).
	prefix := d.layout.snapshotsPrefixForPartition(d.id, "")
	if opts.Prefix != "" {
		prefix = d.layout.partitionsPrefix(d.id, opts.Prefix)
	}
	listing, err := d.listManifests(ctx, prefix)
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
		manifest, err := d.loadListed(ctx, m)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, f := range manifest.Files {
			p := d.layout.extractPartitionPath(f.Path)
			if p == "" || seen[p] || !strings.HasPrefix(p, opts.Prefix) {
				continue
			}
			seen[p] = true
			path, err := listedPartitionPath(p, m.path)
			if err != nil {
				return nil, err
			}
			partitions = append(partitions, PartitionRef{Path: path})
		}
	}

	sort.Slice(partitions, func(i, j int) bool { return partitions[i].Path.path < partitions[j].Path.path })
	return cursorWindow(partitions, func(p PartitionRef) bool { return p.Path.path > opts.StartAfter.path }, opts.Limit), nil
}

// ListManifests returns the dataset's committed manifests, within a
// partition when one is given.
func (d *dataset) ListManifests(ctx context.Context, partition PartitionPath, opts ManifestListOptions) ([]ManifestRef, error) {
	listing, err := d.listManifests(ctx, d.layout.snapshotsPrefixForPartition(d.id, partition.path))
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
		_, err := d.loadListed(ctx, m)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		path, err := listedPartitionPath(m.partition, m.path)
		if err != nil {
			return nil, err
		}
		refs = append(refs, ManifestRef{ID: m.id, Partition: path})
	}
	// A layout without partitions has no snapshot in any named partition: the
	// selector matches nothing (the dataset itself was found and validated).
	if partition.path != "" && !d.layout.supportsPartitions() {
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
func (d *dataset) listManifests(ctx context.Context, prefix string) (manifestListing, error) {
	paths, err := d.store.List(ctx, prefix)
	if err != nil {
		return manifestListing{}, err
	}

	var listing manifestListing
	var all []listedManifest
	for _, p := range paths {
		sp, ok := d.layout.manifest(p)
		if !ok {
			continue
		}
		listing.any = true
		if sp.snapshot == "" || (d.layout.supportsPartitions() && sp.partition == "") {
			continue
		}
		all = append(all, listedManifest{id: sp.snapshot, partition: sp.partition, path: p})
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
	return !strings.HasPrefix(prefix, "/") && validPartitionPath(strings.TrimSuffix(prefix, "/"))
}

// ParsePartitionPath parses a canonical relative partition path. The empty
// string parses to the zero value. It returns ErrInvalidPath when the path
// begins with "/", contains a backslash, or has an empty, "." or ".."
// segment, which includes a trailing "/".
func ParsePartitionPath(s string) (PartitionPath, error) {
	if !validPartitionPath(s) {
		return PartitionPath{}, fmt.Errorf("lode: partition path %q: %w", s, ErrInvalidPath)
	}
	return PartitionPath{path: s}, nil
}

// String returns the path as it appears in storage keys, or "" for the zero
// value.
func (p PartitionPath) String() string { return p.path }

// MarshalText encodes the path as its string, so that listing results and
// cursors keep their partition when encoded, for example as JSON.
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

// listedPartitionPath returns a partition path that a listing read from
// storage, under the manifest at source. A listing returns only canonical
// paths, so a manifest that names another is reported as invalid rather
// than returned or omitted.
func listedPartitionPath(p, source string) (PartitionPath, error) {
	if !validPartitionPath(p) {
		return PartitionPath{}, fmt.Errorf("lode: manifest %s names partition %q, which is not a canonical path: %w", source, p, ErrManifestInvalid)
	}
	return PartitionPath{path: p}, nil
}

// validPartitionPath reports whether path is a canonical relative partition
// path (ParsePartitionPath); the empty path is the dataset root.
func validPartitionPath(path string) bool {
	if path == "" {
		return true
	}
	// Backslash is a separator for the filesystem store on Windows, so it
	// could smuggle ".." past a "/"-only split; partition paths never contain it.
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

// manifestRefLess orders ManifestRefs by (Partition, ID), the order
// ManifestListOptions promises and ManifestListOptions.StartAfter resumes in.
func manifestRefLess(a, b ManifestRef) bool {
	if a.Partition != b.Partition {
		return a.Partition.path < b.Partition.path
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

// GetManifest loads and validates the manifest a reference names.
func (d *dataset) GetManifest(ctx context.Context, ref ManifestRef) (*Manifest, error) {
	return d.loadManifest(ctx, d.layout.manifestPathInPartition(d.id, ref.ID, ref.Partition.path))
}

// OpenObject returns a reader for a data object of this dataset.
func (d *dataset) OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error) {
	if err := d.checkObject(obj); err != nil {
		return nil, err
	}
	return d.store.Get(ctx, obj.Path)
}

// ReaderAt returns an io.ReaderAt for random access to a data object of
// this dataset.
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

// checkObject requires obj to name a data object of this dataset and, when
// it names a snapshot, of that snapshot, so that a handle reads only within
// its own dataset: ObjectRef carries no dataset, and every dataset of a
// root shares one store.
func (d *dataset) checkObject(obj ObjectRef) error {
	sp, ok := d.layout.parse(obj.Path)
	if !ok || sp.dataset != d.id || sp.kind != dataLeaf {
		return validate.Argument("object %q is not a data object of dataset %s", obj.Path, d.id)
	}
	if obj.Manifest.ID != "" && sp.snapshot != obj.Manifest.ID {
		return validate.Argument("object %q is not a data object of snapshot %s", obj.Path, obj.Manifest.ID)
	}
	return nil
}

// loadListed loads and validates a manifest a listing returned. ErrNotFound
// means it was deleted after the listing: an expiry removed it, which
// enumeration skips rather than reports (CONTRACT_RETENTION §Lineage).
func (d *dataset) loadListed(ctx context.Context, m listedManifest) (*Manifest, error) {
	manifest, err := d.loadManifest(ctx, m.path)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest %s: %w", m.path, err)
	}
	return manifest, nil
}

func (d *dataset) loadManifest(ctx context.Context, manifestPath string) (*Manifest, error) {
	rc, err := d.store.Get(ctx, manifestPath)
	if err != nil {
		return nil, err
	}
	defer closer(rc)()

	var manifest Manifest
	if err := json.NewDecoder(rc).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("lode: failed to decode manifest: %w", err)
	}

	if err := validateManifest(&manifest); err != nil {
		return nil, err
	}

	return &manifest, nil
}

// -----------------------------------------------------------------------------
// Manifest Validation
// -----------------------------------------------------------------------------

// validateManifest checks that a manifest contains all required fields
// per CONTRACT_CORE.md and CONTRACT_READ_API.md.
func validateManifest(m *Manifest) error {
	if m == nil {
		return &validate.ManifestError{Field: "manifest", Message: "is nil"}
	}

	// Codec is optional: empty is valid for raw blob storage.
	if err := validate.FirstInvalid([]validate.Check{
		{Invalid: m.SchemaName == "", Field: "schema_name", Message: "is required"},
		{Invalid: m.FormatVersion == "", Field: "format_version", Message: "is required"},
		{Invalid: m.DatasetID == "", Field: "dataset_id", Message: "is required"},
		{Invalid: m.SnapshotID == "", Field: "snapshot_id", Message: "is required"},
		{Invalid: m.CreatedAt.IsZero(), Field: "created_at", Message: "is required"},
		{Invalid: !m.Metadata.valid(), Field: "metadata", Message: "must be a JSON object (use {} for no metadata)"},
		{Invalid: m.Files == nil, Field: "files", Message: "must not be nil (use empty slice for no files)"},
		{Invalid: m.RowCount < 0, Field: "row_count", Message: "must be non-negative"},
		{Invalid: m.Compressor == "", Field: "compressor", Message: "is required"},
		{Invalid: m.Partitioner == "", Field: "partitioner", Message: "is required"},
	}); err != nil {
		return err
	}
	if err := missingPaths(m); err != nil {
		return err
	}
	return validateFiles(m.Files)
}

// validateFiles checks every FileRef of an inventory: a manifest's, or the
// copy a retention tombstone records (CONTRACT_ERRORS §File Validation).
func validateFiles(files []FileRef) error {
	for i, f := range files {
		if err := validate.FirstInvalid([]validate.Check{
			{Invalid: f.Path == "", Field: fmt.Sprintf("files[%d].path", i), Message: "is required"},
			{Invalid: f.SizeBytes < 0, Field: fmt.Sprintf("files[%d].size_bytes", i), Message: "must be non-negative"},
		}); err != nil {
			return err
		}
	}
	return nil
}

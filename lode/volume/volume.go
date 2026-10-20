package volume

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/pithecene-io/lode/internal/history"
	"github.com/pithecene-io/lode/internal/retry"
	"github.com/pithecene-io/lode/internal/validate"
	"github.com/pithecene-io/lode/lode"
)

const (
	volumeManifestSchemaName    = "lode-volume-manifest"
	volumeManifestFormatVersion = "1.0.0"
)

// volume implements the Volume interface.
type volume struct {
	id          ID
	store       lode.Store
	totalLength int64
	checksum    lode.Checksum
	retry       retry.Config

	// history owns the snapshot sequence and its head pointer (internal/history).
	history history.History[SnapshotID, Manifest]
}

// volumeKind is the volume's snapshot grammar for history: a fixed layout
// (CONTRACT_VOLUME.md), and manifests validated against this volume on read.
type volumeKind struct {
	id          ID
	totalLength int64
}

func (k volumeKind) SnapshotsPrefix() string           { return volumeSnapshotsPrefix(k.id) }
func (k volumeKind) PointerPath() string               { return volumeLatestPointerPath(k.id) }
func (k volumeKind) ManifestPath(id SnapshotID) string { return volumeManifestPath(k.id, id) }

func (k volumeKind) ParseManifest(p string) (SnapshotID, bool) {
	if !strings.HasSuffix(p, "manifest.json") {
		return "", false
	}
	id := parseVolumeSnapshotID(p)
	return id, id != ""
}

// NotExpired is always true: Volumes have no retention.
func (volumeKind) NotExpired(context.Context, SnapshotID) (bool, error) {
	return true, nil
}

func (k volumeKind) Decode(r io.Reader) (*Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, fmt.Errorf("lode: failed to decode volume manifest: %w", err)
	}
	if err := validateVolumeManifest(&m); err != nil {
		return nil, err
	}
	if m.VolumeID != k.id {
		return nil, fmt.Errorf("lode: manifest volume_id %q does not match volume %q", m.VolumeID, k.id)
	}
	if m.TotalLength != k.totalLength {
		return nil, fmt.Errorf("lode: manifest total_length %d does not match volume total_length %d", m.TotalLength, k.totalLength)
	}
	return &m, nil
}

var _ history.Kind[SnapshotID, Manifest] = volumeKind{}

func toVolumeSnapshot(s *history.Snapshot[SnapshotID, Manifest]) *Snapshot {
	return &Snapshot{ID: s.ID, Manifest: s.Manifest}
}

// historyErrors are the sentinels a volume's history returns, so history
// errors keep lode's public identities.
var historyErrors = history.Errors{
	NotFound:         lode.ErrNotFound,
	NoSnapshots:      lode.ErrNoSnapshots,
	SnapshotConflict: lode.ErrSnapshotConflict,
}

// New creates a volume with a fixed total length.
func New(id ID, storeFactory lode.StoreFactory, totalLength int64, opts ...Option) (Volume, error) {
	if storeFactory == nil {
		return nil, validate.Argument("store factory must not be nil")
	}
	if id == "" {
		return nil, validate.Argument("volume ID must not be empty")
	}
	if totalLength <= 0 {
		return nil, validate.Argument("total length must be positive")
	}

	cfg := &config{
		retry: retry.Default(),
	}
	for _, opt := range opts {
		if err := opt.apply(cfg); err != nil {
			return nil, err // option errors carry the lode: prefix
		}
	}

	// Arguments are checked before any store work (CONTRACT_ERRORS §5).
	store, err := storeFactory()
	if err != nil {
		return nil, fmt.Errorf("lode: failed to create store: %w", err)
	}
	if store == nil {
		return nil, validate.Argument("store factory returned nil store")
	}

	return &volume{
		id:          id,
		store:       store,
		totalLength: totalLength,
		checksum:    cfg.checksum,
		retry:       cfg.retry,
		history:     history.New(store, volumeKind{id: id, totalLength: totalLength}, "volume manifest", historyErrors),
	}, nil
}

// ID returns the volume's identifier.
func (v *volume) ID() ID { return v.id }

// -----------------------------------------------------------------------------
// Path helpers (fixed layout per CONTRACT_VOLUME.md)
// -----------------------------------------------------------------------------

func volumeSnapshotsPrefix(id ID) string {
	return path.Join("volumes", string(id), "snapshots") + "/"
}

func volumeManifestPath(id ID, snapID SnapshotID) string {
	return path.Join("volumes", string(id), "snapshots", string(snapID), "manifest.json")
}

func volumeBlockPath(id ID, offset, length int64) string {
	return path.Join("volumes", string(id), "data", fmt.Sprintf("%d-%d.bin", offset, length))
}

func volumeLatestPointerPath(id ID) string {
	return path.Join("volumes", string(id), "latest")
}

// -----------------------------------------------------------------------------
// Write surface
// -----------------------------------------------------------------------------

// StageWriteAt writes data at an offset and returns a block handle.
// Staged data is not visible until Commit is called.
func (v *volume) StageWriteAt(ctx context.Context, offset int64, r io.Reader) (BlockRef, error) {
	if offset < 0 {
		return BlockRef{}, validate.Argument("offset must be non-negative")
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return BlockRef{}, fmt.Errorf("lode: failed to read block data: %w", err)
	}

	length := int64(len(data))
	if length == 0 {
		return BlockRef{}, validate.Argument("block data must not be empty")
	}
	if length > v.totalLength-offset {
		return BlockRef{}, validate.Argument("block exceeds volume address space (offset=%d, length=%d, totalLength=%d)", offset, length, v.totalLength)
	}

	blockPath := volumeBlockPath(v.id, offset, length)

	var checksumStr string
	if v.checksum != nil {
		hasher := v.checksum.NewHasher()
		if _, err := hasher.Write(data); err != nil {
			return BlockRef{}, fmt.Errorf("lode: failed to compute checksum: %w", err)
		}
		checksumStr = hasher.Sum()
	}

	if err := v.store.Put(ctx, blockPath, bytes.NewReader(data)); err != nil {
		return BlockRef{}, fmt.Errorf("lode: failed to stage block: %w", err)
	}

	return BlockRef{
		Offset:   offset,
		Length:   length,
		Path:     blockPath,
		Checksum: checksumStr,
	}, nil
}

// Commit records the provided blocks into a new immutable snapshot.
func (v *volume) Commit(ctx context.Context, blocks []BlockRef, metadata lode.Metadata) (*Snapshot, error) {
	if metadata == nil {
		metadata = lode.Metadata{}
	}
	if len(blocks) == 0 {
		return nil, validate.Argument("commit must include at least one new block")
	}

	// Validate all new blocks have required fields, conform to fixed layout, and are within bounds.
	// This is invariant across retries — checked once before the commit loop.
	for _, b := range blocks {
		if b.Offset < 0 {
			return nil, validate.Argument("block offset must be non-negative (offset=%d)", b.Offset)
		}
		if b.Length <= 0 {
			return nil, validate.Argument("block length must be positive (length=%d)", b.Length)
		}
		if b.Length > v.totalLength-b.Offset {
			return nil, validate.Argument("block exceeds volume address space (offset=%d, length=%d, totalLength=%d)", b.Offset, b.Length, v.totalLength)
		}
		expectedPath := volumeBlockPath(v.id, b.Offset, b.Length)
		if b.Path != expectedPath {
			return nil, validate.Argument("block path %q does not match expected layout path %q", b.Path, expectedPath)
		}
	}

	var result *Snapshot
	err := v.history.CommitLoop(ctx, &v.retry, func() error {
		parent, expectedPointer, err := v.history.ResolveParent(ctx)
		if err != nil {
			return err
		}
		var existing []BlockRef
		var parentID SnapshotID
		if parent != nil {
			existing, parentID = parent.Manifest.Blocks, parent.ID
		}
		if !hasNewBlock(existing, blocks) {
			return validate.Argument("commit must include at least one new block (all provided blocks already committed)")
		}

		// Cumulative block set, sorted by offset for O(log B) lookups: existing
		// blocks are already sorted, so only the new ones are sorted and merged.
		// Overlaps are a logic error and are not retried.
		cumulative := mergeBlocks(existing, blocks)
		if err := validateNoOverlaps(cumulative); err != nil {
			return err
		}

		manifest := v.buildManifest(SnapshotID(history.NewID()), parentID, metadata, cumulative)
		if err := v.history.Advance(ctx, expectedPointer, manifest.SnapshotID, manifest, []string{volumeManifestPath(v.id, manifest.SnapshotID)}); err != nil {
			return err
		}
		result = &Snapshot{ID: manifest.SnapshotID, Manifest: manifest}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// hasNewBlock reports whether any of blocks is absent from existing.
func hasNewBlock(existing, blocks []BlockRef) bool {
	seen := make(map[string]struct{}, len(existing))
	for _, b := range existing {
		seen[b.Path] = struct{}{}
	}
	for _, b := range blocks {
		if _, found := seen[b.Path]; !found {
			return true
		}
	}
	return false
}

// buildManifest constructs the manifest for a snapshot of this volume.
func (v *volume) buildManifest(snapshotID, parentID SnapshotID, metadata lode.Metadata, blocks []BlockRef) *Manifest {
	var checksumAlgorithm string
	if v.checksum != nil {
		checksumAlgorithm = v.checksum.Name()
	}
	return &Manifest{
		SchemaName:        volumeManifestSchemaName,
		FormatVersion:     volumeManifestFormatVersion,
		VolumeID:          v.id,
		SnapshotID:        snapshotID,
		CreatedAt:         time.Now().UTC(),
		Metadata:          metadata,
		TotalLength:       v.totalLength,
		Blocks:            blocks,
		ParentSnapshotID:  parentID,
		ChecksumAlgorithm: checksumAlgorithm,
	}
}

// mergeBlocks merges two block slices into a single sorted-by-offset result.
// sorted must already be in ascending Offset order (from a prior commit).
// unsorted is sorted first, then the two are merged in O(N + K log K).
func mergeBlocks(sorted, unsorted []BlockRef) []BlockRef {
	if len(unsorted) == 0 {
		out := make([]BlockRef, len(sorted))
		copy(out, sorted)
		return out
	}

	// Sort new blocks (K is small — typically 1–10 blocks per commit).
	newBlocks := make([]BlockRef, len(unsorted))
	copy(newBlocks, unsorted)
	sort.Slice(newBlocks, func(i, j int) bool {
		return newBlocks[i].Offset < newBlocks[j].Offset
	})

	// Merge two sorted slices.
	result := make([]BlockRef, 0, len(sorted)+len(newBlocks))
	i, j := 0, 0
	for i < len(sorted) && j < len(newBlocks) {
		if sorted[i].Offset <= newBlocks[j].Offset {
			result = append(result, sorted[i])
			i++
		} else {
			result = append(result, newBlocks[j])
			j++
		}
	}
	result = append(result, sorted[i:]...)
	result = append(result, newBlocks[j:]...)
	return result
}

// validateNoOverlaps checks that blocks do not overlap in the cumulative set.
// Blocks MUST be sorted by Offset before calling. Callers:
//   - validateVolumeManifest: sorts at load time
//   - Commit: receives output of mergeBlocks (always sorted)
func validateNoOverlaps(blocks []BlockRef) error {
	if len(blocks) <= 1 {
		return nil
	}

	for i := 1; i < len(blocks); i++ {
		// Overflow-safe: equivalent to blocks[i-1].Offset + blocks[i-1].Length > blocks[i].Offset.
		// Safe because blocks is in ascending Offset order, so the subtraction is non-negative.
		if blocks[i-1].Length > blocks[i].Offset-blocks[i-1].Offset {
			return ErrOverlappingBlocks
		}
	}

	return nil
}

// -----------------------------------------------------------------------------
// Read surface
// -----------------------------------------------------------------------------

// ReadAt reads a fully committed range from a snapshot.
func (v *volume) ReadAt(ctx context.Context, snapshotID SnapshotID, offset, length int64) ([]byte, error) {
	if offset < 0 {
		return nil, validate.Argument("offset must be non-negative")
	}
	if length <= 0 {
		return nil, validate.Argument("length must be positive")
	}
	if length > v.totalLength-offset {
		return nil, validate.Argument("read exceeds volume address space (offset=%d, length=%d, totalLength=%d)", offset, length, v.totalLength)
	}
	if length > math.MaxInt {
		return nil, validate.Argument("read length %d exceeds maximum allocation size", length)
	}

	snapshot, err := v.Snapshot(ctx, snapshotID)
	if err != nil {
		return nil, err
	}

	covering, err := findCoveringBlocks(snapshot.Manifest.Blocks, offset, length)
	if err != nil {
		return nil, err
	}

	result := make([]byte, int(length))
	for _, b := range covering {
		// Compute the intersection of [offset, offset+length) and [b.Offset, b.Offset+b.Length).
		readStart := max(offset, b.Offset)
		readEnd := min(offset+length, b.Offset+b.Length)
		readLen := readEnd - readStart
		storeOffset := readStart - b.Offset

		data, err := v.store.ReadRange(ctx, b.Path, storeOffset, readLen)
		if err != nil {
			return nil, fmt.Errorf("lode: failed to read block at offset %d: %w", b.Offset, err)
		}
		if int64(len(data)) != readLen {
			return nil, fmt.Errorf("lode: block data truncated at offset %d (expected %d bytes, got %d)", b.Offset, readLen, len(data))
		}

		copy(result[readStart-offset:], data)
	}

	return result, nil
}

// findCoveringBlocks returns the blocks that fully cover [offset, offset+length).
// Returns ErrRangeMissing if any sub-range is not covered.
//
// Blocks MUST be sorted by Offset (guaranteed by validateVolumeManifest at load time).
// Uses binary search for O(log B + R) where R is the number of covering blocks.
func findCoveringBlocks(blocks []BlockRef, offset, length int64) ([]BlockRef, error) {
	requestEnd := offset + length

	// Binary search: find first block whose end exceeds the requested offset.
	start := sort.Search(len(blocks), func(i int) bool {
		return blocks[i].Offset+blocks[i].Length > offset
	})

	// Collect relevant blocks (walk forward from start).
	var relevant []BlockRef
	for i := start; i < len(blocks); i++ {
		if blocks[i].Offset >= requestEnd {
			break
		}
		relevant = append(relevant, blocks[i])
	}

	// Verify contiguous coverage from offset to requestEnd.
	cursor := offset
	for _, b := range relevant {
		if b.Offset > cursor {
			return nil, ErrRangeMissing
		}
		blockEnd := b.Offset + b.Length
		if blockEnd > cursor {
			cursor = blockEnd
		}
	}
	if cursor < requestEnd {
		return nil, ErrRangeMissing
	}

	return relevant, nil
}

// Latest returns the head snapshot.
func (v *volume) Latest(ctx context.Context) (*Snapshot, error) {
	s, err := v.history.Latest(ctx)
	if err != nil {
		return nil, err
	}
	return toVolumeSnapshot(s), nil
}

// Snapshots lists every snapshot, oldest first.
func (v *volume) Snapshots(ctx context.Context) ([]*Snapshot, error) {
	all, err := v.history.Snapshots(ctx)
	if err != nil {
		return nil, err
	}
	snapshots := make([]*Snapshot, len(all))
	for i, s := range all {
		snapshots[i] = toVolumeSnapshot(s)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Manifest.CreatedAt.Before(snapshots[j].Manifest.CreatedAt)
	})
	return snapshots, nil
}

// Snapshot loads a snapshot by ID.
func (v *volume) Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error) {
	s, err := v.history.Snapshot(ctx, id)
	if err != nil {
		if errors.Is(err, lode.ErrNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("lode: failed to get volume manifest: %w", err)
	}
	return toVolumeSnapshot(s), nil
}

// parseVolumeSnapshotID extracts the snapshot ID from a volume manifest path.
// Expected format: volumes/<id>/snapshots/<snapID>/manifest.json
func parseVolumeSnapshotID(p string) SnapshotID {
	parts := strings.Split(p, "/")
	// volumes/<id>/snapshots/<snapID>/manifest.json → 5 parts
	if len(parts) < 5 {
		return ""
	}
	return SnapshotID(parts[len(parts)-2])
}

// -----------------------------------------------------------------------------
// Volume manifest validation
// -----------------------------------------------------------------------------

// validateBlock checks one block's bounds against the volume's total length.
// Written as an if-chain, not a tagless switch: gremlins reports switch case
// expressions as not covered (go-gremlins/gremlins#298), which would hide
// these guards from the mutation gate.
func validateBlock(i int, b BlockRef, totalLength int64) error {
	if b.Offset < 0 {
		return &validate.ManifestError{Field: fmt.Sprintf("blocks[%d].offset", i), Message: "must be non-negative"}
	}
	if b.Length <= 0 {
		return &validate.ManifestError{Field: fmt.Sprintf("blocks[%d].length", i), Message: "must be positive"}
	}
	if b.Path == "" {
		return &validate.ManifestError{Field: fmt.Sprintf("blocks[%d].path", i), Message: "is required"}
	}
	if b.Length > totalLength-b.Offset {
		return &validate.ManifestError{
			Field:   fmt.Sprintf("blocks[%d]", i),
			Message: fmt.Sprintf("exceeds total_length (offset=%d, length=%d, total_length=%d)", b.Offset, b.Length, totalLength),
		}
	}
	return nil
}

// ensureBlocksSortedByOffset sorts blocks by ascending Offset if not already sorted.
func ensureBlocksSortedByOffset(blocks []BlockRef) {
	if !sort.SliceIsSorted(blocks, func(i, j int) bool {
		return blocks[i].Offset < blocks[j].Offset
	}) {
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Offset < blocks[j].Offset
		})
	}
}

// validateVolumeManifest checks that a volume manifest contains all required
// fields per CONTRACT_VOLUME.md.
func validateVolumeManifest(m *Manifest) error {
	if m == nil {
		return &validate.ManifestError{Field: "manifest", Message: "is nil"}
	}
	checks := []validate.Check{
		{Invalid: m.SchemaName == "", Field: "schema_name", Message: "is required"},
		{Invalid: m.FormatVersion == "", Field: "format_version", Message: "is required"},
		{Invalid: m.VolumeID == "", Field: "volume_id", Message: "is required"},
		{Invalid: m.SnapshotID == "", Field: "snapshot_id", Message: "is required"},
		{Invalid: m.CreatedAt.IsZero(), Field: "created_at", Message: "is required"},
		{Invalid: m.Metadata == nil, Field: "metadata", Message: "must not be nil (use empty map for no metadata)"},
		{Invalid: m.Blocks == nil, Field: "blocks", Message: "must not be nil (use empty slice for no blocks)"},
		{Invalid: m.TotalLength <= 0, Field: "total_length", Message: "must be positive"},
	}
	if err := validate.FirstInvalid(checks); err != nil {
		return err
	}
	// Block checks run only once total_length is known valid. They are plain
	// conditions, not table entries: field names carry the block index, and
	// formatting them for every block on every load would allocate on the
	// read path.
	for i, b := range m.Blocks {
		if err := validateBlock(i, b, m.TotalLength); err != nil {
			return err
		}
	}

	// Sort blocks by offset at load time so downstream consumers
	// (findCoveringBlocks, validateNoOverlaps) can assume sorted input.
	// One-time O(B log B) amortized across all reads.
	ensureBlocksSortedByOffset(m.Blocks)

	if err := validateNoOverlaps(m.Blocks); err != nil {
		return &validate.ManifestError{Field: "blocks", Message: "contain overlapping ranges"}
	}

	return nil
}

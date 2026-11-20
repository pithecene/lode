package volume

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pithecene/lode/internal/history"
	"github.com/pithecene/lode/internal/retry"
	"github.com/pithecene/lode/internal/storepath"
	"github.com/pithecene/lode/internal/validate"
	"github.com/pithecene/lode/lode"
)

const (
	volumeManifestSchemaName    = "lode-volume-manifest"
	volumeManifestFormatVersion = "1.0.0"
)

// volume implements the Volume interface.
type volume struct {
	id          ID
	store       lode.RangeStore
	totalLength int64
	checksum    lode.Checksum
	retry       retry.Config

	history history.History[SnapshotID, Manifest]
}

type volumeKind struct {
	id          ID
	totalLength int64
}

func (k volumeKind) SnapshotsPrefix() string           { return volumeSnapshotsPrefix(k.id) }
func (k volumeKind) ManifestPath(id SnapshotID) string { return volumeManifestPath(k.id, id) }
func (k volumeKind) StagedPath(id SnapshotID) string   { return volumeStagedManifestPath(k.id, id) }
func (volumeKind) StagesCommit(string) bool            { return true }

func (k volumeKind) ParseManifest(p string) (SnapshotID, bool) {
	if !strings.HasSuffix(p, "manifest.json") {
		return "", false
	}
	id := parseVolumeSnapshotID(p)
	return id, id != ""
}

// Volume gives each CAS attempt a fresh ID, so its staged parent is final.
func (k volumeKind) AlternateAncestorPath(id SnapshotID) string { return k.StagedPath(id) }

func (volumeKind) ParentID(m *Manifest) SnapshotID { return m.ParentSnapshotID }

func (volumeKind) NotExpired(context.Context, SnapshotID) (bool, error) {
	return true, nil
}

func (k volumeKind) Decode(id SnapshotID, r io.Reader) (*Manifest, error) {
	var m Manifest
	if err := json.NewDecoder(r).Decode(&m); err != nil {
		return nil, fmt.Errorf("lode: failed to decode volume manifest: %w", err)
	}
	if err := validateVolumeManifest(&m); err != nil {
		return nil, err
	}
	if m.VolumeID != k.id {
		return nil, &validate.ManifestError{
			Field:   "volume_id",
			Message: fmt.Sprintf("got %q, want %q", m.VolumeID, k.id),
		}
	}
	if m.SnapshotID != id {
		return nil, &validate.ManifestError{
			Field:   "snapshot_id",
			Message: fmt.Sprintf("got %q, want %q", m.SnapshotID, id),
		}
	}
	if m.TotalLength != k.totalLength {
		return nil, &validate.ManifestError{
			Field:   "total_length",
			Message: fmt.Sprintf("got %d, want %d", m.TotalLength, k.totalLength),
		}
	}
	return &m, nil
}

var (
	_ history.Kind[SnapshotID, Manifest] = volumeKind{}
	_ history.Stager[SnapshotID]         = volumeKind{}
)

func toVolumeSnapshot(s *history.Snapshot[SnapshotID, Manifest]) *Snapshot {
	return &Snapshot{ID: s.ID, Manifest: s.Manifest}
}

var historyErrors = history.Errors{
	NotFound:         lode.ErrNotFound,
	NoSnapshots:      lode.ErrNoSnapshots,
	SnapshotConflict: lode.ErrSnapshotConflict,
	HeadUncommitted:  lode.ErrHeadUncommitted,
	Removed:          ErrRemoved,
	PathExists:       lode.ErrPathExists,
}

func validateID(id ID) error {
	if !storepath.Segment(string(id)) {
		return validate.Argument("volume ID %q must be %s", id, storepath.SegmentRule)
	}
	return nil
}

func validateSnapshotID(id SnapshotID) error {
	if !storepath.Segment(string(id)) {
		return fmt.Errorf("lode: snapshot %q: a snapshot ID is %s: %w", id, storepath.SegmentRule, lode.ErrNotFound)
	}
	return nil
}

// New creates a volume with a fixed total length.
func New(id ID, storeFactory lode.Factory[lode.RangeStore], totalLength int64, opts ...Option) (Volume, error) {
	if storeFactory == nil {
		return nil, validate.Argument("store factory must not be nil")
	}
	if err := validateID(id); err != nil {
		return nil, err
	}
	if totalLength <= 0 {
		return nil, validate.Argument("total length must be positive")
	}

	cfg := &config{
		retry: retry.Default(),
	}
	for _, opt := range opts {
		if opt == nil {
			return nil, validate.Argument("option must not be nil")
		}
		if err := opt.apply(cfg); err != nil {
			return nil, err
		}
	}

	store, err := storeFactory()
	if err != nil {
		return nil, fmt.Errorf("failed to create store: %w", err)
	}
	if validate.Nil(store) {
		return nil, validate.Argument("store factory returned nil store")
	}

	return &volume{
		id:          id,
		store:       store,
		totalLength: totalLength,
		checksum:    cfg.checksum,
		retry:       cfg.retry,
		history:     history.New(store, volumeKind{id: id, totalLength: totalLength}, volumeLatestPointerPath(id), "volume manifest", historyErrors),
	}, nil
}

// ID returns the volume's identifier.
func (v *volume) ID() ID { return v.id }

// Remove removes volume id from the store that storeFactory creates
// (CONTRACT_VOLUME §Removal). It first replaces the head pointer with the
// removal marker, so Latest and Commit on any handle of the volume return
// ErrRemoved from then on, and then deletes every other object of the
// volume: manifests, staged manifests, and blocks.
//
// With WithRetry the pointer swap is retried on racing commits; otherwise, or
// once retries are spent, Remove returns lode.ErrSnapshotConflict and deletes
// nothing.
func Remove(ctx context.Context, id ID, storeFactory lode.Factory[lode.RangeStore], opts ...Option) error {
	if storeFactory == nil {
		return validate.Argument("store factory must not be nil")
	}
	if err := validateID(id); err != nil {
		return err
	}
	cfg := &config{retry: retry.Default()}
	for _, opt := range opts {
		if opt == nil {
			return validate.Argument("option must not be nil")
		}
		if err := opt.apply(cfg); err != nil {
			return err
		}
	}
	store, err := storeFactory()
	if err != nil {
		return fmt.Errorf("failed to create store: %w", err)
	}
	if validate.Nil(store) {
		return validate.Argument("store factory returned nil store")
	}

	pointer := volumeLatestPointerPath(id)
	previous, err := history.Remove(ctx, store, pointer, historyErrors, &cfg.retry)
	if err != nil {
		return err
	}
	if previous != "" && previous != history.RemovedPointer {
		if err := pendingCommit(ctx, store, id, SnapshotID(previous)); err != nil {
			return err
		}
	}
	keys, err := store.List(ctx, volumePrefix(id))
	if err != nil {
		return fmt.Errorf("failed to list volume objects: %w", err)
	}
	lockKey := pointer + ".lock"
	for _, k := range keys {
		if k == pointer || k == lockKey {
			continue
		}
		if err := store.Delete(ctx, k); err != nil && !errors.Is(err, lode.ErrNotFound) {
			return fmt.Errorf("failed to delete volume object %s: %w", k, err)
		}
	}
	return nil
}

// Head returns the ID of the snapshot that the head pointer of volume id
// names, read without the volume's total length (CONTRACT_VOLUME §Head).
func Head(ctx context.Context, id ID, storeFactory lode.Factory[lode.RangeStore]) (SnapshotID, error) {
	if storeFactory == nil {
		return "", validate.Argument("store factory must not be nil")
	}
	if err := validateID(id); err != nil {
		return "", err
	}
	store, err := storeFactory()
	if err != nil {
		return "", fmt.Errorf("failed to create store: %w", err)
	}
	if validate.Nil(store) {
		return "", validate.Argument("store factory returned nil store")
	}
	h := history.New(store, volumeKind{id: id}, volumeLatestPointerPath(id), "volume manifest", historyErrors)
	return h.PointedHead(ctx)
}

// -----------------------------------------------------------------------------
// Path helpers (fixed layout per CONTRACT_VOLUME.md)
// -----------------------------------------------------------------------------

func pendingCommit(ctx context.Context, store lode.RangeStore, id ID, head SnapshotID) error {
	if !history.IsGeneratedID(head) {
		return nil
	}
	committed, err := store.Exists(ctx, volumeManifestPath(id, head))
	if err != nil {
		return fmt.Errorf("failed to resolve head: %w", err)
	}
	if !committed {
		return fmt.Errorf("lode: latest pointer named %s, whose manifest has not been persisted: %w", head, lode.ErrHeadUncommitted)
	}
	return nil
}

func volumePrefix(id ID) string {
	return path.Join("volumes", string(id)) + "/"
}

func volumeSnapshotsPrefix(id ID) string {
	return path.Join("volumes", string(id), "snapshots") + "/"
}

func volumeManifestPath(id ID, snapID SnapshotID) string {
	return path.Join("volumes", string(id), "snapshots", string(snapID), "manifest.json")
}

func volumeStagedManifestPath(id ID, snapID SnapshotID) string {
	return path.Join("volumes", string(id), "staged", string(snapID)+".json")
}

// newVolumeBlockPath generates a nonced block path
// volumes/<id>/data/<offset>-<length>-<nonce>.bin so an aborted StageWriteAt
// never permanently occupies a byte range with ErrPathExists.
func newVolumeBlockPath(id ID, offset, length int64) string {
	var nonce [8]byte
	_, _ = rand.Read(nonce[:])
	return path.Join("volumes", string(id), "data", fmt.Sprintf("%d-%d-%s.bin", offset, length, hex.EncodeToString(nonce[:])))
}

// isStagedBlockPath reports whether p is a valid block path for (id, offset, length),
// accepting both nonced paths (<offset>-<length>-<16hex>.bin) and legacy
// pre-v1 paths (<offset>-<length>.bin).
func isStagedBlockPath(id ID, offset, length int64, p string) bool {
	rest, ok := strings.CutPrefix(p, "volumes/")
	if !ok {
		return false
	}
	rest, ok = strings.CutPrefix(rest, string(id))
	if !ok {
		return false
	}
	rest, ok = strings.CutPrefix(rest, "/data/")
	if !ok || !storepath.Segment(rest) {
		return false
	}
	rest, ok = strings.CutPrefix(rest, strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(length, 10))
	if !ok {
		return false
	}
	if rest == ".bin" {
		return true
	}
	rest, ok = strings.CutPrefix(rest, "-")
	if !ok {
		return false
	}
	nonce, ok := strings.CutSuffix(rest, ".bin")
	return ok && isLowerHex16(nonce)
}

func isLowerHex16(s string) bool {
	if len(s) != 16 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
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
	if validate.Nil(r) {
		return BlockRef{}, validate.Argument("block reader must not be nil")
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

	blockPath := newVolumeBlockPath(v.id, offset, length)

	var checksumStr string
	if v.checksum != nil {
		hasher := v.checksum.NewHasher()
		if _, err := hasher.Write(data); err != nil {
			return BlockRef{}, fmt.Errorf("lode: failed to compute checksum: %w", err)
		}
		checksumStr = hasher.Sum()
	}

	if err := v.store.Put(ctx, blockPath, bytes.NewReader(data)); err != nil {
		return BlockRef{}, fmt.Errorf("failed to stage block: %w", err)
	}

	return BlockRef{
		Offset:   offset,
		Length:   length,
		Path:     blockPath,
		Checksum: checksumStr,
	}, nil
}

// Commit records the provided blocks and metadata into a new immutable snapshot.
func (v *volume) Commit(ctx context.Context, blocks []BlockRef, metadata any) (*Snapshot, error) {
	encodedMeta, err := lode.NewMetadata(metadata)
	if err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, validate.Argument("commit must include at least one new block")
	}

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
		if !isStagedBlockPath(v.id, b.Offset, b.Length, b.Path) {
			return nil, validate.Argument("block path %q does not match layout path for volume %q at (%d, %d)", b.Path, v.id, b.Offset, b.Length)
		}
	}

	var result *Snapshot
	err = v.history.CommitLoop(ctx, &v.retry, func() error {
		parent, expectedPointer, err := v.history.ResolveParent(ctx)
		if err != nil {
			return err
		}
		var existing []BlockRef
		var parentID SnapshotID
		if parent != nil {
			if want, have := v.checksumAlgorithm(), parent.Manifest.ChecksumAlgorithm; want != have {
				return fmt.Errorf("lode: checksum mismatch: volume records %q, writer has %q: %w", have, want, lode.ErrSettingsMismatch)
			}
			existing, parentID = parent.Manifest.Blocks, parent.ID
		}
		if !hasNewBlock(existing, blocks) {
			return v.completeParent(ctx, parent, &result)
		}

		cumulative := mergeBlocks(existing, blocks)
		if err := validateNoOverlaps(cumulative); err != nil {
			return err
		}

		manifest := v.buildManifest(SnapshotID(history.NewID()), parentID, encodedMeta, cumulative)
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

func (v *volume) completeParent(ctx context.Context, parent *history.Snapshot[SnapshotID, Manifest], result **Snapshot) error {
	if parent == nil || !parent.Pending {
		return validate.Argument("commit must include at least one new block (all provided blocks already committed)")
	}
	if err := v.history.CompletePending(ctx, parent.ID); err != nil {
		return err
	}
	*result = toVolumeSnapshot(parent)
	return nil
}

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

func (v *volume) checksumAlgorithm() string {
	if v.checksum == nil {
		return ""
	}
	return v.checksum.Name()
}

func (v *volume) buildManifest(snapshotID, parentID SnapshotID, metadata lode.Metadata, blocks []BlockRef) *Manifest {
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
		ChecksumAlgorithm: v.checksumAlgorithm(),
	}
}

func mergeBlocks(sorted, unsorted []BlockRef) []BlockRef {
	if len(unsorted) == 0 {
		out := make([]BlockRef, len(sorted))
		copy(out, sorted)
		return out
	}

	newBlocks := make([]BlockRef, len(unsorted))
	copy(newBlocks, unsorted)
	sort.Slice(newBlocks, func(i, j int) bool {
		return newBlocks[i].Offset < newBlocks[j].Offset
	})

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

func validateNoOverlaps(blocks []BlockRef) error {
	if len(blocks) <= 1 {
		return nil
	}
	for i := 1; i < len(blocks); i++ {
		if blocks[i-1].Length > blocks[i].Offset-blocks[i-1].Offset {
			return ErrOverlappingBlocks
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// Read surface
// -----------------------------------------------------------------------------

func (v *volume) ReadAt(ctx context.Context, snapshotID SnapshotID, offset, length int64) ([]byte, error) {
	if err := validateSnapshotID(snapshotID); err != nil {
		return nil, err
	}
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

	plan, err := planRead(snapshot.Manifest, offset, length)
	if err != nil {
		return nil, err
	}
	result := make([]byte, int(length))
	if err := v.fillRead(ctx, plan, result); err != nil {
		return nil, err
	}
	return result, nil
}

type readPlan struct {
	offset, length int64
	blocks         []BlockRef
}

func planRead(m *Manifest, offset, length int64) (readPlan, error) {
	blocks, err := findCoveringBlocks(m.Blocks, offset, length)
	if err != nil {
		return readPlan{}, err
	}
	return readPlan{offset: offset, length: length, blocks: blocks}, nil
}

func (v *volume) fillRead(ctx context.Context, p readPlan, dst []byte) error {
	for _, b := range p.blocks {
		readStart := max(p.offset, b.Offset)
		readEnd := min(p.offset+p.length, b.Offset+b.Length)
		readLen := readEnd - readStart
		storeOffset := readStart - b.Offset

		data, err := v.store.ReadRange(ctx, b.Path, storeOffset, readLen)
		if err != nil {
			return v.blockReadError(ctx, b.Offset, err)
		}
		if int64(len(data)) != readLen {
			return fmt.Errorf("lode: block data truncated at offset %d (expected %d bytes, got %d)", b.Offset, readLen, len(data))
		}

		copy(dst[readStart-p.offset:], data)
	}
	return nil
}

func (v *volume) blockReadError(ctx context.Context, offset int64, err error) error {
	blockErr := fmt.Errorf("failed to read block at offset %d: %w", offset, err)
	if !errors.Is(err, lode.ErrNotFound) {
		return blockErr
	}
	fenceErr := v.history.CheckLive(ctx)
	switch {
	case fenceErr == nil:
		return blockErr
	case errors.Is(fenceErr, ErrRemoved):
		return ErrRemoved
	default:
		return errors.Join(blockErr, fmt.Errorf("failed to check volume removal: %w", fenceErr))
	}
}

func (v *volume) ReaderAt(ctx context.Context, snapshotID SnapshotID) (SnapshotReaderAt, error) {
	snapshot, err := v.Snapshot(ctx, snapshotID)
	if err != nil {
		return nil, err
	}
	return &snapshotReaderAt{ctx: ctx, volume: v, snapshot: snapshot}, nil
}

type snapshotReaderAt struct {
	ctx      context.Context
	volume   *volume
	snapshot *Snapshot
}

var _ SnapshotReaderAt = (*snapshotReaderAt)(nil)

func (r *snapshotReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, validate.Argument("offset must be non-negative")
	}
	total := r.volume.totalLength
	if off >= total {
		return 0, io.EOF
	}
	n := min(int64(len(p)), total-off)
	if n == 0 {
		return 0, nil
	}
	plan, err := planRead(r.snapshot.Manifest, off, n)
	if err != nil {
		return 0, err
	}
	if err := r.volume.fillRead(r.ctx, plan, p[:n]); err != nil {
		return 0, err
	}
	if n < int64(len(p)) {
		return int(n), io.EOF
	}
	return int(n), nil
}

func (r *snapshotReaderAt) Blocks() []BlockRef { return slices.Clone(r.snapshot.Manifest.Blocks) }

func findCoveringBlocks(blocks []BlockRef, offset, length int64) ([]BlockRef, error) {
	requestEnd := offset + length

	start := sort.Search(len(blocks), func(i int) bool {
		return blocks[i].Offset+blocks[i].Length > offset
	})

	end := start
	for end < len(blocks) && blocks[end].Offset < requestEnd {
		end++
	}
	relevant := blocks[start:end:end]

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

func (v *volume) Latest(ctx context.Context) (*Snapshot, error) {
	s, err := v.history.Latest(ctx)
	if err != nil {
		return nil, err
	}
	return toVolumeSnapshot(s), nil
}

func (v *volume) Snapshots(ctx context.Context) ([]*Snapshot, error) {
	return history.ListByCreation(ctx, &v.history, toVolumeSnapshot, func(s *Snapshot) time.Time { return s.Manifest.CreatedAt })
}

func (v *volume) Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error) {
	if err := validateSnapshotID(id); err != nil {
		return nil, err
	}
	s, err := v.history.Snapshot(ctx, id)
	if err != nil {
		if errors.Is(err, lode.ErrNotFound) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get volume manifest: %w", err)
	}
	return toVolumeSnapshot(s), nil
}

func parseVolumeSnapshotID(p string) SnapshotID {
	parts := strings.Split(p, "/")
	if len(parts) < 5 {
		return ""
	}
	return SnapshotID(parts[len(parts)-2])
}

// -----------------------------------------------------------------------------
// Volume manifest validation
// -----------------------------------------------------------------------------

func isVolumeBlockPath(id ID, p string) bool {
	if !storepath.Segment(string(id)) {
		return false
	}
	rest, ok := strings.CutPrefix(p, "volumes/")
	if !ok {
		return false
	}
	rest, ok = strings.CutPrefix(rest, string(id))
	if !ok {
		return false
	}
	rest, ok = strings.CutPrefix(rest, "/data/")
	return ok && storepath.Segment(rest)
}

func validateBlock(i int, id ID, b BlockRef, totalLength int64) error {
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
	if !isVolumeBlockPath(id, b.Path) {
		return &validate.ManifestError{
			Field:   fmt.Sprintf("blocks[%d].path", i),
			Message: fmt.Sprintf("path %q is outside volume %s", b.Path, id),
		}
	}
	return nil
}

func ensureBlocksSortedByOffset(blocks []BlockRef) {
	if !sort.SliceIsSorted(blocks, func(i, j int) bool {
		return blocks[i].Offset < blocks[j].Offset
	}) {
		sort.Slice(blocks, func(i, j int) bool {
			return blocks[i].Offset < blocks[j].Offset
		})
	}
}

func validateVolumeManifest(m *Manifest) error {
	if m == nil {
		return &validate.ManifestError{Field: "manifest", Message: "is nil"}
	}
	checks := []validate.Check{
		{Invalid: m.SchemaName == "", Field: "schema_name", Message: "is required"},
		{Invalid: m.FormatVersion == "", Field: "format_version", Message: "is required"},
		{Invalid: m.VolumeID == "", Field: "volume_id", Message: "is required"},
		{Invalid: m.SnapshotID == "", Field: "snapshot_id", Message: "is required"},
		{Invalid: m.ParentSnapshotID == m.SnapshotID, Field: "parent_snapshot_id", Message: "must differ from snapshot_id"},
		{Invalid: m.CreatedAt.IsZero(), Field: "created_at", Message: "is required"},
		{Invalid: !m.Metadata.Valid(), Field: "metadata", Message: "must be a JSON object (use {} for no metadata)"},
		{Invalid: m.Blocks == nil, Field: "blocks", Message: "must not be nil (use empty slice for no blocks)"},
		{Invalid: m.TotalLength <= 0, Field: "total_length", Message: "must be positive"},
	}
	if err := validate.FirstInvalid(checks); err != nil {
		return err
	}
	for i, b := range m.Blocks {
		if err := validateBlock(i, m.VolumeID, b, m.TotalLength); err != nil {
			return err
		}
	}

	ensureBlocksSortedByOffset(m.Blocks)

	if err := validateNoOverlaps(m.Blocks); err != nil {
		return &validate.ManifestError{Field: "blocks", Message: "contain overlapping ranges"}
	}

	return nil
}

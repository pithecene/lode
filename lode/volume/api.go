// Package volume persists a sparse, fixed-length byte address space as
// immutable, cumulative snapshots.
//
// Experimental: this package is outside Lode's v1.0 compatibility promise
// (docs/V1_API_DECISIONS.md D9). Its API may change in any release until it
// passes the Volume stability gate (docs/V1_READINESS.md §4). Data it has
// persisted stays readable across those changes: the on-disk path grammar
// and manifest format are covered by the persisted-format rule (D8).
//
// A Volume suits a byte object of known length, immutable once written,
// that is filled in pieces — in parallel, out of order, or across process
// restarts — where a partly filled object is useful because its gaps are
// explicit (ErrRangeMissing). Committed ranges never overlap, so workloads
// that rewrite regions do not fit.
//
// Contract: docs/contracts/CONTRACT_VOLUME.md.
package volume

import (
	"context"
	"io"
	"time"
)

// Metadata holds user-defined key-value pairs stored with a volume
// snapshot. Volume is experimental (V1_API_DECISIONS D9) and keeps the map
// form that datasets replaced with lode.Metadata (D11).
type Metadata map[string]any

// ID uniquely identifies a volume.
type ID string

// SnapshotID uniquely identifies an immutable volume snapshot.
type SnapshotID string

// BlockRef references a staged or committed byte block within a volume.
type BlockRef struct {
	// Offset is the byte offset within the volume's address space.
	Offset int64 `json:"offset"`

	// Length is the block size in bytes.
	Length int64 `json:"length"`

	// Path is the storage path for the block data.
	Path string `json:"path"`

	// Checksum is an optional integrity hash.
	Checksum string `json:"checksum,omitempty"`
}

// Manifest describes the committed state of a volume snapshot.
//
// Manifests are cumulative: each contains the full set of committed blocks
// across all prior commits, not just the blocks added in the current commit.
type Manifest struct {
	// SchemaName identifies the manifest schema.
	SchemaName string `json:"schema_name"`

	// FormatVersion identifies the manifest schema version.
	FormatVersion string `json:"format_version"`

	// VolumeID identifies the volume this manifest belongs to.
	VolumeID ID `json:"volume_id"`

	// SnapshotID uniquely identifies this volume snapshot.
	SnapshotID SnapshotID `json:"snapshot_id"`

	// CreatedAt records when the snapshot was committed.
	CreatedAt time.Time `json:"created_at"`

	// Metadata contains user-provided key-value pairs.
	Metadata Metadata `json:"metadata"`

	// TotalLength is the volume's total byte address space.
	TotalLength int64 `json:"total_length"`

	// Blocks lists all committed blocks (cumulative).
	Blocks []BlockRef `json:"blocks"`

	// ParentSnapshotID optionally references the previous volume snapshot.
	ParentSnapshotID SnapshotID `json:"parent_snapshot_id,omitempty"`

	// ChecksumAlgorithm records the checksum algorithm used.
	ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`
}

// Snapshot represents an immutable point-in-time view of a volume.
type Snapshot struct {
	// ID uniquely identifies this volume snapshot.
	ID SnapshotID

	// Manifest describes the snapshot's committed blocks.
	Manifest *Manifest
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

// Volume-specific sentinels. A Volume also returns lode's shared sentinels:
// lode.ErrNotFound, lode.ErrNoSnapshots, lode.ErrSnapshotConflict,
// lode.ErrInvalidArgument and lode.ErrManifestInvalid.
var (
	// ErrRangeMissing indicates a requested byte range is not fully committed.
	ErrRangeMissing error = errRangeMissing{}

	// ErrOverlappingBlocks indicates committed blocks overlap in the
	// cumulative manifest.
	ErrOverlappingBlocks error = errOverlappingBlocks{}
)

type errRangeMissing struct{}

func (errRangeMissing) Error() string { return "range missing: requested range is not fully committed" }

type errOverlappingBlocks struct{}

func (errOverlappingBlocks) Error() string { return "overlapping blocks in cumulative manifest" }

// -----------------------------------------------------------------------------
// Interfaces
// -----------------------------------------------------------------------------

// Writer defines the write surface for a volume.
type Writer interface {
	// StageWriteAt writes data at an offset and returns a block handle.
	// Staged data is not visible until Commit is called.
	StageWriteAt(ctx context.Context, offset int64, r io.Reader) (BlockRef, error)

	// Commit records the provided blocks into a new immutable snapshot.
	// The resulting manifest is cumulative: it includes all previously committed
	// blocks plus the new blocks. Commit MUST include at least one new block.
	Commit(ctx context.Context, blocks []BlockRef, metadata Metadata) (*Snapshot, error)
}

// Reader defines the read surface for a volume.
type Reader interface {
	// ReadAt reads a fully committed range from a snapshot.
	// Returns ErrRangeMissing if any sub-range is uncommitted.
	ReadAt(ctx context.Context, snapshotID SnapshotID, offset, length int64) ([]byte, error)

	// Latest returns the most recently committed snapshot.
	// Returns lode.ErrNoSnapshots if no snapshots exist.
	Latest(ctx context.Context) (*Snapshot, error)

	// Snapshots lists all committed snapshots.
	Snapshots(ctx context.Context) ([]*Snapshot, error)

	// Snapshot retrieves a specific snapshot by ID.
	// Returns lode.ErrNotFound if the snapshot does not exist.
	Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error)
}

// Volume composes the full read/write surface for a sparse byte space.
type Volume interface {
	Writer
	Reader
	ID() ID
}

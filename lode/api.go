// Package lode provides dataset snapshots, manifests, and metadata for object
// storage systems.
//
// Lode focuses on persistence structure: datasets, immutable snapshots, and
// explicit metadata. It does not implement query execution or background
// processing.
package lode

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/pithecene/lode/internal/validate"
)

// -----------------------------------------------------------------------------
// Core types
// -----------------------------------------------------------------------------

// DatasetID uniquely identifies a dataset and is stable for its lifetime.
type DatasetID string

// SnapshotID uniquely identifies an immutable snapshot within a dataset.
type SnapshotID string

// D is a shorthand alias for map-based records in examples and callsites.
// It is equivalent to map[string]any.
type D = map[string]any

// R collects record literals into the []D that Records[D].Write takes.
// This is a convenience helper for callsites and examples.
func R(records ...D) []D {
	return records
}

// Timestamped is implemented by records that have an associated timestamp.
// When the records a *Records[T] handle writes implement this interface, the
// manifest's MinTimestamp and MaxTimestamp fields are computed from the data.
// A record type that does not implement it is not tracked.
type Timestamped interface {
	Timestamp() time.Time
}

// -----------------------------------------------------------------------------
// Manifest
// -----------------------------------------------------------------------------

// Manifest describes the complete contents of a snapshot.
//
// A manifest is self-contained and includes the dataset ID, snapshot ID,
// format version, metadata, and all data object references.
type Manifest struct {
	// SchemaName identifies the manifest schema (e.g., "lode-manifest").
	SchemaName string `json:"schema_name"`

	// FormatVersion identifies the manifest schema version ("1.2.0").
	FormatVersion string `json:"format_version"`

	// DatasetID identifies the dataset this manifest belongs to.
	DatasetID DatasetID `json:"dataset_id"`

	// SnapshotID uniquely identifies this snapshot.
	SnapshotID SnapshotID `json:"snapshot_id"`

	// CreatedAt records when the snapshot was committed.
	CreatedAt time.Time `json:"created_at"`

	// Metadata is the caller's metadata, a JSON object.
	Metadata Metadata `json:"metadata"`

	// Objects lists all data objects comprising this snapshot.
	Objects []ObjectRef `json:"objects"`

	// ParentSnapshotID optionally references the previous snapshot.
	ParentSnapshotID SnapshotID `json:"parent_snapshot_id,omitempty"`

	// RowCount is the total number of data units in this snapshot.
	RowCount int64 `json:"row_count"`

	// MinTimestamp is the earliest timestamp in the snapshot (if data units are timestamped).
	// Omitted when not applicable.
	MinTimestamp *time.Time `json:"min_timestamp,omitempty"`

	// MaxTimestamp is the latest timestamp in the snapshot (if data units are timestamped).
	// Omitted when not applicable.
	MaxTimestamp *time.Time `json:"max_timestamp,omitempty"`

	// Codec records the codec used to serialize structured data (e.g., "jsonl", "parquet").
	// Omitted when no codec is configured.
	Codec string `json:"codec,omitempty"`

	// Compressor records the compression format (e.g., "gzip", "zstd", "noop").
	Compressor string `json:"compressor"`

	// Partitioner records the partitioning strategy (e.g., "hive", "noop").
	Partitioner string `json:"partitioner"`

	// ChecksumAlgorithm records the writer's checksum algorithm (e.g., "sha256",
	// "crc32c", "md5"), configured with WithChecksum or adopted from the head with
	// WithRecordedComponents, even on a snapshot that stores no object. Omitted
	// when the writer has no checksum. Each ObjectRef carries its own digest.
	ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`

	// CodecDescription records the codec's configuration as Codec.Describe
	// returns it. Omitted when the codec describes none or no codec is
	// configured.
	CodecDescription string `json:"codec_description,omitempty"`

	// PartitionKeys records the Hive partition keys, in order. Omitted for a
	// layout without partitions.
	PartitionKeys []string `json:"partition_keys,omitempty"`

	// Flat records whether the dataset was persisted with WithFlatPaths().
	Flat bool `json:"flat,omitempty"`

	// legacyPaths retains the 1.1.0 paths object when decoding pre-1.2.0
	// manifests so recordedSettings can detect custom directory mismatches.
	legacyPaths *legacyManifestPaths
}

// legacyManifestPaths decodes the "paths" field written by format version 1.1.0.
type legacyManifestPaths struct {
	Datasets   string `json:"datasets,omitempty"`
	Snapshots  string `json:"snapshots,omitempty"`
	Partitions string `json:"partitions"`
	Segments   string `json:"segments"`
	Manifest   string `json:"manifest"`
	Data       string `json:"data"`
	Expired    string `json:"expired"`
	Staged     string `json:"staged"`
	Flat       bool   `json:"flat,omitempty"`
}

// UnmarshalJSON decodes format 1.2.0 manifests ("objects", "flat") as well as
// legacy 1.0.0 and 1.1.0 manifests ("files", "paths").
func (m *Manifest) UnmarshalJSON(data []byte) error {
	type manifestAlias Manifest
	var wire struct {
		manifestAlias
		RowCount    *int64               `json:"row_count"` // nil when absent or null
		LegacyFiles []ObjectRef          `json:"files"`
		LegacyPaths *legacyManifestPaths `json:"paths"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	*m = Manifest(wire.manifestAlias)
	if wire.RowCount != nil {
		m.RowCount = *wire.RowCount
	}
	if m.FormatVersion != legacyFormatVersion && m.FormatVersion != v11FormatVersion {
		return nil
	}
	if m.Objects == nil {
		m.Objects = wire.LegacyFiles
	}
	// A record writer before format 1.2.0 recorded no files as null. Any other
	// manifest without an inventory stays invalid, as one whose count of rows is
	// not recorded, and a blob, which always has an object.
	if m.Objects == nil && m.Codec != "" && wire.RowCount != nil && *wire.RowCount == 0 {
		m.Objects = []ObjectRef{}
	}
	if wire.LegacyPaths != nil {
		m.legacyPaths = wire.LegacyPaths
		m.Flat = wire.LegacyPaths.Flat
	}
	return nil
}

// ObjectRef describes a single persisted data object within a snapshot.
// OpenObject and ReaderAt accept an ObjectRef directly.
type ObjectRef struct {
	// Path is the storage key of the object within the store.
	Path string `json:"path"`

	// SizeBytes is the stored object size in bytes.
	SizeBytes int64 `json:"size_bytes"`

	// Checksum is an optional hex-encoded integrity digest of the stored bytes.
	Checksum string `json:"checksum,omitempty"`

	// Stats contains per-object column statistics reported by the codec.
	// Omitted when the codec does not report statistics.
	Stats *ObjectStats `json:"stats,omitempty"`
}

// ObjectStats holds per-object statistics reported by a statistical codec.
type ObjectStats struct {
	// RowCount is the number of rows in this object.
	RowCount int64 `json:"row_count"`

	// Columns contains per-column statistics. May be empty if the codec
	// does not report column-level stats.
	Columns []ColumnStats `json:"columns,omitempty"`
}

// ColumnStats holds per-column statistics for a single data object.
type ColumnStats struct {
	// Name is the column/field name.
	Name string `json:"name"`

	// Min is the minimum value observed. nil if no finite non-null values exist.
	// Values are JSON-serializable Go types matching the codec's decoded types.
	Min any `json:"min,omitempty"`

	// Max is the maximum value observed. nil if no finite non-null values exist.
	Max any `json:"max,omitempty"`

	// NullCount is the number of null values observed for this column.
	NullCount int64 `json:"null_count"`

	// DistinctCount is the approximate number of distinct values.
	// Zero means not computed or not available.
	DistinctCount int64 `json:"distinct_count,omitempty"`
}

// -----------------------------------------------------------------------------
// Snapshot
// -----------------------------------------------------------------------------

// Snapshot represents an immutable point-in-time view of a dataset.
// It embeds its manifest, so snap.Metadata and snap.Objects name the
// manifest's fields directly.
type Snapshot struct {
	// ID uniquely identifies this snapshot.
	ID SnapshotID

	// Manifest describes the snapshot's contents.
	*Manifest
}

// -----------------------------------------------------------------------------
// Store interfaces
// -----------------------------------------------------------------------------

// Store abstracts the underlying object storage system: the object
// operations of ObjectStore and the conditional write of ConditionalWriter,
// which every commit uses to advance the latest pointer.
//
// Implementations may target filesystems, S3, or other object stores. A
// backend without a conditional write can still back a Store through
// SingleProcess, within one process.
type Store interface {
	ObjectStore
	ConditionalWriter
}

// ObjectStore is the object operations of a Store, without the conditional
// write. SingleProcess turns an ObjectStore into a Store.
//
// The interface is intentionally minimal to avoid backend-specific leakage.
type ObjectStore interface {
	// Put writes data to the given path.
	Put(ctx context.Context, path string, r io.Reader) error

	// Get retrieves data from the given path.
	Get(ctx context.Context, path string) (io.ReadCloser, error)

	// Exists checks whether a path exists.
	Exists(ctx context.Context, path string) (bool, error)

	// List returns paths under the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes the path if it exists.
	Delete(ctx context.Context, path string) error
}

// PrefixLister is an optional store capability that lists immediate child
// prefixes (each ending in "/") beneath prefix without enumerating every
// descendant object. ListDatasets uses it when implemented by the store.
type PrefixLister interface {
	ListPrefixes(ctx context.Context, prefix string) ([]string, error)
}

// RangeReader is the optional range-read capability of a store. A handle's
// ReaderAt returns ErrRangeReadNotSupported over a store without it. A store
// that cannot read ranges does not implement it, so that the capability is
// known before any read.
type RangeReader interface {
	// ReadRange reads a byte range from the given path.
	// Returns ErrNotFound if the path does not exist.
	ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error)

	// ReaderAt returns an io.ReaderAt for random access reads.
	// Returns ErrNotFound if the path does not exist.
	ReaderAt(ctx context.Context, path string) (io.ReaderAt, error)
}

// RangeStore is a Store that reads ranges. The stores of FS, Memory, and
// the S3 adapter are RangeStores, and a Volume requires one, so that a store
// without range reads is refused when the program compiles rather than
// when it runs.
type RangeStore interface {
	Store
	RangeReader
}

// ConditionalWriter is the conditional write every Store provides. Commits
// advance the latest pointer with CompareAndSwap, which detects concurrent
// writers (optimistic concurrency) without external coordination.
type ConditionalWriter interface {
	// CompareAndSwap atomically replaces the content at path if and only if
	// the current content matches expected.
	//
	// If path does not exist and expected is empty, the file is created.
	// If path does not exist and expected is non-empty, returns ErrSnapshotConflict.
	// If path exists and content matches expected, content is replaced.
	// If path exists and content does not match expected, returns ErrSnapshotConflict.
	CompareAndSwap(ctx context.Context, path, expected, replacement string) error
}

// Factory creates a store when a root opens. FS, Memory, Use, and s3.Bucket
// return one; a factory performs no I/O until it is called, and Open and
// OpenReadOnly record its error rather than return it, so a caller checks one
// error at the first dataset it opens.
type Factory[S ObjectStore] func() (S, error)

// -----------------------------------------------------------------------------
// Codec interfaces
// -----------------------------------------------------------------------------

// Codec serializes records of type T. A dataset's codec fixes its record
// type: Root.Records infers T from it.
type Codec[T any] interface {
	// Name returns the codec identifier (for example, "jsonl" or "parquet").
	Name() string

	// Describe returns the codec's configuration in a deterministic,
	// canonical form, so that equal configurations yield equal descriptions;
	// "" for a codec without configuration.
	Describe() string

	// Encode writes records to the given writer.
	Encode(w io.Writer, records []T) error

	// Decode reads records from the given reader.
	Decode(r io.Reader) ([]T, error)
}

// StreamingCodec is implemented by codecs that encode one record at a time.
// Records.StreamWrite requires one.
type StreamingCodec[T any] interface {
	Codec[T]

	// NewStreamEncoder creates a streaming encoder that writes to w.
	NewStreamEncoder(w io.Writer) (StreamEncoder[T], error)
}

// StreamEncoder writes records one at a time to a stream.
type StreamEncoder[T any] interface {
	// WriteRecord encodes and writes a single record.
	WriteRecord(record T) error

	// Close finalizes the stream and flushes any buffered data.
	Close() error
}

// StatisticalCodec is implemented by batch codecs that compute per-object
// statistics statelessly during encoding.
type StatisticalCodec[T any] interface {
	Codec[T]

	// EncodeWithStats encodes records to w and returns statistics for the
	// written object, or nil when none apply.
	EncodeWithStats(w io.Writer, records []T) (*ObjectStats, error)
}

// StatisticalStreamEncoder is optionally implemented by a per-stream
// StreamEncoder that reports per-object statistics after Close.
type StatisticalStreamEncoder interface {
	ObjectStats() *ObjectStats
}

// -----------------------------------------------------------------------------
// Compressor & Checksum interfaces
// -----------------------------------------------------------------------------

// Compressor handles compression and decompression of data streams.
type Compressor interface {
	// Name returns the compressor identifier (for example, "gzip", "zstd", "noop").
	Name() string

	// Extension returns the file extension (for example, ".gz", ".zst", "").
	Extension() string

	// Compress wraps a writer with compression.
	Compress(w io.Writer) (io.WriteCloser, error)

	// Decompress wraps a reader with decompression.
	Decompress(r io.Reader) (io.ReadCloser, error)
}

// Checksum computes and verifies integrity digests for data objects.
type Checksum interface {
	// Name returns the checksum algorithm identifier (e.g., "sha256", "crc32c", "md5").
	Name() string

	// NewHasher returns a new HashWriter for computing digests.
	NewHasher() HashWriter
}

// HashWriter combines hash accumulation with hex digest formatting.
type HashWriter interface {
	io.Writer

	// Sum returns the computed digest as a lowercase hex-encoded string.
	Sum() string
}

// -----------------------------------------------------------------------------
// Retention types
// -----------------------------------------------------------------------------

// ExpireRequest selects the snapshots Expire removes. Set fields intersect;
// at least one of IDs, Partition, and Before must be set.
type ExpireRequest struct {
	// IDs selects these snapshots, including any whose expiry an earlier
	// run recorded but did not finish: the request completes it.
	IDs []SnapshotID

	// Partition selects snapshots with a partition manifest in this
	// partition or beneath it. Matching is by whole path segment:
	// "run_id=run-1" never selects "run_id=run-10". Hive layouts only.
	Partition string

	// Before selects snapshots whose write began strictly before this
	// instant. The instant is read from the snapshot ID, so selection loads
	// no manifest.
	Before time.Time

	// DryRun resolves the selection and reports what would be removed,
	// without writing or deleting anything.
	DryRun bool
}

// SweepRequest configures Sweep on a dataset writer handle.
type SweepRequest struct {
	// Grace is how old an unreferenced object must be before it is deleted,
	// measured from when its write began. Must be positive; there is no default.
	Grace time.Duration

	// DryRun reports what would be reclaimed, without deleting anything.
	DryRun bool
}

// SweepReport describes what Sweep reclaimed, or would reclaim on a dry run.
type SweepReport struct {
	// Settled lists expired snapshots whose remaining manifests this run
	// removed, oldest first.
	Settled []SnapshotID

	// Reindexed lists committed snapshots, oldest first, whose missing
	// partition manifests this run persisted from their canonical manifests.
	Reindexed []SnapshotID

	// Objects counts the data objects this run deleted.
	Objects int

	// Bytes sums the sizes recorded for those objects in a tombstone's
	// inventory.
	Bytes int64

	// Unsized counts, of Objects, those with no recorded size.
	Unsized int

	// Orphans counts, of Unsized, those of snapshots never committed.
	Orphans int

	// Unparseable lists invalid or contradicted tombstones and unreferenced
	// data objects whose snapshot ID carries no instant.
	Unparseable []string
}

// ExpireReport describes what Expire removed, or would remove on a dry run.
type ExpireReport struct {
	// Expired lists the expired snapshots, oldest first.
	Expired []SnapshotID

	// Mixed lists snapshots selected by Partition that also have objects
	// outside it. They are kept.
	Mixed []SnapshotID

	// RetainedHead is the head snapshot when the selection included it.
	RetainedHead SnapshotID

	// Objects counts the data objects this run deleted that its listing showed.
	Objects int

	// Bytes sums the sizes the snapshots' inventories record for those objects.
	Bytes int64

	// Skipped lists inventory paths that are not data objects of the expired
	// snapshot in this dataset. They are left in place.
	Skipped []string
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

// Error sentinel values for common conditions.
var (
	// ErrNotFound indicates a requested resource does not exist.
	ErrNotFound = errNotFound{}

	// ErrNoSnapshots indicates a dataset or volume has no committed snapshots.
	ErrNoSnapshots = errors.New("no snapshots")

	// ErrPathExists indicates an attempt to write to an existing path.
	ErrPathExists = errors.New("path exists")

	// ErrRangeReadNotSupported indicates the store does not support range reads.
	ErrRangeReadNotSupported = errors.New("range read not supported")

	// ErrCodecNotStreamable indicates Records.StreamWrite was called with a
	// codec that is not a StreamingCodec.
	ErrCodecNotStreamable = errors.New("codec does not support streaming")

	// ErrPartitioningNotSupported indicates Records.StreamWrite was called on
	// a partitioned layout.
	ErrPartitioningNotSupported = errors.New("StreamWrite does not support partitioning; use Write for partitioned data")

	// ErrNotPartitioned indicates a partition filter or partition expiry was
	// requested on a dataset whose layout does not partition data.
	ErrNotPartitioned = errors.New("operation requires a partitioned layout")

	// ErrSchemaViolation indicates a schema or record violates the Parquet schema.
	ErrSchemaViolation = errors.New("schema violation")

	// ErrInvalidFormat indicates a persisted data file is malformed or corrupted.
	ErrInvalidFormat = errors.New("invalid format")

	// ErrChecksumMismatch indicates an object's stored bytes did not match the
	// digest recorded in its manifest.
	ErrChecksumMismatch = errors.New("checksum mismatch")

	// ErrSnapshotConflict indicates another writer committed since parent
	// was resolved. Retry-safe: re-read Latest(), merge state, re-commit.
	ErrSnapshotConflict = errors.New("snapshot conflict: another writer committed")

	// ErrInvalidArgument indicates a caller-supplied value that cannot be used.
	ErrInvalidArgument error = validate.ErrInvalidArgument

	// ErrInvalidPath indicates a storage path or range the store refuses.
	ErrInvalidPath = errors.New("invalid path")

	// ErrSnapshotExpired indicates the snapshot was removed by Expire.
	// It also matches ErrNotFound.
	ErrSnapshotExpired error = errSnapshotExpired{}

	// ErrRemoved indicates the dataset or volume was removed via
	// Root.RemoveDataset or volume.Remove.
	ErrRemoved = errors.New("removed")

	// ErrHeadUncommitted indicates retention or removal cannot tell the
	// committed head because a commit is pending or the pointer is missing.
	ErrHeadUncommitted = errors.New("head snapshot is not committed yet")

	// ErrSettingsMismatch indicates a handle's settings differ from those the
	// dataset's manifests record.
	ErrSettingsMismatch = errors.New("settings mismatch")

	// ErrManifestInvalid indicates a manifest failed validation.
	ErrManifestInvalid error = validate.ErrManifestInvalid

	// ErrReadOnly indicates a write attempted through a read-only store.
	ErrReadOnly = errors.New("read-only store")
)

type errNotFound struct{}

func (errNotFound) Error() string { return "not found" }

type errSnapshotExpired struct{}

func (errSnapshotExpired) Error() string { return "snapshot expired" }

// Is makes an expired snapshot also a missing one, so callers matching
// ErrNotFound keep working.
func (errSnapshotExpired) Is(target error) bool { return target == ErrNotFound }

// -----------------------------------------------------------------------------
// Listing
// -----------------------------------------------------------------------------

// ManifestRef references a committed manifest (snapshot) within a dataset.
type ManifestRef struct {
	// ID is the snapshot identifier.
	ID SnapshotID

	// Partition is the partition path where this manifest resides, or the zero
	// value on unpartitioned layouts.
	Partition PartitionPath
}

// LoadedManifest pairs a manifest reference from Manifests with its already
// loaded and validated Manifest.
type LoadedManifest struct {
	Ref      ManifestRef
	Manifest *Manifest
}

// PartitionPath is the canonical relative path of a partition within a dataset,
// such as "day=2024-01-01/source=foo". The zero value is the dataset root.
type PartitionPath struct {
	path string
}

// DatasetListOptions controls dataset listing.
type DatasetListOptions struct {
	// Limit is the maximum number of results to return (0 means no limit).
	Limit int

	// StartAfter resumes listing after this dataset ID (exclusive).
	StartAfter DatasetID
}

// PartitionListOptions controls partition listing.
type PartitionListOptions struct {
	// Limit is the maximum number of results to return (0 means no limit).
	Limit int

	// Prefix restricts results to partition paths that start with this string.
	// Returns ErrNotPartitioned on layouts without partitions, and
	// ErrInvalidPath if the prefix is not a canonical relative partition path.
	Prefix string

	// StartAfter resumes listing after this partition path (exclusive).
	StartAfter PartitionPath
}

// ManifestListOptions controls manifest iteration in Manifests.
type ManifestListOptions struct {
	// Limit is the maximum number of results to yield (0 means no limit).
	Limit int

	// StartAfter resumes iteration after this reference (exclusive) in
	// (Partition, ID) order.
	StartAfter ManifestRef

	// Prefetch is the number of manifests Manifests may load ahead of the
	// caller, concurrently. Zero or less loads each manifest only when the
	// caller advances. For the same load outcomes, the order, the validation
	// and the errors stay those of a lazy listing, but a load observes storage
	// when it runs, so a manifest removed after its load is still yielded.
	// Manifests loads no manifest before StartAfter or beyond Limit, and a
	// caller that cancels receives the context's error at its next advance.
	Prefetch int
}

// ErrDatasetsNotModeled indicates that flat paths place datasets at the store
// root without a common datasets directory, so ListDatasets cannot enumerate
// them.
var ErrDatasetsNotModeled = errDatasetsNotModeled{}

type errDatasetsNotModeled struct{}

func (errDatasetsNotModeled) Error() string { return "datasets not modeled by this layout" }

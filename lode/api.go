// Package lode provides dataset snapshots, manifests, and metadata for object
// storage systems.
//
// Lode focuses on persistence structure: datasets, immutable snapshots, and
// explicit metadata. It does not implement query execution or background
// processing.
package lode

import (
	"context"
	"io"
	"time"
)

// -----------------------------------------------------------------------------
// Core types
// -----------------------------------------------------------------------------

// DatasetID uniquely identifies a dataset and is stable for its lifetime.
type DatasetID string

// DatasetSnapshotID uniquely identifies an immutable snapshot within a dataset.
type DatasetSnapshotID string

// Metadata holds user-defined key-value pairs stored with a snapshot.
type Metadata map[string]any

// D is a shorthand alias for map-based records in examples and callsites.
// It is equivalent to map[string]any.
type D = map[string]any

// R converts record literals into the []any form expected by Dataset.Write.
// This is a convenience helper for callsites and examples.
func R(records ...D) []any {
	out := make([]any, len(records))
	for i, r := range records {
		out[i] = r
	}
	return out
}

// Timestamped is implemented by records that have an associated timestamp.
// When records passed to Dataset.Write implement this interface, the manifest's
// MinTimestamp and MaxTimestamp fields are automatically computed from the data.
// Records that do not implement this interface are ignored for timestamp tracking.
type Timestamped interface {
	Timestamp() time.Time
}

// -----------------------------------------------------------------------------
// Manifest
// -----------------------------------------------------------------------------

// Manifest describes the complete contents of a snapshot.
//
// A manifest is self-contained and includes the dataset ID, snapshot ID,
// format version, metadata, and all data file references.
type Manifest struct {
	// SchemaName identifies the manifest schema (e.g., "lode-manifest").
	SchemaName string `json:"schema_name"`

	// FormatVersion identifies the manifest schema version.
	FormatVersion string `json:"format_version"`

	// DatasetID identifies the dataset this manifest belongs to.
	DatasetID DatasetID `json:"dataset_id"`

	// SnapshotID uniquely identifies this snapshot.
	SnapshotID DatasetSnapshotID `json:"snapshot_id"`

	// CreatedAt records when the snapshot was committed.
	CreatedAt time.Time `json:"created_at"`

	// Metadata contains user-provided key-value pairs.
	Metadata Metadata `json:"metadata"`

	// Files lists all data files comprising this snapshot.
	Files []FileRef `json:"files"`

	// ParentSnapshotID optionally references the previous snapshot.
	ParentSnapshotID DatasetSnapshotID `json:"parent_snapshot_id,omitempty"`

	// RowCount is the total number of data units in this snapshot.
	RowCount int64 `json:"row_count"`

	// MinTimestamp is the earliest timestamp in the snapshot (if data units are timestamped).
	// Omitted when not applicable.
	MinTimestamp *time.Time `json:"min_timestamp,omitempty"`

	// MaxTimestamp is the latest timestamp in the snapshot (if data units are timestamped).
	// Omitted when not applicable.
	MaxTimestamp *time.Time `json:"max_timestamp,omitempty"`

	// Codec records the codec used to serialize structured data (e.g., "jsonl").
	// Omitted when no codec is configured.
	Codec string `json:"codec,omitempty"`

	// Compressor records the compression format (e.g., "gzip", "noop").
	Compressor string `json:"compressor"`

	// Partitioner records the partitioning strategy (e.g., "hive-dt", "noop").
	Partitioner string `json:"partitioner"`

	// ChecksumAlgorithm records the checksum algorithm used (e.g., "md5").
	// Omitted when no checksum is configured.
	ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`
}

// FileRef describes a single data file within a snapshot.
type FileRef struct {
	// Path is the relative path to the file within the dataset.
	Path string `json:"path"`

	// SizeBytes is the file size in bytes.
	SizeBytes int64 `json:"size_bytes"`

	// Checksum is an optional integrity hash.
	Checksum string `json:"checksum,omitempty"`

	// Stats contains per-file column statistics reported by the codec.
	// Omitted when the codec does not report statistics.
	Stats *FileStats `json:"stats,omitempty"`
}

// FileStats holds per-file statistics reported by a codec after encoding.
type FileStats struct {
	// RowCount is the number of rows in this file.
	RowCount int64 `json:"row_count"`

	// Columns contains per-column statistics. May be empty if the codec
	// does not report column-level stats.
	Columns []ColumnStats `json:"columns,omitempty"`
}

// ColumnStats holds per-column statistics for a single data file.
type ColumnStats struct {
	// Name is the column/field name.
	Name string `json:"name"`

	// Min is the minimum value observed. nil if no non-null values exist.
	// Values are JSON-serializable Go types matching the codec's decoded types.
	Min any `json:"min,omitempty"`

	// Max is the maximum value observed. nil if no non-null values exist.
	Max any `json:"max,omitempty"`

	// NullCount is the number of null values observed for this column.
	NullCount int64 `json:"null_count"`

	// DistinctCount is the approximate number of distinct values.
	// Zero means not computed or not available.
	DistinctCount int64 `json:"distinct_count,omitempty"`
}

// -----------------------------------------------------------------------------
// DatasetSnapshot
// -----------------------------------------------------------------------------

// DatasetSnapshot represents an immutable point-in-time view of a dataset.
type DatasetSnapshot struct {
	// ID uniquely identifies this snapshot.
	ID DatasetSnapshotID

	// Manifest describes the snapshot's contents.
	Manifest *Manifest
}

// -----------------------------------------------------------------------------
// Store interface
// -----------------------------------------------------------------------------

// Store abstracts the underlying object storage system.
//
// Implementations may target filesystems, S3, or other object stores.
// The interface is intentionally minimal to avoid backend-specific leakage.
type Store interface {
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

	// ReadRange reads a byte range from the given path.
	// Returns ErrNotFound if the path does not exist.
	// Returns ErrRangeReadNotSupported if the store does not support range reads.
	ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error)

	// ReaderAt returns an io.ReaderAt for random access reads.
	// Returns ErrNotFound if the path does not exist.
	// Returns ErrRangeReadNotSupported if the store does not support range reads.
	ReaderAt(ctx context.Context, path string) (io.ReaderAt, error)
}

// ConditionalWriter is an optional interface that storage adapters MAY
// implement to enable optimistic concurrency (CAS) on the latest pointer.
//
// When a Store implements ConditionalWriter, the commit path uses
// CompareAndSwap instead of Delete+Put for pointer updates, enabling
// safe concurrent writes without external coordination.
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

// StoreFactory creates a Store. Used for deferred store construction.
type StoreFactory func() (Store, error)

// -----------------------------------------------------------------------------
// Codec interface
// -----------------------------------------------------------------------------

// Codec handles serialization and deserialization of records.
//
// Codecs are pluggable and orthogonal to storage, compression, and partitioning.
type Codec interface {
	// Name returns the codec identifier (for example, "jsonl" or "parquet").
	Name() string

	// Encode writes records to the given writer.
	Encode(w io.Writer, records []any) error

	// Decode reads records from the given reader.
	Decode(r io.Reader) ([]any, error)
}

// -----------------------------------------------------------------------------
// Streaming codec interfaces
// -----------------------------------------------------------------------------

// StreamingRecordCodec is implemented by codecs that support streaming record encoding.
//
// StreamWriteRecords requires a codec that implements this interface. Codecs that
// do not support streaming should not implement this interface.
type StreamingRecordCodec interface {
	Codec

	// NewStreamEncoder creates a streaming encoder that writes to w.
	NewStreamEncoder(w io.Writer) (RecordStreamEncoder, error)
}

// RecordStreamEncoder writes records one at a time to a stream.
type RecordStreamEncoder interface {
	// WriteRecord encodes and writes a single record.
	WriteRecord(record any) error

	// Close finalizes the stream and flushes any buffered data.
	Close() error
}

// -----------------------------------------------------------------------------
// Statistical codec interfaces
// -----------------------------------------------------------------------------

// StatisticalCodec is implemented by codecs that report per-file statistics
// after encoding. This is an optional extension to the Codec interface.
//
// The codec accumulates statistics during Encode and returns them via FileStats.
// FileStats must be called after a successful Encode call and before the next
// Encode call; behavior is undefined otherwise.
type StatisticalCodec interface {
	Codec

	// FileStats returns statistics accumulated during the most recent Encode call.
	// Returns nil if no statistics are available.
	FileStats() *FileStats
}

// StatisticalStreamEncoder is implemented by stream encoders that report
// per-file statistics after the stream is finalized. This is an optional
// extension to the RecordStreamEncoder interface.
//
// FileStats must be called after Close returns successfully.
type StatisticalStreamEncoder interface {
	RecordStreamEncoder

	// FileStats returns statistics accumulated during stream encoding.
	// Returns nil if no statistics are available.
	FileStats() *FileStats
}

// -----------------------------------------------------------------------------
// Compressor interface
// -----------------------------------------------------------------------------

// Compressor handles compression and decompression of data streams.
//
// Compressors are pluggable and orthogonal to storage, codecs, and partitioning.
type Compressor interface {
	// Name returns the compressor identifier (for example, "gzip", "zstd", "none").
	Name() string

	// Extension returns the file extension (for example, ".gz", ".zst", "").
	Extension() string

	// Compress wraps a writer with compression.
	Compress(w io.Writer) (io.WriteCloser, error)

	// Decompress wraps a reader with decompression.
	Decompress(r io.Reader) (io.ReadCloser, error)
}

// -----------------------------------------------------------------------------
// Checksum interface
// -----------------------------------------------------------------------------

// Checksum computes integrity checksums for data files.
//
// Checksums are optional and configured via WithChecksum. When configured,
// checksums are computed during write and recorded in manifests.
type Checksum interface {
	// Name returns the checksum algorithm identifier (e.g., "md5", "sha256").
	Name() string

	// NewHasher returns a new hash.Hash for computing checksums.
	// The returned Hash can be used as an io.Writer to accumulate data.
	NewHasher() HashWriter
}

// HashWriter combines hash computation with io.Writer.
// Write data to accumulate the hash, then call Sum to get the result.
type HashWriter interface {
	io.Writer

	// Sum returns the computed checksum as a hex-encoded string.
	Sum() string
}

// -----------------------------------------------------------------------------
// Dataset interface
// -----------------------------------------------------------------------------

// Dataset provides operations on a named collection of snapshots.
//
// A dataset is a logical container for related data. All writes to a dataset
// produce immutable snapshots.
type Dataset interface {
	// ID returns the dataset's unique identifier.
	ID() DatasetID

	// Write commits new data and metadata as an immutable snapshot.
	Write(ctx context.Context, data []any, metadata Metadata) (*DatasetSnapshot, error)

	// Snapshot retrieves a specific snapshot by ID.
	Snapshot(ctx context.Context, id DatasetSnapshotID) (*DatasetSnapshot, error)

	// Snapshots lists all committed snapshots.
	Snapshots(ctx context.Context) ([]*DatasetSnapshot, error)

	// Read retrieves all data units from a specific snapshot.
	Read(ctx context.Context, id DatasetSnapshotID) ([]any, error)

	// Latest returns the most recently committed snapshot.
	Latest(ctx context.Context) (*DatasetSnapshot, error)

	// StreamWrite returns a StreamWriter for single-pass streaming of a binary payload.
	// Returns an error if metadata is nil or if a codec is configured.
	StreamWrite(ctx context.Context, metadata Metadata) (StreamWriter, error)

	// StreamWriteRecords consumes records via a pull-based iterator and streams them
	// through a streaming-capable codec. Returns an error if metadata is nil or if
	// the configured codec does not support streaming.
	StreamWriteRecords(ctx context.Context, records RecordIterator, metadata Metadata) (*DatasetSnapshot, error)

	// Expire removes the selected snapshots: their manifests and the data
	// objects their manifests list. The head is never expired; when the
	// selection includes it, it is kept and reported in RetainedHead. The
	// report is non-nil, also on error, and describes the work done before
	// the error. See CONTRACT_RETENTION.md.
	Expire(ctx context.Context, req ExpireRequest) (*ExpireReport, error)

	// Sweep completes interrupted expiries and reclaims data objects of
	// snapshots that were never committed, once they are older than
	// req.Grace. It never deletes an object a manifest still describes, nor
	// any object outside the layout's data topology. The report is non-nil,
	// also on error. See CONTRACT_RETENTION.md.
	Sweep(ctx context.Context, req SweepRequest) (*SweepReport, error)
}

// -----------------------------------------------------------------------------
// Retention
// -----------------------------------------------------------------------------

// ExpireRequest selects the snapshots Dataset.Expire removes. Set fields
// intersect; at least one of IDs, Partition and Before must be set.
type ExpireRequest struct {
	// IDs selects these snapshots, including any whose expiry an earlier
	// run recorded but did not finish: the request completes it.
	IDs []DatasetSnapshotID

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

// SweepRequest configures Dataset.Sweep.
type SweepRequest struct {
	// Grace is how old an unreferenced object must be before it is deleted,
	// measured from when its write began. It must exceed the longest write
	// in flight plus the largest clock difference between a writing host
	// and the sweeping one, and must be positive; there is no default.
	Grace time.Duration

	// DryRun reports what would be reclaimed, without deleting anything.
	DryRun bool
}

// SweepReport describes what Dataset.Sweep reclaimed, or would reclaim on a
// dry run.
type SweepReport struct {
	// Settled lists expired snapshots whose remaining manifests this run
	// removed, oldest first.
	Settled []DatasetSnapshotID

	// Objects counts the data objects this run deleted.
	Objects int

	// Bytes sums the sizes recorded for those objects in a tombstone's
	// inventory.
	Bytes int64

	// Unsized counts, of Objects, those with no recorded size: orphans and
	// objects a settled snapshot's inventory does not list. Lode never
	// recorded their size, so they are counted, never estimated.
	Unsized int

	// Orphans counts, of Unsized, those of snapshots never committed.
	Orphans int

	// Unparseable lists invalid tombstones, tombstones that do not describe
	// a manifest still present (another commit instant or inventory), and
	// unreferenced data objects whose snapshot ID carries no instant.
	// Nothing listed is deleted.
	Unparseable []string
}

// ExpireReport describes what Dataset.Expire removed, or would remove on a
// dry run.
type ExpireReport struct {
	// Expired lists the expired snapshots, oldest first.
	Expired []DatasetSnapshotID

	// Mixed lists snapshots selected by Partition that also have files
	// outside it. They are kept.
	Mixed []DatasetSnapshotID

	// RetainedHead is the head snapshot when the selection included it.
	// The head is never expired. Empty otherwise.
	RetainedHead DatasetSnapshotID

	// Objects counts the data objects this run deleted that its listing
	// showed. An object an earlier, interrupted run already deleted is not
	// counted again; runs that overlap can both count one.
	Objects int

	// Bytes sums the sizes the snapshots' inventories record for those
	// objects.
	Bytes int64

	// Skipped lists inventory paths that are not data objects of the
	// expired snapshot in this dataset. They are left in place.
	Skipped []string
}

// -----------------------------------------------------------------------------
// StreamWriter interface
// -----------------------------------------------------------------------------

// StreamWriter supports single-pass streaming writes of binary data.
//
// A StreamWriter writes bytes directly to the final object path. The manifest
// is written only on Commit. If Close is called before Commit, the stream is
// aborted and no snapshot is created.
type StreamWriter interface {
	// Write writes bytes to the stream. Implements io.Writer.
	Write(p []byte) (n int, err error)

	// Commit finalizes the stream and writes the manifest.
	// Returns the new snapshot on success.
	Commit(ctx context.Context) (*DatasetSnapshot, error)

	// Abort discards the stream without creating a snapshot.
	// Attempts best-effort cleanup of partial objects.
	Abort(ctx context.Context) error

	// Close closes the stream. If Commit was not called, behaves as Abort.
	Close() error
}

// -----------------------------------------------------------------------------
// RecordIterator interface
// -----------------------------------------------------------------------------

// RecordIterator provides pull-based iteration over records.
//
// The typical usage pattern is:
//
//	for iter.Next() {
//	    record := iter.Record()
//	    // process record
//	}
//	if err := iter.Err(); err != nil { ... }
type RecordIterator interface {
	Next() bool  // Advances to the next record. Returns false when exhausted.
	Record() any // Returns the current record. Only valid after Next returns true.
	Err() error  // Returns any error encountered during iteration.
}

// -----------------------------------------------------------------------------
// Errors
// -----------------------------------------------------------------------------

// Error sentinel values for common conditions.
var (
	// ErrNotFound indicates a requested resource does not exist.
	ErrNotFound = errNotFound{}

	// ErrNoSnapshots indicates a dataset or volume has no committed snapshots.
	ErrNoSnapshots = errNoSnapshots{}

	// ErrPathExists indicates an attempt to write to an existing path.
	ErrPathExists = errPathExists{}

	// ErrNoManifests indicates storage contains objects but no valid manifests.
	ErrNoManifests = errNoManifests{}

	// ErrRangeReadNotSupported indicates the store does not support range reads.
	ErrRangeReadNotSupported = errRangeReadNotSupported{}

	// ErrCodecConfigured indicates StreamWrite was called with a codec configured.
	ErrCodecConfigured = errCodecConfigured{}

	// ErrCodecNotStreamable indicates the configured codec does not support streaming.
	ErrCodecNotStreamable = errCodecNotStreamable{}

	// ErrNilIterator indicates a nil iterator was passed to StreamWriteRecords.
	ErrNilIterator = errNilIterator{}

	// ErrPartitioningNotSupported indicates StreamWriteRecords was called with partitioning configured.
	ErrPartitioningNotSupported = errPartitioningNotSupported{}

	// ErrPartitionPrefixNotSupported indicates PartitionListOptions.Prefix was
	// set on a layout that does not model partitions.
	ErrPartitionPrefixNotSupported = errPartitionPrefixNotSupported{}

	// ErrSchemaViolation indicates a record does not conform to the Parquet schema.
	// Returned when: missing required field, type mismatch, nil for non-nullable field,
	// or invalid timestamp string.
	ErrSchemaViolation = errSchemaViolation{}

	// ErrInvalidFormat indicates the Parquet file is malformed or corrupted.
	ErrInvalidFormat = errInvalidFormat{}

	// ErrSnapshotConflict indicates another writer committed since parent
	// was resolved. Retry-safe: re-read Latest(), merge state, re-commit.
	// Only returned when the store implements ConditionalWriter.
	ErrSnapshotConflict = errSnapshotConflict{}

	// ErrInvalidArgument indicates a caller-supplied value that cannot be
	// used: a nil store factory, a negative offset, a block outside the
	// volume, a blob write without exactly one []byte. The message names the
	// value. Not retry-safe; fix the call.
	ErrInvalidArgument error = errInvalidArgument{}

	// ErrInvalidPath indicates a storage path or range the store refuses:
	// a key that is empty or escapes the root, a partition prefix that is not
	// a canonical relative path, or a ReadRange with negative or overflowing
	// bounds.
	ErrInvalidPath error = errInvalidPath{}

	// ErrSnapshotExpired indicates the snapshot was removed by
	// Dataset.Expire. It also matches ErrNotFound.
	ErrSnapshotExpired error = errSnapshotExpired{}

	// ErrHeadUncommitted indicates retention cannot tell the committed head:
	// the latest pointer names a snapshot whose manifest is not written yet
	// (a commit is in flight, or crashed before writing it), or is missing
	// beside existing snapshots. Retry once the commit completes; Latest and
	// the next commit repair a pointer a crashed commit left behind, and any
	// commit creates a missing one.
	ErrHeadUncommitted error = errHeadUncommitted{}

	// ErrManifestInvalid indicates a manifest failed validation.
	ErrManifestInvalid error = errManifestInvalid{}

	// ErrOptionNotValidForDataset indicates an option was used with NewDataset
	// that only applies to NewDatasetReader.
	ErrOptionNotValidForDataset error = errOptionNotValidForDataset{}

	// ErrOptionNotValidForDatasetReader indicates an option was used with
	// NewDatasetReader that only applies to NewDataset.
	ErrOptionNotValidForDatasetReader error = errOptionNotValidForDatasetReader{}

	// ErrOptionNotValidForVolume indicates an option was used with NewVolume
	// that only applies to Dataset or DatasetReader.
	ErrOptionNotValidForVolume error = errOptionNotValidForVolume{}
)

type errNotFound struct{}

func (errNotFound) Error() string { return "not found" }

type errSnapshotExpired struct{}

func (errSnapshotExpired) Error() string { return "snapshot expired" }

// Is makes an expired snapshot also a missing one, so callers matching
// ErrNotFound keep working.
func (errSnapshotExpired) Is(target error) bool { return target == ErrNotFound }

type errHeadUncommitted struct{}

func (errHeadUncommitted) Error() string { return "head snapshot is not committed yet" }

type errNoSnapshots struct{}

func (errNoSnapshots) Error() string { return "no snapshots" }

type errPathExists struct{}

func (errPathExists) Error() string { return "path exists" }

type errNoManifests struct{}

func (errNoManifests) Error() string { return "no manifests found (storage contains objects)" }

type errRangeReadNotSupported struct{}

func (errRangeReadNotSupported) Error() string { return "range read not supported" }

type errCodecConfigured struct{}

func (errCodecConfigured) Error() string {
	return "StreamWrite requires no codec; use StreamWriteRecords for structured data"
}

type errCodecNotStreamable struct{}

func (errCodecNotStreamable) Error() string { return "codec does not support streaming" }

type errPartitionPrefixNotSupported struct{}

func (errPartitionPrefixNotSupported) Error() string {
	return "partition prefix requires a partition-aware layout"
}

type errNilIterator struct{}

func (errNilIterator) Error() string { return "records iterator must be non-nil" }

type errPartitioningNotSupported struct{}

func (errPartitioningNotSupported) Error() string {
	return "StreamWriteRecords does not support partitioning; use Write for partitioned data"
}

type errSchemaViolation struct{}

func (errSchemaViolation) Error() string { return "parquet: schema violation" }

type errInvalidFormat struct{}

func (errInvalidFormat) Error() string { return "parquet: invalid format" }

type errSnapshotConflict struct{}

func (errSnapshotConflict) Error() string { return "snapshot conflict: another writer committed" }

type errInvalidArgument struct{}

func (errInvalidArgument) Error() string { return "invalid argument" }

type errInvalidPath struct{}

func (errInvalidPath) Error() string { return "invalid path" }

type errManifestInvalid struct{}

func (errManifestInvalid) Error() string { return "invalid manifest" }

type errOptionNotValidForDataset struct{}

func (errOptionNotValidForDataset) Error() string { return "option not valid for dataset" }

type errOptionNotValidForDatasetReader struct{}

func (errOptionNotValidForDatasetReader) Error() string { return "option not valid for reader" }

type errOptionNotValidForVolume struct{}

func (errOptionNotValidForVolume) Error() string { return "option not valid for volume" }

// -----------------------------------------------------------------------------
// DatasetReader interface
// -----------------------------------------------------------------------------

// ManifestRef references a committed manifest (snapshot) within a dataset.
type ManifestRef struct {
	// ID is the snapshot identifier.
	ID DatasetSnapshotID

	// Partition is the partition path where this manifest resides.
	// For partition-first layouts (e.g., HiveLayout), this is populated during
	// discovery. Empty for segment-first layouts (e.g., DefaultLayout).
	Partition string
}

// PartitionRef references a partition within a dataset.
type PartitionRef struct {
	// Path is the partition path (e.g., "day=2024-01-01/source=foo").
	Path string
}

// ObjectRef references a data object within a dataset manifest.
type ObjectRef struct {
	// Dataset is the dataset containing this object.
	Dataset DatasetID

	// Manifest is the manifest containing this object.
	Manifest ManifestRef

	// Path is the full storage key for the object.
	Path string
}

// DatasetListOptions controls dataset listing.
//
// Results are returned in lexicographic order of DatasetID. Limit bounds the
// result set, not the number of store calls.
type DatasetListOptions struct {
	// Limit is the maximum number of results to return.
	// Zero means no limit.
	Limit int

	// StartAfter resumes listing after this dataset ID (exclusive). The last
	// result from a previous call is a valid cursor. Empty disables the cursor.
	StartAfter DatasetID
}

// PartitionListOptions controls partition listing.
//
// Results are returned in lexicographic order of PartitionRef.Path. Limit
// bounds the result set, not the number of store calls.
type PartitionListOptions struct {
	// Limit is the maximum number of results to return.
	// Zero means no limit.
	Limit int

	// Prefix restricts results to partition paths that start with this
	// string, for example "source=a/category=b" or "day=2026-10". It is a
	// raw path prefix: no key/value boundary is implied. On partition-aware
	// layouts it also narrows the storage listing to the matching subtree.
	// Returns ErrPartitionPrefixNotSupported on layouts without partitions,
	// and ErrInvalidPath if the prefix is not a canonical relative partition
	// path (leading "/", or an empty, "." or ".." segment), since it is
	// resolved beneath the dataset's partitions directory.
	// When set, an empty result means no partitions under the prefix; the
	// dataset's existence is not checked separately.
	Prefix string

	// StartAfter resumes listing after this partition path (exclusive).
	// The Path of the last result from a previous call is a valid cursor.
	// Empty disables the cursor.
	StartAfter string
}

// ManifestListOptions controls manifest listing.
//
// Results are ordered by (Partition, ID) lexicographically. Limit bounds the
// result set, not the number of store calls.
type ManifestListOptions struct {
	// Limit is the maximum number of results to return.
	// Zero means no limit.
	Limit int

	// StartAfter resumes listing after this reference (exclusive). The last
	// ManifestRef from a previous call is a valid cursor. The zero value
	// disables the cursor.
	StartAfter ManifestRef
}

// DatasetReader provides read operations over stored datasets.
//
// DatasetReader is a façade over storage and layout that performs no interpretation.
// Per CONTRACT_READ_API.md: "Lode's read API exposes stored facts, not interpretations.
// Planning and meaning belong to consumers."
type DatasetReader interface {
	// ListDatasets returns all dataset IDs found in storage.
	// Returns ErrDatasetsNotModeled if the layout doesn't support dataset enumeration.
	ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error)

	// ListPartitions returns partition paths found across all committed manifests.
	// Returns ErrNotFound if the dataset does not exist.
	ListPartitions(ctx context.Context, dataset DatasetID, opts PartitionListOptions) ([]PartitionRef, error)

	// ListManifests returns committed manifests (snapshots) within a dataset.
	// Returns ErrNotFound if the dataset does not exist.
	ListManifests(ctx context.Context, dataset DatasetID, partition string, opts ManifestListOptions) ([]ManifestRef, error)

	// GetManifest loads the manifest for a specific snapshot.
	// Returns ErrNotFound if the dataset or snapshot does not exist.
	GetManifest(ctx context.Context, dataset DatasetID, ref ManifestRef) (*Manifest, error)

	// OpenObject returns a reader for a data object.
	// The caller must close the reader when done.
	OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error)

	// ReaderAt returns an io.ReaderAt for random access reads on a data object.
	// Returns ErrRangeReadNotSupported if the underlying store does not support range reads.
	ReaderAt(ctx context.Context, obj ObjectRef) (io.ReaderAt, error)
}

// ErrDatasetsNotModeled indicates that the current layout does not support
// dataset enumeration.
var ErrDatasetsNotModeled = errDatasetsNotModeled{}

type errDatasetsNotModeled struct{}

func (errDatasetsNotModeled) Error() string { return "datasets not modeled by this layout" }

// -----------------------------------------------------------------------------
// Volume types
// -----------------------------------------------------------------------------

// VolumeID uniquely identifies a volume.
type VolumeID string

// VolumeSnapshotID uniquely identifies an immutable volume snapshot.
type VolumeSnapshotID string

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

// VolumeManifest describes the committed state of a volume snapshot.
//
// Manifests are cumulative: each contains the full set of committed blocks
// across all prior commits, not just the blocks added in the current commit.
type VolumeManifest struct {
	// SchemaName identifies the manifest schema.
	SchemaName string `json:"schema_name"`

	// FormatVersion identifies the manifest schema version.
	FormatVersion string `json:"format_version"`

	// VolumeID identifies the volume this manifest belongs to.
	VolumeID VolumeID `json:"volume_id"`

	// SnapshotID uniquely identifies this volume snapshot.
	SnapshotID VolumeSnapshotID `json:"snapshot_id"`

	// CreatedAt records when the snapshot was committed.
	CreatedAt time.Time `json:"created_at"`

	// Metadata contains user-provided key-value pairs.
	Metadata Metadata `json:"metadata"`

	// TotalLength is the volume's total byte address space.
	TotalLength int64 `json:"total_length"`

	// Blocks lists all committed blocks (cumulative).
	Blocks []BlockRef `json:"blocks"`

	// ParentSnapshotID optionally references the previous volume snapshot.
	ParentSnapshotID VolumeSnapshotID `json:"parent_snapshot_id,omitempty"`

	// ChecksumAlgorithm records the checksum algorithm used.
	ChecksumAlgorithm string `json:"checksum_algorithm,omitempty"`
}

// VolumeSnapshot represents an immutable point-in-time view of a volume.
type VolumeSnapshot struct {
	// ID uniquely identifies this volume snapshot.
	ID VolumeSnapshotID

	// Manifest describes the snapshot's committed blocks.
	Manifest *VolumeManifest
}

// volumeConfig holds the resolved configuration for a volume.
type volumeConfig struct {
	checksum Checksum
	retry    retryConfig
}

// -----------------------------------------------------------------------------
// Volume errors
// -----------------------------------------------------------------------------

// ErrRangeMissing indicates a requested byte range is not fully committed.
var ErrRangeMissing = errRangeMissing{}

type errRangeMissing struct{}

func (errRangeMissing) Error() string { return "range missing: requested range is not fully committed" }

// ErrOverlappingBlocks indicates committed blocks overlap in the cumulative manifest.
var ErrOverlappingBlocks = errOverlappingBlocks{}

type errOverlappingBlocks struct{}

func (errOverlappingBlocks) Error() string { return "overlapping blocks in cumulative manifest" }

// -----------------------------------------------------------------------------
// Volume interfaces
// -----------------------------------------------------------------------------

// VolumeWriter defines the write surface for a volume.
type VolumeWriter interface {
	// StageWriteAt writes data at an offset and returns a block handle.
	// Staged data is not visible until Commit is called.
	StageWriteAt(ctx context.Context, offset int64, r io.Reader) (BlockRef, error)

	// Commit records the provided blocks into a new immutable snapshot.
	// The resulting manifest is cumulative: it includes all previously committed
	// blocks plus the new blocks. Commit MUST include at least one new block.
	Commit(ctx context.Context, blocks []BlockRef, metadata Metadata) (*VolumeSnapshot, error)
}

// VolumeReader defines the read surface for a volume.
type VolumeReader interface {
	// ReadAt reads a fully committed range from a snapshot.
	// Returns ErrRangeMissing if any sub-range is uncommitted.
	ReadAt(ctx context.Context, snapshotID VolumeSnapshotID, offset, length int64) ([]byte, error)

	// Latest returns the most recently committed snapshot.
	// Returns ErrNoSnapshots if no snapshots exist.
	Latest(ctx context.Context) (*VolumeSnapshot, error)

	// Snapshots lists all committed snapshots.
	Snapshots(ctx context.Context) ([]*VolumeSnapshot, error)

	// Snapshot retrieves a specific snapshot by ID.
	// Returns ErrNotFound if the snapshot does not exist.
	Snapshot(ctx context.Context, id VolumeSnapshotID) (*VolumeSnapshot, error)
}

// Volume composes the full read/write surface for a sparse byte space.
type Volume interface {
	VolumeWriter
	VolumeReader
	ID() VolumeID
}

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
	"iter"
	"time"

	"github.com/pithecene/lode/internal/validate"
)

// -----------------------------------------------------------------------------
// Core types
// -----------------------------------------------------------------------------

// DatasetID uniquely identifies a dataset and is stable for its lifetime.
type DatasetID string

// DatasetSnapshotID uniquely identifies an immutable snapshot within a dataset.
type DatasetSnapshotID string

// D is a shorthand alias for map-based records in examples and callsites.
// It is equivalent to map[string]any.
type D = map[string]any

// R collects record literals into the []D that Records[D].Write takes.
// This is a convenience helper for callsites and examples.
func R(records ...D) []D {
	return records
}

// Timestamped is implemented by records that have an associated timestamp.
// When the records a Records handle writes implement this interface, the
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

	// Metadata is the caller's metadata, a JSON object.
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

	// CodecDescription records the codec's configuration as Codec.Describe
	// returns it. Omitted when the codec describes none or no codec is
	// configured.
	CodecDescription string `json:"codec_description,omitempty"`

	// PartitionKeys records the Hive partition keys, in order. Omitted for a
	// layout without partitions.
	PartitionKeys []string `json:"partition_keys,omitempty"`

	// Paths records the names within the dataset's paths. Zero (absent)
	// in a manifest written before format version 1.1.0.
	Paths ManifestPaths `json:"paths,omitzero"`
}

// ManifestPaths records the names within a dataset's paths, so that writers
// using another convention are detected (CONTRACT_CORE §Manifest). Flat
// paths have no datasets or snapshots directory, and those fields are empty.
type ManifestPaths struct {
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
// It embeds its manifest, so snap.Metadata and snap.Files name the
// manifest's fields.
type DatasetSnapshot struct {
	// ID uniquely identifies this snapshot.
	ID DatasetSnapshotID

	// Manifest describes the snapshot's contents.
	*Manifest
}

// -----------------------------------------------------------------------------
// Store interface
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

	// ReadRange reads a byte range from the given path.
	// Returns ErrNotFound if the path does not exist.
	// Returns ErrRangeReadNotSupported if the store does not support range reads.
	ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error)

	// ReaderAt returns an io.ReaderAt for random access reads.
	// Returns ErrNotFound if the path does not exist.
	// Returns ErrRangeReadNotSupported if the store does not support range reads.
	ReaderAt(ctx context.Context, path string) (io.ReaderAt, error)
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

// Factory creates a store when a root opens. FS, Memory, Use, and the S3
// adapter return one; a factory performs no I/O until it is called, and
// Open and OpenReadOnly record its error rather than return it, so a caller
// checks one error at the first dataset it opens. The type parameter
// carries the store's capability: Open requires a Store, which provides
// CompareAndSwap, and OpenReadOnly accepts any ObjectStore.
type Factory[S ObjectStore] func() (S, error)

// -----------------------------------------------------------------------------
// Codec interface
// -----------------------------------------------------------------------------

// Codec serializes records of type T. A dataset's codec fixes its record
// type: Root.Records infers T from it.
//
// Codecs are pluggable and orthogonal to storage, compression, and partitioning.
type Codec[T any] interface {
	// Name returns the codec identifier (for example, "jsonl" or "parquet").
	Name() string

	// Describe returns the codec's configuration in a deterministic,
	// canonical form, so that equal configurations yield equal descriptions;
	// "" for a codec without configuration. A writer whose codec's
	// description differs from the one a dataset's manifests record is
	// rejected with ErrSettingsMismatch.
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

// StatisticalCodec is implemented by codecs, and by stream encoders, that
// report per-file statistics after encoding. FileStats returns the
// statistics of the most recent Encode call, or of the stream after Close;
// nil when none are available.
type StatisticalCodec interface {
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
// Dataset handles
// -----------------------------------------------------------------------------

// SnapshotReader is what every dataset handle reads: its snapshots and the
// stored facts behind them. Per CONTRACT_READ_API.md, Lode's read API exposes
// stored facts, not interpretations.
type SnapshotReader interface {
	// ID returns the dataset's identifier.
	ID() DatasetID

	// Latest returns the head snapshot; ErrNoSnapshots for an empty dataset.
	Latest(ctx context.Context) (*DatasetSnapshot, error)

	// Snapshot retrieves a snapshot by ID.
	Snapshot(ctx context.Context, id DatasetSnapshotID) (*DatasetSnapshot, error)

	// Snapshots lists every committed snapshot, oldest first.
	Snapshots(ctx context.Context) ([]*DatasetSnapshot, error)

	// ListPartitions returns the partition paths that committed manifests
	// name. Returns ErrNotFound if the dataset holds no manifest.
	ListPartitions(ctx context.Context, opts PartitionListOptions) ([]PartitionRef, error)

	// ListManifests returns committed manifests (snapshots), within a
	// partition when one is given. Returns ErrNotFound if the dataset holds
	// no manifest.
	ListManifests(ctx context.Context, partition PartitionPath, opts ManifestListOptions) ([]ManifestRef, error)

	// GetManifest loads and validates the manifest a reference names.
	GetManifest(ctx context.Context, ref ManifestRef) (*Manifest, error)

	// OpenObject returns a reader for a data object. The caller closes it.
	OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error)

	// ReaderAt returns an io.ReaderAt for random access to a data object.
	// Returns ErrRangeReadNotSupported if the store does not support range
	// reads.
	ReaderAt(ctx context.Context, obj ObjectRef) (io.ReaderAt, error)
}

// Retention removes snapshots and reclaims their objects. Only writer
// handles retain: removal is a write. See CONTRACT_RETENTION.md.
type Retention interface {
	// Expire removes the selected snapshots: their manifests and the data
	// objects their manifests list. The head is never expired; when the
	// selection includes it, it is kept and reported in RetainedHead. The
	// report is non-nil, also on error, and describes the work done before
	// the error.
	Expire(ctx context.Context, req ExpireRequest) (*ExpireReport, error)

	// Sweep completes interrupted expiries and reclaims data objects of
	// snapshots that were never committed, once they are older than
	// req.Grace. It never deletes an object a manifest still describes, nor
	// any object outside the layout's data topology. The report is non-nil,
	// also on error.
	Sweep(ctx context.Context, req SweepRequest) (*SweepReport, error)
}

// RecordView reads a dataset of records of type T. ReadRoot.Records returns
// one; Records embeds it.
type RecordView[T any] interface {
	SnapshotReader

	// Read returns every record of the snapshot, decoded with the view's
	// codec and the compressor the snapshot's manifest names.
	Read(ctx context.Context, id DatasetSnapshotID) ([]T, error)
}

// Records writes and reads a dataset of records of type T. Root.Records
// returns one. A Records value is not safe for concurrent use by several
// goroutines; several values, in one process or many, may write the same
// dataset (CONTRACT_WRITE_API §Concurrency).
type Records[T any] interface {
	RecordView[T]
	Retention

	// Write commits records and metadata as an immutable snapshot. metadata
	// is any value that encodes as a JSON object, a struct or a map; nil
	// persists as {}.
	Write(ctx context.Context, records []T, metadata any) (*DatasetSnapshot, error)

	// StreamWrite consumes records from an iterator and streams them
	// through the codec, which must be a StreamingCodec, into one data file.
	// An error the iterator yields aborts the write. A partitioned layout is
	// not supported, because partitioning requires every record's keys
	// before any file is written.
	StreamWrite(ctx context.Context, records iter.Seq2[T, error], metadata any) (*DatasetSnapshot, error)
}

// BlobView reads a dataset whose snapshots each hold one blob.
// ReadRoot.Blobs returns one; Blobs embeds it.
type BlobView interface {
	SnapshotReader

	// Read returns the snapshot's blob.
	Read(ctx context.Context, id DatasetSnapshotID) ([]byte, error)
}

// Blobs writes and reads a dataset whose snapshots each hold one blob.
// Root.Blobs returns one. Concurrency is as for Records.
type Blobs interface {
	BlobView
	Retention

	// Write commits one blob and metadata as an immutable snapshot.
	Write(ctx context.Context, data []byte, metadata any) (*DatasetSnapshot, error)

	// StreamWrite returns a StreamWriter that streams one blob directly to
	// its final object; Commit creates the snapshot.
	StreamWrite(ctx context.Context, metadata any) (StreamWriter, error)
}

// -----------------------------------------------------------------------------
// Retention
// -----------------------------------------------------------------------------

// ExpireRequest selects the snapshots Retention.Expire removes. Set fields
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

	// ErrCodecNotStreamable indicates Records.StreamWrite was called with a
	// codec that is not a StreamingCodec.
	ErrCodecNotStreamable = errCodecNotStreamable{}

	// ErrNilIterator indicates a nil iterator was passed to Records.StreamWrite.
	ErrNilIterator = errNilIterator{}

	// ErrPartitioningNotSupported indicates Records.StreamWrite was called on
	// a partitioned layout.
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
	// used: a nil store factory, an invalid dataset ID or path name, a
	// negative offset, a block outside the volume. The message names the
	// value. Not retry-safe; fix the call.
	ErrInvalidArgument error = validate.ErrInvalidArgument

	// ErrInvalidPath indicates a storage path or range the store refuses:
	// a key that is empty or escapes the root, a partition prefix that is not
	// a canonical relative path, or a ReadRange with negative or overflowing
	// bounds.
	ErrInvalidPath error = errInvalidPath{}

	// ErrSnapshotExpired indicates the snapshot was removed by
	// Retention.Expire. It also matches ErrNotFound.
	ErrSnapshotExpired error = errSnapshotExpired{}

	// ErrHeadUncommitted indicates retention cannot tell the committed head:
	// the latest pointer names a snapshot whose manifest is not written yet
	// (a commit is in flight, or crashed before writing it), or is missing
	// beside existing snapshots. Retry once the commit completes; the next
	// commit replaces a pointer a crashed commit left behind, and any commit
	// creates a missing one.
	ErrHeadUncommitted error = errHeadUncommitted{}

	// ErrSettingsMismatch indicates a writer whose settings (codec and its
	// description, compressor, checksum, layout and partition keys, or path
	// names) differ from those the dataset's head manifest records, or whose
	// Hive keys cannot be verified against an existing dataset. Not
	// retry-safe; configure the writer as the dataset is.
	ErrSettingsMismatch error = errSettingsMismatch{}

	// ErrManifestInvalid indicates a manifest failed validation.
	ErrManifestInvalid error = validate.ErrManifestInvalid

	// ErrReadOnly indicates a write through a store that a read-only root
	// opened: OpenReadOnly accepts an ObjectStore, which has no
	// CompareAndSwap, and its views never write.
	ErrReadOnly error = errReadOnly{}
)

type errNotFound struct{}

func (errNotFound) Error() string { return "not found" }

type errSnapshotExpired struct{}

func (errSnapshotExpired) Error() string { return "snapshot expired" }

// Is makes an expired snapshot also a missing one, so callers matching
// ErrNotFound keep working.
func (errSnapshotExpired) Is(target error) bool { return target == ErrNotFound }

type errSettingsMismatch struct{}

func (errSettingsMismatch) Error() string { return "settings mismatch" }

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
	return "StreamWrite does not support partitioning; use Write for partitioned data"
}

type errSchemaViolation struct{}

func (errSchemaViolation) Error() string { return "parquet: schema violation" }

type errInvalidFormat struct{}

func (errInvalidFormat) Error() string { return "parquet: invalid format" }

type errSnapshotConflict struct{}

func (errSnapshotConflict) Error() string { return "snapshot conflict: another writer committed" }

type errInvalidPath struct{}

func (errInvalidPath) Error() string { return "invalid path" }

type errReadOnly struct{}

func (errReadOnly) Error() string { return "read-only store" }

// -----------------------------------------------------------------------------
// Listing
// -----------------------------------------------------------------------------

// ManifestRef references a committed manifest (snapshot) within a dataset.
type ManifestRef struct {
	// ID is the snapshot identifier.
	ID DatasetSnapshotID

	// Partition is the partition path where this manifest resides.
	// For partition-first layouts (e.g., HiveLayout), this is populated during
	// discovery. The zero value for segment-first layouts (e.g., DefaultLayout).
	Partition PartitionPath
}

// PartitionRef references a partition within a dataset.
type PartitionRef struct {
	// Path is the partition path (e.g., "day=2024-01-01/source=foo").
	Path PartitionPath
}

// PartitionPath is the path of a partition within a dataset, such as
// "day=2024-01-01/source=foo". Only ParsePartitionPath and the listings
// build one, so every value is a canonical relative path. The zero value is
// the dataset root: no partition.
type PartitionPath struct {
	path string
}

// ObjectRef references a data object within a dataset manifest.
type ObjectRef struct {
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
	// The zero value disables the cursor.
	StartAfter PartitionPath
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

// ErrDatasetsNotModeled indicates that the root's paths do not support
// dataset enumeration: flat paths place datasets at the store root, which
// has no common prefix.
var ErrDatasetsNotModeled = errDatasetsNotModeled{}

type errDatasetsNotModeled struct{}

func (errDatasetsNotModeled) Error() string { return "datasets not modeled by this layout" }

# Lode Error Taxonomy — Contract

This document defines the error taxonomy for Lode's read and write APIs.
Errors are categorized by source and purpose so that callers can handle them
precisely.

---

## Goals

1. **Predictable**: callers know which errors to expect from each operation.
2. **Actionable**: each error type indicates whether retrying,
   reconfiguring, or abandoning the operation is appropriate.
3. **Consistent with immutability**: no error implies mutable state or
   background recovery.

---

## Error Categories

### 1. Not Found Errors

These errors indicate that a requested resource does not exist.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrNotFound` | Storage | The object path does not exist |
| `lode.ErrNotFound` | Read API | The dataset or snapshot was not found (no manifests exist) |
| `lode.ErrNoSnapshots` | Dataset, Volume | The Dataset or Volume exists but has no committed snapshots |
| `lode.ErrNoManifests` | Read API | Storage contains objects but no valid manifests |

**Behavior**:
- `ListManifests` returns `ErrNotFound` when the dataset has no committed
  manifests.
- `ListPartitions` returns `ErrNotFound` when the dataset has no committed
  manifests.
- `GetManifest` returns `ErrNotFound` when the manifest path does not exist.
- `Snapshot` returns `ErrNotFound` when the snapshot ID does not exist.
- `ListDatasets` returns `ErrNoManifests` when storage contains objects but
  no valid manifests.

---

### 2. Layout Errors

These errors indicate a semantic incompatibility between the layout and the
requested operation.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrDatasetsNotModeled` | Read API | The layout does not support dataset enumeration |
| `lode.ErrPartitionPrefixNotSupported` | Read API | `PartitionListOptions.Prefix` was specified on a layout without partitions |

**Behavior**:
- `ListDatasets` returns `ErrDatasetsNotModeled` when the layout does not
  support dataset enumeration.
- `ListPartitions` returns `ErrPartitionPrefixNotSupported` when `Prefix` is
  specified and the layout does not model partitions.
- An empty list is returned **only** when storage is actually empty, never
  as a substitute for an unsupported operation.

**Layout-Specific Notes**:
- `DefaultLayout` supports dataset enumeration (prefix: `datasets/`).
- `HiveLayout` supports dataset enumeration (prefix: `datasets/`).
- `FlatLayout` does not support dataset enumeration, because it has no
  common prefix.

---

### 3. Manifest Validation Errors

These errors indicate that a manifest fails structural or semantic
validation.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrManifestInvalid` | Manifest loader | The manifest lacks required fields or contains invalid values |

**Required Manifest Fields** (per CONTRACT_CORE.md):
- `SchemaName`: a non-empty string that identifies the schema
- `FormatVersion`: a non-empty version string
- `DatasetID`: a non-empty dataset identifier
- `SnapshotID`: a non-empty snapshot identifier
- `CreatedAt`: a non-zero timestamp
- `Metadata`: a non-nil map (an empty `{}` is valid)
- `Files`: a non-nil slice (an empty `[]` is valid)
- `RowCount`: a non-negative integer
- `Compressor`: a non-empty compressor name
- `Partitioner`: a non-empty partitioner name

**Optional Fields**:
- `Codec`: may be empty when no codec is configured
- `ParentSnapshotID`: may be empty for the first snapshot
- `MinTimestamp`, `MaxTimestamp`: may be `nil` when not applicable
- `Checksum` in `FileRef`: may be empty
- `Stats` in `FileRef`: may be `nil` (omitted when the codec does not report
  statistics)

**File Validation**:
- Each `FileRef.Path` MUST be non-empty.
- Each `FileRef.SizeBytes` MUST be non-negative.

**Behavior**:
- `GetManifest` returns a wrapped `ManifestValidationError` for an invalid
  manifest.
- `ListManifests` returns an error, rather than omitting the manifest, when
  manifest validation fails.
- `ListPartitions` returns an error, rather than omitting the manifest, when
  manifest validation fails.

---

### 4. Storage Errors

These errors indicate failures in the storage layer.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrPathExists` | Storage | An attempt to write to an existing path (a violation of immutability) |
| `lode.ErrInvalidPath` | Storage, Read API | The key is empty or escapes the root; the partition prefix is not a canonical relative path; or the `ReadRange` bounds are negative or overflow |
| `lode.ErrRangeReadNotSupported` | Read API | The store does not support range reads |

**Behavior**:
- `Put` returns `ErrPathExists` when it detects an existing path (see the
  detection table below).
- `Put` returns `ErrInvalidPath` for a path that is empty or escapes the
  root.
- `Get` returns `ErrInvalidPath` for an invalid path.
- `ReadRange` returns `ErrInvalidPath` for:
  - a negative offset or length,
  - a length that exceeds the capacity of the platform's `int`, or
  - an offset and length whose sum overflows.
- `ReaderAt` returns `ErrRangeReadNotSupported` for a store that cannot read
  ranges.

**ErrPathExists Detection by Put Path** (see CONTRACT_STORAGE.md):

| Put Path | Detection | Guarantee |
|----------|-----------|-----------|
| Atomic (at or below the threshold) | Conditional write | Always detected |
| Multipart (above the threshold) | Conditional completion (if supported) or a preliminary check | Atomic (conditional) or best-effort (preliminary check only) |

When the backend supports conditional completion of multipart uploads (for
example, S3 with `If-None-Match`), both paths detect an existing object
atomically. When only the preliminary check is available, a
time-of-check-to-time-of-use (TOCTOU) interval exists, and single-writer
semantics or external coordination is required. Threshold values are
specific to each adapter; consult the adapter's documentation.

---

### 5. Configuration Errors

These errors indicate a value supplied by the caller that cannot be used.
They are not safe to retry: the call itself must be corrected.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrInvalidArgument` | Constructors | A `nil` store factory, a factory that returned a `nil` store, a dataset ID that is not one path segment (§Dataset IDs), a Hive partition key that is not one path segment or contains `=`, a `nil` layout or compressor, an empty volume ID, a total length that is not positive, an option value out of range (`WithRetry*`), `NewHiveLayout` without keys, or raw blob mode with a partitioning layout |
| `lode.ErrInvalidArgument` | Dataset | `Write` in raw blob mode without exactly one `[]byte`; `StreamWriteRecords` without a codec |
| `lode.ErrInvalidArgument` | Volume | A negative offset, a length that is not positive, an empty block, a block or read outside the address space, a block path not produced by the volume, or a commit without a new block |
| `lode.ErrOptionNotValidForDataset`, `…DatasetReader` | Constructors | An option used with a constructor to which it does not apply (Volume options are a separate type, verified at compile time) |

**Behavior**:
- Every argument check above MUST return an error matching
  `ErrInvalidArgument` under `errors.Is`, and the message MUST name the
  rejected value.
- Argument errors are returned before any store call.

#### Dataset IDs

A dataset ID MUST be one canonical path segment: not empty, `.`, or `..`,
without `/` or a backslash, and not beginning with the reserved prefix
`.lode-tmp-` (CONTRACT_STORAGE §Put). Lode cleans the paths it builds from
an ID, so an ID such as `a/../events` would otherwise address the dataset
`events`. `=` is permitted. Every operation that takes a dataset ID,
reader methods included, rejects an invalid one before any store call.

---

### 6. Codec/Component Mismatch Errors

These errors indicate a mismatch between stored data and the current
configuration.

| Error | Source | Meaning |
|-------|--------|---------|
| Error | Dataset.Read | The snapshot's codec does not match the dataset's codec |
| Error | Dataset.Read | The snapshot's compressor does not match the dataset's compressor |
| `lode.ErrCodecNotStreamable` | Dataset.StreamWriteRecords | The configured codec does not support streaming |
| `lode.ErrSettingsMismatch` | Dataset.Write, Dataset.StreamWrite, Dataset.StreamWriteRecords | The writer's codec, codec description, compressor, checksum, partitioner, partition keys, or path names differ from those the dataset's manifests record; the dataset's directory holds a history persisted under another layout; or the writer's Hive keys cannot be verified against a dataset written before format version 1.1.0 (CONTRACT_WRITE_API §Settings) |

**Behavior**:
- `Read` validates the manifest's components against the dataset's
  configuration before reading.
- A mismatch produces a descriptive error rather than silently corrupt
  output.
- `StreamWriteRecords` returns `ErrCodecNotStreamable` if the codec does not
  implement `StreamingRecordCodec`.
- A writer returns `ErrSettingsMismatch` before it commits, and the error
  names the setting that differs. It is not retried.

---

### 7. Streaming API Errors

These errors indicate invalid use of the streaming write APIs.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrCodecConfigured` | Dataset.StreamWrite | `StreamWrite` was called while a codec is configured |
| `lode.ErrNilIterator` | Dataset.StreamWriteRecords | A `nil` record iterator was supplied |
| `lode.ErrPartitioningNotSupported` | Dataset.StreamWriteRecords | `StreamWriteRecords` was called with a partitioner other than `noop` |

**Behavior**:
- `StreamWrite` accepts raw binary payloads only; it returns
  `ErrCodecConfigured` if a codec is configured.
- Use `StreamWriteRecords` for structured data with streaming codecs.
- Use `Write` for structured data with codecs that do not stream.
- `StreamWriteRecords` returns `ErrNilIterator` when the iterator argument
  is `nil`.
- `StreamWriteRecords` returns `ErrPartitioningNotSupported` when
  partitioning is configured.

**Removal of Partial Objects on Error**:
- On an abort, or on an error before commit, no manifest is written.
- Partial data objects may remain in storage; their removal is best-effort.
- Callers should not rely on the automatic removal of partial objects
  (`Dataset.Sweep` reclaims them; see `CONTRACT_RETENTION.md`).
- A failure to delete a partial object does not create a snapshot.

---

### 8. Parquet Codec Errors

These errors indicate encoding or decoding failures specific to Parquet.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSchemaViolation` | Parquet codec | A record does not conform to the schema |
| `lode.ErrInvalidFormat` | Parquet codec | The Parquet file is malformed or corrupt |

**ErrSchemaViolation Triggers**:
- A required (non-nullable) field is absent from the record.
- A type remains mismatched after coercion is attempted (for example, a
  string for an integer field).
- A non-nullable field has a `nil` value.
- A timestamp string is invalid (not in RFC 3339 format).

**ErrInvalidFormat Triggers**:
- The file is empty.
- The Parquet magic bytes are invalid.
- The footer or metadata is corrupt.
- The file is truncated.

**Behavior**:
- `Encode` returns `ErrSchemaViolation` for record validation failures.
- `Decode` returns `ErrInvalidFormat` for invalid Parquet files.
- Both errors wrap the underlying error when one is available.

See [CONTRACT_PARQUET.md](CONTRACT_PARQUET.md) for complete Parquet codec semantics.

---

### 9. Volume Errors

These errors indicate failures specific to Volume. Volume is experimental
and resides in the package `lode/volume` (`V1_API_DECISIONS.md` D9), so its
two sentinels are declared there; the shared sentinels that it also returns
are those of `lode`.

| Error | Source | Meaning |
|-------|--------|---------|
| `volume.ErrRangeMissing` | Volume.ReadAt | The requested range is not fully committed |
| `volume.ErrOverlappingBlocks` | Volume.Commit | Committed blocks overlap in the cumulative manifest |

**ErrRangeMissing Behavior**:
- `ReadAt` returns `ErrRangeMissing` if any part of the range is
  uncommitted.
- Partial data MUST NOT be returned on a committed read path.

**ErrOverlappingBlocks Behavior**:
- `Commit` validates the complete cumulative set of blocks (existing and new).
- If any blocks overlap, `Commit` returns `ErrOverlappingBlocks`.
- Two blocks overlap when their byte ranges `[offset, offset+length)`
  intersect.

**Other Volume Error Behavior**:
- `Commit` with an empty block list returns an error.
- `Commit` with `nil` metadata returns an error.
- `Volume.Latest` returns `ErrNoSnapshots` when no snapshots exist.
- `Volume.Snapshot` returns `ErrNotFound` when the snapshot ID does not
  exist.
- `volume.New` with a `nil` factory, an empty ID, or a `totalLength` that is
  not positive returns `lode.ErrInvalidArgument`.

See [CONTRACT_VOLUME.md](CONTRACT_VOLUME.md) for the complete Volume
semantics.

---

### 10. Concurrency Errors

These errors indicate a conflict detected during a commit through
optimistic concurrency control.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSnapshotConflict` | Dataset.Write, Dataset.StreamWrite, Dataset.StreamWriteRecords, Volume.Commit | Another writer committed after the parent was resolved; (for `Volume.Commit`) the parent's manifest has not been persisted; or (for a dataset writer) the pointer names a snapshot whose settings no manifest records (CONTRACT_WRITE_API §Settings) |

**ErrSnapshotConflict Behavior**:
- The commit's expected parent snapshot ID does not match the current value
  of the latest pointer, because another writer committed in the interval.
- Data files are immutable and already persisted, so the cost of a retry is
  one manifest write plus one pointer swap.

**Manual Retry Guidance**:
1. Call `Latest()` again to obtain the current head.
2. Merge or reconstruct state against the new head.
3. Commit again.

**Automatic Retry**: `WithRetryCount(n)` performs this procedure
automatically within the commit path. Data files are written once; only the
reassignment of the manifest's parent and the pointer CAS are retried. See
`CONTRACT_WRITE_API.md` §Optimistic Concurrency (CAS).

---

### 11. Retention Errors

These errors are returned by snapshot expiry and by the sweep of
unreferenced objects (`CONTRACT_RETENTION.md`).

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSnapshotExpired` | Dataset.Snapshot, Dataset.Read | The snapshot was expired by `Expire`; the error also matches `ErrNotFound` |
| `lode.ErrHeadUncommitted` | Dataset.Expire, Dataset.Sweep | The head is not yet committed: the latest pointer names a snapshot whose manifest has not been persisted (`Expire`), or the pointer is absent while snapshots exist (both) |
| `lode.ErrInvalidArgument` | Dataset.Expire, Dataset.Sweep | An `ExpireRequest` without a selector field; a `SweepRequest.Grace` that is zero or negative |
| `lode.ErrInvalidPath` | Dataset.Expire | `ExpireRequest.Partition` is not a canonical relative partition path |
| `lode.ErrPartitionPrefixNotSupported` | Dataset.Expire | `ExpireRequest.Partition` was specified on a layout without partitions |

**Behavior**:
- Selecting the head snapshot is not an error: `Expire` retains it and
  reports it in `ExpireReport.RetainedHead`.
- `ErrHeadUncommitted` is retryable: retry once the pending commit
  completes. A pointer left by an abandoned commit is replaced by the next
  commit; `Latest` never replaces an existing pointer (CONTRACT_CORE §Head
  Pointer).
- `Expire` and `Sweep` return a non-nil report with every error, describing
  the work completed before the error. After a retryable failure, `Sweep`,
  or an `Expire` that names the affected IDs, completes the remainder.
  Corruption (an invalid or contradicted tombstone, or an invalid manifest)
  recurs until an operator repairs it (CONTRACT_RETENTION §Accounting).

---

## Error Handling Guidelines

### Retry-Safe Errors
- Storage I/O errors (network failures, timeouts): the caller may retry.
- `ErrNotFound` during a race: the caller may retry if the store is
  eventually consistent.
- `ErrSnapshotConflict`: call `Latest()` again, merge state, and commit
  again.

### Non-Retry Errors
- `ErrDatasetsNotModeled`: reconfigure the reader with a different layout.
- `ManifestValidationError`: the data is corrupt; investigate its source.
- `ErrPathExists`: a logic error in the caller (an attempt to write the same
  path twice). Retention's creation of a tombstone is the exception: there
  the error means that another run recorded the expiry first, which
  retention verifies and accepts (`CONTRACT_RETENTION.md` §Order of
  Operations).
- `ErrOverlappingBlocks`: a logic error in the caller (overlapping byte
  ranges).
- A component mismatch: reconfigure the dataset or read a snapshot with
  matching components.
- `ErrSettingsMismatch`: configure the writer as the dataset is.

### Fatal Errors
- No panic is part of the public contract; invalid configuration and invalid
  use of the API MUST return errors.

---

## Immutability Invariants

Errors MUST NOT imply:
- Mutable state (no semantics of an "update failed").
- Background recovery (no semantics of "retry later").
- Partial success (writes are atomic through the manifest). Retention is
  the single exception: `Expire` and `Sweep` report the work completed
  before an error, and every step they perform is idempotent
  (`CONTRACT_RETENTION.md`).

All operations provide:
- **Idempotent reads**: the same input yields the same output, subject to
  the store's consistency.
- **Atomic writes**: the presence of the manifest is the sole signal that a
  commit is complete.

  Retention (`Expire`, `Sweep`) is not a snapshot commit. Its only writes
  are tombstones, created with create-only semantics (`CONTRACT_RETENTION.md`
  §Order of Operations); every other action it performs is an idempotent
  deletion.

---

## Error Flow Examples

### ListDatasets with FlatLayout
```
ListDatasets(ctx, opts)
  → Layout.SupportsDatasetEnumeration() == false
  → return nil, ErrDatasetsNotModeled
```

### GetManifest with Invalid Schema
```
GetManifest(ctx, dataset, ref)
  → store.Get(manifestPath)
  → json.Decode() succeeds
  → ValidateManifest() fails (missing SchemaName)
  → return nil, ManifestValidationError{Field: "SchemaName", ...}
```

### ListManifests with Corrupt Manifest
```
ListManifests(ctx, dataset, partition, opts)
  → for each manifest path:
      → loadManifest(path)
      → ValidateManifest() fails
      → return nil, fmt.Errorf("failed to load manifest %s: %w", path, err)
```

---

## Design Invariant

> **Errors expose facts about storage state and configuration.
> They never imply mutable operations or deferred recovery.**

# Lode Error Taxonomy Contract

This document defines the error taxonomy for Lode's read and write APIs.
It categorizes errors by source and purpose so that callers can handle them
precisely.

---

## Goals

1. **Predictable**: callers know which errors to expect from each operation.
2. **Actionable**: each error type indicates whether retrying,
   reconfiguring, or abandoning the operation is appropriate.
3. **Faithful to immutability**: no error implies mutable state or
   background recovery.

---

## Error Categories

### 1. Not Found Errors

These errors indicate that a requested resource does not exist.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrNotFound` | Storage | The object path does not exist |
| `lode.ErrNotFound` | Read API | No manifest exists for the dataset or snapshot |
| `lode.ErrNoSnapshots` | Dataset, Volume | The Dataset or Volume exists but has no committed snapshots |
| `lode.ErrNoManifests` | Read API | Storage contains objects but no valid manifests |

#### Behavior
- `ListManifests` returns `ErrNotFound` when the dataset has no committed
  manifests.
- `ListPartitions` returns `ErrNotFound` when the dataset has no committed
  manifests.
- `GetManifest` returns `ErrNotFound` when the manifest path does not exist.
- `Snapshot` returns `ErrNotFound` when the snapshot ID does not exist.
- `Snapshot`, `Read`, and `GetManifest` return `ErrNotFound` for a snapshot
  ID that is not one path segment, because no snapshot has such an ID
  (CONTRACT_STORAGE §Names in Keys).
- `ListDatasets` returns `ErrNoManifests` when storage contains objects but
  no valid manifests.

---

### 2. Layout Errors

These errors indicate a semantic incompatibility between the layout and the
requested operation.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrDatasetsNotModeled` | `ListDatasets` (roots) | Flat paths do not support dataset enumeration |
| `lode.ErrPartitionPrefixNotSupported` | `ListPartitions` | The caller specified `PartitionListOptions.Prefix` on a dataset without partitions |

#### Behavior
- `ListDatasets` returns `ErrDatasetsNotModeled` for a root with flat paths.
- `ListPartitions` returns `ErrPartitionPrefixNotSupported` when the caller
  specifies `Prefix` and the dataset is not partitioned.
- These operations return an empty list **only** when storage is empty,
  never as a substitute for an unsupported operation.

#### Path-Specific Notes
- A root with a datasets directory (`datasets/` by default) supports
  dataset enumeration, for unpartitioned and Hive datasets alike.
- A root with flat paths does not support dataset enumeration, because it
  has no common prefix.

---

### 3. Manifest Validation Errors

These errors indicate that a manifest fails structural or semantic
validation.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrManifestInvalid` | Manifest loader | The manifest lacks required fields or contains invalid values |

#### Required Manifest Fields (CONTRACT_CORE.md)
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

#### Optional Fields
- `Codec`: may be empty when the caller configures no codec
- `ParentSnapshotID`: may be empty for the first snapshot
- `MinTimestamp`, `MaxTimestamp`: may be `nil` when not applicable
- `Checksum` in `FileRef`: may be empty
- `Stats` in `FileRef`: may be `nil` (Lode omits it when the codec does not
  report statistics)

#### File Validation
- Each `FileRef.Path` MUST be non-empty.
- Each `FileRef.SizeBytes` MUST be non-negative.

#### Behavior
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
| `lode.ErrInvalidPath` | Storage, Read API | The key is empty or escapes the root |
| `lode.ErrInvalidPath` | Storage, Read API | The partition prefix, or a string passed to `ParsePartitionPath`, is not a canonical relative path |
| `lode.ErrInvalidPath` | Storage, Read API | The `ReadRange` bounds are negative or overflow |
| `lode.ErrRangeReadNotSupported` | Read API | The store does not implement `RangeReader` |

#### Behavior
- `Put` returns `ErrPathExists` when it detects an existing path (see the
  detection table below).
- `Put` returns `ErrInvalidPath` for a path that is empty or escapes the
  root.
- `Get` returns `ErrInvalidPath` for an invalid path.
- `ReadRange` returns `ErrInvalidPath` for:
  - a negative offset or length,
  - a length that exceeds the capacity of the platform's `int`, or
  - an offset and length whose sum overflows.
- `ReaderAt` on a handle or view returns `ErrRangeReadNotSupported` for a
  store that does not implement `RangeReader`.

#### `ErrPathExists` Detection by `Put` Path (CONTRACT_STORAGE.md)

| Put Path | Detection | Guarantee |
|----------|-----------|-----------|
| Atomic (at or below the threshold) | Conditional write | Always detected |
| Multipart (above the threshold) | Conditional completion (if supported) or a preliminary check | Atomic (conditional) or best-effort (preliminary check only) |

When the backend can complete multipart uploads conditionally (for
example, S3 with `If-None-Match`), both paths detect an existing object
atomically. If only the preliminary check is available, a
time-of-check-to-time-of-use (TOCTOU) interval exists, and callers must use
a single writer or coordinate writes externally. Threshold values are
specific to each adapter, and the adapter's documentation states them.

---

### 5. Configuration Errors

These errors indicate a caller-supplied value that Lode rejects. A retry
repeats the failure, so the caller must correct the call.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrInvalidArgument` | Roots (`Open`, `OpenReadOnly`), and every root method records and returns the error | A `nil` store factory, a factory that returned a `nil` store, or a `nil` root option |
| `lode.ErrInvalidArgument` | Roots (`Open`, `OpenReadOnly`), and every root method records and returns the error | A path name that is invalid, duplicated, or `latest`, or flat paths with a datasets or snapshots directory (CONTRACT_LAYOUT §Path Names) |
| `lode.ErrInvalidArgument` | Opening a handle or view (`Records`, `Blobs`) | A dataset ID that is not one path segment (§Dataset IDs) |
| `lode.ErrInvalidArgument` | Opening a handle or view (`Records`, `Blobs`) | A `nil` codec, option, layout, or compressor |
| `lode.ErrInvalidArgument` | Opening a handle or view (`Records`, `Blobs`) | A Hive layout without keys, or with a key that is not one path segment or contains `=` |
| `lode.ErrInvalidArgument` | Opening a handle or view (`Records`, `Blobs`) | A Hive layout without `By` for records that are not maps, or with a `By` function for another record type |
| `lode.ErrInvalidArgument` | Opening a handle or view (`Records`, `Blobs`) | A blob dataset with a partitioned layout |
| `lode.ErrInvalidArgument` | Opening a handle or view (`Records`, `Blobs`) | A writer's compressor whose extension is not part of one path segment (CONTRACT_STORAGE §Names in Keys) |
| `lode.ErrInvalidArgument` | Opening a handle or view (`Records`, `Blobs`) | An option value out of range (`WithRetry*`) |
| `lode.ErrInvalidArgument` | `Write`, `StreamWrite` | Metadata that does not encode as a JSON object |
| `lode.ErrInvalidArgument` | Volume | A volume ID that is not one path segment (CONTRACT_STORAGE §Names in Keys) |
| `lode.ErrInvalidArgument` | Volume | A negative offset, a length that is not positive, an empty block, a block or read outside the address space, a block path not produced by the volume, or a commit without a new block |

Each option type has one target (`RootOption`, `DatasetOption`,
`ViewOption`, and Volume's own `Option`), so an option used where it does
not apply is a compile error rather than a run-time one.

#### Behavior
- Every argument check above MUST return an error matching
  `ErrInvalidArgument` under `errors.Is`, and the message MUST name the
  rejected value.
- Lode returns argument errors before any store call, and a root returns
  its argument errors before it calls its factory.

#### Dataset IDs

A dataset ID MUST be one canonical path segment: not empty, `.`, or `..`,
without `/` or a backslash, and not beginning with the reserved prefix
`.lode-tmp-` (CONTRACT_STORAGE §Put). Because Lode cleans the paths that it
builds from an ID, an ID such as `a/../events` would, without this rule,
address the dataset `events`. The rule permits `=`. Every operation that takes a dataset ID, the
views included, rejects an invalid one before any store call.
CONTRACT_STORAGE §Names in Keys states the rule for every name that Lode
builds into a key, including volume and snapshot IDs.

---

### 6. Codec and Component Mismatch Errors

These errors indicate a mismatch between stored data and the current
configuration.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSettingsMismatch` | `Read` (handles, views) | The snapshot's codec differs from the handle's, or its codec description differs and its format version is not 1.0.0 |
| `lode.ErrSettingsMismatch` | `Read` (handles, views) | The snapshot's compressor is neither bundled nor passed with `WithCompressor`, or its checksum is neither bundled nor passed with `WithChecksum` |
| `lode.ErrSettingsMismatch` | Opening a view | The head's codec or description, or path names, differ from the view's |
| `lode.ErrCodecNotStreamable` | `Records.StreamWrite` | The codec is not a `StreamingCodec` |
| `lode.ErrSettingsMismatch` | Opening a writer, `Write`, `StreamWrite` | The writer's codec, codec description, compressor, checksum, partitioner, partition keys, or path names differ from those the dataset's manifests record (CONTRACT_WRITE_API §Settings) |
| `lode.ErrSettingsMismatch` | Opening a writer, `Write`, `StreamWrite` | The dataset's directory holds a history persisted under another layout (CONTRACT_WRITE_API §Settings) |
| `lode.ErrSettingsMismatch` | Opening a writer, `Write`, `StreamWrite` | Lode cannot verify the writer's Hive keys against a dataset written before format version 1.1.0 (CONTRACT_WRITE_API §Settings) |

#### Behavior
- `Read` validates each snapshot's codec against the handle's codec before
  reading the snapshot, and reads the snapshot with the compressor that its
  manifest names.
- A mismatch produces a descriptive error that names the setting or
  component, rather than silently corrupt output.
- `Records.StreamWrite` returns `ErrCodecNotStreamable` if the codec is not
  a `StreamingCodec`.
- A writer returns `ErrSettingsMismatch` on opening or before committing,
  and the error names the differing setting. A retry cannot
  succeed until the caller corrects that setting.

---

### 7. Streaming API Errors

These errors indicate invalid use of the streaming write APIs.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrNilIterator` | `Records.StreamWrite` | The caller supplied a `nil` iterator |
| `lode.ErrPartitioningNotSupported` | `Records.StreamWrite` | The caller invoked `Records.StreamWrite` on a partitioned layout |

#### Behavior
- `Blobs.StreamWrite` streams one blob. A blob dataset has no codec, so
  the v0 error for a stream with a codec no longer exists.
- `Records.StreamWrite` writes records with streaming codecs.
- `Records.Write` writes records with codecs that do not stream.
- `Records.StreamWrite` returns `ErrNilIterator` when the iterator is
  `nil`.
- `Records.StreamWrite` returns `ErrPartitioningNotSupported` when the
  layout is partitioned.

#### Partial Objects After an Error
- On an abort, or on an error before commit, Lode writes no manifest.
- Partial data objects may remain in storage, and their removal is
  best-effort.
- Callers should not rely on Lode to remove partial objects
  automatically. `Sweep` reclaims them ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md)).
- A failure to delete a partial object does not create a snapshot.

---

### 8. Parquet Codec Errors

These errors indicate encoding or decoding failures specific to Parquet.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSchemaViolation` | Parquet codec | A record does not conform to the schema |
| `lode.ErrInvalidFormat` | Parquet codec | The Parquet file is malformed or corrupt |

#### `ErrSchemaViolation` Triggers
- A required (non-nullable) field is absent from the record.
- A type remains mismatched after the codec attempts coercion (for example,
  a string for an integer field).
- A non-nullable field has a `nil` value.
- A timestamp string is invalid (not in RFC 3339 format).

#### `ErrInvalidFormat` Triggers
- The file is empty.
- The Parquet magic bytes are invalid.
- The footer or metadata is corrupt.
- The file is truncated.

#### Behavior
- `Encode` returns `ErrSchemaViolation` for record validation failures.
- `Decode` returns `ErrInvalidFormat` for invalid Parquet files.
- Both errors wrap the underlying error when one is available.

See [CONTRACT_PARQUET.md](CONTRACT_PARQUET.md) for complete Parquet codec semantics.

---

### 9. Volume Errors

These errors indicate failures specific to Volume. The package
`lode/volume` defines Volume, so that package declares the two Volume
sentinels.
Volume also
returns the shared sentinels of `lode`.

| Error | Source | Meaning |
|-------|--------|---------|
| `volume.ErrRangeMissing` | Volume.ReadAt | The requested range is not fully committed |
| `volume.ErrOverlappingBlocks` | Volume.Commit | Committed blocks overlap in the cumulative manifest |

#### `ErrRangeMissing` Behavior
- `ReadAt` returns `ErrRangeMissing` if any part of the range is
  uncommitted.
- A read MUST NOT return partial data for a range that is partly
  uncommitted.

#### `ErrOverlappingBlocks` Behavior
- `Commit` validates the complete cumulative set of blocks (existing and new).
- If any blocks overlap, `Commit` returns `ErrOverlappingBlocks`.
- Two blocks overlap when their byte ranges `[offset, offset+length)`
  intersect.

#### Other Volume Error Behavior
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

These errors indicate a conflict that optimistic concurrency control
detects during a commit.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSnapshotConflict` | `Write`, `StreamWrite` (`Records`, `Blobs`), Volume.Commit | Another writer committed after this writer resolved the parent |
| `lode.ErrSnapshotConflict` | `Volume.Commit` | No writer has yet persisted the parent's manifest |
| `lode.ErrSnapshotConflict` | `Write`, `StreamWrite` (`Records`, `Blobs`) | The pointer names a snapshot whose settings no manifest records (CONTRACT_WRITE_API §Settings) |

#### `ErrSnapshotConflict` Behavior
- The commit's expected parent snapshot ID does not match the current value
  of the latest pointer, because another writer committed in the interval.
- Data files are immutable and already persisted, so the cost of a retry is
  one manifest write plus one pointer swap.

#### Manual Retry
1. Call `Latest()` again to obtain the current head.
2. Merge or reconstruct state against the new head.
3. Commit again.

`WithRetry(n)`, or `volume.WithRetryCount(n)` for a Volume, performs this
procedure within the commit path. Lode writes data files once and retries
only two steps: it reassigns the manifest's parent and swaps the pointer
with a compare-and-swap (CAS) operation ([`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md) §Optimistic
Concurrency (CAS)).

---

### 11. Retention Errors

Snapshot expiry and the sweep of unreferenced objects
([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md)) return these errors.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSnapshotExpired` | `Snapshot`, `Read` (handles, views) | `Expire` expired the snapshot, and the error also matches `ErrNotFound` |
| `lode.ErrHeadUncommitted` | `Expire`, `Sweep` | No commit has completed the head: the latest pointer names a snapshot whose manifest the writer has not yet persisted (`Expire` only), or the pointer is absent while snapshots exist (both `Expire` and `Sweep`) |
| `lode.ErrInvalidArgument` | `Expire`, `Sweep` | An `ExpireRequest` without a selector field, or a `SweepRequest.Grace` that is zero or negative |
| `lode.ErrInvalidPath` | `Expire` | `ExpireRequest.Partition` is not a canonical relative partition path |
| `lode.ErrPartitionPrefixNotSupported` | `Expire` | The caller specified `ExpireRequest.Partition` on a layout without partitions |

#### Behavior
- Selecting the head snapshot is not an error: `Expire` retains it and
  reports it in `ExpireReport.RetainedHead`.
- `ErrHeadUncommitted` is retryable: the caller can retry once the pending
  commit completes. `Latest` never replaces an existing pointer, so a
  pointer that an abandoned commit left remains until the next commit
  replaces it (CONTRACT_CORE §Head Pointer).
- With every error, `Expire` and `Sweep` return a non-nil report that
  describes the work completed before the error. After a retryable failure, `Sweep`,
  or an `Expire` that names the affected IDs, completes the remainder.
  Corruption (an invalid or contradicted tombstone, or an invalid manifest)
  recurs until an operator repairs it (CONTRACT_RETENTION §Accounting).

---

## Error Handling Guidelines

### Retry-Safe Errors
- Storage I/O errors (network failures, timeouts): the caller may retry.
- `ErrNotFound` during a race: the caller may retry if the store is
  eventually consistent.
- `ErrSnapshotConflict`: the caller may call `Latest()` again, merge state,
  and commit again.

### Non-Retryable Errors
- `ErrDatasetsNotModeled`: list datasets under a root with a datasets
  directory.
- `ManifestValidationError`: the data is corrupt, so investigate its
  source.
- `ErrPathExists`: a logic error in the caller (an attempt to write the same
  path twice). Tombstone creation in retention is the exception: there the
  error means that another run recorded the expiry first, and retention
  verifies and accepts that tombstone ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md) §Order of
  Operations).
- `ErrOverlappingBlocks`: a logic error in the caller (overlapping byte
  ranges).
- A component mismatch: reconfigure the handle or read a snapshot with
  matching components.
- `ErrSettingsMismatch`: configure the writer with the settings that the
  dataset's manifests record.

### Fatal Errors
- No panic is part of the public contract. Invalid configuration and
  invalid use of the API MUST return errors.

---

## Error Messages

Callers classify errors with `errors.Is` and `errors.As`, never by their
messages. Each message serves human readers and begins with a source prefix
that names the component in which the failure originated.

| Prefix | Component |
|--------|-----------|
| `lode:` | Datasets, Volumes, retention, and every other part of package `lode` and its internal packages that the rows below do not name |
| `fs:` | The filesystem store (`NewFS`, `FS`) |
| `memory:` | The in-memory store (`NewMemory`, `Memory`) |
| `s3:` | The S3 adapter (`lode/s3`) |
| `jsonl:` | The JSON Lines codec |
| `parquet:` | The Parquet codec |

#### Prefix Rules
1. A sentinel MUST NOT carry a prefix. Callers match a sentinel rather than
   read it, and the message that contains it supplies the prefix.
2. An error that originates in Lode MUST begin with the prefix of its
   component. An error originates where Lode creates it, or where Lode first
   wraps a sentinel with detail. An error from code outside Lode, such as
   the operating system, the AWS SDK, an encoding library, or a caller's
   reader, originates where Lode first wraps it.
3. Code that wraps an error returned by another part of Lode MUST add
   context without a prefix. That error already names its source, and only
   the code that creates an error knows its source.

A message therefore names its source once, after the context that each
wrapping function added: `failed to write data file: fs: put: open …: permission
denied`. An error returned by a custom store or codec is the caller's own,
and Lode adds context to it but no prefix.

The test in `internal/errprefix` enforces these rules. It type-checks every
package under `lode/` and `internal/` and fails on any call to `fmt.Errorf`
or `errors.New` that breaks them. The test lists each function that Lode
declares but that stands for code outside Lode, such as the filesystem
store's `openRead`, and gives the reason for exempting it. The test does
not detect an error from outside Lode that Lode returns without wrapping
it.

---

## Immutability Invariants

Errors MUST NOT imply:
- Mutable state (no error reports that an update failed).
- Background recovery (no error asks the caller to retry later).
- Partial success (writes are atomic through the manifest). Retention is
  the single exception: `Expire` and `Sweep` report the work completed
  before an error, and every step they perform is idempotent
  ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md)).

All operations provide:
- **Idempotent reads**: the same input yields the same output, subject to
  the store's consistency.
- **Atomic writes**: a commit is complete as CONTRACT_CORE §Head Pointer
  defines it.
- **Bounded retention writes**: retention (`Expire`, `Sweep`) is not a
  snapshot commit. `Sweep` writes
  copies of a canonical manifest to restore a partition index
  ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md) §Definition, step 3).
  Apart from those copies, retention writes only tombstones, created with
  create-only semantics ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md)
  §Order of Operations). Every other action it performs is an idempotent
  deletion.

---

## Error Flow Examples

### ListDatasets with Flat Paths
```
root.ListDatasets(ctx, opts)
  → the root's names are flat (no datasets directory)
  → return nil, ErrDatasetsNotModeled
```

### `GetManifest` with a Missing Schema Name
```
view.GetManifest(ctx, ref)
  → store.Get(manifestPath)
  → json.Decode() succeeds
  → ValidateManifest() fails (missing SchemaName)
  → return nil, ManifestValidationError{Field: "SchemaName", ...}
```

### ListManifests with a Corrupt Manifest
```
view.ListManifests(ctx, partition, opts)
  → for each manifest path:
      → loadManifest(path)
      → ValidateManifest() fails
      → return nil, fmt.Errorf("failed to load manifest %s: %w", path, err)
```

---

## Design Invariant

> **Errors expose facts about storage state and configuration.
> They never imply mutable operations or deferred recovery.**

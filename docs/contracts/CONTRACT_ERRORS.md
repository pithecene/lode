# Lode Error Taxonomy — Contract

This document defines the error taxonomy for Lode's read and write APIs.
Errors are categorized by source and intent to enable precise error handling.

---

## Goals

1. **Predictable**: Callers know which errors to expect from each operation.
2. **Actionable**: Error types indicate whether retry, reconfiguration, or abort is appropriate.
3. **Immutability-safe**: No error implies mutable state or background recovery.

---

## Error Categories

### 1. Not Found Errors

These indicate a requested resource does not exist.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrNotFound` | Storage | Object path does not exist |
| `lode.ErrNotFound` | Read API | Dataset or snapshot not found (no manifests) |
| `lode.ErrNoSnapshots` | Dataset, Volume | Dataset or Volume exists but has no committed snapshots |
| `lode.ErrNoManifests` | Read API | Storage contains objects but no valid manifests |

**Behavior**:
- `ListManifests` returns `ErrNotFound` when dataset has no committed manifests.
- `ListPartitions` returns `ErrNotFound` when dataset has no committed manifests.
- `GetManifest` returns `ErrNotFound` when manifest path doesn't exist.
- `Snapshot` returns `ErrNotFound` when snapshot ID doesn't exist.
 - `ListDatasets` returns `ErrNoManifests` when storage contains objects but no valid manifests.

---

### 2. Layout Errors

These indicate semantic incompatibility between the layout and the requested operation.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrDatasetsNotModeled` | Read API | Layout doesn't support dataset enumeration |
| `lode.ErrPartitionPrefixNotSupported` | Read API | `PartitionListOptions.Prefix` set on a layout without partitions |

**Behavior**:
- `ListDatasets` returns `ErrDatasetsNotModeled` when `Layout.SupportsDatasetEnumeration()` is false.
- `ListPartitions` returns `ErrPartitionPrefixNotSupported` when `Prefix` is set and the layout does not model partitions.
- An empty list is returned **only** when storage is truly empty (not as a fallback for unsupported operations).

**Layout-Specific Notes**:
- `DefaultLayout`: Supports dataset enumeration (prefix: `datasets/`).
- `HiveLayout`: Supports dataset enumeration (prefix: `datasets/`).
- `FlatLayout`: Does NOT support dataset enumeration (no common prefix).

---

### 3. Manifest Validation Errors

These indicate a manifest fails structural or semantic validation.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrManifestInvalid` | Manifest loader | Manifest missing required fields or has invalid values |

**Required Manifest Fields** (per CONTRACT_CORE.md):
- `SchemaName`: Non-empty string identifying schema
- `FormatVersion`: Non-empty version string
- `DatasetID`: Non-empty dataset identifier
- `SnapshotID`: Non-empty snapshot identifier
- `CreatedAt`: Non-zero timestamp
- `Metadata`: Non-nil map (empty `{}` is valid)
- `Files`: Non-nil slice (empty `[]` is valid)
- `RowCount`: Non-negative integer
- `Compressor`: Non-empty compressor name
- `Partitioner`: Non-empty partitioner name

**Optional Fields**:
- `Codec`: May be empty when no codec configured
- `ParentSnapshotID`: May be empty for first snapshot
- `MinTimestamp`, `MaxTimestamp`: May be nil when not applicable
- `Checksum` in `FileRef`: May be empty
- `Stats` in `FileRef`: May be nil (omitted when codec does not report statistics)

**File Validation**:
- Each `FileRef.Path` must be non-empty
- Each `FileRef.SizeBytes` must be non-negative

**Behavior**:
- `GetManifest` returns wrapped `ManifestValidationError` for invalid manifests.
- `ListManifests` returns error (not skip) when manifest validation fails.
- `ListPartitions` returns error (not skip) when manifest validation fails.

---

### 4. Storage Errors

These indicate storage-level failures.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrPathExists` | Storage | Attempt to write to existing path (immutability violation) |
| `lode.ErrInvalidPath` | Storage, Read API | Key is empty or escapes the root; partition prefix is not a canonical relative path; `ReadRange` bounds are negative or overflow |
| `lode.ErrRangeReadNotSupported` | Read API | Store doesn't support range reads |

**Behavior**:
- `Put` returns `ErrPathExists` when an existing path is detected (see detection table below).
- `Put` returns `ErrInvalidPath` for paths that escape root or are empty.
- `Get` returns `ErrInvalidPath` for invalid paths.
- `ReadRange` returns `ErrInvalidPath` for:
  - negative offset or length
  - length exceeding platform `int` capacity
  - offset+length overflow
- `ReaderAt` returns `ErrRangeReadNotSupported` for stores without range capability.

**ErrPathExists Detection by Put Path** (see CONTRACT_STORAGE.md):

| Put Path | Detection | Guarantee |
|----------|-----------|-----------|
| Atomic (≤ threshold) | Conditional write | Always detected |
| Multipart (> threshold) | Conditional completion (if supported) or preflight | Atomic (conditional) or best-effort (preflight only) |

When the backend supports conditional multipart completion (e.g., S3 `If-None-Match`),
both paths provide atomic no-overwrite detection. When only preflight is available,
a TOCTOU window exists and single-writer semantics or external coordination is required.
Threshold values are adapter-specific; consult adapter documentation.

---

### 5. Configuration Errors

These indicate a caller-supplied value that cannot be used. They are not
retry-safe: the call must change.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrInvalidArgument` | Constructors | Nil store factory, factory returned nil store, nil layout or compressor, empty volume ID, non-positive total length, option value out of range (`WithRetry*`), `NewHiveLayout` without keys, raw blob mode with a partitioning layout |
| `lode.ErrInvalidArgument` | Dataset | `Write` in raw blob mode without exactly one `[]byte`; `StreamWriteRecords` without a codec |
| `lode.ErrInvalidArgument` | Volume | Negative offset, non-positive length, empty block, block or read outside the address space, block path not produced by the volume, commit with no new block |
| `lode.ErrOptionNotValidForDataset`, `…DatasetReader`, `…Volume` | Constructors | Option used with a constructor it does not apply to |

**Behavior**:
- Every argument check above MUST return an error matching
  `ErrInvalidArgument` under `errors.Is`, and the message MUST name the
  rejected value.
- Argument errors are returned before any store call is made.

---

### 6. Codec/Component Mismatch Errors

These indicate a mismatch between stored data and current configuration.

| Error | Source | Meaning |
|-------|--------|---------|
| Error | Dataset.Read | Snapshot codec doesn't match dataset codec |
| Error | Dataset.Read | Snapshot compressor doesn't match dataset compressor |
| `lode.ErrCodecNotStreamable` | Dataset.StreamWriteRecords | Configured codec does not support streaming |

**Behavior**:
- `Read` validates manifest components against dataset config before reading.
- Mismatch returns descriptive error (not silent corruption).
- `StreamWriteRecords` returns `ErrCodecNotStreamable` if codec doesn't implement `StreamingRecordCodec`.

---

### 7. Streaming API Errors

These indicate invalid use of streaming write APIs.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrCodecConfigured` | Dataset.StreamWrite | StreamWrite called with a codec configured |
| `lode.ErrNilIterator` | Dataset.StreamWriteRecords | Nil record iterator passed |
| `lode.ErrPartitioningNotSupported` | Dataset.StreamWriteRecords | StreamWriteRecords called with non-noop partitioner |

**Behavior**:
- `StreamWrite` is for raw binary payloads only; it returns `ErrCodecConfigured` if a codec is set.
- Use `StreamWriteRecords` for structured data with streaming codecs.
- Use `Write` for structured data with non-streaming codecs.
- `StreamWriteRecords` returns `ErrNilIterator` when the iterator argument is nil.
- `StreamWriteRecords` returns `ErrPartitioningNotSupported` when partitioning is configured.

**Cleanup on Error**:
- On abort or error before commit, no manifest is written.
- Partial data objects may remain in storage; cleanup is best-effort.
- Callers should not rely on automatic cleanup of partial objects.
- Failure to delete a partial object does not create a snapshot.

---

### 8. Parquet Codec Errors

These indicate Parquet-specific encoding or decoding failures.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSchemaViolation` | Parquet codec | Record does not conform to schema |
| `lode.ErrInvalidFormat` | Parquet codec | Parquet file is malformed or corrupted |

**ErrSchemaViolation Triggers**:
- Missing required (non-nullable) field in record
- Type mismatch after coercion attempts (e.g., string for int field)
- Nil value for non-nullable field
- Invalid timestamp string (not RFC3339 format)

**ErrInvalidFormat Triggers**:
- Empty file
- Invalid Parquet magic bytes
- Corrupted footer or metadata
- Truncated file

**Behavior**:
- `Encode` returns `ErrSchemaViolation` for record validation failures.
- `Decode` returns `ErrInvalidFormat` for invalid Parquet files.
- Both errors wrap underlying errors when available.

See [CONTRACT_PARQUET.md](CONTRACT_PARQUET.md) for complete Parquet codec semantics.

---

### 9. Volume Errors

These indicate Volume-specific failures.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrRangeMissing` | Volume.ReadAt | Requested range is not fully committed |
| `lode.ErrOverlappingBlocks` | Volume.Commit | Committed blocks overlap in the cumulative manifest |

**ErrRangeMissing Behavior**:
- `ReadAt` returns `ErrRangeMissing` if any sub-range is uncommitted.
- Partial data MUST NOT be returned for committed read paths.

**ErrOverlappingBlocks Behavior**:
- `Commit` validates the full cumulative block set (existing + new blocks).
- If any blocks overlap, `Commit` returns `ErrOverlappingBlocks`.
- Overlap is defined as two blocks whose byte ranges `[offset, offset+length)`
  intersect.

**Other Volume Error Behavior**:
- `Commit` with empty block list returns an error.
- `Commit` with nil metadata returns an error.
- `Volume.Latest` returns `ErrNoSnapshots` when no snapshots exist.
- `Volume.Snapshot` returns `ErrNotFound` when snapshot ID doesn't exist.
- `NewVolume` with nil factory, empty ID, or non-positive totalLength returns an error.

See [CONTRACT_VOLUME.md](CONTRACT_VOLUME.md) for full Volume semantics.

---

### 10. Concurrency Errors

These indicate a conflict detected during commit via optimistic concurrency.

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSnapshotConflict` | Dataset.Write, Dataset.StreamWrite, Dataset.StreamWriteRecords, Volume.Commit | Another writer committed since parent was resolved |

**ErrSnapshotConflict Behavior**:
- Returned only when the store implements `ConditionalWriter`.
- The commit's expected parent snapshot ID does not match the current
  latest pointer value — another writer committed in between.
- Data files are immutable and already persisted; retry cost is one
  manifest write plus one pointer swap.

**Manual Retry Guidance**:
1. Re-read `Latest()` to get the current head.
2. Merge or rebuild state against the new head.
3. Re-commit.

**Automatic Retry**: `WithRetryCount(n)` automates this loop within the
commit path. Data files are written once; only the manifest re-parent
and pointer CAS are retried. See `CONTRACT_WRITE_API.md` §Automatic retry.

When the store does not implement `ConditionalWriter`, this error is
never returned; callers must ensure single-writer semantics as before.

---

### 11. Retention Errors

These are returned by snapshot expiry and the unreferenced-object sweep
(`CONTRACT_RETENTION.md`).

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSnapshotExpired` | Dataset.Snapshot, Dataset.Read | The snapshot was expired by `Expire`; also matches `ErrNotFound` |
| `lode.ErrHeadUncommitted` | Dataset.Expire | The latest pointer names a snapshot whose manifest is not yet written (a commit in flight) |
| `lode.ErrInvalidArgument` | Dataset.Expire, Dataset.Sweep | `ExpireRequest` with no selector field set; `SweepRequest.Grace` zero or negative |
| `lode.ErrInvalidPath` | Dataset.Expire | `ExpireRequest.Partition` is not a canonical relative partition path |
| `lode.ErrPartitionPrefixNotSupported` | Dataset.Expire | `ExpireRequest.Partition` set on a layout without partitions |

**Behavior**:
- Selecting the head snapshot is not an error: `Expire` keeps it and reports
  it in `ExpireReport.RetainedHead`.
- `ErrHeadUncommitted` is retryable: retry once the in-flight commit
  completes. A pointer left behind by a crashed commit is repaired by the
  next `Latest` or commit.
- `Expire` and `Sweep` return a non-nil report with every error, describing
  the work completed before it. The next `Expire` or `Sweep` completes the
  rest.

---

## Error Handling Guidelines

### Retry-Safe Errors
- Storage I/O errors (network, timeout) — may retry.
- `ErrNotFound` during race — may retry if expecting eventual consistency.
- `ErrSnapshotConflict` — re-read `Latest()`, merge state, re-commit.

### Non-Retry Errors
- `ErrDatasetsNotModeled` — reconfigure with different layout.
- `ManifestValidationError` — data corruption, investigate source.
- `ErrPathExists` — logic error in caller (double-write attempt).
- `ErrOverlappingBlocks` — logic error in caller (overlapping byte ranges).
- Component mismatch — reconfigure dataset or use matching snapshot.

### Fatal Errors
- No fatal panics are part of the public contract; invalid configuration and
  invalid API usage MUST return errors.

---

## Immutability Invariants

Errors MUST NOT imply:
- Mutable state (no "update failed" semantics).
- Background recovery (no "will retry later" semantics).
- Partial success (writes are atomic via manifest). Retention is the one
  exception: `Expire` and `Sweep` report the work completed before an error,
  and every step they take is idempotent (`CONTRACT_RETENTION.md`).

All operations are:
- **Idempotent reads**: Same input, same output (modulo storage consistency).
- **Atomic writes**: Manifest presence is the sole commit signal. Retention
  (`Expire`, `Sweep`) is not a write: it deletes in idempotent steps and
  records intent in a tombstone, as above.

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

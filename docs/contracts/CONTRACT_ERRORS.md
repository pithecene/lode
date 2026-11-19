# Lode Error Taxonomy Contract

This document defines the error taxonomy for Lode's read, write, retention,
storage, and volume APIs. It categorizes sentinel errors by source and
purpose so callers can handle them deterministically with `errors.Is` and
`errors.As`.

---

## Goals

1. **Predictable**: callers know which sentinel errors each entry point
   returns.
2. **Actionable**: each sentinel distinguishes retryable transient or
   concurrency states from permanent configuration or integrity faults.
3. **Faithful to immutability**: no error implies hidden mutation or
   background recovery.

---

## Error Categories

### 1. Not Found and Empty History Errors

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrNotFound` | Storage, views, handles, `volume` | Requested object, snapshot ID, or dataset manifest does not exist |
| `lode.ErrNoSnapshots` | Writer handles, `volume` | Dataset or volume has no committed snapshots (`Latest`, `volume.Head`) |
| `lode.ErrSnapshotExpired` | `Snapshot`, `Read`, `WithAnchor` | Snapshot was removed by `Expire` (also matches `lode.ErrNotFound`) |

#### Behavior
- Opening a view (`ReadRoot.Records`, `ReadRoot.Blobs`), `Manifests`, or
  unfiltered `ListPartitions` on a dataset without manifests (and not fenced
  with `lode:removed`) returns `ErrNotFound`.
- `ListDatasets` on an empty store (or a store holding only uncommitted or
  removed datasets) returns an empty slice `[]DatasetID{}`, not an error.
- `Snapshot` and `Read` (and `volume.Reader.Snapshot`, `volume.Reader.ReadAt`)
  return `ErrNotFound` when the snapshot ID does not exist or is not a single
  canonical path segment.

---

### 2. Layout and Partitioning Errors

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrDatasetsNotModeled` | `Root.ListDatasets`, `ReadRoot.ListDatasets` | Root was opened with `WithFlatPaths()`, which has no common `datasets/` directory |
| `lode.ErrNotPartitioned` | `ListPartitions`, `Expire` | Non-empty `PartitionListOptions.Prefix` or `ExpireRequest.Partition` supplied on an unpartitioned dataset |
| `lode.ErrPartitioningNotSupported` | `Records.StreamWrite` | Single-pass streaming attempted on a partitioned dataset |

---

### 3. Manifest and Checksum Integrity Errors

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrManifestInvalid` | Manifest loader (`Latest`, `Snapshot`, `Snapshots`, `Read`, `Manifests`, `Expire`, `Sweep`, `volume`) | Manifest lacks required fields, contains invalid values, or references paths outside its snapshot or container |
| `lode.ErrChecksumMismatch` | `Read` on dataset handles and views | Object's stored bytes did not match `ObjectRef.Checksum`, or `Checksum` was empty when `Manifest.ChecksumAlgorithm` was set |

#### Required Dataset Manifest Fields ([`CONTRACT_CORE.md`](CONTRACT_CORE.md))
- Non-empty `SchemaName`, `FormatVersion`, `DatasetID`, `SnapshotID`,
  `Compressor`, and `Partitioner`
- Non-zero `CreatedAt`, valid JSON-object `Metadata`, non-nil `Objects` slice
  (legacy `"files"` accepted when unmarshaling pre-`1.2.0` manifests), and
  non-negative `RowCount`
- Every `ObjectRef` has non-empty `Path` and non-negative `SizeBytes`, and on
  `Read` every `ObjectRef.Path` lies inside the snapshot's data directory in
  the dataset
- Format `1.1.0` manifests carry a non-empty `paths` object (without
  `datasets` or `snapshots` when `flat` is `true`)

---

### 4. Storage and Capability Errors

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrPathExists` | Storage (`Put`) | Attempt to write to an already-populated immutable key |
| `lode.ErrInvalidPath` | Storage, `ParsePartitionPath`, `ListPartitions`, `Expire` | Empty, rooted, or escaping path/prefix, non-canonical partition path, or invalid `ReadRange` bounds |
| `lode.ErrRangeReadNotSupported` | `ReaderAt` on dataset handles and views | Underlying `Store` does not implement `RangeReader` |
| `lode.ErrReadOnly` | `ReadRoot` store wrapper | `Put`, `Delete`, or `CompareAndSwap` called through a read-only root's store |

---

### 5. Configuration and Argument Errors

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrInvalidArgument` | `Open`, `OpenReadOnly`, `FS`, `NewFS`, `Prefixed`, `s3.New`, `s3.Bucket` | `nil` factory, `nil` store, `nil` option, missing S3 client/bucket, or invalid `Prefixed` prefix |
| `lode.ErrInvalidArgument` | `Records`, `Blobs`, `RemoveDataset`, `WithAnchor` | Non-segment `DatasetID` or `WithAnchor` `SnapshotID`; `nil` codec, option, layout, compressor, or typed-nil checksum; empty codec name; partitioned `Blobs`; invalid Hive keys or `By` binding; invalid compressor extension; out-of-range retry option |
| `lode.ErrInvalidArgument` | `Write`, `StreamWrite`, `NewMetadata`, `volume.Writer.Commit` | `nil` `io.Reader` (`Blobs.StreamWrite`), `nil` iterator (`Records.StreamWrite`), or metadata that does not encode as a JSON object |
| `lode.ErrInvalidArgument` | `Expire`, `Sweep` | `ExpireRequest` with all selectors unset, or `SweepRequest.Grace <= 0` |
| `lode.ErrInvalidArgument` | `volume.New`, `volume.Remove`, `volume.Head`, `StageWriteAt`, `Commit`, `ReadAt` | Non-segment `volume.ID`, non-positive `totalLength`, `nil` option/factory/checksum, negative offset, non-positive length, empty or out-of-bounds block, foreign block path, or commit with zero new blocks |

Argument validation runs before any store I/O and wraps `ErrInvalidArgument`
with a message naming the rejected argument.

---

### 6. Settings, Codec, and Parquet Errors

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSettingsMismatch` | Opening handles/views, `Write`, `StreamWrite`, `Read` | Codec name, `Codec.Describe()`, compressor, checksum algorithm, partitioner, Hive keys, `WithFlatPaths()`, or legacy `1.1.0` custom path names differ from the dataset's manifests (or unbundled compressor/checksum omitted on a view) |
| `lode.ErrCodecNotStreamable` | `Records.StreamWrite` | Codec does not implement `StreamingCodec[T]` |
| `lode.ErrSchemaViolation` | `Parquet[T]`, `Records.Write` | Invalid `ParquetSchema`, unsupported or incompatible Go record type `T`, missing required field, type mismatch, or numeric overflow |
| `lode.ErrInvalidFormat` | `Parquet[T].Decode`, `Read` | Empty, truncated, corrupt, or physical-schema-mismatched Parquet file |

---

### 7. Concurrency, Retention, Removal, and Volume Errors

| Error | Source | Meaning |
|-------|--------|---------|
| `lode.ErrSnapshotConflict` | `Write`, `StreamWrite`, `RemoveDataset`, `volume.Writer.Commit`, `volume.Remove` | Another writer advanced `latest` after parent resolution (retryable via `Latest()` or `WithRetry`) |
| `lode.ErrHeadUncommitted` | `Expire`, `Sweep`, `RemoveDataset`, `volume.Remove`, `volume.Head` | `latest` names a pending snapshot without a committed manifest, or `latest` is absent while snapshots/tombstones exist |
| `lode.ErrRemoved` (`volume.ErrRemoved`) | Dataset handles/views, `RemoveDataset`, `volume` | Dataset or volume was permanently removed behind the `lode:removed` fence |
| `volume.ErrRangeMissing` | `volume.Reader.ReadAt`, `SnapshotReaderAt.ReadAt` | Requested byte range is not completely covered by committed blocks |
| `volume.ErrOverlappingBlocks` | `volume.Writer.Commit` | Committed blocks overlap in the cumulative manifest |

---

## Retryability Classification

- **Retry-safe**: transient storage/network errors, `ErrSnapshotConflict`
  (refresh via `Latest()` or configure `WithRetry`), and `ErrHeadUncommitted`
  once the pending commit finishes (or after one commit creates a missing
  pointer on a pre-pointer dataset).
- **Non-retryable**: `ErrInvalidArgument`, `ErrInvalidPath`, `ErrPathExists`,
  `ErrDatasetsNotModeled`, `ErrNotPartitioned`,
  `ErrPartitioningNotSupported`, `ErrCodecNotStreamable`,
  `ErrSettingsMismatch`, `ErrSchemaViolation`, `ErrInvalidFormat`,
  `ErrManifestInvalid`, `ErrChecksumMismatch`, `ErrRemoved`,
  `ErrSnapshotExpired`, and `volume.ErrOverlappingBlocks`.
- **No panics**: invalid inputs or corrupt storage states return errors rather
  than panicking.

---

## Error Messages and Source Prefixes

Callers match errors with `errors.Is` and `errors.As`, never by string
matching. Every error message originating in Lode carries the prefix of its
originating component exactly once:

| Prefix | Component |
|--------|-----------|
| `lode:` | Datasets, volumes, retention, validation, and shared internals |
| `fs:` | Filesystem store (`FS`, `NewFS`) |
| `memory:` | In-memory store (`Memory`, `NewMemory`) |
| `s3:` | S3 store (`lode/s3`) |
| `jsonl:` | JSON Lines codec (`JSONL[T]`) |
| `parquet:` | Apache Parquet codec (`Parquet[T]`) |

Prefix rules (enforced statically by `internal/errprefix`):
1. Sentinel values themselves carry **no** prefix.
2. Errors created in Lode (or first wrapping an external error or bare
   sentinel) include their component prefix.
3. Functions wrapping an internal Lode error add context without repeating a
   component prefix.

---

## Error Flow Examples

### `ListDatasets` with `WithFlatPaths()`
```
root.ListDatasets(ctx, opts)
  → root has flat == true
  → return nil, ErrDatasetsNotModeled
```

### `Read` with Checksum Corruption
```
view.Read(ctx, id)
  → load and validate manifest (ChecksumAlgorithm == "sha256")
  → stream object through TeeReader(store.Get, sha256)
  → digest != obj.Checksum
  → return nil, err (matches ErrChecksumMismatch)
```

### `Manifests` with a Corrupt Manifest
```
for m, err := range view.Manifests(ctx, partition, opts)
  → store.List(prefix)
  → store.Get(manifestPath)
  → validateManifest() fails
  → yield LoadedManifest{}, err (matches ErrManifestInvalid), then stop
```

---

## Design Invariant

> **Errors expose facts about storage state and configuration.
> They never imply mutable operations or background recovery.**

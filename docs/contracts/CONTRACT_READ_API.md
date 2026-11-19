# Lode Read API Contract

This document defines the **Lode Read API**: its goals, non-goals, adapter
contracts, view and listing semantics, and complexity bounds. It binds every
storage adapter that serves Lode's data and every consumer that reads it.

---

## Goals

1. **Backend independence**: uniform behavior across S3-compatible object
   stores, local filesystems, and in-memory stores.
2. **Manifest-driven planning**: callers inspect self-describing manifests
   and statistics before reading data objects.
3. **Range reads**: adapters that implement `RangeReader` serve genuine byte
   ranges without reading whole objects.
4. **Shallow discovery**: prefix and partition listings minimize object
   reads.
5. **Stable semantics**: the API exposes stored facts, not interpretations.

---

## Non-Goals

- An SQL execution engine
- A join planner or optimizer
- Arbitrary predicate filtering beyond partition pruning and manifest
  statistics
- Mutable state or background services

---

## Storage Adapter Interface

Reads use the `ObjectStore` operations, optional `PrefixLister` shallow
prefix enumeration, and optional `RangeReader` random access (see
[`CONTRACT_STORAGE.md`](CONTRACT_STORAGE.md)):

```go
type Store interface {
    ObjectStore
    ConditionalWriter
}

type ObjectStore interface {
    Put(ctx context.Context, path string, r io.Reader) error
    Get(ctx context.Context, path string) (io.ReadCloser, error)
    Exists(ctx context.Context, path string) (bool, error)
    List(ctx context.Context, prefix string) ([]string, error)
    Delete(ctx context.Context, path string) error
}

type PrefixLister interface {
    ListPrefixes(ctx context.Context, prefix string) ([]string, error)
}

type ConditionalWriter interface {
    CompareAndSwap(ctx context.Context, path, expected, replacement string) error
}

type RangeReader interface {
    ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error)
    ReaderAt(ctx context.Context, path string) (io.ReaderAt, error)
}

type RangeStore interface {
    Store
    RangeReader
}
```

### Adapter Obligations (Read Path)

- An adapter that cannot serve genuine byte ranges MUST NOT implement
  `RangeReader`.
- `RangeReader.ReaderAt` MUST be safe for concurrent reads at distinct
  offsets.
- Store wrappers (`Prefixed`, `SingleProcess`, and the read-only wrapper used
  by `OpenReadOnly`) MUST preserve `RangeReader` and `PrefixLister` whenever
  the wrapped store implements them.

### Raw Object Access vs Decoded Snapshot Reads

Every concrete view (`*RecordView[T]`, `*BlobView`) and writer handle
(`*Records[T]`, `*Blobs`) provides two distinct read levels:

1. **Decoded snapshot reads (`Read(ctx, id)`)**: load and validate snapshot
   `id`'s manifest, verify that every `ObjectRef.Path` belongs to snapshot
   `id` of this dataset (`ErrManifestInvalid` otherwise), verify the stored
   bytes against `ObjectRef.Checksum` when `ChecksumAlgorithm` is recorded
   (`ErrChecksumMismatch` on mismatch or missing digest), decompress with the
   snapshot's compressor, and decode records (`[]T`) or return raw blob bytes
   (`[]byte`).
2. **Raw object reads (`OpenObject(ctx, obj)` and `ReaderAt(ctx, obj)`)**:
   verify that `obj.Path` is a canonical data object path of this dataset
   (`ErrInvalidArgument` otherwise) and return the **raw stored bytes** as
   persisted in storage—compressed when the snapshot used compression, and
   without automatic decompression or checksum verification. `ReaderAt`
   returns `ErrRangeReadNotSupported` when the store does not implement
   `RangeReader`.

---

## Views and Dataset Read Surface

`OpenReadOnly(factory, opts...)` opens a `*ReadRoot` over any `ObjectStore`.
`ReadRoot.Records` returns a `*RecordView[T]`, and `ReadRoot.Blobs` returns a
`*BlobView`. Writer handles (`*Records[T]`, `*Blobs`) expose the identical
read and listing methods directly on the concrete types:

```text
// On *Root and *ReadRoot:
ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error)

// On *RecordView[T], *Records[T], *BlobView, and *Blobs:
ID() DatasetID
Latest(ctx context.Context) (*Snapshot, error)
Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error)
Snapshots(ctx context.Context) ([]*Snapshot, error)
ListPartitions(ctx context.Context, opts PartitionListOptions) ([]PartitionPath, error)
Manifests(ctx context.Context, partition PartitionPath, opts ManifestListOptions) iter.Seq2[LoadedManifest, error]
OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error)
ReaderAt(ctx context.Context, obj ObjectRef) (io.ReaderAt, error)

// Typed payload read:
(*RecordView[T]).Read(ctx context.Context, id SnapshotID) ([]T, error)
(*Records[T]).Read(ctx context.Context, id SnapshotID) ([]T, error)
(*BlobView).Read(ctx context.Context, id SnapshotID) ([]byte, error)
(*Blobs).Read(ctx context.Context, id SnapshotID) ([]byte, error)
```

- A codec's `Decode` MUST return every record that its `Encode` or
  `StreamEncoder` wrote, without imposing tighter size limits on decode than
  on encode.

### Opening a View

- Opening a view MUST never write to storage. Every write method on a
  `ReadRoot` store wrapper MUST return `ErrReadOnly`.
- Without `WithAnchor`, opening a view resolves the dataset's head
  ([`CONTRACT_CORE.md`](CONTRACT_CORE.md) §Head Pointer). Without
  `WithLayout`, it probes the head's canonical manifest under `snapshots/`
  and then `segments/` (falling back to scanning when the pointer is absent
  or pending) and adopts the dataset's layout from the opened manifest.
- With `WithLayout(l)`, the view probes and scans only the manifest directory
  of `l`, returning `ErrSettingsMismatch` if the opened manifest records a
  different partitioner or Hive key list, if the head's canonical manifest
  exists in the excluded directory, or if a foreign manifest exists under the
  dataset prefix. For format `1.0.0` Hive manifests, the view verifies
  partition keys along the parent chain as a writer does
  ([`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md) §Settings).
- Opening a view on a dataset with no committed snapshots MUST return
  `ErrNotFound`. Opening a view on a dataset fenced with `lode:removed` MUST
  return `ErrRemoved`.
- `WithAnchor(id)` opens the view at `SnapshotID` `id` in place of the head:
  - It MUST reject with `ErrInvalidArgument` any `id` that is not a single
    canonical path segment before any store call.
  - It MUST NOT read `latest` or the head manifest unless `id` is absent (to
    check whether the dataset was removed via `lode:removed`, returning
    `ErrRemoved` when fenced).
  - When `id` has expired, opening with `WithAnchor(id)` MUST return
    `ErrSnapshotExpired`; when `id` does not exist, it MUST return
    `ErrNotFound`.
  - It validates codec and layout settings against `id`'s manifest and
    **retains** that immutable `*Snapshot` on the view so a subsequent
    `Snapshot(ctx, id)` or `Read(ctx, id)` reuses the validated manifest with
    zero additional manifest `Get` calls. All other methods (`Latest`,
    `Snapshots`, `ListPartitions`, `Manifests`, and `Snapshot`/`Read` for
    other IDs) consult live storage normally.
- Opening a view validates the anchor or head manifest (`ErrManifestInvalid`
  on failure) and verifies the codec name, `Codec.Describe()` (for format
  `1.1.0` and `1.2.0`), and `WithFlatPaths()` (`ErrSettingsMismatch` on
  mismatch, including custom `1.1.0` directory names).
- On `Read`, the view reconstructs bundled compressors (`noop`, `gzip`,
  `zstd`) and bundled checksums (`sha256`, `crc32c`, `md5`) by name, or
  matches custom implementations passed via `WithCompressor` and
  `WithChecksum` (`ErrSettingsMismatch` when an unbundled name is not
  supplied).

---

## Dataset, Partition, and Manifest Listing

### `ListDatasets`

- On a root opened with `WithFlatPaths()`, `ListDatasets` MUST return
  `ErrDatasetsNotModeled`.
- When the store implements `PrefixLister`, `ListDatasets` MUST enumerate
  immediate child prefixes under `datasets/` via `ListPrefixes` and check
  each candidate dataset for a committed manifest (reading `datasets/<id>/latest`
  and checking canonical manifest existence, falling back to a per-dataset
  `List` only when pointerless or pending).
- Uncommitted datasets (data or staged objects without any committed
  manifest) and removed datasets (`latest` holding `lode:removed`) MUST be
  omitted.
- When no committed datasets exist, `ListDatasets` MUST return an empty
  non-nil slice (`[]DatasetID{}`), without error.
- Results MUST be sorted lexicographically by `DatasetID` before applying
  `StartAfter` and `Limit`.

### `ListPartitions`

- `ListPartitions` returns `[]PartitionPath` sorted lexicographically by
  `PartitionPath.String()` before applying `StartAfter` and `Limit`.
- On Hive layouts, `ListPartitions` MUST extract partition paths directly
  from listed partition-manifest keys (`0` `Get` calls when every committed
  snapshot has its partition manifests). Only canonical-only snapshots (whose
  canonical manifest is listed without any partition manifest for that
  snapshot ID) load their canonical manifest to extract partitions from
  `Manifest.Objects`.
- On unpartitioned layouts, `ListPartitions` with an empty `Prefix` returns
  an empty slice once at least one manifest exists (or `ErrNotFound` when no
  manifests exist, and `ErrRemoved` when fenced); with a non-empty `Prefix`,
  it MUST return `ErrNotPartitioned`.
- `PartitionListOptions.Prefix` is a raw string prefix over canonical
  relative partition paths (`day=2026-10` matches `day=2026-10-18`, and a
  single trailing `/` is permitted). Any prefix with a leading `/`, a `\`, or
  an empty, `.`, or `..` segment MUST be rejected with `ErrInvalidPath`
  before any store call. An unmatched valid prefix returns an empty slice
  without error.

### `Manifests`

- `Manifests(ctx, partition, opts)` returns an `iter.Seq2[LoadedManifest,
  error]` ordered lexicographically by `(Partition, ID)`.
- When a Hive snapshot has multiple partition manifests in the listed scope,
  `Manifests` deduplicates by `SnapshotID`, assigning the snapshot to its
  lexicographically smallest listed partition path; canonical-only Hive
  snapshots appear under the empty `PartitionPath{}`.
- `Manifests` MUST load and validate each manifest lazily as the caller
  advances the iterator, and MUST NOT issue `Get` calls for manifests
  preceding `opts.StartAfter` or beyond `opts.Limit`.
- If a listed manifest disappears before `Get` (`ErrNotFound` due to
  concurrent expiry), `Manifests` skips it without counting it toward
  `Limit`. Any other load or validation error (`ErrManifestInvalid`) is
  yielded once and stops iteration.
- When the dataset holds no manifests in an unfiltered listing, `Manifests`
  yields `ErrRemoved` if fenced with `lode:removed`, or `ErrNotFound`
  otherwise, and stops.

### Cursors vs Commit Order

`StartAfter` resumes strictly after the given cursor in lexicographic
listing order (`DatasetID`, `PartitionPath`, or `(Partition, ID)`). Because
snapshot IDs encode when a write *began* rather than when it *committed*
([`CONTRACT_CORE.md`](CONTRACT_CORE.md) §Snapshot ID), `(Partition, ID)`
order is not commit order: a slower write can commit after a snapshot with a
larger ID. Consumers that must observe every commit reconcile against full
snapshot enumeration rather than tailing `StartAfter`.

### `PartitionPath`

`PartitionPath` is an opaque value constructed only by `ParsePartitionPath`
and listing methods:
- `ParsePartitionPath("")` returns the zero `PartitionPath{}`, denoting the
  dataset root.
- `ParsePartitionPath` MUST return `ErrInvalidPath` for any string with a
  leading `/`, trailing `/`, `\`, or empty, `.`, or `..` segment.
- `MarshalText` encodes `p.String()`, and `UnmarshalText` validates via
  `ParsePartitionPath`.
- A manifest path or object path naming a non-canonical partition during
  listing MUST fail with `ErrManifestInvalid`.

---

## Complexity Summary

See [`CONTRACT_COMPLEXITY.md`](CONTRACT_COMPLEXITY.md) for full definitions
and variables (`D` candidate dataset prefixes, `N` listed keys, `M` yielded
manifests, `C` unindexed canonical-only snapshots, `F` snapshot objects, `S`
committed snapshots).

| Operation | Store Calls | Memory |
|-----------|-------------|--------|
| `ListDatasets` (with `PrefixLister`) | 1 `ListPrefixes` + O(D) pointer/manifest checks | O(D) |
| `ListDatasets` (fallback without `PrefixLister`) | 1 `List` + O(D) pointer checks | O(N) |
| `ListPartitions` (Hive, fully indexed) | 1 `List` + 0 `Get` calls | O(N) |
| `ListPartitions` (Hive, `C` canonical-only) | 1 `List` + `C` `Get` calls | O(N + manifest) |
| `Manifests` (`M` yielded) | 1 `List` + `M` `Get` calls | O(N + manifest) |
| `Latest` (warm) | 2 `Get` calls (pointer + manifest) | O(manifest) |
| `Snapshot(id)` | 1 `Get` (`0` `Get` calls when `id` matches `WithAnchor(id)`) | O(manifest) |
| `Read(id)` | `1 + F` `Get` calls (`F` `Get` calls when `id` matches `WithAnchor(id)`) | O(R_total) |
| `Snapshots` | 1 `List` + `S` `Get` calls | O(S × manifest) |
| `OpenObject` | 1 `Get` | O(1) streaming |

---

## Design Invariant

> **Lode's read API exposes stored facts, not interpretations.
> Planning and meaning belong to consumers.**

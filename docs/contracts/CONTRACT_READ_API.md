# Lode Read API Contract

This document defines the **Lode Read API**: its goals, non-goals, adapter
contracts, and implementation considerations. It binds every storage
adapter that serves Lode's data and every higher-level system (the Quarry
CLI, user interfaces, and query engines) that consumes that data.

---

## Goals

1. **Backend independence**: the API supports S3-compatible object stores,
   local filesystems, and similar backends.
2. **Manifest-driven planning**: a caller makes planning decisions from
   metadata before it reads any data.
3. **Range reads**: an adapter that offers range reads serves byte ranges
   efficiently.
4. **Streaming**: the API supports sequential scans and reads from the end
   of an object.
5. **Stable semantics**: the API exposes stored facts, not interpretations.

---

## Non-Goals

- An SQL execution engine
- A join planner or optimizer
- Arbitrary filtering beyond partition pruning and manifest statistics
- Mutable state or control-plane semantics

---

## Data Model

### Dataset → Partition → Segment → Object

- **Dataset**: a logical collection (for example, `nyc-rent/items`), **when
  the layout models datasets**.
- **Partition**: a grouping of objects that **the layout encodes in
  paths**.
- **Segment**: an immutable unit of appended data (often a run, an attempt,
  or a time interval).
- **Object**: an immutable blob (a data file, index, manifest, or artifact).

---

## Storage Adapter Interface

Every read depends on the storage adapter interface. An adapter that
implements
`RangeReader` reads genuine ranges and never emulates one by reading the
whole object.

`Store` defines the authoritative adapter interface (see `lode/api.go` and
[`docs/contracts/CONTRACT_STORAGE.md`](CONTRACT_STORAGE.md)):

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

type ConditionalWriter interface {
    CompareAndSwap(ctx context.Context, path, expected, replacement string) error
}

// RangeReader is optional (V1_API_DECISIONS D5).
type RangeReader interface {
    ReadRange(ctx context.Context, path string, offset, length int64) ([]byte, error)
    ReaderAt(ctx context.Context, path string) (io.ReaderAt, error)
}
```

The read path uses the `ObjectStore` methods, and commits and retention also
use `CompareAndSwap` (CONTRACT_STORAGE §ConditionalWriter Capability).

### Adapter Obligations (Read Path)

- An adapter that cannot read ranges MUST NOT implement `RangeReader`, so
  that a type assertion reports the capability before any read.
- `ReadRange` must correspond to genuine range requests (for example, HTTP
  `Range` requests on S3).
- `ReaderAt` must provide random-access reads and be safe for concurrent
  reads at different offsets.
- Adapters must document their consistency guarantees and mitigations (see
  [`CONTRACT_STORAGE.md`](CONTRACT_STORAGE.md) for the complete obligations).

### Range-Read Access Paths

Every dataset handle and view provides `ReaderAt(ctx, ObjectRef)` for
random access. Each handle's `ReaderAt` MUST return
`ErrRangeReadNotSupported` when the store does not implement
`RangeReader`. Lode's store wrappers (`SingleProcess` and the
store of a read-only root) MUST implement `RangeReader` exactly when the
wrapped store implements it. Callers that need direct byte-range reads have two
options:

1. **`ReaderAt`** returns an `io.ReaderAt` for standard random access,
   which suits most uses (Parquet footers, block indexes, and similar
   structures).

2. **`RangeReader.ReadRange`** reads a byte range directly from the
   underlying store. Callers that hold the store assert it to
   `RangeReader` and call `ReadRange(ctx, path, offset, length)`.

The handles deliberately omit a direct `ReadRange` method, because `io.ReaderAt` covers most range-read uses and is a standard
Go interface that existing libraries already accept.

---

## ReaderAt Caching (Recommended)

- Page size: 256 KiB to 1 MiB
- Cache size: 32 to 256 pages
- Prefetching: optional and sequential
- Metrics: range calls and bytes fetched

Caching is essential for Parquet footers, Arrow metadata, and block
indexes.

---

## Lode Read API (Views)

A view reads one dataset. `ReadRoot.Records(ctx, id, codec, opts...)` and
`ReadRoot.Blobs(ctx, id, opts...)` open a view over any `ObjectStore`, and
writer handles (`Records`, `Blobs`) embed the same read surface. Dataset
enumeration, by contrast, belongs to the root, which lists datasets rather
than reading one.

```go
// On Root and ReadRoot:
ListDatasets(ctx context.Context, opts DatasetListOptions) ([]DatasetID, error)

// On every view and writer handle (SnapshotReader):
ID() DatasetID
Latest(ctx context.Context) (*DatasetSnapshot, error)
Snapshot(ctx context.Context, id DatasetSnapshotID) (*DatasetSnapshot, error)
Snapshots(ctx context.Context) ([]*DatasetSnapshot, error)
ListPartitions(ctx context.Context, opts PartitionListOptions) ([]PartitionRef, error)
ListManifests(ctx context.Context, partition PartitionPath, opts ManifestListOptions) ([]ManifestRef, error)
GetManifest(ctx context.Context, ref ManifestRef) (*Manifest, error)
OpenObject(ctx context.Context, obj ObjectRef) (io.ReadCloser, error)
ReaderAt(ctx context.Context, obj ObjectRef) (io.ReaderAt, error)
// OpenObject and ReaderAt read only a data object of the handle's dataset
// (and of obj.Manifest's snapshot, when it names one); any other path
// returns ErrInvalidArgument.

// RecordView[T]: Read(ctx, id) ([]T, error); BlobView: Read(ctx, id) ([]byte, error)
```

This layer resolves paths according to Lode's layout and semantics but
does **not interpret** the data.

### Opening a View

- A view MUST resolve the dataset's head without writing. It reads the
  pointer and then looks for the manifest that the pointer names, first
  under the snapshots directory and then under the segments directory. The pointer does not say which
  layout wrote it, and at most one such manifest exists. When the pointer is
  absent or names no persisted manifest, the view scans as `Latest` does
  instead. It MUST NOT create a missing pointer (CONTRACT_CORE §Head Pointer), so it
  needs no `CompareAndSwap`.
- A view MUST read the dataset's layout from the head's manifest, and it
  accepts no layout option.
- Opening a view of a dataset without snapshots MUST yield `ErrNotFound`.
- A view MUST validate the head's manifest (§Manifests), and an invalid
  manifest yields `ErrManifestInvalid`.
- The name of the view's codec MUST match the codec name that the head's
  manifest records. Unless the manifest's format version is 1.0.0, the codec MUST
  also match the recorded codec description. The root's path names MUST
  match the manifest's. A mismatch yields
  `ErrSettingsMismatch`.
- A view MUST read each snapshot with the compressor that the snapshot's
  manifest names. That compressor is either a bundled compressor, which the
  view reconstructs from the recorded name, or a custom compressor that the
  caller supplies with `WithCompressor`. A custom compressor's name MUST
  match the recorded name. Reading a snapshot whose compressor
  is neither yields `ErrSettingsMismatch`, and reading a snapshot written
  with another codec yields `ErrSettingsMismatch`.

### Read API Error Semantics

- `ListDatasets` MUST return `ErrDatasetsNotModeled`, not a generic "not
  supported" error, for a root with flat paths, because flat paths have no
  datasets directory.
- `ListDatasets` MUST return an empty list only when storage contains no
  objects at all.

### Listing Order and Cursors

The order of a store listing is unspecified (CONTRACT_STORAGE §List), so the
read API imposes its own order:

- `ListDatasets` MUST return results in lexicographic order of `DatasetID`.
- `ListPartitions` MUST return results in lexicographic order of
  `PartitionRef.Path`.
- `ListManifests` MUST return results in lexicographic order of
  `(Partition, ID)`. When a snapshot has several partition manifests, the
  returned `ManifestRef` MUST carry the lexicographically smallest
  partition, so that the same storage state always yields the same
  references.
- On a Hive layout, an unfiltered listing MUST report each committed
  snapshot that lacks a partition manifest through the snapshot's canonical
  manifest and MUST set the empty partition in that reference's
  `ManifestRef`. `ListPartitions` MUST then report
  the partitions of that manifest's files. Such a snapshot has no files, or
  its writer terminated before persisting its partition manifests
  (CONTRACT_CORE §Head Pointer). Once its writer persists a partition
  manifest, the listing reports the snapshot through that manifest instead.
  A listing filtered by partition reflects only the partition manifests that exist (CONTRACT_LAYOUT
  §Layout-Specific Requirements).
- `StartAfter` resumes strictly after the given cursor in that order. The
  last element of a previous result is always a valid cursor. A cursor that
  matches no element resumes at the next element greater than the cursor.
- `Limit` bounds the result set, not the number of store calls. The listing
  sorts results and applies the cursor after it enumerates the store.

A cursor is a position in this order, not in commit order. Within one
partition, `(Partition, ID)` order is snapshot ID order, and a snapshot ID
records when its write began (CONTRACT_CORE §Snapshot ID), so a snapshot
can commit after a snapshot with a larger ID. A consumer that passes its
last reference as `StartAfter` to read new snapshots can therefore skip a
snapshot permanently. Version 1.0 offers no cursor over commits. A
consumer that must observe every commit reconciles against `Snapshots`,
which enumerates canonical manifests (V1_API_DECISIONS D10a). A listing
filtered by partition cannot serve that purpose on a Hive layout, because
it omits a committed snapshot whose writer did not persist its partition
manifests.

### Partition Paths

A partition path has the type `PartitionPath`, an opaque value that only
`ParsePartitionPath` and the listings construct (V1_API_DECISIONS D4). The
`ListManifests` parameter, `PartitionRef.Path`, `ManifestRef.Partition`,
and `PartitionListOptions.StartAfter` use this type.

- `ParsePartitionPath` MUST return `ErrInvalidPath` for a path that begins
  with `/`, contains a backslash, or has an empty, `.`, or `..` segment,
  including a trailing `/`.
- The empty string MUST parse to the zero value, which denotes the dataset
  root (no partition).
- `String` MUST return the path as it appears in storage keys.
- A `PartitionPath` MUST encode as text (`MarshalText`) to its string form,
  and
  `UnmarshalText` MUST parse as `ParsePartitionPath` does, so that encoded
  listing results and cursors retain their partition.
- A listing MUST return only canonical partition paths. A manifest whose
  location or file paths name a partition that is not canonical MUST cause
  `ErrManifestInvalid`, not a returned or omitted path.

`PartitionListOptions.Prefix` remains a raw string, because a prefix is not
a path: `day=a/` ends at a segment boundary, whereas `day=a` also matches
`day=a-b`.

### Partition Prefix

`PartitionListOptions.Prefix` is a raw string prefix over partition paths, and
it implies no key or value boundary (`day=2026-10` matches `day=2026-10-18`).

- On layouts that model partitions, the prefix MUST also restrict the
  storage listing to the matching subtree. Hive-style manifests reside
  under partition prefixes, so the listing omits no committed segment
  (CONTRACT_LAYOUT §Layout-Specific Requirements).
- A snapshot found under the prefix MAY reference files in partitions
  outside it, but those partitions MUST NOT appear in the result.
- An empty result under a prefix means that no partition matches. A
  listing returns `ErrNotFound` only when it has no prefix and the dataset
  has no manifests (a view of such a dataset does not open).
- On layouts without partitions, a listing with a non-empty prefix MUST
  return `ErrPartitionPrefixNotSupported`.
- The prefix MUST be a canonical fragment of a relative partition path, with
  no leading `/`, no `\` anywhere, and no empty, `.`, or `..` segment (a
  trailing `/` is permitted). The listing resolves the prefix beneath the
  dataset's partitions directory, so any other value could address another
  dataset.
  Such a prefix MUST return `ErrInvalidPath` before any store call.

---

## Manifests

Manifests are immutable, and they are the authoritative record of a
snapshot's contents.

### Minimum Required Fields

- the schema name and version
- the list of files, with sizes and, when configured, checksums
- the row or event count (the total number of data units in the snapshot)
- the minimum and maximum timestamps, when data units carry timestamps
  (omitted otherwise)
- references to auxiliary indexes (optional)

A blob dataset's reads (`BlobView.Read`, `Blobs.Read`) MUST return the raw
bytes of the object that the manifest references.

A reader must be able to fetch a manifest in a single small-object read.

---

## Range-Read Use Cases

### Columnar Formats (Parquet, Arrow)

- Read the footer through `ReadRange`.
- Plan the row groups and columns.
- Fetch only the required ranges.

### Block-Framed Logs

- Fixed block headers record size and statistics.
- An optional auxiliary index records block offsets.
- The planner selects blocks and then issues range reads.

### Artifacts

- Partial reads serve previews.
- A full read occurs only when the caller requests it explicitly.

---

## Consistency and Commit Semantics

- CONTRACT_CORE §Head Pointer defines when a commit is complete.
- Listing serves discovery, and manifests are authoritative.

---

## Layout

A dataset's paths come from two sources (CONTRACT_LAYOUT §Layout
Ownership). The root's **path names** govern the topology: the datasets,
snapshots, partitions, segments, data, expired, and staged directories and
the manifest file. The dataset's **Layout** governs partitioning
(`Unpartitioned` or `Hive` with its keys). Together they govern:
- manifest discovery (commit visibility)
- snapshot ID parsing from manifest paths
- dataset modeling (flat paths model no datasets) and dataset ID discovery
- partition encoding in object paths, if any

Implementations compose the two internally and MUST reject incoherent
combinations (an invalid or duplicate name, flat paths with a datasets
directory).

The library does **not strictly enforce** a directory topology, because
users configure both the dataset's layout and the root's path names. The following is
a **reference layout suitable for new users** (not a canonical one):

```
/datasets/<dataset>/
  /partition/<k=v>/<k=v>/
    /segments/<segment_id>/
      manifest.json
      /data/
      /index/
      /artifacts/
```

Alternative layouts are valid provided that:
- manifests remain discoverable through listing,
- the object paths that manifests record are accurate and resolvable, and
- the layout preserves the commit protocol of CONTRACT_CORE §Head Pointer.

The layout abstraction enables efficient prefix listing and pruning on
particular backends, but a layout may also adopt manifest-driven discovery
when no path topology exists.

### Partition Semantics

The layout encodes partitioning schemes (Hive-style keys, ranges, hash
buckets, spatial tiling, and similar schemes). When a layout does **not**
encode partition information in paths, the writer MUST record that
information explicitly in manifests so that consumers can interpret it.

### Reference Layouts (Curated)

The library SHOULD provide a **small, curated set** of layout
implementations that combine soundly, to simplify adoption and avoid
incoherent pairings. Examples include:
- the default layout (dataset-scoped snapshots with flat data paths)
- the Hive-style layout (key/value partitions encoded in paths)
- the flat layout (minimal topology, without dataset enumeration)

Manifest-driven discovery is a **pattern**, not a distinct layout:
discovery is always manifest-driven, but each layout still defines where
discovery finds manifests and how it parses dataset and segment IDs from
paths.

Any additional layout MUST satisfy the three conditions that §Layout states
for alternative layouts.

#### Default Layout

The default layout MUST:
- model datasets as the top-level discriminator,
- anchor each snapshot directory at its **segment ID**,
- use **flat (unpartitioned)** data paths by default,
- recognize manifests **only** at
  `datasets/<dataset>/snapshots/<segment>/manifest.json`, and
- treat a dataset as existing **only** when at least one valid manifest path
  matches the strict pattern of the default layout.

The default layout has the following reference shape:
```
/datasets/<dataset>/
  /snapshots/<segment_id>/
    manifest.json
    /data/
      file.<ext>
```

---

## Adapter Checklist

Every adapter must demonstrate:

1. prefix listing with pagination,
2. inexpensive and accurate metadata queries, and
3. documented consistency semantics.

An adapter that implements `RangeReader` must also demonstrate genuine
range reads and efficient random access that is safe for concurrent reads
(§Adapter Obligations (Read Path)).

---

## Complexity Bounds

See [CONTRACT_COMPLEXITY.md](CONTRACT_COMPLEXITY.md) for complete definitions
and a glossary of variables.

### Enumeration

| Operation | Store Calls | Memory |
|-----------|-------------|--------|
| `ListDatasets` | 1 `List` | O(N) |
| `ListManifests` | 1 `List` + M `Get` calls (validation) | O(N + M × manifest) |
| `ListPartitions` | 1 `List` + M `Get` calls | O(N + M × manifest) |
| `GetManifest` | 1 `Get` | O(manifest) |
| `OpenObject` | 1 `Get` | O(1) streaming |

`ListManifests` MUST extract snapshot IDs from paths and MUST NOT
deserialize content beyond what validation under CONTRACT_ERRORS.md
requires.

`ListPartitions` MUST NOT deserialize manifests that `ListManifests` has
already deserialized.

### Dataset Operations

| Operation | Store Calls (warm) | Memory |
|-----------|-------------------|--------|
| `Latest` | 2 (pointer + manifest) | O(manifest) |
| `Snapshot(id)` | 1 `Get` (canonical path) | O(manifest) |
| `Snapshots` | 1 `List` + S `Get` calls | O(S × manifest) |
| `Read(id)` | 1 + F `Get` calls | O(R_total) |

`Snapshots()` is a cold-path enumeration whose cost is proportional to the
depth of the history. Callers MUST NOT use `Snapshots()` on hot paths.

---

## Design Invariant

> **Lode's read API exposes stored facts, not interpretations.
> Planning and meaning belong to consumers.**

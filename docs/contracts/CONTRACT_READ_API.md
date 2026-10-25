# Lode Read API — Contract

This document defines the **Lode Read API**: its goals, non-goals, adapter
contracts, and implementation considerations. It is authoritative for any
storage adapter or higher-level tool (the Quarry CLI, user interfaces, query
engines) that consumes data written by Lode.

---

## Goals

1. **Independence from adapters**: support S3-compatible object stores, local
   filesystems, and similar backends.
2. **Manifest-driven planning**: planning decisions are made from metadata
   before any data is read.
3. **Range reads**: an adapter that offers range reads serves byte ranges
   efficiently.
4. **Streaming**: sequential scans and reads from the end of an object are
   straightforward.
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
- **Partition**: an organization encoded in paths, **as defined by the
  layout**.
- **Segment**: an immutable unit of appended data (often a run, an attempt,
  or a time interval).
- **Object**: an immutable blob (a data file, index, manifest, or artifact).

---

## Storage Adapter Interface

Adapters are the critical contract. An adapter that implements
`RangeReader` reads genuine ranges and never emulates one by reading the
whole object.

The authoritative adapter shape is `Store` (see `lode/api.go` and
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

The read path uses the `ObjectStore` methods; commits and retention also use
`CompareAndSwap` (CONTRACT_STORAGE §ConditionalWriter Capability).

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
random access; it MUST return `ErrRangeReadNotSupported` when the store does
not implement `RangeReader`. Lode's store wrappers (`SingleProcess` and the
store of a read-only root) MUST implement `RangeReader` exactly when the
store they wrap does. Callers that need direct byte-range reads have two
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
- Optional sequential prefetching
- Metrics: range calls and bytes fetched

Caching is essential for Parquet footers, Arrow metadata, and block
indexes.

---

## Lode Read API (Views)

A view reads one dataset. `ReadRoot.Records(ctx, id, codec, opts...)` and
`ReadRoot.Blobs(ctx, id, opts...)` open one over any `ObjectStore`; writer
handles (`Records`, `Blobs`) embed the same read surface. Datasets are
listed at the root.

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

This layer understands Lode's layout and semantics but performs **no
interpretation**.

### Opening a View

- A view MUST resolve the dataset's head without writing: it reads the
  pointer and the manifest the pointer names, under the snapshots
  directory and then the segments directory (the pointer does not say which
  layout wrote it, and at most one exists), and falls back to the scan of
  `Latest` when the pointer is absent or names no persisted manifest. It
  MUST NOT create a missing pointer (CONTRACT_CORE §Head Pointer), so it
  needs no `CompareAndSwap`.
- A view MUST take the dataset's layout from the head's manifest; it takes
  no layout option.
- A dataset without snapshots MUST yield `ErrNotFound` when the view opens.
- The head's manifest MUST be validated (§Manifests); an invalid one yields
  `ErrManifestInvalid`.
- The view's codec MUST match the codec name the head's manifest records,
  and, unless the manifest's format version is 1.0.0, its description; the
  root's path names MUST match the manifest's. A mismatch yields
  `ErrSettingsMismatch`.
- A view MUST read each snapshot with the compressor that snapshot's
  manifest names: a bundled compressor reconstructed by name, or a custom
  one passed with `WithCompressor` whose name matches. A snapshot whose
  compressor is neither yields `ErrSettingsMismatch` when it is read. A
  snapshot written with another codec yields `ErrSettingsMismatch` when it
  is read.

### Read API Error Semantics

- `ListDatasets` MUST return `ErrDatasetsNotModeled` for a root with flat
  paths, which have no datasets directory (not a generic "not supported"
  error).
- `ListDatasets` MUST return an empty list only when storage is truly empty.

### Listing Order and Cursors

The order of a store listing is unspecified (CONTRACT_STORAGE §List), so the
read API imposes its own order:

- `ListDatasets` MUST return results in lexicographic order of `DatasetID`.
- `ListPartitions` MUST return results in lexicographic order of
  `PartitionRef.Path`.
- `ListManifests` MUST return results ordered by `(Partition, ID)`,
  lexicographically. When a snapshot has several partition manifests, the
  returned `ManifestRef` MUST carry the lexicographically smallest
  partition, so that the same storage state always yields the same
  references.
- An unfiltered listing on a Hive layout MUST report a committed snapshot
  that has no partition manifest through its canonical manifest, with the
  empty partition in its `ManifestRef`, and `ListPartitions` MUST then
  report the partitions of that manifest's files. Such a snapshot has no
  files, or its writer terminated before persisting its partition
  manifests (CONTRACT_CORE §Head Pointer); once one is persisted, the
  snapshot is reported through it instead. A listing filtered by partition
  reflects only the partition manifests that exist (CONTRACT_LAYOUT
  §Layout-Specific Requirements).
- `StartAfter` resumes strictly after the given cursor in that order. The
  last element of a previous result is always a valid cursor. A cursor that
  matches no element resumes at the next element greater than the cursor.
- `Limit` bounds the result set, not the number of store calls. Sorting and
  the cursor are applied after enumeration.

A cursor is a position in this order, not in commit order. Within one
partition, `(Partition, ID)` order is snapshot ID order, and a snapshot ID
records when its write began (CONTRACT_CORE §Snapshot ID), so a snapshot
can commit after a snapshot with a larger ID. A consumer that passes its
last reference as `StartAfter` to read new snapshots therefore MAY skip a
snapshot permanently. v1.0 offers no cursor over commits; a consumer that
must observe every commit reconciles against `Snapshots`, which enumerates
canonical manifests (V1_API_DECISIONS D10a). A listing filtered by
partition cannot serve that purpose on a Hive layout, because it omits a
committed snapshot whose partition manifests were not persisted.

### Partition Paths

A partition path has the type `PartitionPath`, an opaque value that only
`ParsePartitionPath` and the listings construct (V1_API_DECISIONS D4). It
is used for the `ListManifests` parameter, `PartitionRef.Path`,
`ManifestRef.Partition`, and `PartitionListOptions.StartAfter`.

- `ParsePartitionPath` MUST return `ErrInvalidPath` for a path that begins
  with `/`, contains a backslash, or has an empty, `.`, or `..` segment,
  including a trailing `/`.
- The empty string MUST parse to the zero value, which denotes the dataset
  root (no partition).
- `String` MUST return the path as it appears in storage keys.
- A `PartitionPath` MUST encode as text (`MarshalText`) as its string, and
  `UnmarshalText` MUST parse as `ParsePartitionPath` does, so that encoded
  listing results and cursors retain their partition.
- A listing MUST return only canonical partition paths. A manifest whose
  location or file paths name a partition that is not canonical MUST cause
  `ErrManifestInvalid`, not a returned or omitted path.

`PartitionListOptions.Prefix` remains a raw string, because a prefix is not
a path: `day=a/` ends at a segment boundary, whereas `day=a` also matches
`day=a-b`.

### Partition Prefix

`PartitionListOptions.Prefix` is a raw string prefix over partition paths; it
implies no key or value boundary (`day=2026-10` matches `day=2026-10-18`).

- On layouts that model partitions, the prefix MUST also restrict the
  storage listing to the matching subtree (CONTRACT_LAYOUT §Layout-Specific
  Requirements: Hive-style manifests reside under partition prefixes, so no
  committed segment can be omitted).
- A snapshot found under the prefix MAY reference files in partitions
  outside it; those partitions MUST NOT appear in the result.
- An empty result under a prefix means that no partition matches.
  `ErrNotFound` is reserved for the case without a prefix in which the
  dataset has no manifests at all (a view of such a dataset does not open).
- On layouts without partitions, a non-empty prefix MUST return
  `ErrPartitionPrefixNotSupported`.
- The prefix MUST be a canonical fragment of a relative partition path, with
  no leading `/`, no `\` anywhere, and no empty, `.`, or `..` segment (a
  trailing `/` is permitted). It is resolved beneath the dataset's
  partitions directory, so any other value could address another dataset;
  such a prefix MUST return `ErrInvalidPath` before any store call.

---

## Manifests

Manifests are immutable and authoritative.

### Minimum Required Fields

- the schema name and version
- the list of files, with sizes and, when configured, checksums
- the row or event count (the total number of data units in the snapshot)
- the minimum and maximum timestamps, when data units carry timestamps
  (omitted otherwise)
- references to auxiliary indexes (optional)

A blob dataset's reads (`BlobView.Read`, `Blobs.Read`) MUST return the raw
bytes of the object that the manifest references.

A manifest must be readable in a single fetch of a small object.

---

## Range Read Use Cases

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
- Full reads occur only when explicitly requested.

---

## Consistency and Commit Semantics

- When a commit is complete is stated in CONTRACT_CORE §Head Pointer.
- Listing serves discovery; manifests are authoritative.

---

## Layout

A dataset's paths come from two sources (CONTRACT_LAYOUT §Layout
Ownership): the root's **path names**, which govern the topology (the
datasets, snapshots, partitions, segments, data, expired, and staged
directories and the manifest file), and the dataset's **Layout**, which
governs partitioning (`Unpartitioned` or `Hive` with its keys). Together
they govern:
- how manifests are discovered (commit visibility);
- how snapshot IDs are parsed from manifest paths;
- whether datasets are modeled (flat paths do not), and how dataset IDs are
  discovered; and
- whether and how partitions are encoded in object paths.

Implementations compose the two internally and MUST reject incoherent
combinations (an invalid or duplicate name, flat paths with a datasets
directory).

The library does **not strictly enforce** a directory topology. Users may
configure layout strategies through an abstraction layer. The following is
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
- manifests remain discoverable through listing;
- object paths recorded in manifests are accurate and resolvable; and
- the commit protocol of CONTRACT_CORE §Head Pointer is preserved.

The layout abstraction enables efficient prefix listing and pruning on
particular backends, but a layout may also adopt manifest-driven discovery
when no path topology exists.

### Partition Semantics

The layout encodes partitioning schemes (Hive-style keys, ranges, hash
buckets, spatial tiling, and similar schemes). If partition information is
**not** encoded in paths, it MUST be recorded explicitly in manifests so
that consumers can interpret it.

### Reference Layouts (Curated)

The library SHOULD provide a **small, curated set** of layout
implementations that are known to combine soundly, in order to simplify
adoption and avoid incoherent pairings. Examples include:
- the default layout, suitable for new users;
- the Hive-style layout (key/value partitions encoded in paths); and
- the flat layout (minimal topology, without dataset enumeration).

Manifest-driven discovery is a **pattern**, not a distinct layout:
discovery is always manifest-driven, but layouts still define how manifests
are found and how dataset and segment IDs are parsed from paths.

Any additional layout MUST observe the same invariants.

#### Default Layout

The default layout MUST:
- model datasets as the top-level discriminator;
- be **anchored at segments**;
- use **flat (unpartitioned)** data paths by default;
- recognize manifests **only** at
  `datasets/<dataset>/snapshots/<segment>/manifest.json`; and
- treat a dataset as existing **only** when at least one valid manifest path
  matches the strict pattern of the default layout.

Reference shape:
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

1. prefix listing with pagination;
2. inexpensive and accurate metadata queries; and
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
| `ListDatasets` | 1 List | O(N) |
| `ListManifests` | 1 List + M Gets (validation) | O(N + M × manifest) |
| `ListPartitions` | 1 List + M Gets | O(N + M × manifest) |
| `GetManifest` | 1 Get | O(manifest) |
| `OpenObject` | 1 Get | O(1) streaming |

`ListManifests` MUST extract snapshot IDs from paths and MUST NOT
deserialize content beyond what validation under CONTRACT_ERRORS.md
requires.

`ListPartitions` MUST NOT deserialize manifests that `ListManifests` has
already deserialized.

### Dataset Operations

| Operation | Store Calls (warm) | Memory |
|-----------|-------------------|--------|
| `Latest` | 2 (pointer + manifest) | O(manifest) |
| `Snapshot(id)` | 1 Get (canonical path) | O(manifest) |
| `Snapshots` | 1 List + S Gets | O(S × manifest) |
| `Read(id)` | 1 + F Gets | O(R_total) |

`Snapshots()` is a cold-path enumeration whose cost is proportional to the
depth of the history. Callers MUST NOT use `Snapshots()` on hot paths.

---

## Design Invariant

> **Lode's read API exposes stored facts, not interpretations.
> Planning and meaning belong to consumers.**

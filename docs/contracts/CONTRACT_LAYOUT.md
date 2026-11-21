# Lode Layouts and Components Contract

This document defines dataset path topology, layout ownership, and the
normative behavior of partitioners, compressors, checksums, and codecs.

---

## Goals

1. A portable, deterministic path grammar independent of the storage backend.
2. Explicit persistence of every dataset-wide component setting in manifests.
3. Zero implicit inference of schemas, partition keys, or metadata.

---

## Scope

Layouts and `WithFlatPaths()` apply **exclusively to datasets**. Volumes use
their own fixed internal path grammar defined in
[`CONTRACT_VOLUME.md`](CONTRACT_VOLUME.md).

---

## Path Topology and Layout Ownership

A dataset's object keys are determined by two orthogonal choices:

1. **Root path topology (`RootOption`)**: internal segment names are fixed
   constants (`datasets`, `snapshots`, `partitions`, `segments`,
   `manifest.json`, `data`, `expired`, `staged`, and `latest`). By default,
   datasets live under `datasets/<id>/...`. Passing `WithFlatPaths()` to
   `Open` or `OpenReadOnly` omits `datasets/` and, on unpartitioned layouts,
   `snapshots/`. To place a root beneath an arbitrary bucket or directory
   prefix, callers wrap the store factory with `lode.Prefixed(factory,
   prefix)` ([`CONTRACT_STORAGE.md`](CONTRACT_STORAGE.md) §Prefixed Stores).
2. **Dataset partitioning (`Layout` via `WithLayout`)**: `Unpartitioned()`
   (the default) or `Hive(keys...)` (optionally bound to typed records via
   `HiveLayout.By`).

### Canonical Path Grammars

- **Default unpartitioned**:
  - Canonical manifest: `datasets/<id>/snapshots/<snapshot>/manifest.json`
  - Data objects: `datasets/<id>/snapshots/<snapshot>/data/<file>`
  - Head pointer: `datasets/<id>/latest`
- **Default Hive-partitioned**:
  - Canonical manifest: `datasets/<id>/segments/<snapshot>/manifest.json`
  - Partition manifests: `datasets/<id>/partitions/<k=v>.../segments/<snapshot>/manifest.json`
  - Data objects: `datasets/<id>/partitions/<k=v>.../segments/<snapshot>/data/<file>`
  - Head pointer: `datasets/<id>/latest`
- **Flat unpartitioned (`WithFlatPaths()`)**:
  - Canonical manifest: `<id>/<snapshot>/manifest.json`
  - Data objects: `<id>/<snapshot>/data/<file>`
  - Head pointer: `<id>/latest`
- **Flat Hive-partitioned (`WithFlatPaths()`)**:
  - Canonical manifest: `<id>/segments/<snapshot>/manifest.json`
  - Partition manifests: `<id>/partitions/<k=v>.../segments/<snapshot>/manifest.json`
  - Data objects: `<id>/partitions/<k=v>.../segments/<snapshot>/data/<file>`
  - Head pointer: `<id>/latest`

Auxiliary control paths on every dataset layout (`<dataset dir>` is
`datasets/<id>` by default and `<id>` when flat):
- Staged first-commit manifest: `<dataset dir>/staged/<snapshot>.json`
- Expiry tombstones: `<dataset dir>/expired/<snapshot>`

### Topology Persistence and Compatibility

- These path grammars are part of Lode's stable persisted format: no release
  may alter segment names, order, or parsing for existing layouts.
- Format `1.2.0` manifests record `flat: true` when written under
  `WithFlatPaths()` (omitting `flat` when `false`). Writers and views MUST
  reject a `flat` mismatch with `ErrSettingsMismatch`.
- Format `1.0.0` manifests predate `flat` and `paths`; Lode determines
  whether they are flat from their storage location.
- Format `1.1.0` manifests carry a `paths` object: Lode accepts `1.1.0`
  manifests whose `paths` match the standard segment names above and MUST
  reject with `ErrSettingsMismatch` any `1.1.0` manifest that recorded custom
  segment names.
- Storage adapters MUST NOT invent persistence structure of their own; key
  scoping belongs exclusively in wrappers such as `Prefixed`.

### Hive Partition Index Requirements

- Hive layouts MUST anchor partition paths under `partitions/<k1=v1>/<k2=v2>...`
  so prefix listings prune non-matching partitions at the store level.
- Each Hive commit MUST write its canonical manifest first and then write an
  identical manifest copy under each touched partition's `segments/<snapshot>/manifest.json`
  ([`CONTRACT_CORE.md`](CONTRACT_CORE.md) §Head Pointer).
- Each Hive key MUST be a single canonical path segment without `=`.
  Partition values format `time.Time` as `2006-01-02` and strings/scalars via
  URL path escaping so partition segments never contain `/` or `.` / `..`
  traversal.

---

## Components

### Partitioner
- Manifests MUST record `partitioner` (`"noop"` or `"hive"`) and, when
  `"hive"`, `partition_keys` in declared key order.
- Blob datasets (`Root.Blobs`) MUST reject partitioned layouts with
  `ErrInvalidArgument`.

### Compressor
- Bundled constructors: `Uncompressed()` (`"noop"`, extension `""`), `Gzip()`
  (`"gzip"`, extension `".gz"`), and `Zstd()` (`"zstd"`, extension `".zst"`).
- Custom `Compressor` implementations MUST return a non-empty `Name()` and an
  `Extension()` that keeps `data<ext>` a single canonical path segment
  (`ErrInvalidArgument` otherwise).
- Manifests MUST record `compressor` explicitly (`"noop"` when uncompressed,
  never empty).

### Checksum
- Bundled constructors: `SHA256()` (`"sha256"`), `CRC32C()` (`"crc32c"`), and
  `MD5()` (`"md5"`).
- Custom `Checksum` implementations MUST return a non-empty `Name()`
  (`ErrInvalidArgument` when a writer or view opens otherwise) and provide
  `NewHasher() HashWriter`.
- A writer's checksum is the one that `WithChecksum` configures or, under
  `WithRecordedComponents()` with a head, the one that the head records, which
  may be none. A writer with a checksum records `checksum_algorithm` on its
  manifests, even on a snapshot that stores no object, and a hex digest over
  the stored bytes of each object. A writer without a checksum writes neither.

### Codec
- Bundled constructors: `JSONL[T]()` (`"jsonl"`) and `Parquet[T](schema,
  opts...)` (`"parquet"`).
- Custom `Codec[T]` implementations MUST return a non-empty `Name()` (empty
  names are reserved for codeless blob datasets and rejected with
  `ErrInvalidArgument`) and a deterministic canonical `Describe()` string
  (`""` when unparameterized).
- Manifests of record datasets MUST record `codec` and, when non-empty,
  `codec_description`; blob manifests MUST omit both fields.

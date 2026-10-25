# Lode Layouts and Components Contract

This document defines layout ownership and the behavior of the components
that layouts govern: partitioners, compressors, and codecs.

---

## Goals

1. A portable layout, independent of the backend.
2. Explicit records of component choices in manifests.
3. No implicit defaults or inference.

---

## Scope

Path names and layouts are **specific to datasets**. They do not apply to
Volume, which uses a fixed internal path scheme defined in
[`CONTRACT_VOLUME.md`](CONTRACT_VOLUME.md). The root's path options and `WithLayout` apply
exclusively to dataset handles and views.

---

## Layout Ownership

A dataset's paths have two sources (V1_API_DECISIONS D11).
- The root's **path names** govern the topology of datasets, snapshots,
  partitions, segments, manifests, and data objects. A root option, with a
  documented default, names each of the datasets, snapshots, partitions,
  segments, data, expired, and staged directories and the manifest file.
  `WithFlatPaths` omits the datasets and snapshots directories.
- The dataset's **Layout** governs whether and how paths encode partition
  semantics: `Unpartitioned()` or `Hive(keys...)`.

### Path Names

- Each name MUST be one canonical path segment (CONTRACT_STORAGE §Put):
  not empty, `.`, or `..`, without `/` or a backslash, and not beginning
  with `.lode-tmp-`. A name MUST NOT contain `=`, which marks a partition
  segment.
- The names a root uses MUST be distinct, and none MAY be `latest`, the
  pointer's name. The pointer's name is not configurable. The writers of a
  dataset coordinate at the pointer, and a second pointer name would let
  two roots commit independent histories over shared snapshot paths.
- Flat paths omit the datasets and snapshots directories, so a root MUST
  NOT combine flat paths with options that name those directories.
- The default names and the flat-path structure are part of the persisted
  format: a later release MUST NOT change them (V1_API_DECISIONS
  D8). The order, placement, and parsing of path segments are likewise
  fixed for every set of names.
- The names are part of a dataset's address, as the store is. Each manifest
  records them (CONTRACT_CORE §Manifest Requirements). Lode MUST reject
  with `ErrSettingsMismatch` any writer or view whose names differ from
  those that the dataset's head manifest records. A manifest of format version 1.0.0
  records no names: it was persisted under the default names, flat or not
  according to where it lies, and a root with other names MUST be rejected
  by it.
- A root whose datasets directory differs addresses another prefix and sees
  another dataset. A root whose other names differ shares the pointer. If
  that root finds the manifest that the pointer names, the names recorded
  in that manifest reject the root. If it does not, a writer returns `ErrSnapshotConflict`, as for a
  pending commit, and never appends.
- Lode recognizes another history's manifest by its file name, from a
  listing alone. That name is either the root's manifest name or the
  default name. Every manifest persisted before the names became root
  options bears the default name (CONTRACT_WRITE_API §Settings).

The following rules also apply:
- The persistence layer defines the logical layout of keys.
- Storage adapters MUST NOT introduce their own persistence structure.
- Adapters MAY rewrite keys, but they MUST return the canonical keys that
  Lode persists.

### Layout-Specific Requirements

- **Flat layouts** (without partitions encoded in paths) MUST pair with
  `partitioner = "noop"`. Lode MUST reject any other partitioner at
  configuration time, to prevent colliding paths.
- **Hive-style layouts** MUST anchor paths at partitions and MUST enable
  prefix pruning when a caller supplies a partition filter. A layout that claims
  partition-first semantics but requires scans of every manifest is
  invalid.
- **Hive-style layouts** MUST place a copy of each manifest under every
  partition prefix that holds its files. A listing filtered by partition
  then finds the snapshot without scanning every manifest. These copies are an index of the canonical manifest, persisted
  after the commit is complete (CONTRACT_CORE §Head Pointer). A listing
  filtered by partition reflects the copies that exist: it omits a
  committed snapshot whose writer terminated before persisting the copy for
  that partition.
- Every layout MUST place a dataset's staged first manifest
  (CONTRACT_CORE §Head Pointer) at `<dataset dir>/staged/<snapshot_id>.json`,
  beside the tombstones in `<dataset dir>/expired/`. No layout parses a
  manifest or a data object there, so no listing reports the staged
  manifest as a snapshot, and retention never treats it as data. The dataset directory is
  `datasets/<id>` in the default and Hive layouts and `<id>` in the flat
  layout.

---

## Partitioner (Logical Semantics)

- The partitioner defines the logical semantics of partitions (keys,
  values, normalization).
- The layout determines whether and how paths encode those semantics.
- Manifests MUST record the partitioner by name and MUST record a Hive
  partitioner's keys, in order, as `partition_keys`.
- Unpartitioned layouts MUST accept any **semantically inert** partitioner
  (not only one named `"noop"`) and MUST reject any partitioner that emits
  non-empty partition keys.

---

## Compressor

- The compressor defines the compression format and the file extension.
- Manifests MUST record the compressor by name.
- The absence of compression is explicit (the `noop` compressor), not
  implicit.

---

## Codec

- The codec defines the serialization format of structured data.
- Configuring a codec is optional.
- When a dataset configures a codec, manifests MUST record it by name and
  by description (`Codec.Describe`), a deterministic, canonical form of its
  configuration. Equal configurations MUST yield equal descriptions. A
  description omits settings that affect neither the records in a file nor
  their decoding, such as the internal compression of a Parquet file.

---

## NoOp Components

- Canonical NoOp implementations MUST exist.
- A Dataset configuration MUST NEVER use a `nil` partitioner or compressor.
- Manifests MUST record `partitioner = "noop"` and `compression = "noop"`
  when applicable.
- When a dataset configures no codec, manifests MUST omit codec fields.

---

## Key Persistence

- Lode persists only the canonical object keys that adapters return.
- Lode MUST NOT persist transient metadata or hints.

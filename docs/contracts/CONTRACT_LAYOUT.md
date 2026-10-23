# Lode Layout & Components — Contract

This document defines the ownership of layouts and the behavior of the
components that layouts govern: partitioners, compressors, and codecs.

---

## Goals

1. A portable layout, independent of the backend.
2. Explicit records of component choices in manifests.
3. No implicit defaults or inference.

---

## Scope

Path names and layouts are **specific to datasets**. They do not apply to
Volume, which uses a fixed internal path scheme defined in
`CONTRACT_VOLUME.md`. The root's path options and `WithLayout` apply
exclusively to dataset handles and views.

---

## Layout Ownership

A dataset's paths have two sources (V1_API_DECISIONS D11):
- the root's **path names** govern the topology of datasets, snapshots,
  partitions, segments, manifests, and data objects: the datasets,
  snapshots, partitions, segments, data, expired, and staged directories
  and the manifest file are each named by a root option, with documented
  defaults, and `WithFlatPaths` omits the datasets and snapshots
  directories; and
- the dataset's **Layout** governs whether and how partition semantics are
  encoded in paths: `Unpartitioned()` or `Hive(keys...)`.

### Path Names

- Each name MUST be one canonical path segment (CONTRACT_STORAGE §Put):
  not empty, `.`, or `..`, without `/` or a backslash, and not beginning
  with `.lode-tmp-`; it MUST NOT contain `=`, which marks a partition
  segment.
- The names a root uses MUST be distinct, and none MAY be `latest`, the
  pointer's name. The pointer's name is not configurable: it is where the
  writers of a dataset coordinate, and two names would let two roots commit
  independent histories over shared snapshot paths.
- Flat paths omit the datasets and snapshots directories; they MUST NOT be
  combined with options that name those directories.
- The default names, and the paths of flat paths, are part of the
  persisted format: a later release MUST NOT change them (V1_API_DECISIONS
  D8). The order, placement, and parsing of path segments are likewise
  fixed for every set of names.
- The names are part of a dataset's address, as the store is. Each manifest
  records them (CONTRACT_CORE §Manifest Requirements), and a writer or view
  whose names differ from those the dataset's head manifest records MUST be
  rejected with `ErrSettingsMismatch`. A manifest of format version 1.0.0
  records no names: it was persisted under the default names, flat or not
  according to where it lies, and a root with other names MUST be rejected
  by it.
- A root whose datasets directory differs addresses another prefix and sees
  another dataset. A root whose other names differ shares the pointer: if
  it finds the manifest the pointer names, the recorded names reject it,
  and if it does not, the dataset's directory holds a manifest that its
  names do not persist, which rejects a writer and a view with
  `ErrSettingsMismatch` (CONTRACT_WRITE_API §Settings). Lode recognizes a
  manifest by its content, because a manifest persisted under other names
  may bear any file name.

The following rules also apply:
- The persistence layer defines the logical layout of keys.
- Storage adapters MUST NOT introduce their own persistence structure.
- Adapters MAY rewrite keys, but they MUST return the canonical keys that
  are persisted.

### Layout-Specific Requirements

- **Flat layouts** (without partitions encoded in paths) MUST be paired with
  `partitioner = "noop"`. Any other partitioner MUST be rejected at
  configuration time, to prevent colliding paths.
- **Hive-style layouts** MUST be anchored at partitions and MUST enable
  prefix pruning when a partition filter is supplied. A layout that claims
  partition-first semantics but requires scans of every manifest is
  invalid.
- **Hive-style layouts** MUST place manifests under partition prefixes when
  partitioning is in use, so that a listing filtered by partition cannot
  omit committed segments.
- Every layout MUST place a dataset's staged first manifest
  (CONTRACT_CORE §Head Pointer) at `<dataset dir>/staged/<snapshot_id>.json`,
  beside the tombstones in `<dataset dir>/expired/`, where no layout parses
  a manifest or a data object, so that no listing reports it as a snapshot
  and retention never treats it as data. The dataset directory is
  `datasets/<id>` in the default and Hive layouts and `<id>` in the flat
  layout.

---

## Partitioner (Logical Semantics)

- The partitioner defines the logical semantics of partitions (keys,
  values, normalization).
- The layout determines whether and how those semantics are encoded in
  paths.
- The partitioner MUST be recorded in manifests by name, and a Hive
  partitioner's keys, in order, as `partition_keys`.
- Layouts that do not support partitions MUST accept any partitioner that
  is **semantically inert** (not only one named `"noop"`), and MUST reject
  partitioners that emit non-empty partition keys.

---

## Compressor

- The compressor defines the compression format and the file extension.
- The compressor MUST be recorded in manifests by name.
- The absence of compression is explicit (the `noop` compressor), not
  implicit.

---

## Codec

- The codec defines the serialization format of structured data.
- Configuring a codec is optional.
- When a codec is configured, it MUST be recorded in manifests by name and
  by description (`Codec.Describe`), a deterministic, canonical form of its
  configuration: equal configurations MUST yield equal descriptions. A
  description omits configuration that does not affect which records a file
  holds or how they decode, such as the internal compression of a Parquet
  file.

---

## NoOp Components

- Canonical NoOp implementations MUST exist.
- A Dataset configuration MUST NEVER use a `nil` partitioner or compressor.
- Manifests MUST record `partitioner = "noop"` and `compression = "noop"`
  when applicable.
- When no codec is configured, codec fields MUST be omitted from manifests.

---

## Key Persistence

- Only canonical object keys returned by adapters are persisted.
- Transient metadata and hints MUST NOT be persisted.

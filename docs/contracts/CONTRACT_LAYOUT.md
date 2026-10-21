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

Layout is an **abstraction specific to Datasets**. It does not apply to
Volume.

Volume uses a fixed internal path scheme defined in `CONTRACT_VOLUME.md`.
The layout interface, its implementations, and all of its configuration
options (`WithLayout`, `WithHiveLayout`) apply exclusively to Dataset and
DatasetReader.

---

## Layout Ownership

Layout is the **unified abstraction** that governs:
- the path topology of datasets, segments, manifests, and objects; and
- whether and how partition semantics are encoded in paths.

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

---

## Partitioner (Logical Semantics)

- The partitioner defines the logical semantics of partitions (keys,
  values, normalization).
- The layout determines whether and how those semantics are encoded in
  paths.
- The partitioner MUST be recorded in manifests by name.
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
- When a codec is configured, it MUST be recorded in manifests by name.

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

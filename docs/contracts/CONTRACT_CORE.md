# Lode Core Model Contract

This document defines the **core data model** and invariants for Lode. It is
authoritative for all implementations and adapters.

Lode defines persistence structure only. It does not define execution,
planning, scheduling, or optimization.

---

## Goals

1. Immutable, self-describing snapshots.
2. Explicit, persisted metadata.
3. Linear history per dataset or volume.
4. Backend-agnostic layout semantics.

---

## Non-Goals

- Query planning or execution.
- Background compaction or mutation.
- Backend-specific behavioral flags.

---

## Core Entities

### Dataset
A dataset is a logical collection of snapshots identified by a stable
`DatasetID`.

### Snapshot
A `Snapshot` records the immutable state of a dataset at one instant. Each
snapshot has a stable `SnapshotID`, embeds its `*Manifest`, and optionally
names a parent snapshot ID.

### Snapshot ID
Lode generates every `SnapshotID` (and `volume.SnapshotID`), and callers
never choose one. An ID in the current generated form is `<instant>-<nonce>`:

- `<instant>`: the Unix time, in nanoseconds, at which the write begins,
  expressed in decimal without a sign or leading zero.
- `<nonce>`: sixteen lowercase hexadecimal digits from a cryptographically
  secure random source.

Uniqueness does not depend on the clock. Two IDs coincide only when two
writes begin in the same nanosecond *and* draw the same 64-bit nonce (a
probability of 2⁻⁶⁴ per pair, even if the clock repeats or moves backward).
If IDs coincide while the earlier snapshot exists, the colliding commit fails
at its create-only manifest write after advancing the pointer; the pointer
then names the shared ID, which resolves to the earlier snapshot until the
next commit advances it, and retention rejects any tombstone that does not
describe the manifest ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md)
§Tombstone).

IDs are opaque strings to callers, and callers MUST NOT parse them as
numbers. Lexicographic string comparison orders IDs by instant from
2001-09-09 01:46:40 UTC until signed 64-bit nanosecond time overflows in
April 2262 (breaking ties by random nonce, which does not reflect commit
order).

Instant-only IDs written before v0.11.0 consist of `<instant>` alone. They
remain valid everywhere: Lode treats both `<instant>-<nonce>` and
`<instant>` as IDs in the *generated form*.

### Head Pointer
This section is the sole normative statement of the commit protocol. Other
contracts state their own obligations and refer here rather than restating it.

The `latest` pointer identifies the head. A write persists its data objects
before its commit begins: a dataset write persists its data objects and then
commits, and `StageWriteAt` persists a volume's blocks before the caller
invokes `Commit`. A failure while persisting data precedes the commit and
leaves the head unchanged.

The commit itself resolves its parent, advances the `latest` pointer with
`CompareAndSwap`, and then persists its canonical manifest followed by its
partition manifests (when partitioned), one at a time. The commit is complete
once the writer persists its canonical manifest, from which point `Latest`
and `Snapshot` return the snapshot. Partition manifests are an index of the
canonical manifest: a writer that terminates before persisting all of them
leaves a committed snapshot with an incomplete partition index, which
unfiltered listings still report through its canonical manifest
([`CONTRACT_READ_API.md`](CONTRACT_READ_API.md) §Listing Order and Cursors)
and `Sweep` later reindexes ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md)
§Unreferenced-Object Sweep). A pre-canonical legacy Hive snapshot has
partition manifests only and is committed when any of them exists.

Between the pointer update and the canonical manifest write, the commit is
*pending*: the pointer names a snapshot that has no canonical manifest. A
pending commit whose writer terminates before persisting the manifest is
*abandoned*. Because no persisted state distinguishes a pending commit from
an abandoned one, Lode applies uniform rules:

- When the pointer is missing or names a snapshot without a manifest,
  `Latest` scans and selects the largest non-expired snapshot ID that has a
  manifest. A writer handle's `Latest` creates a missing pointer from that
  scan result (`CompareAndSwap` with `expected == ""`) but MUST NOT replace
  an existing pointer, which would revert the head past a pending commit. A
  view (`ReadRoot.Records` or `ReadRoot.Blobs`) resolves the head the same
  way but MUST NOT create a missing pointer, and every write through a view's
  store (`Put`, `Delete`, or `CompareAndSwap`) MUST return `ErrReadOnly`.
- A new commit adopts the snapshot that the pointer names as its parent,
  whether or not that snapshot has a manifest, preserving a strictly linear
  chain. Readers traversing `ParentSnapshotID` MUST treat a parent without a
  manifest as the end of the explainable chain, just like an expired parent
  ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md) §Lineage).
- A volume commit requires its parent's manifest to merge blocks. It
  therefore persists a staged copy of its own manifest at
  `volumes/<volume_id>/staged/<snapshot_id>.json` before advancing the
  pointer, and deletes the staged copy after persisting the canonical
  manifest ([`CONTRACT_VOLUME.md`](CONTRACT_VOLUME.md) §Error Semantics).
- A dataset's first commit (one expecting an empty pointer) persists a staged
  copy of its manifest at `<dataset dir>/staged/<snapshot_id>.json` before
  advancing the pointer and deletes the staged copy after persisting the
  canonical manifest, so concurrent writers can verify dataset settings while
  the first commit is pending ([`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md)
  §Settings). A writer that adopts a pending first commit MUST continue
  staging its own commits until one of them persists a canonical manifest.
- If a conditional write reports an error, the pointer or its persisted
  ancestry can confirm that the write took effect. The writer then publishes
  its original manifest and preserves the current head. It may retry the same
  ID with a new parent only after ruling out the attempt from the head's
  ancestry. A chain can be incomplete because an ancestor is still pending, so
  the writer MUST read a missing ancestor again for a bounded time, unless
  retention expired it. A chain that stays
  incomplete, or a cyclic one, leaves the outcome uncertain and
  the writer MUST preserve the attempt's data. Snapshot ancestry MUST be acyclic.
- The pointer content `lode:removed` is reserved as the permanent removal
  fence written by `Root.RemoveDataset`
  ([`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md) §Dataset Removal) and
  `volume.Remove` ([`CONTRACT_VOLUME.md`](CONTRACT_VOLUME.md) §Removal). When
  the pointer holds `lode:removed`, dataset opens, `Latest`, `Snapshots`,
  listings, retention, and commits MUST return `ErrRemoved` (aliased in
  `lode/volume` as `volume.ErrRemoved`) rather than scanning.
- "Missing" means the store returned `ErrNotFound`. Any other error when
  reading the pointer or its manifest MUST be propagated directly; falling
  back to a scan on a transient read error could select an older snapshot and
  fork the history.

### Manifest
A `Manifest` is the authoritative, self-describing record of a snapshot and
its persisted data objects.

---

## Manifest Requirements

Manifests MUST be **self-describing** and **persisted**. Current writers emit
schema `"lode-manifest"` at `format_version` `"1.2.0"`.

Required fields:
- `schema_name` and `format_version` (`"1.2.0"` on write)
- `dataset_id` and `snapshot_id`
- `created_at` (UTC commit timestamp)
- `metadata` (an explicit JSON object; see §Metadata Rules)
- `objects` (`[]ObjectRef`, non-nil slice listing every persisted data object
  with `path`, `size_bytes`, and optional `checksum` and `stats`)
- `row_count` (non-negative total number of records, or `1` for a blob
  snapshot)
- `compressor` (`"noop"`, `"gzip"`, `"zstd"`, or custom compressor name)
- `partitioner` (`"noop"` or `"hive"`)

Optional and conditional fields:
- `parent_snapshot_id` (omitted on the first snapshot)
- `min_timestamp` and `max_timestamp` (present when records implement
  `Timestamped`, omitted otherwise)
- `codec` and `codec_description` (present when a codec is configured;
  `codec_description` records `Codec.Describe()` and is omitted when empty)
- `checksum_algorithm` (the writer's checksum setting, which `WithChecksum`
  configures or `WithRecordedComponents()` adopts from the head: present
  whenever the writer has a checksum, including on a snapshot that stores no
  object, and omitted otherwise)
- `partition_keys` (ordered Hive keys when `partitioner == "hive"`, omitted
  when unpartitioned)
- `flat` (`true` when persisted under `WithFlatPaths()`, omitted when
  `false`)

Backward-compatible manifest decoding:
- Readers MUST support formats `"1.0.0"` and `"1.1.0"` in standard and flat
  layouts that use the fixed internal names.
- When decoding pre-`1.2.0` manifests, `"files"` populates `Manifest.Objects`,
  and `"paths"` populates `Manifest.Flat` from `paths.flat`. A null or absent
  `"files"` with a zero `row_count` MUST read as no objects. With rows it MUST
  return `ErrManifestInvalid`.
- Format `"1.1.0"` manifests MUST contain a non-empty `paths` object (without
  `datasets` or `snapshots` when `paths.flat` is `true`, else
  `ErrManifestInvalid`). If a `"1.1.0"` manifest recorded custom directory or
  manifest names differing from Lode's fixed segment names, opening or
  committing against it MUST return `ErrSettingsMismatch`
  ([`CONTRACT_LAYOUT.md`](CONTRACT_LAYOUT.md) §Path Topology).

### Per-Object Statistics

An `ObjectRef` MAY carry per-object statistics (`*ObjectStats`) reported by
the codec:

- Batch codecs report statistics statelessly through `StatisticalCodec[T]`
  (`EncodeWithStats`), and streaming encoders report them through
  `StatisticalStreamEncoder` (`ObjectStats`).
- When a codec reports statistics, the writer MUST persist them on
  `ObjectRef.Stats`; otherwise, `Stats` MUST be omitted.
- `ObjectStats` records `RowCount` and per-column `ColumnStats` (`Name`,
  `Min`, `Max`, `NullCount`, and optional `DistinctCount`, where `0` means
  not computed).
- Bounds MUST be JSON-serializable values derived strictly from observed
  records; non-finite floats (`NaN`, `±Inf`) MUST be omitted.

### Checksum Rules

- Checksum computation is explicit and disabled by default.
- A writer's checksum (`SHA256`, `CRC32C`, `MD5`, or a custom `Checksum`) is
  the one that `WithChecksum` configures or, under `WithRecordedComponents()`
  with a head, the one that the head records, which may be none.
- A writer with a checksum MUST record `checksum_algorithm` on its manifests,
  even when the snapshot stores no object, and a lowercase hex-encoded
  `checksum` on each `ObjectRef`, computed over the exact stored bytes (after
  compression, if any).
- When `checksum_algorithm` is recorded on a manifest, `Read` on handles and
  views MUST recompute the digest over the complete stored object bytes and
  return `ErrChecksumMismatch` if an object's `Checksum` is empty or does not
  match.
- When the writer has no checksum, its manifests MUST omit
  `checksum_algorithm` and every `checksum`.
  Under `WithRecordedComponents()`, a head that records no checksum gives the
  writer none, whatever `WithChecksum` configures.

Manifests are immutable once written.

---

## Metadata Rules

- Metadata MUST be explicit on every snapshot.
- Writers (`Records.Write`, `Records.StreamWrite`, `Blobs.Write`,
  `Blobs.StreamWrite`, and `volume.Writer.Commit`) accept `any` value that
  encodes as a JSON object via `NewMetadata`: a struct, a string-keyed map,
  `Metadata`, or `nil` (including typed-nil string maps and struct pointers),
  which coalesces to `{}`.
- Any value that does not encode as a JSON object MUST be rejected with
  `ErrInvalidArgument` before any store call.
- `Metadata` retains the persisted JSON object verbatim and decodes it on
  request via `Metadata.Decode`. A manifest whose `metadata` is absent or not
  a JSON object fails validation with `ErrManifestInvalid`.

---

## History and Immutability

- Snapshot history is strictly linear, with a single head.
- Snapshots and data objects are immutable once written; commits create new
  state and never mutate existing objects.
- Dataset retention (`Expire`, `Sweep`) removes selected historical snapshots
  and orphaned objects behind create-only tombstones without rewriting
  surviving manifests or data objects
  ([`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md)).
- Whole-container removal (`Root.RemoveDataset`, `volume.Remove`) fences the
  `latest` pointer with `lode:removed` and deletes the container's objects
  without mutating snapshots in place.

---

## Design Invariant

> **Lode stores facts, not interpretations.**

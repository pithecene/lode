# Lode Core Model — Contract

This document defines the **core data model** and invariants for Lode.
It is authoritative for all implementations and adapters.

Lode defines persistence structure only. It does not define execution,
planning, scheduling, or optimization.

---

## Goals

1. Immutable, self-describing snapshots.
2. Explicit, persisted metadata.
3. Linear history per dataset.
4. Backend-agnostic layout semantics.

---

## Non-goals

- Query planning or execution.
- Background compaction or mutation.
- Backend-specific behavioral flags.

---

## Core Entities

### Dataset
Logical collection of snapshots. A dataset has a stable `DatasetID`.

### Snapshot
Immutable point-in-time state of a dataset. Each snapshot has a stable
`SnapshotID` and (optionally) a parent snapshot ID.

### Snapshot ID
Lode generates every snapshot ID, for Datasets and Volumes alike; callers
never choose one. From v0.11.0 an ID has the form `<instant>-<nonce>`:

- `<instant>` — the Unix time in nanoseconds when the write begins, in
  decimal with no sign or leading zero.
- `<nonce>` — 16 lowercase hexadecimal digits from a cryptographically
  secure random source.

Uniqueness does not depend on the clock: two IDs coincide only when two
writes begin in the same nanosecond *and* draw the same 64-bit nonce, a
probability of 2⁻⁶⁴ for each such pair, however often the clock repeats or
rolls back. Lode does not reserve IDs against existing ones; should IDs
coincide anyway, create-only writes refuse a reused path while the earlier
snapshot exists, and retention refuses a tombstone that does not describe
the manifest (CONTRACT_RETENTION §Tombstone). IDs are opaque strings to
callers and MUST NOT be parsed as numbers. Compared as strings, IDs order by instant (the instant has a fixed
19 digits from 2001 through 2286); IDs with the same instant order by nonce.

IDs written before v0.11.0 are `<instant>` alone. They remain valid and
are read, selected and retained exactly like new IDs. An ID is in the
*generated form* when it is in either form; a legacy ID sorts before every
new ID with the same instant.

### Manifest
The authoritative description of a snapshot and its data files.

---

## Manifest Requirements

Manifests MUST be **self-describing** and **persisted**.

Minimum required fields:
- schema name + version
- dataset ID
- snapshot ID
- creation time
- explicit metadata object (see below)
- list of files with sizes and checksums (when configured)
- parent snapshot ID (when applicable)
- row/event count (total data units in snapshot)
- min/max timestamp (when data units implement `Timestamped`; omit if not applicable)
Optional fields:
- codec name (omit when no codec is configured)
- per-file statistics (when the codec reports them via `StatisticalCodec`; omit when not available)

### Per-File Statistics

FileRef MAY contain per-file statistics reported by the codec.

- Statistics are codec-agnostic: any codec may report them via the `StatisticalCodec` interface.
- When a codec reports statistics, they MUST be persisted on the FileRef.
- When a codec does not report statistics, the stats field MUST be omitted.
- Statistics values MUST be JSON-serializable.
- Statistics MUST NOT be inferred; they are reported by the codec from observed data.
- Per-file statistics include: row count, and per-column min, max, null count, and distinct count.
- Distinct count is optional; zero means not computed.

### Checksum Rules

- Checksum computation is opt-in and explicit.
- When a checksum component is configured, manifests MUST record:
  - the checksum component name, and
  - a checksum value for each file written by the dataset.
- When no checksum component is configured, checksum fields MUST be omitted.

Manifests are immutable once written.

---

## Metadata Rules

- Metadata MUST be explicit on every snapshot.
- `nil` metadata is coalesced to empty (`{}`) at the API boundary.
- Empty metadata (`{}`) is valid and MUST be persisted as an explicit object.
- Metadata values MUST be JSON-serializable.
- Manifests always contain a non-nil metadata map.

---

## History & Immutability

- Snapshot history is strictly linear (single head).
- Snapshots are immutable after commit.
- Data files are immutable after write.
- Commits create new state; they do not mutate existing state.
- Retention removes committed snapshots and their data files; it never
  rewrites them. Expiring a committed snapshot is recorded by a new object
  (a tombstone), and retention never reassigns a snapshot ID. Objects of
  writes that never committed are reclaimed without a record, because no
  snapshot ever existed (`CONTRACT_RETENTION.md`).

---

## Design Invariant

> **Lode stores facts, not interpretations.**

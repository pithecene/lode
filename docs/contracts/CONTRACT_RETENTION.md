# Lode Retention — Contract

This document defines the contract for **snapshot expiry** and the
**unreferenced-object sweep**. It is authoritative for any retention
implementation.

Status: **Draft** (proposed for v0.10; no code implements this yet)

Issue: [#174](https://github.com/pithecene/lode/issues/174)

---

## Goals

1. Bound dataset storage without reaching under Lode's layout.
2. Keep every CONTRACT_CORE invariant observable and true after a delete.
3. Make removal explicit, caller-driven, resumable, and previewable (dry run).

---

## Non-goals

- Retention **policy** (what to keep, for how long). The caller decides.
- Scheduling or background execution. The caller runs retention from its own cron.
- Automatic compaction, rewriting of data files, or merging of snapshots.
- Volume retention. Volume snapshots are cumulative; expiry semantics differ and
  are deferred to a later revision of this contract.

---

## Relationship to Immutability

CONTRACT_CORE states that snapshots and data files are immutable and that
commits never mutate existing state. Retention does not weaken this:

- A data file is **never rewritten**. It is either present and byte-identical
  to what was committed, or absent.
- A manifest is **never rewritten**. It is either present and byte-identical,
  or absent.
- Expiry is a **state transition on the dataset**, recorded by a new object
  (a tombstone), never by editing an existing one.
- Snapshot IDs are never reused. An expired ID stays expired.

Immutable means *unchanging while it exists*, not *unremovable*.

---

## Conceptual Model

### Expired snapshot

A snapshot whose ID is recorded in a **tombstone** and whose canonical
manifest is absent. Its remaining partition manifests and data objects, if
any, are garbage that `Sweep` or a repeated `Expire` removes.

### Expiring snapshot

A snapshot whose tombstone exists but whose canonical manifest still exists.
This is the window between steps 1 and 2 below. The snapshot is still fully
readable and consistent during this window; the tombstone records intent, not
completion.

### Tombstone

A small object at a layout-defined path, one per expired snapshot:

```
<dataset root>/expired/<snapshot_id>
```

Contents: the snapshot ID, the UTC instant the expiry was requested, and the
**planned** object count and byte total taken from the manifest's `Files[]`.
A tombstone records intent, not outcome; completion is observable only as the
absence of the snapshot's manifests and objects. Tombstones are immutable once
written and are the only persisted record of an expiry.

### Head

The snapshot referenced by the `latest` pointer.

### Referenced object

A data object path that appears in `Files[]` of at least one existing
manifest of the dataset.

### Unreferenced object

A data object under one of the dataset's `segments/<snapshot ID>/data/`
prefixes (canonical or per-partition) whose snapshot ID names no existing
manifest. Produced by aborted streams (see PUBLIC_API.md §Cleanup Behavior)
and by expiries interrupted after the tombstone was written.

Reclaim granularity is the **snapshot, not the file**: an object under a live
snapshot's data prefix is never unreferenced, whether or not that snapshot's
`Files[]` lists it. PUBLIC_API.md presents a file inventory kept in
`Manifest.Metadata` as a correct pattern, so `Files[]` membership cannot be
the test for liveness. Objects anywhere else under the dataset (partition-level
sidecars such as `partitions/<k=v>/run.json`, pointers, tombstones) are
outside the sweep's scan: `Sweep` MUST NOT list, report, or delete them. This
scope is a consumer dependency (quarry keeps its run record there), not an
implementation detail.

---

## Snapshot Expiry

### Selection

Expiry operates on an explicit set of snapshot IDs. Selectors that resolve to
a set:

- `IDs` — explicit snapshot IDs.
- `Prefix` — a partition path prefix (Hive layouts only; see CONTRACT_LAYOUT).
  Resolves to every snapshot that has a partition manifest under
  `partitions/<prefix>`. It MUST be a canonical relative partition path
  fragment — no leading `/`, no empty, `.` or `..` segment — exactly as
  `PartitionListOptions.Prefix` requires (CONTRACT_READ_API §Partition
  Prefix); otherwise `ErrInvalidPath` and nothing is resolved. Every manifest
  path the prefix resolves to MUST lie beneath the selected dataset's
  partitions directory before it is used.
- `Before` — a UTC instant. Resolves to every snapshot whose
  `Manifest.CreatedAt` is strictly earlier.

Selectors compose by intersection. A field at its zero value (`nil` or empty
`IDs`, empty `Prefix`, zero `Before`) is **unset** and does not constrain the
set. A selector with every field unset MUST return `ErrEmptySelector` and
expire nothing: for a destructive operation the empty intersection is never
the universe. A selector that is set but resolves to zero snapshots is not an
error.

### Mixed snapshots

A snapshot whose `Files[]` span partitions both inside and outside `Prefix`
is **mixed**. Expiry is whole-snapshot or nothing, so a mixed snapshot MUST be
reported in the plan under `Mixed` and MUST NOT be expired by `Prefix` alone.
It MAY be expired by naming it in `IDs`.

### Head protection

The head MUST NOT be expired. `Expire` MUST read the pointer **after**
resolving the selection and **before any mutation**; if the resolved set
contains the head, it returns `ErrCannotExpireHead` and expires nothing.
Callers commit a new snapshot (or accept that the newest snapshot is
retained) before expiring it.

Rationale: the `latest` pointer is the live CAS target for every writer.
Retention MUST never move it, and MUST never remove the manifest it names:
doing either would re-expose a parent as head or strand concurrent writers
that CAS against the current value.

Head is determined by reading the pointer, and the check MUST be repeated
**immediately before step 2 for each snapshot**, not once per `Expire` call:
a writer may commit between selection and deletion, and a snapshot that was
listed during selection may have become head in that window. Once a snapshot
has been observed as non-head it can never become head later, because
commits only ever advance the pointer to a newly created ID, so the recheck
closes the race completely.

A pointer MAY transiently name a snapshot whose manifest is not yet written,
because `commitSnapshot` writes the pointer before the manifest. In that
state the previous head is still the only readable committed state, and the
pointer alone does not say which snapshot that is. Retention therefore MUST
NOT proceed while the head is uncommitted: if the pointer names an ID whose
canonical manifest is absent, `Expire` returns `ErrHeadUncommitted` and
expires nothing (at preflight) or stops before step 2 (at the per-snapshot
recheck), and `Sweep` MUST NOT settle any snapshot. Callers retry once the
in-flight commit completes or is cleaned up.

### Order of operations (per snapshot)

1. Write the tombstone `expired/<id>`. When the store implements
   `ConditionalWriter`, the write MUST be an atomic conditional create
   (`CompareAndSwap` from absent); an `ErrSnapshotConflict` result means the
   tombstone already exists. Otherwise `Store.Put` is used; its create-only
   guarantee is best-effort (CONTRACT_STORAGE §Put) and `ErrPathExists`
   means the tombstone already exists. In both cases an existing tombstone
   MUST be read back and, if it names this snapshot ID, treated as success:
   the same expiry is already recorded. A tombstone naming a different ID is
   corruption and MUST fail the operation.
   Tombstone content is never empty. `CompareAndSwap` with `expected == ""`
   also matches an existing zero-length object (CONTRACT_STORAGE
   §ConditionalWriter); replacing one is acceptable because a zero-length
   tombstone carries no information, and every non-empty existing tombstone
   goes through the read-back validation above.
2. Re-read the `latest` pointer. If it names this snapshot, stop with
   `ErrCannotExpireHead` for this snapshot (the tombstone written in step 1 is
   left in place and is harmless: a tombstone whose canonical manifest still
   exists marks an expiring snapshot, and a later `Sweep` MUST NOT settle a
   snapshot that is the current head). Otherwise delete the canonical
   manifest. From this point `Snapshot(ctx, id)` misses and the snapshot is
   **expired**.
3. Delete every partition manifest for the snapshot. From this point the
   snapshot is absent from enumeration.
4. Delete every data object listed in the snapshot's `Files[]` whose path
   is canonical, lies under this dataset's data prefixes for the current
   layout, and parses to this snapshot's ID. Layouts place every data object
   under its own snapshot's segment directory (CONTRACT_LAYOUT §Path
   topology; `dataFilePath` in every layout), so manifests never share
   objects and the check is a guard, not a lookup. Any `FileRef.Path` that
   fails the guard (foreign dataset, different snapshot ID, legacy or
   malformed path) MUST be left in place and reported under `Skipped`; the
   expiry still completes for the remaining objects.

Each step MUST be idempotent; a step that finds its target already absent
succeeds. If the process stops after step 1 the snapshot is still readable
and consistent (expiring). If it stops after step 2 or later the snapshot is
expired and its remaining manifests and objects are reclaimed by `Sweep`
(below) or by re-running `Expire` with the same ID.

Manifests are deleted **before** data so that any manifest loaded **after**
the manifest delete describes only objects that still exist. This does not
protect a reader that loaded the manifest *before* expiry began and fetches
its files afterwards: `Dataset.Read` loads the manifest and the files as
separate store calls with no lease, so such a read MAY fail with
`ErrNotFound` on a file. That is the documented consequence of expiring a
snapshot a reader is concurrently using, in the same class as a reader holding
an expired ID; Lode provides no read lease, and callers that need one
coordinate retention with their readers.

Rationale: tombstone-first makes expiry observable before anything is
destroyed and makes a crashed expiry resumable rather than a silent orphan;
manifests-before-data keeps every visible snapshot internally consistent.

### Lineage

A child's `ParentSnapshotID` MAY reference an expired ID. Readers resolving
lineage MUST treat an expired parent as the end of the explainable chain, not
as corruption.

`Snapshot(ctx, id)` on an expired ID MUST return `ErrSnapshotExpired`
(a distinct sentinel; `errors.Is(err, ErrNotFound)` MUST also hold so existing
callers keep working). The check is one extra `Get` on the tombstone path and
occurs only on the canonical-manifest-miss path, preserving the warm-path
bound in CONTRACT_COMPLEXITY. It MUST run **before** any fallback lookup
(`findSnapshotByID` scanning partition manifests): between steps 2 and 3 a
Hive snapshot still has partition manifests, and without this ordering the
fallback would resurrect an expired snapshot. During the expiring window (tombstone present, canonical
manifest present) `Snapshot` returns the snapshot normally.

`Snapshots()`, `ListManifests`, and `ListPartitions` MUST NOT return expired
snapshots once step 3 has completed. They MAY return an expiring snapshot or
one whose partition manifests have not all been deleted yet; every manifest
they return is complete and all of its files exist. Implementations MUST NOT
read tombstones during enumeration.

### Concurrency

Expiry of a non-head snapshot does not touch the `latest` pointer and does not
require CAS. Concurrent writers are unaffected. Concurrent readers holding an
expired ID observe `ErrSnapshotExpired` on their next access; this is the
documented and expected outcome.

Two concurrent expiries of the same ID are safe on stores that implement
`ConditionalWriter`: every step is idempotent and tombstone creation is
atomic. On stores without `ConditionalWriter` two concurrent expiries may
both create the tombstone; the deletes remain idempotent, so no data is lost,
but tombstone immutability is best-effort. This mirrors the single-writer
requirement in CONTRACT_WRITE_API §Concurrency: callers on non-CAS stores
MUST run at most one retention operation per dataset at a time.

---

## Unreferenced-Object Sweep

### Definition

`Sweep` performs, in order:

1. **Complete expiries.** List tombstones under `expired/`. Load each
   tombstone and verify it is non-empty and records the snapshot ID in its
   path; a tombstone that fails validation MUST be reported under
   `Unparseable` and MUST NOT settle anything. For every valid tombstoned ID
   that is not the current head (re-read the pointer per snapshot, as in
   §Head protection; an uncommitted head stops the run), perform steps 2–3 of
   §Order of operations (manifest deletes). Only once **every** manifest of that snapshot is confirmed
   absent is the snapshot *settled*; a settled snapshot's objects are deleted
   (step 4) without regard to the grace period.
   If any manifest delete fails, the snapshot is **not** settled: it remains
   in the reference union in step 2 and none of its objects are reclaimed in
   this run. A manifest that can still be loaded always protects the objects
   it describes.
2. **Reclaim orphans.** List data objects under the dataset's
   `segments/<snapshot ID>/data/` prefixes, group them by the snapshot ID in
   their path, and drop every group whose snapshot ID has an existing manifest
   (settled snapshots have none left). The remainder — objects of snapshots
   that were never committed or have been settled — is reported or deleted
   subject to the grace period. `Files[]` is consulted only to delete a
   settled snapshot's objects in step 1; it never decides reclaim inside a
   live snapshot (§Unreferenced object).

### Grace period

An unreferenced object MUST NOT be deleted unless it is older than the
caller-supplied grace period. Age is derived from the snapshot ID embedded in
the object's path: dataset snapshot IDs are nanosecond Unix timestamps
(`dataset.go` `newSnapshotID`), so every data path carries its creation
instant. This requires no `Stat` capability from the store.

Rationale: an in-flight stream has written data objects but not yet its
manifest. The grace period MUST exceed the longest expected write; the
default MUST be conservative (proposed: 24h) and MUST be explicit in the call.

`grace` MUST be strictly positive. A zero or negative `time.Duration` makes
every unreferenced object immediately eligible, which is never intended;
`Sweep` MUST return `ErrInvalidGracePeriod` before any listing or mutation.

Objects whose path does not parse to a snapshot ID MUST be reported under
`Unparseable` and MUST NOT be deleted.

### Interaction with expiry

Objects belonging to a **settled** tombstoned snapshot are reclaimed in step 1
regardless of grace period: the tombstone records the intent and no manifest
describes them any more. Objects of an unsettled tombstoned snapshot are
protected by its surviving manifests until a later run settles it. This is
what makes a crashed expiry recoverable without ever deleting data a reader
could still be directed to.

---

## Dry Run

Every retention operation MUST accept a dry-run mode that performs all
listing and resolution but no writes or deletes, and returns the same report
shape as a real run:

| Field | Expiry | Sweep |
|-------|:------:|:-----:|
| `Snapshots []DatasetSnapshotID` | ✅ | ✅ (settled this run) |
| `Mixed []DatasetSnapshotID` | ✅ | — |
| `Objects int` | ✅ | ✅ |
| `Bytes int64` | ✅ (from `FileRef.SizeBytes`) | ⚠️ unknown for orphans |
| `Skipped []string` | ✅ (paths failing the step-4 guard) | — |
| `Unparseable []string` | — | ✅ (paths without a snapshot ID) |

`Bytes` for orphans is reported as `-1` (unknown). The `Store` interface has
no size query, and adding one for this purpose is out of scope.

A dry-run `Sweep` MUST report what the real run would do: tombstoned
snapshots that are not the head are treated as **virtually settled** when
building the reference union, so their objects count as reclaimable exactly
as they would after step 1 of a real run. No write or delete is issued.

---

## Errors

| Error | Condition |
|-------|-----------|
| `ErrCannotExpireHead` | Selection includes the head snapshot |
| `ErrHeadUncommitted` | Pointer names a snapshot whose canonical manifest is absent (commit in flight); nothing expired or settled |
| `ErrEmptySelector` | Every `RetentionSelector` field is unset |
| `ErrInvalidGracePeriod` | `Sweep` grace period is zero or negative |
| `ErrInvalidPath` | `Prefix` is not a canonical relative partition path |
| `ErrSnapshotExpired` | `Snapshot(id)` for a tombstoned ID **whose canonical manifest is absent**; also matches `ErrNotFound`. An expiring snapshot (tombstone present, canonical manifest present) is returned normally |
| `ErrPartitionPrefixNotSupported` | `Prefix` selector on a layout without partitions (shared with `PartitionListOptions.Prefix`; distinct from the `StreamWriteRecords` error `ErrPartitioningNotSupported`) |

All deletes propagate store errors wrapped with the object path. A partial
failure leaves the dataset in a state the next `Sweep` or `Expire` completes.

---

## Complexity Bounds

| Operation | Store Calls | Memory |
|-----------|-------------|--------|
| `Expire` (K snapshots) | 1 List + M Gets (resolution) + (K + 1) pointer Gets + K tombstone Puts + Σ(files + manifests) Deletes | O(N + M × manifest) |
| `Sweep` (T tombstones) | 3 Lists (tombstones, manifests, objects) + M Gets (reference union) + T tombstone Gets (validation) + T pointer Gets + D Deletes | O(N + T + Σ files) |
| `Snapshot(id)` expired path | +1 Get (tombstone) | O(1) |

Pointer Gets: one preflight read of `latest` plus one re-read immediately
before each snapshot's manifest deletes (§Head protection).

`Expire` and `Sweep` are cold-path administrative operations with cost
proportional to history depth. Callers MUST NOT run them on hot paths.

---

## Proposed Public Shape (non-binding until implemented)

```text
type RetentionSelector struct {
    IDs    []DatasetSnapshotID
    Prefix string
    Before time.Time
}

type RetentionReport struct { ... fields from §Dry Run ... }

Dataset.Expire(ctx, sel RetentionSelector, opts ...Option) (*RetentionReport, error)
Dataset.Sweep(ctx, grace time.Duration, opts ...Option) (*RetentionReport, error)

WithDryRun() Option   // valid for Expire and Sweep only
```

The final shape is settled when this contract leaves Draft. Adding methods to
`Dataset` versus a separate `Retention` type is an open question; the
contract above is agnostic.

---

## Open Questions

1. Should `Prefix` expiry also delete the now-empty partition prefix tree?
   Object stores have no directories; filesystems do. Proposed: no, out of scope.
2. Should the head be expirable when the dataset has exactly one snapshot
   (i.e. "empty the dataset")? Proposed: no; delete the dataset through the
   store if that is the intent.
3. Tombstone retention: tombstones are tiny but unbounded. Proposed: callers
   may `Sweep` tombstones older than a second, longer grace period in a later
   revision.
4. Reclaiming caller-owned sidecar objects. A consumer (quarry) writes
   auxiliary objects via `Store.Put`, outside `Manifest.Files`, tracked only in
   `Manifest.Metadata`. This draft is safe for them by construction, in both
   places they can live: inside a live snapshot's data prefix they are kept by
   the snapshot-granularity rule (§Unreferenced object) and reclaimed only
   when that snapshot is expired and settled; outside the data topology (for
   example `partitions/<k=v>/files/<name>`) they are never scanned, so never
   reclaimed. What is open is **reclaim**, not protection: out-of-topology
   sidecars accumulate forever, and recording them in `Manifest.Files` at
   flush time does not help because objects written before a run's first
   flush are tracked nowhere. Options requested: (a) a metadata-declared
   reference list that extends the scan, or (b) a reclaim-prefix option on
   `Sweep`. Proposed: out of scope for the first revision; tracked in a
   separate issue once the consumer picks between (a) and (b).

---

## Design Invariant

> **Nothing is removed that a manifest still describes, and nothing described
> is removed without a record that it was.**

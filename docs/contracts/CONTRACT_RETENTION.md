# Lode Retention — Contract

This document defines the contract for **snapshot expiry** and the
**unreferenced-object sweep**. It is authoritative for any retention
implementation.

Status: **Draft** — design settled for v0.11.0; becomes Active when
`Dataset.Expire` and `Dataset.Sweep` ship. No code implements it yet.

Issue: [#174](https://github.com/pithecene-io/lode/issues/174)

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

The `expired/` directory is reserved for Lode: callers MUST NOT write under
it.

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

A data object of the layout — a path the layout's grammar parses as
`.../<snapshot ID>/data/<file>` for this dataset (`snapshots/<id>/data/` in
the default layout, `[partitions/<k=v>.../]segments/<id>/data/` in Hive,
`<dataset>/<id>/data/` in the flat layout) — whose snapshot ID names no
existing manifest. Produced by aborted streams (see PUBLIC_API.md §Cleanup Behavior)
and by expiries interrupted after the tombstone was written.

Reclaim granularity is the **snapshot, not the file**: an object under a live
snapshot's data prefix is never unreferenced, whether or not that snapshot's
`Files[]` lists it. PUBLIC_API.md presents a file inventory kept in
`Manifest.Metadata` as a correct pattern, so `Files[]` membership cannot be
the test for liveness. Every other object under the dataset that is not one
of its manifests (partition-level sidecars such as
`partitions/<k=v>/run.json`, the pointer, tombstones) is outside retention's
scope: a store listing of the dataset MAY return them, but `Sweep` and
`Expire` MUST NOT report or delete them. Manifests are deleted only as
§Order of operations prescribes. This scope is a
consumer dependency (quarry keeps its run record there), not an
implementation detail.

### Caller-owned objects

Objects a caller writes through `Store.Put` outside the layout's data
topology are **caller-owned**. Lode never deletes them; reclaiming them is
the caller's responsibility, typically after `Expire` reports the snapshots
that described them as expired. Lode deletes only manifests and data objects
whose paths its layout produces for a snapshot it is expiring, settling, or
reclaiming as unreferenced.

---

## Snapshot Expiry

### Selection

Expiry operates on an explicit set of snapshot IDs. `ExpireRequest` fields
that resolve to a set:

- `IDs` — explicit snapshot IDs. An ID with no manifest left is not
  selected: there is no `Files[]` left to act on, and `Sweep` reclaims what
  remains of it.
- `Partition` — a partition path (Hive layouts only; see CONTRACT_LAYOUT).
  Resolves to every snapshot that has a partition manifest in that partition
  or beneath it. Matching is **whole-segment**: `run_id=run-1` selects
  `run_id=run-1` and `run_id=run-1/event_type=x`, never `run_id=run-10`.
  This deliberately differs from the raw prefix of
  `PartitionListOptions.Prefix` (CONTRACT_READ_API §Partition Prefix): a raw
  prefix is acceptable for listing and never for deletion. The value MUST be
  a canonical relative partition path — no leading `/`, no `\`, no empty,
  `.` or `..` segment; one trailing `/` is allowed and ignored — otherwise
  `ErrInvalidPath` before any store call. Every manifest path it resolves to
  MUST lie beneath the selected dataset's partitions directory before it is
  used.
- `Before` — an instant. Resolves to every snapshot whose **ID instant** is
  strictly earlier. Dataset snapshot IDs are decimal nanosecond Unix
  timestamps taken when the write begins (`dataset.go` `generateID`), before
  any data object is written; `Manifest.CreatedAt` is set at commit and is
  never earlier. Using the ID makes selection a listing, with no manifest
  read per candidate. An ID that does not parse as a decimal integer is never
  selected by `Before`.

Fields compose by intersection. A field at its zero value (`nil` or empty
`IDs`, empty `Partition`, zero `Before`) is **unset** and does not constrain
the set. A request with every field unset MUST return `ErrInvalidArgument`
and expire nothing: for a destructive operation the empty intersection is
never the universe. A request that is set but resolves to zero snapshots is
not an error.

### Mixed snapshots

A snapshot whose `Files[]` span partitions both inside and outside
`Partition` is **mixed**. Expiry is whole-snapshot or nothing, so a mixed
snapshot MUST be reported under `Mixed` and MUST NOT be expired by
`Partition` alone. It MAY be expired by a request that names it in `IDs`
without `Partition`.

### Head protection

The head MUST NOT be expired. `Expire` MUST read the pointer **after**
resolving the selection and **before any mutation**. If the resolved set
contains the head, the head is removed from the set, reported in
`RetainedHead`, and the rest of the set is expired. Selecting the head is not
an error: a `Before` or `Partition` request against a dataset with no newer
writes always selects it, and failing would make such a request unusable
until something new is committed. A caller that needs a whole partition gone
checks `RetainedHead` and runs the request again after a newer commit.

When the pointer is absent (a dataset written before the pointer existed),
the head is the snapshot a scan returns (CONTRACT_CORE: the largest ID).

Rationale: the `latest` pointer is the live CAS target for every writer.
Retention MUST never move it, and MUST never remove the manifest it names:
doing either would re-expose a parent as head or strand concurrent writers
that CAS against the current value.

Head is determined by reading the pointer, and the check MUST be repeated
**immediately before step 1 for each snapshot**, not once per `Expire` call:
a writer may commit between selection and deletion, and a snapshot that was
listed during selection may have become head in that window. A snapshot the
recheck finds to be the head is retained and reported in `RetainedHead`
exactly as at preflight, and no tombstone is written for it. Once a snapshot
has been observed as non-head it can never become head later, because
commits only ever advance the pointer to a newly created ID, so one recheck
before the tombstone closes the race for every later step.

A pointer MAY transiently name a snapshot whose manifest is not yet written,
because `history.Advance` writes the pointer before the manifests. In that
state the previous head is still the only readable committed state, and the
pointer alone does not say which snapshot that is. Retention therefore MUST
NOT proceed while the head is uncommitted: if the pointer names an ID whose
canonical manifest is absent, `Expire` returns `ErrHeadUncommitted` and
expires nothing (at preflight) or stops before step 1 (at the per-snapshot
recheck). Callers retry once the in-flight commit completes; a pointer left
behind by a crashed commit is repaired by the next `Latest` or commit.

### Order of operations (per snapshot)

Before step 1, re-read the pointer (§Head protection). If it names this
snapshot, the snapshot is retained and none of the steps run for it; if it
names an uncommitted ID, `Expire` stops with `ErrHeadUncommitted`.

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
2. Delete the canonical manifest. From this point `Snapshot(ctx, id)`
   misses and the snapshot is **expired**.
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
(below), or by re-running `Expire` with the same ID while any of its
manifests remain.

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

`Snapshot(ctx, id)` and `Read(ctx, id)` on an expired ID MUST return
`ErrSnapshotExpired` (a distinct sentinel; `errors.Is(err, ErrNotFound)` MUST
also hold so existing callers keep working). The check is one extra `Get` on
the tombstone path and occurs only on the canonical-manifest-miss path,
preserving the warm-path bound in CONTRACT_COMPLEXITY. It MUST run **before**
any fallback lookup (`history.find` scanning partition manifests): between
steps 2 and 3 a Hive snapshot still has partition manifests, and without this
ordering the fallback would resurrect an expired snapshot. During the expiring window
(tombstone present, canonical manifest present) `Snapshot` returns the
snapshot normally.

`Snapshots()`, `ListManifests`, and `ListPartitions` MUST NOT return expired
snapshots once step 3 has completed. They MAY return an expiring snapshot or
one whose partition manifests have not all been deleted yet; every manifest
they return is complete and all of its files exist. Implementations MUST NOT
read tombstones during enumeration.

Deletion is a normal event once retention exists, so enumeration MUST
tolerate it: a manifest that a listing returned and that is absent when
loaded (`ErrNotFound`) was expired in between and MUST be skipped, not
reported as a failure. This applies to `Snapshots()`, the scan behind
`Latest()`, and every `DatasetReader` operation that loads listed
manifests. Any other load error still fails the operation.

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

`Sweep` works from two listings taken at the start of the run: the
tombstones under `expired/`, and the dataset's snapshot prefix (every
manifest and data object of the layout). It then performs, in order:

1. **Complete expiries.** Load each tombstone and verify it is non-empty and
   records the snapshot ID in its path; a tombstone that fails validation
   MUST be reported under `Unparseable` and MUST NOT settle anything. For
   every valid tombstoned ID, perform steps 2–3 of §Order of operations on
   the manifests the listing found for it. Only once **every** manifest of
   that snapshot is confirmed absent is the snapshot *settled*. If any
   manifest delete fails, the snapshot is **not** settled: it keeps its
   surviving manifests and none of its objects are reclaimed in this run. A
   manifest that can still be loaded always protects the objects it
   describes.
2. **Reclaim objects.** Group the listed data objects by the snapshot ID in
   their path. A group is:
   - **live** when its snapshot ID still has a manifest — never touched;
   - **settled** when its snapshot was settled in step 1 — deleted without
     regard to the grace period;
   - **orphaned** otherwise (a snapshot that was never committed) — deleted
     only when its ID instant is older than the grace period.

`Files[]` is never consulted by `Sweep`. Reclaim is per snapshot
(§Unreferenced object), so a settled snapshot's objects include any its
`Files[]` did not list: partial objects of an aborted stream, and caller
objects inside its data prefix.

`Sweep` reads the pointer once, before step 1, and never settles the
snapshot it names. When the pointer is absent, or names an ID with no
manifest in the listing, the largest listed manifest ID is protected the
same way (the snapshot a scan would return). `Expire` writes a tombstone only
for a snapshot observed as non-head (§Head protection), so this guard never
fires for tombstones Lode wrote; it keeps a misplaced object under
`expired/` from ever removing the head. A protected tombstoned snapshot is
left unsettled and its objects stay live.

The data objects of an in-flight commit have no manifest yet and are
orphaned by the definition above; the grace period protects them, not the
pointer. `Sweep` therefore never returns `ErrHeadUncommitted`.

### Grace period

An orphaned object MUST NOT be deleted unless it is older than
`SweepRequest.Grace`. Its age is the current time minus the instant encoded
in the snapshot ID in its path (§Selection, `Before`), so every data path
carries its creation instant and no `Stat` capability is required from the
store. Clock skew between the writing and the sweeping host shifts the
effective grace by the skew.

Rationale: an in-flight write has written data objects but not yet its
manifest. The grace period MUST exceed the longest expected write. There is
no default: `Grace` MUST be strictly positive, and a zero or negative value
returns `ErrInvalidArgument` before any listing or mutation. A zero grace
would make every unreferenced object immediately eligible, which is never
intended. 24h is a conservative starting value.

An orphaned group whose snapshot ID does not parse as a decimal integer has
no age: its object paths MUST be reported under `Unparseable` and MUST NOT be
deleted. Objects that do not parse as data objects of the layout are outside
the sweep's scope (§Unreferenced object) and are neither reported nor
deleted.

### Interaction with expiry

Objects of a **settled** snapshot are reclaimed regardless of grace period:
the tombstone records the intent and no manifest describes them any more.
Objects of an unsettled tombstoned snapshot are protected by its surviving
manifests until a later run settles it. This is what makes a crashed expiry
recoverable without ever deleting data a reader could still be directed to.

---

## Dry Run

`ExpireRequest.DryRun` and `SweepRequest.DryRun` perform all validation,
listing and resolution but no write or delete, and return the report a real
run would return. A dry-run `Expire` still reads the pointer, so
`RetainedHead` and `ErrHeadUncommitted` are what a real run would produce. A
dry-run `Sweep` reads the pointer the same way and treats every validly
tombstoned snapshot except a protected one as settled, so its objects count
exactly as they would after step 1 of a real run.

---

## Reports

```text
type ExpireReport struct {
    Expired      []DatasetSnapshotID // expired (or would be on a dry run), oldest first
    Mixed        []DatasetSnapshotID // selected by Partition, have files outside it; kept
    RetainedHead DatasetSnapshotID   // the head, when the selection included it; "" otherwise
    Objects      int                 // data objects deleted (or that would be)
    Bytes        int64               // sum of FileRef.SizeBytes over those objects
    Skipped      []string            // Files[] paths that failed the step-4 guard; left in place
}

type SweepReport struct {
    Settled     []DatasetSnapshotID // tombstoned snapshots settled this run, oldest first
    Objects     int                 // data objects deleted (or that would be)
    Orphans     int                 // of Objects, those reclaimed as orphaned
    Unparseable []string            // invalid tombstones; orphaned objects without an ID instant
}
```

`SweepReport` has no byte total: the `Store` interface has no size query,
and adding one for this purpose is out of scope.

Both methods return a non-nil report, also on error. On error the report
describes the work completed before the error; the dataset is left in a
state the next `Expire` or `Sweep` completes.

---

## Errors

| Error | Source | Condition |
|-------|--------|-----------|
| `ErrHeadUncommitted` | `Expire` | The pointer names a snapshot whose canonical manifest is absent (a commit in flight). At preflight nothing is expired; at a per-snapshot recheck the snapshots already expired stay expired and are reported |
| `ErrInvalidArgument` | `Expire`, `Sweep` | `ExpireRequest` with every selector field unset; `SweepRequest.Grace` zero or negative |
| `ErrInvalidPath` | `Expire` | `Partition` is not a canonical relative partition path |
| `ErrPartitionPrefixNotSupported` | `Expire` | `Partition` set on a layout without partitions (shared with `PartitionListOptions.Prefix`; distinct from the `StreamWriteRecords` error `ErrPartitioningNotSupported`) |
| `ErrSnapshotExpired` | `Snapshot`, `Read` | A tombstoned ID **whose canonical manifest is absent**; also matches `ErrNotFound`. An expiring snapshot (tombstone present, canonical manifest present) is returned normally |

Selecting the head is not an error (§Head protection). All deletes propagate
store errors wrapped with the object path.

---

## Complexity Bounds

N is the number of keys the operation lists, K the snapshots `Expire`
selects, T the tombstones, D the objects deleted.

| Operation | Store Calls | Memory |
|-----------|-------------|--------|
| `Expire` | 1 List (the `Partition` subtree, else the snapshot prefix) + (K + 1) pointer Gets, each followed by at most one `Exists` + K manifest Gets + K tombstone writes + Σ(manifests + files) Deletes; +1 List when the pointer is absent | O(N + K × manifest) |
| `Sweep` | 2 Lists (tombstones, snapshot prefix) + 1 pointer Get + T tombstone Gets + D Deletes | O(N + T) |
| `Snapshot(id)`, `Read(id)` expired path | +1 Get (tombstone) | O(1) |

Pointer Gets: one preflight read plus one re-read immediately before each
snapshot's tombstone (§Head protection).

`Expire` and `Sweep` are cold-path administrative operations with cost
proportional to history depth. Callers MUST NOT run them on hot paths.

---

## Public API

```text
type ExpireRequest struct {
    IDs       []DatasetSnapshotID
    Partition string
    Before    time.Time
    DryRun    bool
}

type SweepRequest struct {
    Grace  time.Duration
    DryRun bool
}

Dataset.Expire(ctx context.Context, req ExpireRequest) (*ExpireReport, error)
Dataset.Sweep(ctx context.Context, req SweepRequest) (*SweepReport, error)
```

Retention is a pair of `Dataset` methods: it needs the dataset's store,
layout and pointer, which a `Dataset` already holds, and a separate type
would repeat its construction options. `DatasetReader` stays read-only.

Requests are structs, not `Option`s. A dry run is a field visible at the
call site, so a missing option can never turn a preview into a delete, and
`Option` stays constructor-scoped (V1_API_DECISIONS D3).

---

## Resolved Questions

1. **Empty partition trees.** Lode deletes objects, not directories. On the
   filesystem store, directories emptied by retention remain.
2. **Expiring the only snapshot.** The head is never expirable, including
   when it is the only snapshot. To remove a dataset entirely, delete its
   prefix through the store.
3. **Caller-owned objects.** Reclaimed by the caller (§Caller-owned
   objects). A consumer that keeps auxiliary objects outside the data
   topology deletes them itself after `Expire` reports their snapshots
   expired; inside a snapshot's data prefix they are reclaimed with that
   snapshot.

## Deferred

- **Tombstone reclaim.** Tombstones are tiny but unbounded, and they are
  what makes `ErrSnapshotExpired` durable. A later revision may reclaim
  tombstones older than a second, longer grace period; until then they are
  kept.
- **Volume retention** (§Non-goals).

---

## Design Invariant

> **Nothing is removed that a manifest still describes, and nothing described
> is removed without a record that it was.**

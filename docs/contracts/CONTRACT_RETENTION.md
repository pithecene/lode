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
- Retention never reassigns a snapshot ID, and a tombstone is never removed.
  A write reuses an expired ID only on a 64-bit nonce collision within one
  nanosecond (CONTRACT_CORE §Snapshot ID), never because the clock repeats.
  Should it happen, the old tombstone does not describe the new snapshot's
  manifest (§Tombstone): retention refuses to act on it, so the new
  snapshot is never deleted by it, and is instead held — reported, never
  expired or reclaimed — since the tombstone cannot be removed. The one
  exposure is the window while the reusing write is staging data, before
  any manifest of it exists: nothing yet tells that data from the expired
  snapshot's leftovers, so a `Sweep`, or an `Expire` naming the ID, can
  delete it then; so can an expiry already past its canonical delete, whose
remaining steps work from the old inventory. A tombstone also cannot
  tell the two snapshots apart if both commits landed in the same nanosecond
  with the same files. Lode accepts these 2⁻⁶⁴ events rather than check every
  write against the tombstones.

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

Contents: the snapshot ID, the UTC instant the expiry was requested, the
manifest's `CreatedAt`, and the snapshot's **inventory** — its manifest's
`Files[]` (path and size of every data object), copied when the expiry is
recorded.

A tombstone **describes** a manifest when both carry the same `CreatedAt`
and the same `Files[]` (paths and sizes). Every copy of a snapshot's
manifest is one serialization, so all copies agree; `CreatedAt` is the
commit instant in nanoseconds, so a later snapshot that reused the ID is
told apart even when it lists the same files. Retention acts on a tombstone
only against manifests it describes.

The tombstone is the expiry's own record of what it removes. It is written
before anything is deleted and outlives every manifest, so each later step —
in the same run, a repeated `Expire`, or `Sweep` — works from the tombstone,
never from manifests that may already be gone. A tombstone records intent,
not outcome; completion is observable only as the absence of the snapshot's
manifests and objects. Tombstones are immutable once written, are never
removed (§Deferred), and are the only persisted record of an expiry.

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
`Expire` MUST NOT delete them, and MUST NOT report them unless a manifest
names them (a `FileRef.Path` that fails the step-4 guard is reported in
`Skipped`, never deleted). Manifests are deleted only as §Order of
operations prescribes. This scope is a
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

- `IDs` — explicit snapshot IDs: each that has a manifest, and each that
  has a valid tombstone, whose recorded expiry the request completes from
  the tombstone's inventory (§Order of operations). Only an ID in the
  generated form (CONTRACT_CORE §Snapshot ID) can name a tombstone; any other is never
  turned into a store path. `Partition` and `Before` select from manifests
  only.
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
  strictly earlier. A snapshot ID begins with the Unix nanosecond instant
  taken when the write begins (CONTRACT_CORE §Snapshot ID), before any data
  object is written. `Manifest.CreatedAt` is set at commit, by a clock
  that may have rolled back since, so no ordering between the two is
  guaranteed; selection uses the ID alone. Using the ID makes selection a listing, with no manifest
  read per candidate. An ID not in the generated form is never selected by
  `Before`.

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

Only the snapshot's own data files decide (§Order of operations, step 4): a
`FileRef` outside the dataset's data topology is never deleted, so it never
makes a snapshot mixed. It is reported in `Skipped` only for a snapshot the
run expires; a mixed or retained snapshot reports none.

### Head protection

The head MUST NOT be expired. `Expire` MUST read the pointer **after**
resolving the selection and **before any mutation**. If the resolved set
contains the head, the head is removed from the set, reported in
`RetainedHead`, and the rest of the set is expired. Selecting the head is not
an error: a `Before` or `Partition` request against a dataset with no newer
writes always selects it, and failing would make such a request unusable
until something new is committed. A caller that needs a whole partition gone
checks `RetainedHead` and runs the request again after a newer commit.

Only the pointer names the head. When it is absent beside any snapshot,
`Expire` and `Sweep` return `ErrHeadUncommitted` and remove nothing. A
tombstone counts: it shows that snapshots existed even when no manifest is
left. A dataset with neither needs no pointer. Every commit writes the
pointer, so it is missing only for the moment of a Delete+Put commit on a
store without `ConditionalWriter`, in a dataset written before the pointer
existed, or after a hand edit. None of these lets retention tell the head
safely: the largest ID is what `Latest` serves there, but a clock that
rolled back between two commits can give the last one the smaller ID. Any
commit creates the pointer, after which retention proceeds. `Latest`'s own
pointer-less scan still serves the largest ID that is not expired (an
interrupted expiry can leave a partition manifest of an expired snapshot
behind, and that leftover is never served); with a rolled-back clock or
commits in the same nanosecond that is not necessarily the last commit,
which is why retention never relies on it.

Rationale: the `latest` pointer is the live CAS target for every writer.
Retention MUST never move it, and MUST never remove the manifest it names:
doing either would re-expose a parent as head or strand concurrent writers
that CAS against the current value.

Head is determined by reading the pointer, and the check MUST be repeated
**immediately before step 1 for each snapshot**, not once per `Expire` call:
a writer may commit between selection and deletion, and a snapshot that was
listed during selection may have become head in that window. A snapshot the
recheck finds to be the head is retained and reported in `RetainedHead`
exactly as at preflight, and no tombstone is written for it. A snapshot
already protected is retained without further steps, but its recheck still
runs: a recheck that finds the pointer naming an uncommitted ID stops
`Expire` with `ErrHeadUncommitted`, whichever snapshot it precedes. A commit
that begins after a snapshot's recheck goes unobserved, and needs no
coordination: it can only make a new ID the head. Once a snapshot
has been observed as non-head it can never become head later, because
commits only ever advance the pointer to a newly created ID, so one recheck
before the tombstone closes the race for every later step.

A pointer MAY transiently name a snapshot whose manifest is not yet written,
because `history.Advance` writes the pointer before the manifests. In that
state the previous head is still the only readable committed state, and the
pointer alone does not say which snapshot that is. Retention therefore MUST
NOT proceed while the head is uncommitted: if the pointer names an ID with
no manifest — no canonical manifest, and, for data written before canonical
manifests existed, no partition manifest either — `Expire` returns
`ErrHeadUncommitted` and
expires nothing (at preflight) or stops before step 1 (at the per-snapshot
recheck). Callers retry once the in-flight commit completes; a pointer left
behind by a crashed commit is repaired by the next `Latest` or commit.

### Order of operations (per snapshot)

Before step 1, re-read the pointer (§Head protection). If it names this
snapshot, the snapshot is retained and none of the steps run for it; if it
names an uncommitted ID, `Expire` stops with `ErrHeadUncommitted`.

1. Write the tombstone `expired/<id>`, recording the inventory from the
   manifest. Every manifest copy retention reads MUST first pass manifest
   validation (CONTRACT_ERRORS §3, Manifest Validation Errors) and name the
   snapshot and dataset it was found under; an invalid or misplaced one
   fails the operation before anything is written or deleted. When the store implements
   `ConditionalWriter`, the write MUST be an atomic conditional create
   (`CompareAndSwap` from absent); an `ErrSnapshotConflict` result means the
   tombstone already exists. Otherwise `Store.Put` is used; its create-only
   guarantee is best-effort (CONTRACT_STORAGE §Put) and `ErrPathExists`
   means the tombstone already exists. In both cases an existing tombstone
   MUST be read back and, if it names this snapshot ID, treated as success:
   the same expiry is already recorded, by an earlier or concurrent run, and
   steps 2–4 use **its** inventory. The tombstone — recorded before this run,
   or the one this run is about to write —
   MUST describe (§Tombstone) every manifest copy that still loads at a
   path this expiry would delete — the canonical path, every copy the
   listing found, and every partition copy derived from the tombstone's
   inventory — whether or not the listing found any; one that wins a
   concurrent create MUST describe the manifest this run read. Otherwise it
   belongs to an earlier snapshot with the same ID and the operation MUST
   fail. A snapshot selected by its tombstone
   alone (no manifest left) starts here, with the read-back. A tombstone
   naming a different ID is corruption and MUST fail the operation.
   Tombstone content is never empty. `CompareAndSwap` with `expected == ""`
   also matches an existing zero-length object (CONTRACT_STORAGE
   §ConditionalWriter); replacing one is acceptable because a zero-length
   tombstone carries no information, and every non-empty existing tombstone
   goes through the read-back validation above. With no manifest left to
   record from, nothing can replace it: a zero-length tombstone is then
   corruption and MUST fail the operation.
2. Delete the canonical manifest. From this point `Snapshot(ctx, id)`
   misses and the snapshot is **expired**.
3. Delete every partition manifest for the snapshot: one per partition of
   the inventory's files (only paths that are canonical manifest paths of
   this snapshot), and any other the listing found. From this point the
   snapshot is absent from enumeration.
4. Delete every data object in the tombstone's inventory whose path
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
expired and its remaining manifests and objects are reclaimed by re-running
`Expire` with the same ID, or by `Sweep` (below): both work from the
tombstone, so neither depends on a manifest surviving.

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
they return is complete and all of its files exist, except a partition
manifest an in-flight commit wrote after its snapshot was expired
(§Concurrency), which the next `Sweep` or `Expire` removes.
Implementations MUST NOT read tombstones during enumeration.

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

A commit writes the pointer, then the canonical manifest, then its partition
manifests one by one (the pointer-before-manifest protocol). If a later commit
advances the pointer while an earlier one is still writing partition
manifests, an `Expire` that selects the earlier snapshot can remove it and
the slow writer can then add a partition manifest after the expiry. The
tombstone describes that manifest, so the next `Sweep` or `Expire` of the ID
removes it; until then `ListManifests` and `ListPartitions` can return that
manifest, and reading it fails on the missing data. This is the one
exception to listing completeness that retention introduces. Retention adds no lock against in-flight commits: selecting a snapshot
younger than the slowest commit is the caller's choice, and `Before` with a
margin avoids it.

---

## Unreferenced-Object Sweep

### Definition

`Sweep` works from two listings taken at the start of the run: the
tombstones under `expired/`, and the dataset's snapshot prefix (every
manifest and data object of the layout). It then performs, in order:

1. **Complete expiries.** A tombstone is an object directly under
   `expired/` whose name is a snapshot ID in the generated form
   (CONTRACT_CORE §Snapshot ID); any other object there — such as the
   filesystem store's `.lock` companions (CONTRACT_STORAGE §Implementation
   Notes) — is not a tombstone and is neither reported nor deleted. Load
   each tombstone and verify it is non-empty, records the snapshot ID in
   its path, and carries the manifest's `CreatedAt` and an inventory; a
   tombstone that fails validation MUST be reported under `Unparseable`
   and MUST NOT settle anything. A snapshot of which the listing shows
   neither a manifest nor a data object has nothing for this run to
   remove: once its tombstone has been loaded and validated like every
   other, it costs no further store call, is not reported, and nothing of
   it is deleted. Otherwise a listing MAY omit a manifest
   (a lagging listing, or one written after it), so Sweep MUST load the
   manifest at every path the expiry would delete — the canonical path,
   every listed copy, and every copy derived from the tombstone's inventory
   and from the snapshot's listed data objects — whether or not the listing
   showed it. The tombstone MUST describe (§Tombstone) every copy that
   loads; otherwise it belongs to an earlier snapshot with the same ID,
   MUST be reported under `Unparseable`, and MUST NOT settle the snapshot
   or reclaim its objects. For every valid tombstoned ID, perform steps
   2–3 of §Order of operations on those paths.
   Only once **every** manifest of that snapshot is confirmed absent is the
   snapshot *settled*; `Settled` reports the snapshots whose manifests this
   run found and deleted (in a dry run, would delete), so a snapshot settled
   by an earlier run is not reported again. Like the counts (§Accounting) it
   is an observation, not a claim: overlapping runs can both find and
   delete the same manifest and both report the snapshot. If any
   manifest delete fails, the snapshot is **not** settled: it keeps its
   surviving manifests and none of its objects are reclaimed in this run. A
   manifest that can still be loaded always protects the objects it
   describes.
2. **Reclaim objects.** Group the listed data objects by the snapshot ID in
   their path. A group is:
   - **live** when its snapshot ID still has a manifest — never touched. A
     group the listing shows without a manifest is live too when a
     manifest exists at its canonical path or at a manifest path derived
     from its objects' paths; Sweep MUST check those paths before
     reclaiming an orphaned group, since a listing may omit a manifest.
     Commit writes a manifest copy in every partition that holds its data
     (legacy partition-only snapshots included), so every listed object
     derives a path at which its snapshot's manifest would be found;
   - **held** when its snapshot's tombstone was rejected in step 1 (invalid,
     or contradicted by a manifest copy) or its snapshot is protected —
     never touched, even past the grace period, because a tombstone Lode
     cannot trust says nothing about whether the objects are garbage;
   - **settled** when its snapshot was settled in step 1 — deleted without
     regard to the grace period;
   - **orphaned** otherwise (a snapshot that was never committed) — deleted
     only when its ID instant is older than the grace period.

Reclaim is per snapshot (§Unreferenced object): a settled snapshot's objects
are every object under its data prefixes, including any its inventory does
not list (partial objects of an aborted stream, caller objects inside its
data prefix). The inventory decides only how they are counted
(§Accounting), never whether they are deleted — with one addition: for a
snapshot the listing shows a manifest or object of, an object the inventory
lists but the listing omitted is deleted too and, not having been listed, is
not counted. A snapshot the listing shows nothing of is not probed, so a
later run whose listing shows its leftovers removes them; probing every
tombstone's inventory on every run would grow without bound.

`Sweep` reads the pointer once, before step 1, and never settles the
snapshot it names. While the pointer names a snapshot with no manifest, a
commit is in flight and the head is unknown, so no tombstoned snapshot is
settled in that run; orphans past grace are still reclaimed. With no
pointer, a dataset whose listing shows a manifest or a tombstone is refused
with `ErrHeadUncommitted` (§Head protection); one that shows neither has no
head and only orphans to reclaim. `Expire` writes a tombstone only
for a snapshot observed as non-head (§Head protection), so this guard never
fires for tombstones Lode wrote; it keeps a misplaced object under
`expired/` from ever removing the head. A protected tombstoned snapshot is
left unsettled and its objects stay live.

The data objects of an in-flight commit have no manifest yet and are
orphaned by the definition above; the grace period protects them, not the
pointer, so a pointer naming an uncommitted ID does not stop `Sweep`. A
missing pointer beside snapshots does (§Head protection): `Sweep` then
returns `ErrHeadUncommitted`, like `Expire`.

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

An orphaned group whose snapshot ID is not in the generated form has no
age: its object paths MUST be reported under `Unparseable` and MUST NOT be
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
    Bytes        int64               // their recorded sizes
    Skipped      []string            // inventory paths that failed the step-4 guard; left in place
}

type SweepReport struct {
    Settled     []DatasetSnapshotID // tombstoned snapshots whose manifests this run removed (dry run: would remove), oldest first
    Objects     int                 // data objects deleted (or that would be)
    Bytes       int64               // recorded sizes of those a tombstone inventory lists
    Unsized     int                 // of Objects, those with no recorded size
    Orphans     int                 // of Unsized, those of snapshots never committed
    Unparseable []string            // invalid or mismatched tombstones; orphaned objects without an ID instant
}
```

### Accounting

Both reports meter what the run deletes as its listing showed it, so a
resumed expiry is never counted twice. `Store.Delete` does not say whether
it removed anything, so the counts are only as exact as the listing:

- `Objects` counts data objects the run deleted that its listing showed
  present, each once however often an inventory lists its path. An object already gone (deleted by an earlier, interrupted run)
  is not counted again. Runs that overlap can both list an object and both
  count it, and an object a lagging listing omitted is deleted but not
  counted; sequential runs over consistent listings add up exactly.
- `Bytes` sums the sizes recorded for those objects in the tombstone's
  inventory. Every object `Expire` deletes is in the inventory, so its
  `Bytes` is exact.
- `Unsized` (`Sweep` only) counts deleted objects with no recorded size:
  orphans, whose write never committed, and objects in a settled
  snapshot's data prefix that its inventory does not list. Lode never
  recorded their size, and the `Store` interface has no size query; adding
  one is out of scope. They are counted, never estimated.

Both methods return a non-nil report, also on error. On error the report
describes the work completed before the error. After a retryable failure (a
store error, an uncommitted head) the dataset is left in a state the next
`Expire` or `Sweep` completes. Corruption is not retryable: an invalid or
contradicted tombstone, or an invalid manifest, fails `Expire` whenever it
selects the snapshot, and `Sweep` reports what it examines — tombstones,
and the manifests it loads while settling them — on every run until an
operator repairs it. `Sweep` does not load the manifests of live snapshots
and does not report their corruption. Lode never
rewrites or removes a tombstone to recover.

---

## Errors

| Error | Source | Condition |
|-------|--------|-----------|
| `ErrHeadUncommitted` | `Expire`, `Sweep` | `Expire`: the pointer names a snapshot with no manifest (a commit in flight). At preflight nothing is expired; at a per-snapshot recheck the snapshots already expired stay expired and are reported. Both: the pointer is missing beside existing snapshots (§Head protection); `Sweep` stops before deleting anything |
| `ErrInvalidArgument` | `Expire`, `Sweep` | `ExpireRequest` with every selector field unset; `SweepRequest.Grace` zero or negative |
| `ErrInvalidPath` | `Expire` | `Partition` is not a canonical relative partition path |
| `ErrPartitionPrefixNotSupported` | `Expire` | `Partition` set on a layout without partitions (shared with `PartitionListOptions.Prefix`; distinct from the `StreamWriteRecords` error `ErrPartitioningNotSupported`) |
| `ErrSnapshotExpired` | `Snapshot`, `Read` | A tombstoned ID **whose canonical manifest is absent**; also matches `ErrNotFound`. An expiring snapshot (tombstone present, canonical manifest present) is returned normally |

Selecting the head is not an error (§Head protection). All deletes propagate
store errors wrapped with the object path.

---

## Complexity Bounds

N is the number of keys the operation lists, K the snapshots `Expire`
selects, T the tombstones, D the objects deleted (manifests and data alike).

| Operation | Store Calls | Memory |
|-----------|-------------|--------|
| `Expire` | 1 List (the `Partition` subtree, else the snapshot prefix) + (K + 1) pointer Gets, each followed by at most one `Exists` (and one List of the snapshot prefix when the named snapshot has no canonical manifest, to find a legacy partition-only copy) + K manifest Gets + K tombstone Gets (the recorded inventory, if any; also one per requested ID with no manifest) + 1 Get per manifest path of each selected snapshot (every copy is checked against the tombstone before it is written) + K tombstone writes (each + at most 1 read-back Get when it already exists) + Σ(manifests + files) Deletes; when the pointer is absent, +1 List of the whole snapshot prefix (even for a `Partition` request) to tell an empty dataset from a refused one | O(N + K × manifest) |
| `Sweep` | 2 Lists (tombstones, snapshot prefix) + 1 pointer Get + T tombstone Gets + 1 Get per manifest path of each tombstoned snapshot with anything left in the listing + 1 `Exists` per manifest path of each group past grace with no listed manifest, whether or not it is then reclaimed (stopping at the first present) + D Deletes; when the pointer names a snapshot, 1 `Exists` (+1 List when it has no canonical manifest) to tell whether it is committed | O(N + Σ tombstone inventories) |
| `Snapshot(id)`, `Read(id)` expired path | +1 Get (tombstone) | O(1) |

Pointer Gets: one preflight read plus one re-read per selected snapshot,
immediately before its tombstone or before retaining it (§Head protection).

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

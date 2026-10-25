# Lode Retention Contract

This document defines the contract for **snapshot expiry** and the
**unreferenced-object sweep**. It is authoritative for any retention
implementation.

---

## Goals

1. Bound dataset storage without circumventing Lode's layout.
2. Ensure that every CONTRACT_CORE invariant remains observable and true
   after a deletion.
3. Make removal explicit, caller-initiated, resumable, and previewable
   (through a dry run).

---

## Non-Goals

- Retention **policy** (what to retain and for how long). The caller
  determines policy.
- Scheduling or background execution. The caller invokes retention from its
  own scheduler.
- Automatic compaction, rewriting of data files, or merging of snapshots.
- Volume retention. Volume snapshots are cumulative, and this contract does
  not define their expiry.

---

## Relationship to Immutability

CONTRACT_CORE states that snapshots and data files are immutable and that
commits never mutate existing state. Retention does not weaken these
guarantees:

- A data file is **never rewritten**. It is either present and
  byte-identical to the committed file, or absent.
- A manifest is **never rewritten**. It is either present and
  byte-identical, or absent.
- Expiry is a **state transition of the dataset**, recorded by a new object
  (a tombstone), never by modifying an existing one.
- `Sweep` restores a committed snapshot's partition index by creating
  byte-identical copies of its canonical manifest (§Definition, step 3). It
  creates objects and never replaces one.
- Retention never reassigns a snapshot ID, and a tombstone is never removed.
  A write reuses an expired ID only in the event of a 64-bit nonce collision
  within a single nanosecond (CONTRACT_CORE §Snapshot ID), never because the
  clock repeats. If such a collision occurs, the old tombstone does not
  describe the new snapshot's manifest (§Tombstone), so retention declines
  to act on it. The tombstone therefore never causes the new snapshot to be
  deleted. Because no operation removes the tombstone, retention holds the
  new snapshot. Retention reports a held snapshot but never expires or
  reclaims it. One exposure remains: while the reusing write is staging data,
  before any of its manifests exists, nothing distinguishes that data from
  the expired snapshot's residual objects. A `Sweep`, or an `Expire` that
  names the ID, can delete the data during that interval. So can an expiry
  that has already deleted the canonical manifest, because its remaining
  steps operate on the old inventory. A tombstone also cannot distinguish
  the two snapshots if both commits completed in the same nanosecond with
  the same files. Lode accepts these events, each with a probability near
  2⁻⁶⁴, rather than verifying every write against the tombstones.

In this contract, an immutable object is *unchanging while it exists*, not
*unremovable*, so retention may remove an object but never modifies it.

---

## Conceptual Model

### Expired Snapshot

An expired snapshot is a snapshot whose ID a **tombstone** records and
whose canonical manifest is absent. Its remaining partition manifests and data objects, if
any, are residual objects that `Sweep` or a repeated `Expire` removes.

### Expiring Snapshot

An expiring snapshot has both a tombstone and a canonical manifest, so it
lies in the interval between steps 1 and 2 below. The snapshot remains
fully readable and consistent during this interval, because the tombstone
records an intention, not a completion. A snapshot written before Lode
introduced canonical manifests has no canonical manifest and therefore no
expiring interval: its tombstone alone renders it expired.
`Snapshot` and `Read` return `ErrSnapshotExpired` from step 1 onward, and
the later steps remove its partition manifests and data.

### Tombstone

A tombstone is a small object, one per expired snapshot, at the following
path that the layout defines:

```
<dataset root>/expired/<snapshot_id>
```

The `expired/` directory is reserved for Lode: callers MUST NOT write
under it.

A tombstone contains the snapshot ID, the UTC instant at which the caller
requested the expiry, the manifest's `CreatedAt`, and the snapshot's
**inventory**.
The inventory is the manifest's `Files[]` (the path and size of every data
object), as copied when the expiry is recorded.

A tombstone **describes** a manifest when both contain the same `CreatedAt`
and the same `Files[]` (paths and sizes). Every copy of a snapshot's
manifest is the same serialization, so all copies agree. Because
`CreatedAt` is the commit instant in nanoseconds, a later snapshot that
reused the ID is distinguished even when it lists the same files. Retention
acts on a tombstone only with respect to manifests that the tombstone
describes.

The tombstone is the expiry's own record of what the expiry removes. The
expiry writes the tombstone before it deletes anything, and the tombstone
outlasts every manifest. Each later
step, whether in the same run, in a repeated `Expire`, or in `Sweep`,
therefore operates on the tombstone and never on manifests that may already
have been deleted. A tombstone records an intention, not an outcome.
Completion is observable only as the absence of the snapshot's manifests
and objects.
Tombstones are immutable once written, are never removed, and
are the only persisted record of an expiry.

### Head

The head is the snapshot that the `latest` pointer names.

### Referenced Object

A referenced object is a data object path that appears in the `Files[]` of
at least one existing manifest of the dataset.

### Unreferenced Object

An unreferenced object is a data object of the layout whose snapshot ID
names no existing manifest.
A data object of the layout is a path that the layout's grammar parses as
`.../<snapshot ID>/data/<file>` for this dataset: `snapshots/<id>/data/` in
the default layout, `[partitions/<k=v>.../]segments/<id>/data/` in the Hive
layout, and `<dataset>/<id>/data/` in the flat layout. Unreferenced objects
are produced by aborted streams (see PUBLIC_API.md §Cleanup Behavior) and by
expiries interrupted after the tombstone was written.

The unit of reclamation is the **snapshot, not the file**: an object under a
live snapshot's data prefix is never unreferenced, whether or not that
snapshot's `Files[]` lists it. This rule depends on a layout invariant: a
commit writes every data object that it records under its own snapshot ID,
so a manifest never describes an object under another snapshot's prefix. A
manifest that does so (one constructed by hand, or produced outside Lode)
is outside the model, and `Sweep`, which reads only the manifests of
tombstoned snapshots, does not protect such objects. PUBLIC_API.md presents
a file inventory recorded in `Manifest.Metadata` as a correct pattern, so
membership in `Files[]` cannot serve as the test of liveness.

Every other object under the dataset that is not one of its manifests is
outside the scope of retention. Such objects include partition-level
auxiliary files such as `partitions/<k=v>/run.json`, the pointer, and
tombstones. A store listing of the dataset MAY return them, but `Sweep` and
`Expire` MUST NOT delete them. Neither operation reports such an object
unless a manifest names it. Expiry reports a named `FileRef.Path` that
fails the step-4 guard in `Skipped` and never deletes it. Manifests are
deleted only as §Order of Operations prescribes. This scope is a consumer
dependency, not an implementation detail: the Quarry runtime records its
run state in objects outside the scope of retention.

### Caller-Owned Objects

Objects that a caller writes through `Store.Put` outside the layout's data
topology are **caller-owned**. Lode never deletes them. The caller reclaims
them, typically after `Expire` reports as expired the snapshots that
described them. Retention deletes only the manifests and data objects at
paths that the layout produces for a snapshot that retention is expiring,
settling, or reclaiming as unreferenced.

---

## Snapshot Expiry

### Selection

Expiry operates on an explicit set of snapshot IDs. The following
`ExpireRequest` fields resolve to a set:

- `IDs`: explicit snapshot IDs. The set comprises each named ID that has a
  manifest and each named ID that has a valid tombstone. For an ID with a
  valid tombstone, the request completes the recorded expiry from the
  tombstone's inventory (§Order of Operations). A requested ID selects a
  snapshot only if it matches an ID that the dataset's own listing parsed.
  When no manifest is listed, the ID selects a snapshot only if it is in
  the generated form (CONTRACT_CORE §Snapshot ID) and names a tombstone.
  Paths are derived only from IDs selected in this manner. No other value
  is ever converted into a store path, so no requested ID can reach another
  dataset.

  `Partition` and `Before` select from manifests only. While an interrupted
  expiry retains a manifest (a partition manifest after step 2), they select
  the snapshot and complete its expiry from the tombstone like any other,
  reporting it in `Expired`. Once step 3 has removed the last manifest, they
  no longer detect the snapshot, and `Sweep`, or an `Expire` that names its
  ID, completes the expiry. Tombstones are permanent, so selecting from them
  would revisit every past expiry on every run.
- `Partition`: a partition path (Hive layouts only, as CONTRACT_LAYOUT
  states).
  It resolves to every snapshot that has a partition manifest in that
  partition or beneath it. Matching is by **whole segment**: `run_id=run-1`
  selects `run_id=run-1` and `run_id=run-1/event_type=x`, but never
  `run_id=run-10`. This rule deliberately differs from the raw prefix of
  `PartitionListOptions.Prefix` (CONTRACT_READ_API §Partition Prefix): a raw
  prefix is acceptable for listing but never for deletion. The value MUST be
  a canonical relative partition path, with no leading `/`, no `\`, and no
  empty, `.`, or `..` segment. One trailing `/` is permitted and ignored.
  Any other value MUST produce `ErrInvalidPath` before any store call. Every
  manifest path to which the value resolves MUST lie beneath the selected
  dataset's partitions directory before it is used.
- `Before`: an instant. It resolves to every snapshot whose **ID instant**
  is strictly earlier. A snapshot ID begins with the Unix nanosecond instant
  taken when the write begins (CONTRACT_CORE §Snapshot ID), before any data
  object is written. `Manifest.CreatedAt` is assigned at commit, by a clock
  that may have been set back in the meantime, so no ordering between the
  two values is guaranteed. Selection therefore uses the ID alone. Using the
  ID reduces selection to a listing, with no manifest read per candidate. An
  ID not in the generated form is never selected by `Before`.

The fields compose by intersection. A field at its zero value (a `nil` or
empty `IDs`, an empty `Partition`, or a zero `Before`) is **unset** and does
not constrain the set. A request with every field unset MUST return
`ErrInvalidArgument` and expire nothing, because for a destructive
operation the intersection of no constraints must never denote every
snapshot. A request whose fields are set but resolve to no snapshot is not
an error.

### Mixed Snapshots

A snapshot whose `Files[]` extend across partitions both inside and outside
`Partition` is **mixed**. Expiry applies to a whole snapshot or to none of
it, so a mixed snapshot MUST be reported under `Mixed` and MUST NOT be
expired by `Partition` alone. It MAY be expired by a request that names it
in `IDs` and omits `Partition`.

Only the snapshot's own data files are considered (§Order of Operations,
step 4). A `FileRef` outside the dataset's data topology is never deleted,
so it never renders a snapshot mixed. Such a `FileRef` is reported in
`Skipped` only for a snapshot that the run expires. The report lists no
such `FileRef` for a mixed or retained snapshot.

### Head Protection

The head MUST NOT be expired. `Expire` MUST read the pointer **after**
resolving the selection and **before any mutation**. If the resolved set
contains the head, `Expire` removes the head from the set, reports it in
`RetainedHead`, and expires the remainder of the set. Selecting the head
is not an error: a `Before` or `Partition` request against a dataset with no
newer writes always selects it, and failing would render such a request
unusable until a new commit occurs. A caller that requires an entire
partition to be removed inspects `RetainedHead` and repeats the request
after a newer commit.

Only the pointer identifies the head. When the pointer is absent while any
snapshot exists, `Expire` and `Sweep` return `ErrHeadUncommitted` and remove
nothing. A tombstone counts as evidence of a snapshot, because it shows that
snapshots existed even when no manifest remains. A dataset with neither
snapshots nor tombstones requires no pointer. A pointer whose content is not
a snapshot ID in the generated form (CONTRACT_CORE §Snapshot ID) is
corrupt. Retention fails rather than accepting the pointer, even if a
manifest exists at the path that its content would name.

Every commit writes the pointer, so the pointer is absent only in a dataset
written before the pointer was introduced, after a manual edit, or after a
failed `Put` in the replacement of `lode.SingleProcess`. In none
of these cases can retention determine the head safely. `Latest` serves the
largest ID in these cases, but a clock that was set back between two commits
can assign the most recent commit the smaller ID. Any commit creates the
pointer, after which retention proceeds. Without a pointer, `Latest` still
serves the largest ID that is not expired (an interrupted expiry can leave a
partition manifest of an expired snapshot, and that residual manifest is
never served). If the clock was set back, or two commits began in the same
nanosecond, that snapshot is not necessarily the most recent commit, which
is why retention never relies on this selection.

Rationale: the `latest` pointer is the active CAS target for every writer.
Retention MUST NEVER modify the pointer and MUST NEVER remove the manifest
that it names. Either action would expose a parent as the head again or
cause concurrent writers that compare against the current value to fail.

`Expire` MUST repeat the head check **immediately before step 1 for each
snapshot**, not once per `Expire` call. A writer may commit between selection and deletion, and a snapshot
that was listed during selection may have become the head in that
interval. A snapshot that the repeated check identifies as the head is
retained and reported in `RetainedHead` exactly as at the initial check, and
no tombstone is written for it. A snapshot that is already protected is
retained without further steps, but its check still runs. A check that
finds the pointer naming an uncommitted ID terminates `Expire` with
`ErrHeadUncommitted`, regardless of which snapshot the check precedes.

A commit that begins after a snapshot's check is not observed and requires
no coordination, because it can only make a new ID the head. Once a
snapshot has been observed not to be the head, it can never become the head
later. Commits advance the pointer only to a newly created ID, and nothing
else modifies the pointer (`Latest` only creates a missing pointer).
Therefore one check before the tombstone is written precludes the race for
every later step.

While a commit is pending (CONTRACT_CORE §Head Pointer), the pointer names
a snapshot whose manifest has not been persisted. In that state, the
previous head remains the only readable committed snapshot, and the pointer
alone does not identify it. Retention therefore MUST NOT proceed
while the head is uncommitted. The pointer can name an ID without a
manifest, which means no canonical manifest and, for data written before
canonical manifests were introduced, no partition manifest either. `Expire`
then returns `ErrHeadUncommitted` and either expires nothing (at the
initial check) or terminates before step 1 (at a per-snapshot check).
Callers retry once the pending commit completes. A pointer left by an
abandoned commit is replaced by the next commit, because `Latest` never
replaces an existing pointer (CONTRACT_CORE §Head Pointer).

### Order of Operations (per Snapshot)

Before step 1, `Expire` reads the pointer again (§Head Protection). If the
pointer names this snapshot, the snapshot is retained, and none of the
steps runs for it. A pointer that names an uncommitted ID terminates
`Expire` with `ErrHeadUncommitted`.

1. Write the tombstone `expired/<id>`, recording the inventory from the
   manifest.

   Every manifest copy that retention reads MUST first pass manifest
   validation (CONTRACT_ERRORS §3, Manifest Validation Errors). It MUST also
   name the snapshot and dataset under which it was found. An invalid or
   misplaced copy causes the operation to fail before anything is written
   or deleted.

   The write MUST be an atomic conditional create (`CompareAndSwap` from
   absent), and an `ErrSnapshotConflict` result means that the tombstone
   already exists. An existing tombstone MUST then be read back. If it
   names this snapshot ID, it MUST be treated as success, because an earlier
   or concurrent run has already recorded the same expiry. Steps 2 through
   4 then use **that tombstone's** inventory. A tombstone is written once
   and never rewritten, so
   whatever another run created at the path is a complete record to verify,
   not an object to remove.

   The tombstone, whether recorded before this run or about to be written
   by it, MUST describe (§Tombstone) every manifest copy that still loads at
   a path that this expiry would delete. Those paths are the canonical
   path, every copy that the listing found, and every partition copy
   derived from the tombstone's inventory, whether or not the listing found
   any. A tombstone created by a
   concurrent run that won the conditional create MUST describe the manifest
   that this run read. Otherwise the tombstone belongs to an earlier
   snapshot with the same ID, and the operation MUST fail. A snapshot
   selected by its tombstone alone (with no manifest remaining) begins at
   this point, with the read-back. A tombstone that names a different ID is
   corrupt and MUST cause the operation to fail.

   Tombstone content is never empty. `CompareAndSwap` with `expected == ""`
   also matches an existing zero-length object (CONTRACT_STORAGE
   §ConditionalWriter). Replacing such an object is acceptable, because a
   zero-length tombstone contains no information, and every non-empty
   existing tombstone undergoes the read-back validation above. When no
   manifest remains from which to record an inventory, nothing can replace
   a zero-length tombstone. The tombstone is then corrupt and MUST cause the
   operation to fail.
2. Delete the canonical manifest. From this point, `Snapshot(ctx, id)` no
   longer finds the snapshot, and the snapshot is **expired**.
3. Delete every partition manifest of the snapshot: one per partition of the
   inventory's files (only paths that are canonical manifest paths of this
   snapshot), and any other partition manifest that the listing found. From
   this point, the snapshot is absent from enumeration.
4. Delete every data object in the tombstone's inventory whose path is
   canonical, lies under this dataset's data prefixes for the current
   layout, and parses to this snapshot's ID. Every layout places each data
   object under its own snapshot's segment directory (CONTRACT_LAYOUT §Layout
   Ownership, and `dataFilePath` in every layout). Manifests therefore never
   share objects, and the check is a guard rather than a lookup. A
   `FileRef.Path` can fail the guard by lying in a foreign dataset, by
   carrying a different snapshot ID, or by having a legacy or malformed
   path. Any such path MUST be left in place and reported under `Skipped`.
   The expiry nevertheless completes for the remaining objects.

Each step MUST be idempotent, so a step that finds its target already
absent succeeds. If the process terminates after step 1, the snapshot
remains readable and consistent (expiring), unless it never had a canonical
manifest (§Expiring Snapshot). Should the process terminate after step 2 or
a later step, the snapshot is expired, and its remaining manifests and objects
are reclaimed by repeating `Expire` with the same ID or by `Sweep` (below).
Both operate on the tombstone, so neither depends on the survival of a
manifest.

Manifests are deleted **before** data, so that any manifest loaded **after**
the manifest deletion describes only objects that still exist. The order
does not protect a reader that loaded the manifest *before* the expiry began
and retrieves its files afterward. `Read` loads the manifest and the files
in separate store calls without a lease, so such a read MAY fail with
`ErrNotFound` on a file. This failure is the documented consequence of
expiring a snapshot that a reader is concurrently using, in the same class
as a reader retaining an expired ID. Lode provides no read lease, and
callers that require one coordinate retention with their readers.

Rationale: writing the tombstone first makes an expiry observable before
anything is destroyed. It also makes an interrupted expiry resumable rather
than leaving silent orphans. Deleting manifests before data keeps every
visible snapshot internally consistent.

### Lineage

A child's `ParentSnapshotID` MAY reference an expired ID. Readers resolving
lineage MUST treat an expired parent as the end of the explainable chain,
not as corruption.

`Snapshot(ctx, id)` and `Read(ctx, id)` on an expired ID MUST return
`ErrSnapshotExpired`, a distinct sentinel for which
`errors.Is(err, ErrNotFound)` MUST also hold, so that existing callers
continue to work. The check costs one additional `Get` on the tombstone
path and occurs only when the canonical manifest is absent, which preserves
the warm-path bound in CONTRACT_COMPLEXITY. It MUST run **before** any
fallback lookup (`history.find`, which scans partition manifests): between
steps 2 and 3 a Hive snapshot still has partition manifests, and without
this ordering the fallback would restore an expired snapshot. During the
expiring interval (tombstone present, canonical manifest present),
`Snapshot` returns the snapshot normally.

`Snapshots()`, `ListManifests`, and `ListPartitions` MUST NOT return expired
snapshots once step 3 has completed. They MAY return an expiring snapshot or
one whose partition manifests have not all been deleted. Every manifest that
they return is complete, and all of its files exist, with one exception.
That exception is a partition manifest that a pending commit wrote after
its snapshot was expired (§Concurrency), which the next `Sweep` or `Expire`
removes.
Implementations MUST NOT read tombstones during enumeration.

Once retention is in use, deletion is a normal event, so enumeration MUST
tolerate it. A manifest that a listing returned and that is absent when
loaded (`ErrNotFound`) was expired in the interval, and it MUST be skipped
rather than reported as a failure. This rule applies to `Snapshots()`, the
scan behind `Latest()`, and every listing operation of a handle or view
that loads listed manifests. Any other load error still causes the operation to fail.

### Concurrency

Expiry of a snapshot other than the head does not modify the `latest`
pointer and does not require CAS. Concurrent writers are unaffected. A
concurrent reader that retains an expired ID observes `ErrSnapshotExpired`
on its next access, which is the documented and expected outcome.

Two concurrent expiries of the same ID are safe, because every step is
idempotent and tombstone creation is atomic. With `lode.SingleProcess`,
this safety holds only for expiries within the process that shares the
wrapped store.

A commit persists its partition manifests after the pointer and the
canonical manifest (CONTRACT_CORE §Head Pointer). A later commit can
advance the pointer while an earlier commit is still writing partition
manifests. An `Expire` that selects the earlier snapshot can then remove
it, and the slower writer can add a partition manifest after the expiry.
The tombstone describes that manifest, so the next `Sweep` or `Expire` of
the ID removes it. Until then, `ListManifests` and `ListPartitions` can
return the manifest, and reading it fails because the data is absent. This is the single exception to listing completeness that
retention introduces. Retention imposes no lock against pending commits:
selecting a snapshot more recent than the slowest commit is the caller's
decision, and a `Before` value with a margin avoids the situation.

---

## Unreferenced-Object Sweep

### Definition

`Sweep` operates on two listings taken at the start of the run: the
tombstones under `expired/`, and the dataset's snapshot prefix (every
manifest and data object of the layout). It then performs the following
steps in order:

1. **Complete expiries.** A tombstone is an object directly under
   `expired/` whose name is a snapshot ID in the generated form
   (CONTRACT_CORE §Snapshot ID). Any other object there, such as the
   filesystem store's `.lock` companions (CONTRACT_STORAGE §Implementation
   Notes), is not a tombstone and is neither reported nor deleted.

   `Sweep` loads each tombstone and verifies that it is non-empty, that it
   records the snapshot ID in its path, and that it contains the manifest's
   `CreatedAt` and an inventory. A tombstone that fails validation MUST be
   reported under `Unparseable` and MUST NOT cause any snapshot to be
   settled. A snapshot of which the listing shows neither a manifest nor a
   data object has nothing for this run to remove. Once its tombstone has
   been loaded and validated like any other, it incurs no further store
   call, it is not reported, and none of its objects is deleted.

   A listing MAY omit a manifest, if the listing lags or the manifest was
   written after it. For any other snapshot, `Sweep` MUST therefore load
   the manifest at every path that the expiry would delete, whether or not
   the listing showed it. Those paths are the canonical path, every listed
   copy, and every copy derived from the tombstone's inventory and from the
   snapshot's listed data objects. The tombstone MUST describe (§Tombstone)
   every copy that loads. Otherwise, the tombstone belongs to an earlier
   snapshot with the same ID. It MUST then be reported under `Unparseable`
   and MUST NOT cause the snapshot to be settled or its objects to be
   reclaimed. For every validly tombstoned ID,
   `Sweep` performs steps 2 and 3 of §Order of Operations on those paths.

   `Sweep` *settles* the snapshot only after it confirms that **every**
   manifest of the snapshot is absent. `Settled` reports the snapshots whose manifests this
   run found and deleted (or, in a dry run, would delete), so a snapshot
   settled by an earlier run is not reported again. Like the counts
   (§Accounting), `Settled` is an observation, not an assertion: concurrent
   runs can both find and delete the same manifest and both report the
   snapshot. If any manifest deletion fails, the snapshot is **not**
   settled: it retains its surviving manifests, and none of its objects is
   reclaimed in this run. A manifest that can still be loaded always
   protects the objects that it describes.
2. **Reclaim objects.** `Sweep` groups the listed data objects by the
   snapshot ID in their path. A group is:
   - **live** when its snapshot ID still has a manifest. A live group is
     never modified. A group that the listing shows without a manifest is
     also live when a manifest exists at its canonical path or at a
     manifest path derived from its objects' paths. `Sweep` MUST examine
     those paths before reclaiming an orphaned group, because a listing may
     omit a manifest. A commit writes a manifest copy in every partition
     that holds its data (including legacy snapshots with partition
     manifests only), so every listed object yields a path at which its
     snapshot's manifest would be found.
   - **held** when its snapshot's tombstone was rejected in step 1 (because
     it is invalid or contradicted by a manifest copy) or its snapshot is
     protected. A held group is never modified, even after the grace period,
     because a tombstone that Lode cannot rely on indicates nothing about
     whether the objects are residual.
   - **settled** when its snapshot was settled in step 1. A settled group is
     deleted irrespective of the grace period.
   - **orphaned** in every other case (a snapshot that never committed). An
     orphaned group is deleted only when its ID instant is older than the
     grace period.
3. **Restore partition indexes.** On a partitioned layout, the partition
   manifests of a snapshot are an index of its canonical manifest, which a
   writer persists after the commit is complete (CONTRACT_CORE §Head
   Pointer). A writer that terminates before persisting the partition
   manifests leaves a committed snapshot
   that a listing filtered by partition, and an `Expire` by `Partition`,
   cannot find. `Sweep` restores the index of a snapshot when all of the
   following hold:
   - The listing shows its canonical manifest, and no tombstone, valid or
     not, names it.
   - Its ID instant is older than the grace period, so that its writer has
     finished (§Grace Period).
   - The listing shows one of its data objects in a partition in which it
     shows no manifest of the snapshot. A snapshot whose index the listing
     shows complete incurs no store call.

   `Sweep` then loads the canonical manifest, which MUST be valid and name
   the snapshot of this dataset. It creates an unchanged copy of the
   manifest at the path of every partition that holds one of the snapshot's
   own data objects and no copy. A FileRef outside those partitions names
   no partition (§Mixed Snapshots). The canonical manifest, not the
   listing, decides the partitions. A copy that already exists is left as
   it is and is not counted.

   An `Expire` that selects the snapshot can delete its manifests while
   `Sweep` creates them. `Sweep` therefore checks for a tombstone after it
   creates the copies, even when a later write fails. If a tombstone now
   records the snapshot, `Sweep` deletes the copies again. An expiry writes
   its tombstone before it deletes any manifest (§Order of Operations), so a
   copy created after the expiry deleted the partition manifests is always
   removed. `Reindexed` reports the snapshots for which this run created at
   least one copy that may remain (on a dry run, would create one), oldest
   first, including a snapshot whose repair then fails. Only the copies
   that the listing omits are written.

Reclamation operates per snapshot (§Unreferenced Object). A settled
snapshot's objects are all objects under its data prefixes, including any
that its inventory does not list (partial objects of an aborted stream, or
caller objects inside its data prefix). The inventory determines only how
the objects are counted (§Accounting), never whether they are deleted, with
one addition. For a snapshot of which the listing shows a manifest or an
object, an object that the inventory lists but the listing omitted is also
deleted. Because it was not listed, it is not counted. A snapshot of which
the listing shows nothing is not examined, so a later run whose listing
shows its residual objects removes them. Examining every tombstone's
inventory on every run would incur an unbounded cost.

`Sweep` reads the pointer once, before step 1, and never settles the
snapshot that the pointer names. While the pointer names a snapshot without
a manifest, a commit is pending, and the head is undetermined. No
tombstoned snapshot is then settled in that run, but orphaned objects older
than the grace period are still reclaimed. Without a pointer, `Sweep`
refuses a dataset whose listing shows a manifest or a tombstone, with
`ErrHeadUncommitted` (§Head Protection). A dataset whose listing shows
neither has no head, and only orphaned objects remain to be reclaimed. Objects of the committed head
that the pointer names are never orphaned, even when the listing omitted its
manifests. An abandoned commit that the pointer still names is not
committed, so its data ages like any other orphaned data.

`Expire` writes a tombstone only for a snapshot observed not to be the head
(§Head Protection). This protection therefore never applies to tombstones
that Lode wrote. It prevents a misplaced object under `expired/` from ever
removing the head. A protected tombstoned snapshot remains unsettled, and
its objects remain live.

The data objects of a pending commit have no manifest yet and are
therefore orphaned under the definition above. The grace period, not the
pointer, protects them, so a pointer that names an uncommitted ID does not
prevent `Sweep` from running. An absent pointer while snapshots exist does
prevent it (§Head Protection): `Sweep` then returns `ErrHeadUncommitted`, as
`Expire` does.

### Grace Period

An orphaned object MUST NOT be deleted unless it is older than
`SweepRequest.Grace`. Its age is the current time minus the instant encoded
in the snapshot ID in its path (§Selection, `Before`). Because every data
path carries its creation instant, the store needs no `Stat` capability. A writer's clock can be behind the clock of the host
running `Sweep`, whether from skew or from a clock that was set back. Such
a writer records instants that already appear old, which reduces the
effective grace period by the difference. `Grace` MUST exceed the duration
of the longest expected write plus the largest clock difference between any
writing host and the host running `Sweep`. Otherwise, the objects of a
write in progress can be reclaimed before its manifest is persisted.

Rationale: a write in progress has written data objects but not yet its
manifest, so the grace period MUST exceed the duration of the longest
expected write. `Grace` has no default: it MUST be strictly positive, and
a zero or negative value returns `ErrInvalidArgument` before any listing or
mutation. A zero grace period would make every unreferenced object
immediately eligible for deletion, which is never intended. Twenty-four
hours is a conservative initial value.

An orphaned group whose snapshot ID is not in the generated form has no
determinable age: its object paths MUST be reported under `Unparseable` and
MUST NOT be deleted. Objects that do not parse as data objects of the
layout are outside the scope of `Sweep` (§Unreferenced Object) and are
neither reported nor deleted.

### Interaction with Expiry

Objects of a **settled** snapshot are reclaimed irrespective of the grace
period, because the tombstone records the intention and no manifest
describes them any longer. The surviving manifests of an unsettled
tombstoned snapshot protect its objects until a later run settles it. This
property makes an interrupted expiry recoverable without ever deleting data
to which a reader could still be directed.

---

## Dry Run

`ExpireRequest.DryRun` and `SweepRequest.DryRun` perform all validation,
listing, and resolution but no write or deletion, and they return the report
that a real run would return. A dry-run `Expire` still reads the pointer, so
`RetainedHead` and `ErrHeadUncommitted` are those that a real run would
produce. A dry-run `Sweep` reads the pointer in the same manner. It treats
every validly tombstoned snapshot other than a protected one as settled, so
it counts the snapshot's objects exactly as a real run would after the
first step. In step 3 it checks with `Exists` whether each copy is absent rather
than creating it.

---

## Reports

```text
type ExpireReport struct {
    Expired      []DatasetSnapshotID // expired (on a dry run, to be expired), oldest first
    Mixed        []DatasetSnapshotID // selected by Partition but with files outside it; retained
    RetainedHead DatasetSnapshotID   // the head, when the selection included it; "" otherwise
    Objects      int                 // data objects deleted (on a dry run, to be deleted)
    Bytes        int64               // the recorded sizes of those objects
    Skipped      []string            // inventory paths that failed the step-4 guard; left in place
}

type SweepReport struct {
    Settled     []DatasetSnapshotID // tombstoned snapshots whose manifests this run removed (on a dry run, would remove), oldest first
    Reindexed   []DatasetSnapshotID // committed snapshots whose missing partition manifests this run created (on a dry run, would create), oldest first
    Objects     int                 // data objects deleted (on a dry run, to be deleted)
    Bytes       int64               // the recorded sizes of those that a tombstone inventory lists
    Unsized     int                 // of Objects, those without a recorded size
    Orphans     int                 // of Unsized, those of snapshots that never committed
    Unparseable []string            // invalid or contradicted tombstones; orphaned objects without an ID instant
}
```

### Accounting

Both reports measure the objects that the run deletes as its listing
presented them, so a resumed expiry is never counted twice. `Store.Delete`
does not indicate whether it removed anything, so the counts are only as
exact as the listing:

- `Objects` counts the data objects that the run deleted and that its
  listing showed to be present, each counted once however often an
  inventory lists its path. An object already deleted (by an earlier,
  interrupted run) is not counted again. Concurrent runs can both list an
  object and both count it, and an object that a lagging listing omitted is
  deleted but not counted. An object whose `Delete` returned an error is not
  counted, although the store may have removed it, and a later run whose
  listing no longer shows it does not count it either. Apart from these
  cases, sequential runs over consistent listings sum exactly.
- `Bytes` is the sum of the sizes recorded for those objects in the
  tombstone's inventory. Every object that `Expire` deletes is in the
  inventory, so its `Bytes` is exact.
- `Unsized` (`Sweep` only) counts deleted objects without a recorded size:
  orphaned objects, whose write never committed, and objects in a settled
  snapshot's data prefix that its inventory does not list. Lode never
  recorded their sizes, and the `Store` interface provides no size query.
  Adding one is outside the scope of this contract. Such objects are
  counted, never estimated.

Both methods return a non-nil report, including on error. On error, the
report describes the work completed before the error. After a retryable
failure (a store error or an uncommitted head), the dataset is left in a
state that `Sweep`, or an `Expire` that names the affected IDs, completes.
Corruption is not retryable. An invalid or contradicted tombstone, or an
invalid manifest, causes `Expire` to fail whenever it selects the snapshot.
`Sweep` reports the corrupt objects that it examines (tombstones, and the
manifests that it loads while settling them) on every run until an
operator repairs them. `Sweep` does not load the manifests of live
snapshots and does not report their corruption. Lode never rewrites or
removes a tombstone to recover.

---

## Errors

| Error | Source | Condition |
|-------|--------|-----------|
| `ErrHeadUncommitted` | `Expire` | The pointer names a snapshot without a manifest (a pending commit). At the initial check, nothing is expired. At a per-snapshot check, the snapshots already expired remain expired and are reported. |
| `ErrHeadUncommitted` | `Expire`, `Sweep` | The pointer is absent while snapshots exist (§Head Protection). `Sweep` terminates before deleting anything. |
| `ErrInvalidArgument` | `Expire`, `Sweep` | An `ExpireRequest` with every selector field unset, or a `SweepRequest.Grace` that is zero or negative |
| `ErrInvalidPath` | `Expire` | `Partition` is not a canonical relative partition path |
| `ErrPartitionPrefixNotSupported` | `Expire` | `Partition` specified on a layout without partitions. `PartitionListOptions.Prefix` shares this error, which is distinct from the `StreamWriteRecords` error `ErrPartitioningNotSupported`. |
| `ErrSnapshotExpired` | `Snapshot`, `Read` | An ID with a valid tombstone **whose canonical manifest is absent**. The error also matches `ErrNotFound`. An expiring snapshot (tombstone present, canonical manifest present) is returned normally. An empty object at the tombstone path is not a tombstone. Any other invalid tombstone is reported as an error, never as an expiry. |

Selecting the head is not an error (§Head Protection). Every deletion
propagates store errors, wrapped with the object path.

---

## Complexity Bounds

N is the number of keys that the operation lists, K the number of
snapshots that `Expire` selects, T the number of tombstones, and D the
number of objects deleted (manifests and data alike).

| Operation | Store Calls | Memory |
|-----------|-------------|--------|
| `Expire` | The sum of the terms below | O(N + K × manifest) |
| `Sweep` | The sum of the terms below | O(N + Σ tombstone inventories + the manifest copies of one snapshot at a time) |
| `Snapshot(id)`, `Read(id)` expired path | +1 `Get` (tombstone) | O(1) |

The store calls of `Expire` are the sum of these terms:
- 1 `List` of the `Partition` subtree, or else of the snapshot prefix.
- K + 1 pointer `Get` calls, each followed by at most one `Exists`. When
  the named snapshot has no canonical manifest, a pointer `Get` is also
  followed by one `List` of the snapshot prefix, to find a legacy copy that
  exists only in a partition.
- K manifest `Get` calls.
- K tombstone `Get` calls, for the recorded inventory, if any, and also one
  per requested ID without a manifest.
- 1 `Get` per manifest path of each selected snapshot, because every copy is
  verified against the tombstone before the tombstone is written.
- K tombstone writes, each followed by at most 1 read-back `Get` when the
  tombstone already exists.
- Σ(manifests + files) `Delete` calls.
- When the pointer is absent, 1 `List` of the whole snapshot prefix (even
  for a `Partition` request), to distinguish an empty dataset from a
  refused one. When that listing shows no manifest, 1 further `List` of
  `expired/`.

The store calls of `Sweep` are the sum of these terms:
- 2 `List` calls (of the tombstones and of the snapshot prefix), 1 pointer
  `Get`, and T tombstone `Get` calls.
- 1 `Get` per manifest path of each tombstoned snapshot of which the
  listing shows any object.
- 1 `Exists` per manifest path of each group older than the grace period
  without a listed manifest, whether or not the group is then reclaimed.
  The checks of a group stop at the first manifest found.
- D `Delete` calls.
- The repair of a partition index. It applies to each committed snapshot
  older than the grace period whose listed data lies in a partition without
  a listed manifest of it. The repair costs 1 `Get` of the canonical
  manifest, and 1 `Put` (on a dry run, 1 `Exists`) per partition copy that
  the snapshot's files name and the listing omits. Once a copy is created,
  1 tombstone `Get` follows.
- When the pointer names a snapshot, 1 `Exists`, and 1 `List` when the
  snapshot has no canonical manifest, to determine whether it is
  committed.

Pointer `Get` calls: one initial read, plus one further read per selected
snapshot, immediately before its tombstone is written or before it is
retained (§Head Protection).

`Expire` and `Sweep` are cold-path administrative operations whose cost is
proportional to the depth of the history. Callers MUST NOT run them on hot
paths.

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

Expire(ctx context.Context, req ExpireRequest) (*ExpireReport, error)
Sweep(ctx context.Context, req SweepRequest) (*SweepReport, error)
```

Retention is the `Retention` interface, which the writer handles
(`Records[T]`, `Blobs`) embed. It requires the dataset's store, paths, and
pointer, which a writer handle already holds. Views remain read-only:
removal is a write.

Requests are structs, not `Option` values. A dry run is a field visible at
the call site, so an omitted option can never convert a preview into a
deletion, and options remain limited to opening (V1_API_DECISIONS D3).

---

## Boundaries

1. **Empty partition trees.** Lode deletes objects, not directories. On the
   filesystem store, directories that retention empties remain.
2. **The only snapshot.** The head is never expired, including when it is
   the only snapshot. A dataset is removed entirely only by deleting its
   prefix through the store.
3. **Caller-owned objects.** The caller reclaims them (§Caller-Owned
   Objects). A consumer that maintains auxiliary objects outside the data
   topology deletes them itself after `Expire` reports their snapshots as
   expired. Objects inside a snapshot's data prefix are reclaimed with that
   snapshot.

---

## Design Invariant

> **Nothing that a manifest still describes is removed, and nothing that a
> manifest described is removed without a record of its removal.**

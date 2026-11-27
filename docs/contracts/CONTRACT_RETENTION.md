# Lode Retention Contract

This document defines the contract for **snapshot expiry** (`Expire`) and the
**unreferenced-object sweep** (`Sweep`) on dataset writer handles
(`*Records[T]` and `*Blobs`).

---

## Goals

1. Bound dataset storage without violating Lode's layout or commit invariants.
2. Keep every visible snapshot self-consistent before, during, and after
   deletion.
3. Make removal explicit, caller-initiated, resumable, and previewable via
   `DryRun`.

---

## Non-Goals

- Retention **policy** (which snapshots to keep or how often to run)—callers
  own policy and scheduling.
- Background execution, automatic compaction, or rewriting of surviving data
  objects.
- Single-snapshot Volume expiry (volumes are cumulative and are removed whole
  via `volume.Remove`; see [`CONTRACT_VOLUME.md`](CONTRACT_VOLUME.md)
  §Removal).

---

## Relationship to Immutability

Retention removes objects without mutating committed state:
- Surviving manifests and data objects are **never rewritten**.
- Expiry records its intent in a create-only **tombstone** object before
  deleting any manifest or data object, and tombstones are never modified or
  deleted once recorded.
- `Sweep` repairs missing Hive partition manifests by copying a committed
  snapshot's canonical manifest byte-for-byte via create-only `Put`.

---

## Conceptual Model

### Tombstone

A tombstone is a create-only JSON object at `<dataset dir>/expired/<snapshot_id>`
recording:
- `snapshot_id` (`SnapshotID`),
- `requested_at` (UTC timestamp when expiry was initiated),
- `manifest_created_at` (copied from `Manifest.CreatedAt`), and
- `objects` (`[]ObjectRef`, copied from `Manifest.Objects`).

Backward compatibility: tombstones written before format `1.2.0` recorded the
inventory under `"files"`; Lode unmarshals both `"objects"` and `"files"`
into `Objects`.

A tombstone **describes** a manifest when both have equal `CreatedAt` and
identical `(Path, SizeBytes)` object inventories. Retention acts on a
tombstone only when every surviving manifest copy of that snapshot ID is
described by the tombstone.

### Snapshot Lifecycle States

1. **Live**: manifests and data objects exist; no tombstone exists.
2. **Expiring**: tombstone exists and canonical manifest still exists. The
   snapshot remains readable via `Snapshot` and `Read`.
3. **Expired**: tombstone exists and canonical manifest is deleted.
   `Snapshot`, `Read`, and `WithAnchor` return `ErrSnapshotExpired`, which
   also matches `ErrNotFound`.
4. **Settled**: all canonical and partition manifests are deleted, and all
   owned data objects are deleted; only the tombstone remains.

### Scope of Reclamation

- Reclamation operates **per snapshot**: an object under `<snapshot_id>/data/`
  is live while any manifest of `<snapshot_id>` exists, and eligible for
  deletion only once `<snapshot_id>` is settled (expired) or orphaned past
  `SweepRequest.Grace`.
- Objects outside the layout's manifest and `<snapshot_id>/data/<file>` paths
  (such as `<dataset dir>/latest`, `<dataset dir>/expired/*`,
  `<dataset dir>/staged/*`, and caller sidecar files like
  `partitions/<k=v>/run.json`) are never deleted by `Expire` or `Sweep`.
- If a manifest's `Objects` contains a path outside `<snapshot_id>/data/` of
  this dataset, `Expire` leaves that path untouched and reports it in
  `ExpireReport.Skipped`.

---

## Snapshot Expiry (`Expire`)

```go
type ExpireRequest struct {
    IDs       []SnapshotID
    Partition string
    Before    time.Time
    DryRun    bool
}

type ExpireReport struct {
    Expired      []SnapshotID
    Mixed        []SnapshotID
    RetainedHead SnapshotID
    Objects      int
    Bytes        int64
    Skipped      []string
}
```

### Selection

Set fields on `ExpireRequest` compose by intersection; a request with all
three selectors unset (`len(IDs) == 0 && Partition == "" && Before.IsZero()`)
MUST fail with `ErrInvalidArgument` before any store call:
- `IDs`: selects listed snapshots matching these IDs, plus (when `Partition`
  and `Before` are unset) any generated-form ID whose manifest is already
  gone but whose tombstone exists so interrupted expiries can be resumed by
  ID.
- `Partition`: selects snapshots with a partition manifest in `Partition` or
  beneath it, matched by **whole path segment** (`run_id=r1` matches
  `run_id=r1` and `run_id=r1/type=a`, never `run_id=r10`). Non-canonical
  partition paths MUST return `ErrInvalidPath` before any store call;
  specifying `Partition` on an unpartitioned dataset MUST return
  `ErrNotPartitioned` before any store call.
- `Before`: selects snapshots whose generated `SnapshotID` instant is
  strictly before `Before` (evaluated from keys without loading manifests).

### Mixed Snapshots

When `Partition` is set and a selected snapshot owns data objects outside
`Partition`, `Expire` MUST NOT expire the snapshot and MUST report its ID in
`ExpireReport.Mixed`, unless the head guard protects it, when §Head Protection
applies instead.

### Head Protection

- `Expire` MUST NEVER expire a head snapshot that one of its reads of
  `latest` names. Selecting a head is not an error: the head is retained,
  unless its own recheck of `latest` fails, as with `ErrHeadUncommitted`.
  `ExpireReport.RetainedHead` names the last candidate that the call
  retained as a head.
- `Expire` validates `latest` before it decides any candidate. On a live
  run, a candidate that proceeds to a tombstone or a deletion has `latest`
  read again after its manifest copies are validated and before that write,
  and the snapshot that read names is never deleted. A candidate that the
  head guard already protects has `latest` read again as it is retained,
  also when it is mixed. Another mixed snapshot is reported without another
  read. The read and the
  deletion are not atomic: a commit between them, as of a snapshot ID that
  collides with the candidate's (`CONTRACT_CORE.md` §Snapshot ID), is not
  protected. A dry run deletes nothing and may keep its first observation of
  `latest` for the call (§Dry Run and Accounting).
- If `latest` holds `lode:removed`, `Expire` returns `ErrRemoved`.
- If `latest` names a snapshot without a committed manifest (a pending or
  abandoned commit), or is absent while manifests or tombstones exist,
  `Expire` returns `ErrHeadUncommitted`.

### Order of Operations (per Candidate)

1. Validate all surviving manifest copies (`ErrManifestInvalid` on failure)
   and verify or atomically create `<dataset dir>/expired/<id>` via
   `CompareAndSwap` (`expected == ""`). If the tombstone already exists, read
   it back and verify it describes the same snapshot.
2. Delete the canonical manifest (the snapshot is now **expired** and added
   to `ExpireReport.Expired`).
3. Delete all partition manifests of the snapshot.
4. Delete all owned data objects in the tombstone's inventory (recording
   foreign/malformed inventory paths in `Skipped`, and counting deleted
   objects present in the initial listing in `Objects` and `Bytes`).

---

## Unreferenced-Object Sweep (`Sweep`)

```go
type SweepRequest struct {
    Grace  time.Duration
    DryRun bool
}

type SweepReport struct {
    Settled     []SnapshotID
    Reindexed   []SnapshotID
    Objects     int
    Bytes       int64
    Unsized     int
    Orphans     int
    Unparseable []string
}
```

`SweepRequest.Grace` MUST be strictly positive (`ErrInvalidArgument` before
any store call otherwise) and SHOULD exceed the longest expected write plus
maximum writer clock skew.

`Sweep` lists `<dataset dir>/expired/` and the snapshot prefix, checks
`latest` (`ErrRemoved` if fenced with `lode:removed`; `ErrHeadUncommitted` if
absent alongside manifests or tombstones), and executes three passes:

1. **Settle interrupted expiries**: validate each tombstone (reporting
   corrupt or contradicted tombstones in `Unparseable` and holding their
   snapshots). Unless held by a pending head or naming the committed head,
   delete any remaining canonical and partition manifests and record the
   snapshot in `Settled` when this run removed at least one manifest.
2. **Reclaim data objects**: delete data objects of settled snapshots
   (regardless of `Grace`, counting sizes from tombstone inventories in
   `Bytes` and unlisted objects in `Unsized`), and delete data objects of
   never-committed snapshots older than `Grace` after confirming no manifest
   exists at their canonical or partition manifest paths (counting them in
   `Objects`, `Unsized`, and `Orphans`). Orphaned objects whose snapshot IDs
   lack a parseable timestamp are kept and reported in `Unparseable`.
3. **Reindex Hive partitions**: for live committed Hive snapshots older than
   `Grace` whose canonical manifest is listed without a corresponding
   partition manifest for one or more data objects, copy the canonical
   manifest to each missing partition manifest path via create-only `Put`
   (re-checking for concurrent expiry afterward) and report the snapshot in
   `Reindexed`.

---

## Dry Run and Accounting

- With `DryRun: true`, `Expire` and `Sweep` write and delete no object. A dry
  run is a validated preview of what the call observes: it validates every
  manifest and tombstone that it uses, as a live run does, and returns the
  selection and accounting that a live run would produce if storage did not
  change during the call. It may reuse an observation made earlier in the
  call, such as the head that it validated first. A concurrent commit or
  expiry can change what a later live run meets.
- A live `Expire` keeps every check before its destructive steps: it
  protects the head that its last read of `latest` for each candidate
  names,
  establishes a matching tombstone before it deletes anything, and
  validates an existing or competing tombstone against every manifest copy
  that it would delete.
- Both methods ALWAYS return a non-nil report, including when returning an
  error, reflecting all work completed prior to the error.

---

## Boundaries

1. **Empty directories**: filesystem directories emptied by object deletion
   remain on disk.
2. **Whole-dataset removal**: `Expire` always preserves the head snapshot that
   it observes (§Head Protection). To
   remove an entire dataset—including its head snapshot—call
   `Root.RemoveDataset(ctx, id, opts...)`
   ([`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md) §Dataset Removal).
3. **Caller-owned sidecar objects**: objects outside `<snapshot_id>/data/`
   remain the caller's responsibility.

---

## Errors Summary

| Error | Source | Condition |
|-------|--------|-----------|
| `ErrInvalidArgument` | `Expire`, `Sweep` | Empty `ExpireRequest` selectors, or `SweepRequest.Grace <= 0` |
| `ErrInvalidPath` | `Expire` | Non-canonical `ExpireRequest.Partition` |
| `ErrNotPartitioned` | `Expire` | Non-empty `ExpireRequest.Partition` on an unpartitioned dataset |
| `ErrHeadUncommitted` | `Expire`, `Sweep` | Pending head without manifest (`Expire`), or missing `latest` alongside snapshots/tombstones (`Expire`, `Sweep`) |
| `ErrRemoved` | `Expire`, `Sweep` | Dataset fenced with `lode:removed` |
| `ErrSnapshotExpired` | `Snapshot`, `Read`, `WithAnchor` | Tombstone exists and canonical manifest is absent (also matches `ErrNotFound`) |

---

## Design Invariant

> **Nothing that a manifest still describes is removed, and nothing that a
> manifest described is removed without a record of its removal.**

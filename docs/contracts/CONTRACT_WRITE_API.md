# Lode Write API — Contract

This document defines the required semantics for the write-facing API.
It is authoritative for any `Dataset` implementation.

---

## Goals

1. Deterministic snapshot creation.
2. Explicit metadata on every write.
3. Linear snapshot history.
4. Safe, immutable commits.

---

## Non-Goals

- Query execution or filtering.
- Background compaction.
- Distributed coordination, consensus, or lock management.

---

## Write Semantics

`Records[T]` writes records of type `T`; `Blobs` writes one blob per
snapshot. Both are opened at a root (`Root.Records`, `Root.Blobs`), which
checks the writer's settings against the dataset's head (§Settings).

### Required Behavior

- `Write(ctx, data, metadata)` MUST create a new snapshot on success.
- Metadata MUST encode as a JSON object (a struct, a map, or `nil`); a
  value that does not MUST be rejected with `ErrInvalidArgument` before any
  store call. `nil` metadata MUST be persisted as `{}`.
- Empty metadata is valid and MUST be persisted explicitly.
- The new snapshot MUST reference the previous snapshot, if any, as its parent.
- Writes MUST NOT mutate existing snapshots or manifests.
- The manifest MUST include all required fields defined in `CONTRACT_CORE.md`
  (including the row or event count and, when applicable, the minimum and
  maximum timestamps).
- When the codec implements `StatisticalCodec`, per-file statistics MUST be
  collected after encoding and recorded on the `FileRef`.
- A blob write represents a single data unit, and the row or event count
  MUST be `1`.

### Settings

Every writer of a dataset MUST share the dataset's settings: the codec name
and description (`Codec.Describe`), the compressor, the checksum
algorithm, the partitioner and its keys, and the path names, as the
manifest records them (CONTRACT_CORE §Manifest Requirements).

- When it opens, and again before its first commit, a writer MUST compare
  its settings with one manifest, found in this order: the manifest of the snapshot that the
  pointer names; that snapshot's staged copy (CONTRACT_CORE §Head Pointer);
  and, when the pointer is absent or names neither, the newest manifest, as
  `Latest` selects it. With no pointer and no manifest, the dataset is
  empty, and the commit records the writer's settings, unless its
  directory holds a manifest that the writer's layout does not persist.
- A dataset directory that holds such a manifest, when no manifest is found
  under the writer's layout, contains a history persisted under another
  layout. The writer MUST be rejected with `ErrSettingsMismatch` rather
  than begin a second history beside it. A manifest is recognized by its
  file name, without reading any object: the writer's manifest name, or
  the default name, which every manifest persisted before the names became
  root options bears. A history persisted under two other custom names
  whose pointer is absent is not recognized; Lode persists a pointer with
  every commit, so only a pointer removed by hand produces that state. Data
  objects are not a history:
  another writer persists its data before its first commit, and two first
  writers with different layouts would otherwise reject each other before
  either commits. A dataset under another datasets directory (the flat
  layout's `<id>` rather than `datasets/<id>`) is another dataset
  (V1_API_DECISIONS D11).
- A writer whose settings differ MUST be rejected with
  `ErrSettingsMismatch` and MUST NOT commit. The rejection is not retried.
- A pointer that names a snapshot with neither a manifest nor a staged copy,
  in a dataset with no other manifest, belongs to a first commit whose
  settings are unknown. The writer MUST return `ErrSnapshotConflict` rather
  than commit unchecked settings; only a first commit persisted before
  format version 1.1.0, or a pointer edited by hand, produces that state.
- Two first commits with conflicting settings are serialized by the
  pointer's `CompareAndSwap`: the losing writer is checked against the
  winner's manifest after it refreshes the head, and is rejected. A writer
  never checked against a manifest whose commit ends in a conflict is
  classified this way whatever its retry budget: the conflict is returned
  only when the winner's settings match.
- A writer checks until a check against a manifest of format version
  1.1.0, or its own commit, settles it. Every writer is checked, so every
  later snapshot records the same settings. A check against an earlier
  manifest compares fewer settings, so a writer checks again after a
  conflict.
- A manifest written before format version 1.1.0 records no partition keys.
  For such a dataset with a Hive partitioner, the writer's keys MUST be
  verified, by name and in order, against the partition path of the first
  snapshot with files, found by following parents from the head. A chain
  without files back to its root is consistent with any keys; a chain that
  ends at a missing or expired parent, or repeats a snapshot, before a
  snapshot with files cannot be verified, and the write MUST be rejected
  with `ErrSettingsMismatch`.
  Lode MUST NOT infer keys from partition paths: the writer states them,
  and Lode verifies them.

### Blobs.StreamWrite Semantics

- `Blobs.StreamWrite(ctx, metadata)` MUST persist `nil` metadata as `{}`.
- `Blobs.StreamWrite` MUST return a `StreamWriter` for a single binary data
  unit.
- `StreamWriter.Commit(ctx)` MUST write the manifest and return the new snapshot.
- A snapshot MUST NOT be visible before `Commit` writes the manifest.
- `StreamWriter.Abort(ctx)` MUST ensure that no manifest is written.
- `StreamWriter.Close()` without a preceding `Commit` MUST behave as `Abort`.
- Streamed writes MUST be single-pass writes to the final object path.
- Streamed writes MUST NOT mutate existing objects; every write targets a new
  path.
- Streamed writes MUST set the row or event count to `1`.
- When a checksum component is configured, the checksum MUST be computed during
  streaming and recorded in the manifest for each file written.
- If the store's `Put` returns before consuming the stream (for example,
  with `ErrPathExists`), every `Write` call issued after `Put` returned, and
  `Commit`, MUST fail with that error, even when a compressor would buffer
  the bytes; these calls MUST NOT block.
- After a failed `Put`, the writer MUST NOT access the data path. `Put` is
  atomic (`CONTRACT_STORAGE.md` §Put), so a failed stream leaves no object
  of its own, and a path whose `Put` returned `ErrPathExists` belongs to
  another writer. Removal of data files applies only to objects whose `Put`
  succeeded and whose commit then failed before any manifest write was
  attempted. A failed manifest write does not show whether the manifest was
  stored (a lost response stores it), so the snapshot may be visible; the
  data object MUST then be retained, and `Sweep` reclaims it if the commit
  did fail.

### Records.StreamWrite Semantics

- `Records.StreamWrite(ctx, records, metadata)` MUST persist `nil` metadata
  as `{}`.
- `Records.StreamWrite` MUST return `ErrNilIterator` if the iterator is
  `nil`.
- `Records.StreamWrite` MUST consume records from an `iter.Seq2[T, error]`,
  one at a time; an error the iterator yields aborts the write.
- `Records.StreamWrite` MUST return `ErrCodecNotStreamable` if the codec is
  not a `StreamingCodec`.
- `Records.StreamWrite` MUST return `ErrPartitioningNotSupported` if the
  layout is partitioned, because single-pass streaming cannot partition
  records without buffering them.
- Streamed record writes MUST be single-pass writes to the final object path.
- Streamed record writes MUST NOT mutate existing objects; every write
  targets a new path.
- On success, the manifest is written and the new snapshot is returned.
- On error (an iterator failure, a codec error, or a storage error), no
  manifest is written. A failed `Put` leaves no object (`CONTRACT_STORAGE.md`
  §Put). A data object whose `Put` succeeded is deleted on a best-effort
  basis if the commit failed before any manifest write was attempted, and
  retained otherwise, as for `Blobs.StreamWrite`.
- If the store's `Put` returns before consuming the stream,
  `Records.StreamWrite` MUST return that error in preference to any iterator
  or finalization error reported afterward, and MUST NOT block on the store
  or the pipe. The `Put` result is examined before the first record, after
  the iterator produces each result, after each record is written, and
  after the iterator ends. Lode cannot interrupt the caller's own iterator:
  while it blocks producing a result, `Records.StreamWrite` blocks as well,
  and the storage error is reported when it returns. The writer MUST NOT
  access the path afterward.
- The row or event count MUST equal the total number of records consumed.
- When a checksum component is configured, the checksum MUST be computed during
  streaming and recorded in the manifest for each file written.
- When the stream encoder implements `StatisticalCodec`, per-file statistics
  MUST be collected after the stream is finalized and recorded on the
  `FileRef`.

### Timestamp Computation

- When records implement the `Timestamped` interface, `Records.Write` MUST
  compute `MinTimestamp` and `MaxTimestamp` from the data. Whether the
  record type can implement it is decided once, when the handle opens.
- Timestamps are extracted by calling `Timestamp()` on each record that
  implements the interface.
- Records that do not implement `Timestamped` are ignored for timestamp
  computation.
- When no records implement `Timestamped`, both timestamp fields MUST be `nil`.
- Timestamp computation is explicit (through implementation of the
  interface), not inferred.
- The same timestamp rules apply to `Records.StreamWrite` as it consumes
  records.

### Empty Dataset Behavior

- The first successful write creates the initial snapshot.
- `Latest()` on an empty dataset MUST return `ErrNoSnapshots`.
- `Snapshots()` on an empty dataset MUST return an empty list without error.
- `Snapshot(id)` on an empty dataset MUST return `ErrNotFound`.

---

## Read-After-Write Visibility

A snapshot is visible once its commit is complete, as CONTRACT_CORE §Head
Pointer defines it.

---

## Concurrency

The snapshot history of a Dataset is linear.

### Dataset Write Concurrency Matrix

| Pattern | Writers | Snapshots | History | Status |
|---------|---------|-----------|---------|--------|
| Single writer | 1 process | 1 per Write | Linear (guaranteed) | ✅ Supported |
| Multiple processes, serialized | N processes, external coordination | N (one per writer) | Linear (enforced by the caller) | ✅ Supported (the caller provides coordination) |
| Multiple processes, uncoordinated | N processes, no coordination | N (one per writer) | Linear (enforced by CAS) | ✅ Supported |
| Multiple writers in one process, store wrapped with `SingleProcess` | N goroutines, each with its own `Dataset`, sharing one wrapped store | N (one per writer) | Linear (enforced by the process lock) | ✅ Supported |
| Parallel staging, single commit | 1 process, N goroutines | 1 (all shards merged) | Linear (guaranteed) | ❌ Not available |

Every store implements `ConditionalWriter` (CONTRACT_STORAGE). A writer that
terminates after advancing the pointer and before persisting its manifest
leaves an *abandoned commit* (CONTRACT_CORE §Head Pointer). The history
remains linear, but the next snapshot's `ParentSnapshotID` names an ID that
has no manifest.

### Optimistic Concurrency (CAS)

Lode detects concurrent commits and returns `ErrSnapshotConflict`.

**Mechanism:**
- At commit time, the writer records the expected parent snapshot ID.
- The commit writes the latest pointer with `CompareAndSwap`.
- If the pointer content differs from the expected parent (because another
  writer committed after `Latest()` was read), the commit returns
  `ErrSnapshotConflict`.

**Manual retry pattern:**
1. Receive `ErrSnapshotConflict`.
2. Call `Latest()` again to obtain the current head.
3. Merge or reconstruct state against the new head.
4. Commit again.

Data files are immutable and already persisted, so the cost of a retry is
one manifest write plus one pointer swap.

**Automatic retry (disabled by default):**

`WithRetry(n)` enables a bounded number of automatic retries within the
commit path. On `ErrSnapshotConflict`, the writer:
1. Waits for an exponential backoff interval with jitter.
2. Calls `Latest()` to refresh the in-memory CAS cache.
3. Resolves the parent again and assigns it to the manifest (retaining the
   same snapshot ID).
4. Retries the pointer CAS.

Data files are written once; only the manifest and the pointer are retried.
The snapshot ID is retained because the data paths already persisted contain
it; a Volume retry draws a new ID because its block paths do not
(CONTRACT_VOLUME §Optimistic Concurrency (CAS), V1_API_DECISIONS D10c). If all retries are exhausted, `ErrSnapshotConflict` is returned as it would
be without retries.

Retry options: `WithRetry(n)`, `WithRetryBaseDelay(d)`,
`WithRetryMaxDelay(d)`, `WithRetryJitter(j)`, `WithRetryObserver(fn)`.
Defaults: zero retries, a 10 ms base delay, a 2 s maximum delay, full
jitter, and no observer.

**Retry observer:**

`WithRetryObserver(fn)` registers a callback through which retries can be
observed.
- `fn` is called exactly once per retry attempt, after `ErrSnapshotConflict`
  is observed and before the backoff interval.
- `fn` receives the retry attempt number, counted from 1.
- `fn` is never called for the initial attempt, so the number of calls
  equals the number of retries performed.
- `fn` is called synchronously on the committing goroutine and MUST NOT
  block.
- `fn` has no effect on retry decisions or results.
- A `nil` `fn` is rejected at construction time.

**Activation:**
- CAS is always enabled, because every store implements
  `ConditionalWriter`.
- No configuration is required.

**Single-writer compatibility:**
- Single-writer workflows are unaffected. CAS succeeds on every commit
  when no concurrent writer is active.
- External coordination remains valid but is not required.

### Concurrent Blob Write Safety

Individual `Store.Put` calls to distinct paths may safely be issued
concurrently. Data files use unique paths scoped to their snapshot and are
immutable once written.

However, each `Write` or `StreamWrite` call creates a new snapshot, which
updates the latest pointer. **The serialization point is the pointer update,
not the data write.**

CAS detects concurrent snapshot creation and returns
`ErrSnapshotConflict`, and the caller retries with the new parent. Consumers
that must write several blob files as part of a single logical operation
should either serialize snapshot creation (one `Write` or `StreamWrite` per
file, in sequence) or handle `ErrSnapshotConflict` by retrying.

### Future Direction

**Parallel staging (transaction API):**
- Multiple goroutines stage data files concurrently within a single
  write transaction.
- A single atomic Commit produces one snapshot with all data files.
- Parallelizes the internal write pipeline without altering the model.
- Applies within a single process only; it does not address concurrency
  across processes.

### Storage-Level Concurrency for Streaming Writes

Streaming writes (`Blobs.StreamWrite`, `Records.StreamWrite`) that produce large
payloads may use the storage adapter's multipart (chunked) upload path.

**Backends that support conditional completion (for example, S3 with
`If-None-Match`):**
- Both the atomic and the multipart paths guarantee atomically that no
  object is overwritten.
- No coordination beyond the adapter is required.

**Backends that do not support conditional completion:**
- The multipart path has a time-of-check-to-time-of-use (TOCTOU) interval
  between the preliminary check and completion.
- Callers MUST therefore either ensure single-writer semantics for each
  object path or use external coordination (distributed locks, queues, or
  similar mechanisms).

See `CONTRACT_STORAGE.md` for adapter-specific thresholds and guarantees.

---

## Error Semantics

Implementations MUST use the following error sentinels where applicable:

- `ErrNoSnapshots` for an empty history
- `ErrNotFound` for a missing snapshot ID
- `ErrPathExists` for an attempt to write to an existing path
- `ErrCodecNotStreamable` when the codec used by `Records.StreamWrite` is not
  a `StreamingCodec`
- `ErrNilIterator` when `Records.StreamWrite` is called with a `nil`
  iterator
- `ErrPartitioningNotSupported` when `Records.StreamWrite` is used on a
  partitioned layout
- `ErrInvalidArgument` for metadata that does not encode as a JSON object
- `ErrSnapshotConflict` when another writer committed after the parent was
  resolved, or when the pointer names a snapshot whose settings no
  manifest records (§Settings)
- `ErrSettingsMismatch` when the writer's settings differ from the
  dataset's (§Settings)

Streaming-specific error taxonomy and handling guidance are defined in
`CONTRACT_ERRORS.md`.

---

## Complexity Bounds

See [CONTRACT_COMPLEXITY.md](CONTRACT_COMPLEXITY.md) for complete definitions
and a glossary of variables.

### Parent Resolution

- In-memory cache (within a process): **0 store calls**.
- Persistent pointer (cold start): **1 store call** (1 Get). The snapshot
  that the pointer names is the parent even when its manifest has not been
  persisted, because it may belong to a pending commit; adopting any other
  parent would fork the history (CONTRACT_CORE §Head Pointer).
- Scan fallback (absent pointer, for backward compatibility): **O(N) through
  List**. The commit then creates the pointer.

Parent resolution MUST NOT use List when a valid pointer exists.

### Settings Check

The check runs once per writer, before its first commit:
- Against the manifest that the pointer names: **1 Get**.
- While a first commit is pending: **2 Gets** (the canonical manifest, then
  the staged copy).
- When neither exists: the scan of `Latest`, **O(N) through List**, and
  1 List of the dataset's directory.
- Against an empty dataset: **1 List** of the dataset's directory, to
  detect a history persisted under another layout.
- Against a dataset written before format version 1.1.0 with a Hive
  partitioner: **1 Get per snapshot** followed from the head to the first
  snapshot with files, once per dataset, because the first commit after it
  records the keys.

### Write Operations

| Operation | Store Calls (warm) | Memory |
|-----------|-------------------|--------|
| `Write` (unpartitioned) | 4 fixed | O(R + encoded) |
| `Write` (P partitions) | 2P + 3 | O(R + encoded) |
| `Blobs.StreamWrite` | 4 fixed | O(1) streaming |
| `Records.StreamWrite` | 4 fixed | O(1) streaming |

A dataset's first commit adds **1 Put and 1 Delete** for its staged
manifest (CONTRACT_CORE §Head Pointer).

A commit may incur **one additional read**: `CompareAndSwap` reads the current pointer before the
conditional write when it cannot otherwise establish the precondition. The
S3 adapter omits that read when it creates the pointer and when it
replaces a pointer that it last replaced itself (CONTRACT_STORAGE
§ConditionalWriter Capability). Each commit after the second through the
same store therefore reads the pointer only after another writer has
advanced it. This overhead is constant per commit, not per record or per
file.

Hot-path writes MUST NOT invoke `Store.List`.

---

## Prohibited Behaviors

- In-place mutation of existing data files
- Inference or defaulting of metadata
- Branching, or a snapshot history with more than one head

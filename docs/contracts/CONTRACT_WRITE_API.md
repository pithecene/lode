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

## Dataset.Write Semantics

### Required Behavior

- `Write(ctx, data, metadata)` MUST create a new snapshot on success.
- `nil` metadata MUST be converted to empty metadata (`Metadata{}`).
- Empty metadata is valid and MUST be persisted explicitly.
- The new snapshot MUST reference the previous snapshot, if any, as its parent.
- Writes MUST NOT mutate existing snapshots or manifests.
- The manifest MUST include all required fields defined in `CONTRACT_CORE.md`
  (including the row or event count and, when applicable, the minimum and
  maximum timestamps).
- When the codec implements `StatisticalCodec`, per-file statistics MUST be
  collected after encoding and recorded on the `FileRef`.
- When no codec is configured, each write represents a single data unit, and
  the row or event count MUST be `1`.

### StreamWrite Semantics

- `StreamWrite(ctx, metadata)` MUST convert `nil` metadata to empty metadata
  (`Metadata{}`).
- `StreamWrite` MUST return a `StreamWriter` for a single binary data unit.
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
- When a codec is configured, `StreamWrite` MUST return an error.
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
  data object MUST then be retained, and `Dataset.Sweep` reclaims it if the
  commit did fail.

### StreamWriteRecords Semantics

- `StreamWriteRecords(ctx, records, metadata)` MUST convert `nil` metadata to
  empty metadata (`Metadata{}`).
- `StreamWriteRecords` MUST return an error if the records iterator is `nil`.
- `StreamWriteRecords` MUST consume records through a pull-based iterator.
- `StreamWriteRecords` MUST return an error if the configured codec does not support
  streaming record encoding.
- `StreamWriteRecords` MUST return an error if partitioning is configured (a
  partitioner other than `noop`), because single-pass streaming cannot
  partition records without buffering them.
- Streamed record writes MUST be single-pass writes to the final object path.
- Streamed record writes MUST NOT mutate existing objects; every write
  targets a new path.
- On success, the manifest is written and the new snapshot is returned.
- On error (an iterator failure, a codec error, or a storage error), no
  manifest is written. A failed `Put` leaves no object (`CONTRACT_STORAGE.md`
  §Put). A data object whose `Put` succeeded is deleted on a best-effort
  basis if the commit failed before any manifest write was attempted, and
  retained otherwise, as for `StreamWrite`.
- If the store's `Put` returns before consuming the stream, `StreamWriteRecords`
  MUST return that error in preference to any iterator or finalization error
  reported afterward, and MUST NOT block on the store or the pipe. The `Put`
  result is examined before and after every iterator call (`Next`, `Record`,
  `Err`) and before each record is accepted. Lode cannot interrupt the
  caller's own iterator: while a call to `Next` or `Err` blocks,
  `StreamWriteRecords` blocks as well, and the storage error is reported
  when the call returns. The writer MUST NOT access the path afterward.
- The row or event count MUST equal the total number of records consumed.
- When a checksum component is configured, the checksum MUST be computed during
  streaming and recorded in the manifest for each file written.
- When the stream encoder implements `StatisticalStreamEncoder`, per-file
  statistics MUST be collected after the stream is finalized and recorded on
  the `FileRef`.

### Timestamp Computation

- When records implement the `Timestamped` interface, `Write` MUST compute
  `MinTimestamp` and `MaxTimestamp` from the data.
- Timestamps are extracted by calling `Timestamp()` on each record that
  implements the interface.
- Records that do not implement `Timestamped` are ignored for timestamp
  computation.
- When no records implement `Timestamped`, both timestamp fields MUST be `nil`.
- Timestamp computation is explicit (through implementation of the
  interface), not inferred.
- The same timestamp rules apply to `StreamWriteRecords` as it consumes
  records.

### Empty Dataset Behavior

- The first successful write creates the initial snapshot.
- `Latest()` on an empty dataset MUST return `ErrNoSnapshots`.
- `Snapshots()` on an empty dataset MUST return an empty list without error.
- `Snapshot(id)` on an empty dataset MUST return `ErrNotFound`.

---

## Read-After-Write Visibility

- A snapshot is visible only after its manifest is persisted.
- The presence of the manifest signifies that the commit is complete.

---

## Concurrency

The snapshot history of a Dataset is linear.

### Dataset Write Concurrency Matrix

| Pattern | Writers | Snapshots | History | Status |
|---------|---------|-----------|---------|--------|
| Single writer | 1 process | 1 per Write | Linear (guaranteed) | ✅ Supported |
| Multiple processes, serialized | N processes, external coordination | N (one per writer) | Linear (enforced by the caller) | ✅ Supported (the caller provides coordination) |
| Multiple processes, uncoordinated, store implements `ConditionalWriter` | N processes, no coordination | N (one per writer) | Linear (enforced by CAS) | ✅ Supported |
| Multiple processes, uncoordinated, store without `ConditionalWriter` | N processes, no coordination | N (one per writer) | Undefined: concurrent Delete+Put commits can fork the history | ❌ Unsafe |
| Parallel staging, single commit | 1 process, N goroutines | 1 (all shards merged) | Linear (guaranteed) | ❌ Not available |

Under CAS, a writer that terminates after advancing the pointer and before
persisting its manifest leaves an *abandoned commit* (CONTRACT_CORE §Head
Pointer). The history remains linear, but the next snapshot's
`ParentSnapshotID` names an ID that has no manifest.

### Optimistic Concurrency (CAS)

When the storage adapter implements `ConditionalWriter`, Lode detects
concurrent commits and returns `ErrSnapshotConflict`.

**Mechanism:**
- At commit time, the writer records the expected parent snapshot ID.
- When the store implements `ConditionalWriter`, the commit writes the
  latest pointer with `CompareAndSwap` instead of Delete+Put.
- If the pointer content differs from the expected parent (because another
  writer committed after `Latest()` was read), the commit returns
  `ErrSnapshotConflict`.
- If the store does not implement `ConditionalWriter`, the Delete+Put
  behavior applies, and enforcing single-writer semantics remains the
  caller's responsibility.

**Manual retry pattern:**
1. Receive `ErrSnapshotConflict`.
2. Call `Latest()` again to obtain the current head.
3. Merge or reconstruct state against the new head.
4. Commit again.

Data files are immutable and already persisted, so the cost of a retry is
one manifest write plus one pointer swap.

**Automatic retry (disabled by default):**

`WithRetryCount(n)` enables a bounded number of automatic retries within the
commit path. On `ErrSnapshotConflict`, the writer:
1. Waits for an exponential backoff interval with jitter.
2. Calls `Latest()` to refresh the in-memory CAS cache.
3. Resolves the parent again and assigns it to the manifest (retaining the
   same snapshot ID).
4. Retries the pointer CAS.

Data files are written once; only the manifest and the pointer are retried.
If all retries are exhausted, `ErrSnapshotConflict` is returned as it would
be without retries.

Retry options: `WithRetryCount(n)`, `WithRetryBaseDelay(d)`,
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
- CAS is always enabled when the adapter implements `ConditionalWriter`.
- When it does not, the commit uses Delete+Put without notice.
- No configuration is required.

**Single-writer compatibility:**
- Single-writer workflows are unaffected. CAS succeeds on every commit
  when no concurrent writer is active.
- External coordination remains valid but is no longer required with a
  store that supports CAS.

### Concurrent Blob Write Safety

Individual `Store.Put` calls to distinct paths may safely be issued
concurrently. Data files use unique paths scoped to their snapshot and are
immutable once written.

However, each `Write` or `StreamWrite` call creates a new snapshot, which
updates the latest pointer. **The serialization point is the pointer update,
not the data write.**

| Store capability | Concurrent snapshot creation | Guidance |
|------------------|------------------------------|----------|
| Implements `ConditionalWriter` | Safe: CAS detects conflicts and returns `ErrSnapshotConflict` | The caller retries with the new parent |
| Does not implement `ConditionalWriter` | Unsafe: writers race on the pointer, and the visibility order is undefined | Serialize writes externally |

Consumers that must write several blob files as part of a single logical
operation should either serialize snapshot creation (one `Write` or
`StreamWrite` per file, in sequence) or use a store that supports CAS and
handle `ErrSnapshotConflict` by retrying.

### Future Direction

**Parallel staging (transaction API):**
- Multiple goroutines stage data files concurrently within a single
  write transaction.
- A single atomic Commit produces one snapshot with all data files.
- Parallelizes the internal write pipeline without altering the model.
- Applies within a single process only; it does not address concurrency
  across processes.

### Storage-Level Concurrency for Streaming Writes

Streaming writes (`StreamWrite`, `StreamWriteRecords`) that produce large
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
- `ErrCodecConfigured` when `StreamWrite` is called with a configured codec
- `ErrCodecNotStreamable` when the codec used by `StreamWriteRecords` does
  not support streaming
- `ErrNilIterator` when `StreamWriteRecords` is called with a `nil` iterator
- `ErrPartitioningNotSupported` when `StreamWriteRecords` is used with a
  partitioner other than `noop`
- `ErrSnapshotConflict` when another writer committed after the parent was
  resolved (only when the store implements `ConditionalWriter`)

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

### Write Operations

| Operation | Store Calls (warm) | Memory |
|-----------|-------------------|--------|
| `Write` (unpartitioned) | 4 fixed | O(R + encoded) |
| `Write` (P partitions) | 2P + 3 | O(R + encoded) |
| `StreamWrite` | 4 fixed | O(1) streaming |
| `StreamWriteRecords` | 4 fixed | O(1) streaming |

When the store implements `ConditionalWriter`, a commit may incur **one
additional read**: `CompareAndSwap` reads the current pointer before the
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

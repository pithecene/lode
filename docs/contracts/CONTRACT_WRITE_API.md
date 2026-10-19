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

## Non-goals

- Query execution or filtering.
- Background compaction.
- Distributed coordination, consensus, or lock management.

---

## Dataset.Write Semantics

### Required behavior

- `Write(ctx, data, metadata)` MUST create a new snapshot on success.
- `nil` metadata MUST be coalesced to empty (`Metadata{}`).
- Empty metadata is valid and MUST be persisted explicitly.
- The new snapshot MUST reference the previous snapshot as its parent (if any).
- Writes MUST NOT mutate existing snapshots or manifests.
- The manifest MUST include all required fields defined in `CONTRACT_CORE.md`
  (including row/event count and min/max timestamp when applicable).
- When the codec implements `StatisticalCodec`, per-file statistics MUST be
  collected after encoding and recorded on the FileRef.
- When no codec is configured, each write represents a single data unit and
  the row/event count MUST be `1`.

### StreamWrite Semantics

- `StreamWrite(ctx, metadata)` MUST coalesce `nil` metadata to empty (`Metadata{}`).
- `StreamWrite` MUST return a `StreamWriter` for a single binary data unit.
- `StreamWriter.Commit(ctx)` MUST write the manifest and return the new snapshot.
- A snapshot MUST NOT be visible before `Commit` writes the manifest.
- `StreamWriter.Abort(ctx)` MUST ensure no manifest is written.
- `StreamWriter.Close()` without `Commit` MUST behave as `Abort`.
- Streamed writes MUST be single-pass writes to the final object path.
- Streamed writes MUST NOT mutate existing objects; all writes are new paths.
- Streamed writes MUST set row/event count to `1`.
- When a checksum component is configured, the checksum MUST be computed during
  streaming and recorded in the manifest for each file written.
- When a codec is configured, `StreamWrite` MUST return an error.
- If the store's `Put` returns before consuming the stream (for example with
  `ErrPathExists`), `Write` calls issued after it returned and `Commit` MUST
  fail with that error, even when a compressor would buffer the bytes; they
  MUST NOT block.
- After a failed `Put` the writer MUST NOT touch the data path. `Put` is
  all-or-nothing (`CONTRACT_STORAGE.md` §Put), so a failed stream leaves no
  object of its own, and a path whose `Put` returned `ErrPathExists` belongs
  to another writer. Data-file cleanup applies only to objects whose `Put`
  succeeded and whose commit then failed.

### StreamWriteRecords Semantics

- `StreamWriteRecords(ctx, records, metadata)` MUST coalesce `nil` metadata to empty (`Metadata{}`).
- `StreamWriteRecords` MUST return an error if records iterator is nil.
- `StreamWriteRecords` MUST consume records via a pull-based iterator.
- `StreamWriteRecords` MUST return an error if the configured codec does not support
  streaming record encoding.
- `StreamWriteRecords` MUST return an error if partitioning is configured (non-noop
  partitioner), since single-pass streaming cannot partition without buffering.
- Streamed record writes MUST be single-pass writes to the final object path.
- Streamed record writes MUST NOT mutate existing objects; all writes are new paths.
- On success, the manifest is written and the new snapshot is returned.
- On error (iterator failure, codec error, storage error), no manifest is written.
  A failed `Put` leaves nothing behind (`CONTRACT_STORAGE.md` §Put); a data
  object whose `Put` succeeded but whose commit failed is deleted best-effort.
- If the store's `Put` returns before consuming the stream, `StreamWriteRecords`
  MUST return that error (ahead of any iterator error reported afterwards); it
  MUST NOT block. The writer MUST NOT touch the path afterwards.
- Row/event count MUST equal the total number of records consumed.
- When a checksum component is configured, the checksum MUST be computed during
  streaming and recorded in the manifest for each file written.
- When the stream encoder implements `StatisticalStreamEncoder`, per-file
  statistics MUST be collected after stream finalization and recorded on the FileRef.

### Timestamp computation

- When records implement the `Timestamped` interface, `Write` MUST compute
  `MinTimestamp` and `MaxTimestamp` from the data.
- Timestamps are extracted by calling `Timestamp()` on each record that
  implements the interface.
- Records that do not implement `Timestamped` are ignored for timestamp
  computation.
- When no records implement `Timestamped`, both timestamp fields MUST be `nil`.
- Timestamp computation is explicit (via interface implementation), not inferred.
- The same timestamp rules apply to `StreamWriteRecords` as records are consumed.

### Empty dataset behavior

- The first successful write creates the initial snapshot.
- `Latest()` on an empty dataset MUST return `ErrNoSnapshots`.
- `Snapshots()` on an empty dataset MUST return an empty list without error.
- `Snapshot(id)` on an empty dataset MUST return `ErrNotFound`.

---

## Read-after-write Visibility

- A snapshot is visible only after its manifest is persisted.
- Manifest presence is the commit signal.

---

## Concurrency

Snapshot history for a Dataset is linear.

### Dataset Write Concurrency Matrix

| Pattern | Writers | Snapshots | History | Status |
|---------|---------|-----------|---------|--------|
| Single writer | 1 process | 1 per Write | Linear (guaranteed) | ✅ Supported |
| Multi-process, serialized | N processes, external coordination | N (one per writer) | Linear (caller-enforced) | ✅ Supported (caller owns coordination) |
| Multi-process, uncoordinated | N processes, no coordination | N (one per writer) | Linear (CAS-enforced) | ✅ Safe (CAS) |
| Parallel staging, single commit | 1 process, N goroutines | 1 (all shards merged) | Linear (guaranteed) | ❌ Not available |

### Optimistic Concurrency (CAS)

When the storage adapter implements `ConditionalWriter`, Lode detects
concurrent commits and returns `ErrSnapshotConflict`.

**Mechanism:**
- At commit time, the writer captures the expected parent snapshot ID.
- When writing the latest pointer, if the store implements `ConditionalWriter`,
  the commit uses `CompareAndSwap` instead of Delete+Put.
- If the pointer content differs from the expected parent (another writer
  committed since `Latest()` was read), the commit returns `ErrSnapshotConflict`.
- If the store does not implement `ConditionalWriter`, the current Delete+Put
  behavior applies and single-writer semantics remain the caller's responsibility.

**Manual retry pattern:**
1. Receive `ErrSnapshotConflict`.
2. Re-read `Latest()` to get the current head.
3. Merge or rebuild state against the new head.
4. Re-commit.

Data files are immutable and already persisted — retry cost is one manifest
write plus one pointer swap.

**Automatic retry (opt-in):**

`WithRetryCount(n)` enables bounded automatic retry within the commit path.
On `ErrSnapshotConflict`, the writer:
1. Sleeps with jittered exponential backoff.
2. Calls `Latest()` to refresh the in-memory CAS cache.
3. Re-resolves the parent and re-parents the manifest (same snapshot ID).
4. Retries the pointer CAS.

Data files are written once; only the manifest and pointer are retried.
If all retries are exhausted, `ErrSnapshotConflict` is returned as usual.

Retry options: `WithRetryCount(n)`, `WithRetryBaseDelay(d)`,
`WithRetryMaxDelay(d)`, `WithRetryJitter(j)`, `WithRetryObserver(fn)`.
Defaults: 0 retries, 10ms base, 2s max, full jitter, no observer.

**Retry observer:**

`WithRetryObserver(fn)` registers a callback for retry observability.
- `fn` is called exactly once per retry attempt, after `ErrSnapshotConflict`
  is observed and before the backoff sleep.
- `fn` receives the 1-indexed retry attempt number.
- `fn` is never called for the initial attempt; the number of calls equals
  the number of retries performed.
- `fn` is called synchronously on the committing goroutine; it must not block.
- `fn` has no effect on retry decisions or results.
- A nil `fn` is rejected at construction time.

**Activation:**
- Always-on when the adapter implements `ConditionalWriter`.
- Silent fallback to Delete+Put when it does not.
- No configuration required.

**Single-writer compatibility:**
- Single-writer workflows are unaffected. CAS succeeds on every commit
  when no concurrent writer is active.
- External coordination remains valid but is no longer required when
  using a CAS-capable store.

### Concurrent Blob Write Safety

Individual `Store.Put` calls to distinct paths are safe to issue concurrently.
Data files use unique snapshot-scoped paths and are immutable once written.

However, each `Write` or `StreamWrite` call creates a new snapshot, which
updates the latest pointer. **The serialization point is the pointer update,
not the data write.**

| Store capability | Concurrent snapshot creation | Guidance |
|------------------|------------------------------|----------|
| Implements `ConditionalWriter` | Safe — CAS detects conflicts, returns `ErrSnapshotConflict` | Caller retries with new parent |
| Does not implement `ConditionalWriter` | Unsafe — pointer race, undefined visibility order | Serialize writes externally |

Consumers who need to write multiple blob files as part of a single logical
operation should serialize snapshot creation (one `Write` or `StreamWrite`
per file, sequentially) or use a CAS-capable store and handle
`ErrSnapshotConflict` with retry logic.

### Future Direction

**Parallel staging (transaction API):**
- Multiple goroutines stage data files concurrently within a single
  write transaction.
- A single atomic Commit produces one snapshot with all data files.
- Parallelizes the internal write pipeline without changing the model.
- Single process only; does not address multi-process concurrency.

### Storage-Level Concurrency for Streaming Writes

Streaming writes (`StreamWrite`, `StreamWriteRecords`) that produce large payloads
may use the storage adapter's multipart/chunked upload path.

**Backends with conditional completion support (e.g., S3 with `If-None-Match`):**
- Both atomic and multipart paths provide atomic no-overwrite guarantees.
- No additional coordination required beyond the adapter.

**Backends without conditional completion support:**
- The multipart path has a TOCTOU window between preflight check and completion.
- Callers MUST ensure single-writer semantics per object path, OR
- Callers MUST use external coordination (distributed locks, queues, etc.)

See `CONTRACT_STORAGE.md` for adapter-specific thresholds and guarantees.

---

## Error Semantics

Implementations MUST use the following error sentinels where applicable:

- `ErrNoSnapshots` for empty history
- `ErrNotFound` for missing snapshot ID
- `ErrPathExists` when attempting to write to an existing path
- `ErrCodecConfigured` when `StreamWrite` is called with a configured codec
- `ErrCodecNotStreamable` when `StreamWriteRecords` codec lacks streaming support
- `ErrNilIterator` when `StreamWriteRecords` is called with a nil iterator
- `ErrPartitioningNotSupported` when `StreamWriteRecords` is used with non-noop partitioning
- `ErrSnapshotConflict` when another writer committed since parent was resolved
  (only when store implements `ConditionalWriter`)

Streaming-specific error taxonomy and handling guidance are defined in
`CONTRACT_ERRORS.md`.

---

## Complexity Bounds

See [CONTRACT_COMPLEXITY.md](CONTRACT_COMPLEXITY.md) for full definitions and variable glossary.

### Parent Resolution

- In-memory cache (within process): **0 store calls**.
- Persistent pointer (cold start): **2 store calls** (1 Get + 1 Exists).
- Scan fallback (no pointer, backward compat): **O(N) via List**. Self-heals on completion.

Parent resolution MUST NOT use List when a valid pointer exists.

### Write Operations

| Operation | Store Calls (warm) | Memory |
|-----------|-------------------|--------|
| `Write` (unpartitioned) | 4 fixed | O(R + encoded) |
| `Write` (P partitions) | 2P + 3 | O(R + encoded) |
| `StreamWrite` | 4 fixed | O(1) streaming |
| `StreamWriteRecords` | 4 fixed | O(1) streaming |

When the store implements `ConditionalWriter`, each commit adds **+1 read**
(the `CompareAndSwap` operation reads the current pointer before conditional write).
This is constant overhead per commit, not per record or per file.

Hot-path writes MUST NOT trigger Store.List.

---

## Prohibited Behaviors

- In-place mutation of existing data files
- Metadata inference or defaulting
- Branching or multi-head snapshot history

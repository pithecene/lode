# Lode Write API Contract

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

`Records[T]` writes records of type `T`, and `Blobs` writes one blob per
snapshot. A caller opens both writers at a root (`Root.Records`,
`Root.Blobs`), and each writer checks its settings against the dataset's
head when it opens (§Settings).

### Required Behavior

- `Write(ctx, data, metadata)` MUST create a new snapshot on success.
- Metadata MUST encode as a JSON object (a struct, a map, or `nil`). Before
  any store call, the writer MUST reject with `ErrInvalidArgument` any
  value that does not encode as a JSON object. The writer MUST persist
  `nil` metadata as `{}`.
- Empty metadata is valid, and the writer MUST persist it explicitly.
- The new snapshot MUST reference the previous snapshot, if any, as its parent.
- Writes MUST NOT mutate existing snapshots or manifests.
- The manifest MUST include all required fields defined in [`CONTRACT_CORE.md`](CONTRACT_CORE.md)
  (including the row or event count and, when applicable, the minimum and
  maximum timestamps).
- When the codec implements `StatisticalCodec`, the writer MUST collect
  per-file statistics after encoding and record them on the `FileRef`.
- A blob write represents a single data unit, and the row or event count
  MUST be `1`.

### Settings

Every writer of a dataset MUST share the dataset's settings, as the
manifest records them (CONTRACT_CORE §Manifest Requirements). The settings
are the codec name and description (`Codec.Describe`), the compressor, the
checksum algorithm, the partitioner and its keys, and the path names.

- When it opens, and again before its first commit, a writer MUST compare
  its settings with the first of the following manifests that exists:
  1. the manifest of the snapshot that the pointer names
  2. that snapshot's staged copy (CONTRACT_CORE §Head Pointer)
  3. the newest manifest, as `Latest` selects it, when the pointer is
     absent or neither preceding manifest exists

  With no pointer and no manifest, the dataset is empty, and the writer's
  first commit records the writer's settings, unless the dataset directory
  holds a manifest that the writer's layout does not persist.
- When the writer finds no manifest under its own layout and the dataset
  directory holds such a manifest, the directory contains a history
  persisted under another layout. Lode MUST reject the writer with
  `ErrSettingsMismatch` rather than let it begin a second history beside
  the existing one. The writer recognizes a manifest
  by its file name alone, without reading any object. That name is either
  the writer's own manifest name or the default name, which every manifest
  bore before manifest names became root options. The writer does not
  recognize a history that lacks a pointer and whose manifests bear a
  custom name other than the writer's own. Lode persists a pointer with
  every commit, so only a pointer removed by hand produces that state. Data
  objects are not a history: another writer persists its data before its first commit, and two first
  writers with different layouts would otherwise reject each other before
  either commits. A dataset under another datasets directory (the flat
  layout's `<id>` rather than `datasets/<id>`) is another dataset
  (V1_API_DECISIONS D11).
- Lode MUST reject with `ErrSettingsMismatch` a writer whose settings
  differ from those of the manifest it checks, and the writer MUST NOT
  commit. Lode does not retry after this rejection.
- A pointer that names a snapshot with neither a manifest nor a staged copy,
  in a dataset with no other manifest, belongs to a first commit whose
  settings are unknown. The writer MUST return `ErrSnapshotConflict` rather
  than commit unchecked settings. Only a first commit persisted before
  format version 1.1.0, or a pointer edited by hand, produces that state.
- The pointer's `CompareAndSwap` serializes two first commits with
  conflicting settings: after the losing writer refreshes the head, Lode
  checks its settings against the winner's manifest and rejects the
  writer. A writer
  can lose a commit conflict before Lode has compared it with any manifest.
  Lode then compares the writer's settings with the winner's manifest,
  whatever the writer's retry budget, and returns the conflict only when
  the settings match.
- A writer repeats the settings check until it compares its settings with
  a manifest of format version 1.1.0 or until its own commit succeeds.
  Because every writer performs this check, every later snapshot records
  the same settings. A check against an earlier
  manifest compares fewer settings, so a writer checks again after a
  conflict.
- A manifest written before format version 1.1.0 records no partition keys.
  When such a dataset uses a Hive partitioner, Lode MUST verify the names
  and order of the writer's keys against the partition path of the first
  snapshot that has files. The writer finds that snapshot by following parents
  from the head. When no snapshot back to the root has files, Lode accepts
  any keys.
  When the chain ends at a missing or expired parent, or repeats a
  snapshot, before it reaches a snapshot with files, Lode cannot verify the
  keys and MUST reject the write with `ErrSettingsMismatch`.
  Lode MUST NOT infer keys from partition paths: the writer states them,
  and Lode verifies them.

### Blobs.StreamWrite Semantics

- `Blobs.StreamWrite(ctx, metadata)` MUST persist `nil` metadata as `{}`.
- `Blobs.StreamWrite` MUST return a `StreamWriter` for a single binary data
  unit.
- `StreamWriter.Commit(ctx)` MUST write the manifest and return the new snapshot.
- A snapshot MUST NOT be visible before `Commit` writes the manifest.
- `StreamWriter.Abort(ctx)` MUST NOT write a manifest.
- `StreamWriter.Close()` without a preceding `Commit` MUST behave as `Abort`.
- Streamed writes MUST be single-pass writes to the final object path.
- Streamed writes MUST NOT mutate existing objects, so every write targets a
  new path.
- Streamed writes MUST set the row or event count to `1`.
- When the caller configures a checksum component, the writer MUST compute
  the checksum during streaming and record it in the manifest for each file
  that it writes.
- The store's `Put` can return an error, such as `ErrPathExists`, before
  consuming the stream. Every later `Write` call and the `Commit` call MUST
  then fail with that error without blocking, even when a compressor would
  buffer the bytes.
- After a failed `Put`, the writer MUST NOT access the data path. `Put` is
  atomic ([`CONTRACT_STORAGE.md`](CONTRACT_STORAGE.md) §Put), so a failed stream leaves no object
  of its own, and a path whose `Put` returned `ErrPathExists` belongs to
  another writer. The writer removes a data object only when its `Put`
  succeeded and its commit failed before the writer attempted any manifest
  write. A failed manifest write leaves the outcome unknown: the store may
  have persisted the manifest and lost only the response, in which case
  the snapshot is visible. The writer MUST then retain the
  data object, and `Sweep` reclaims it if the commit did fail.

### Records.StreamWrite Semantics

- `Records.StreamWrite(ctx, records, metadata)` MUST persist `nil` metadata
  as `{}`.
- `Records.StreamWrite` MUST return `ErrNilIterator` if the iterator is
  `nil`.
- `Records.StreamWrite` MUST consume records from an `iter.Seq2[T, error]`,
  one at a time. An error that the iterator yields aborts the write.
- `Records.StreamWrite` MUST return `ErrCodecNotStreamable` if the codec is
  not a `StreamingCodec`.
- `Records.StreamWrite` MUST return `ErrPartitioningNotSupported` if the
  layout is partitioned, because single-pass streaming cannot partition
  records without buffering them.
- Streamed record writes MUST be single-pass writes to the final object path.
- Streamed record writes MUST NOT mutate existing objects, so every write
  targets a new path.
- On success, `Records.StreamWrite` writes the manifest and returns the new
  snapshot.
- On error (an iterator failure, a codec error, or a storage error),
  `Records.StreamWrite` writes no manifest. A failed `Put` leaves no object
  ([`CONTRACT_STORAGE.md`](CONTRACT_STORAGE.md) §Put). If the commit fails
  before any manifest write, the writer attempts to delete the data object
  that its successful `Put` persisted. Otherwise, the
  writer retains the object, as it does for `Blobs.StreamWrite`.
- If the store's `Put` returns before consuming the stream,
  `Records.StreamWrite` MUST return that error in preference to any iterator
  or finalization error reported afterward, and MUST NOT block on the store
  or the pipe. `Records.StreamWrite` examines the `Put` result before the
  first record, after the iterator produces each result, after
  `Records.StreamWrite` writes each record, and after the iterator ends. Lode cannot interrupt the
  caller's iterator: while the iterator blocks to produce a result,
  `Records.StreamWrite` blocks as well, and it reports the storage error
  when the iterator returns. The writer MUST NOT
  access the path afterward.
- The row or event count MUST equal the total number of records consumed.
- When the caller configures a checksum component, the writer MUST compute
  the checksum during streaming and record it in the manifest for each file
  that it writes.
- When the stream encoder implements `StatisticalCodec`, the writer MUST
  collect per-file statistics after it finalizes the stream and record them
  on the `FileRef`.

### Timestamp Computation

- When records implement the `Timestamped` interface, `Records.Write` MUST
  compute `MinTimestamp` and `MaxTimestamp` from the data. Lode decides
  once, when the handle opens, whether the record type implements the
  interface.
- `Records.Write` extracts timestamps by calling `Timestamp()` on each
  record that implements the interface.
- `Records.Write` ignores records that do not implement `Timestamped` when
  it computes timestamps.
- When no records implement `Timestamped`, both timestamp fields MUST be `nil`.
- Lode computes timestamps only from records that implement the interface
  and never infers them.
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
Pointer defines commit completion.

---

## Concurrency

The snapshot history of a Dataset is linear.

### Dataset Write Concurrency Matrix

| Pattern | Writers | Snapshots | History | Supported |
|---------|---------|-----------|---------|-----------|
| Single writer | 1 process | 1 per Write | Linear (guaranteed) | Yes |
| Multiple processes, serialized | N processes, external coordination | N (one per writer) | Linear (enforced by the caller) | Yes, with coordination that the caller provides |
| Multiple processes, uncoordinated | N processes, no coordination | N (one per writer) | Linear (enforced by CAS) | Yes |
| Multiple writers in one process, store wrapped with `SingleProcess` | N goroutines, each with its own `Dataset`, sharing one wrapped store | N (one per writer) | Linear (enforced by the process lock) | Yes |

Every store implements `ConditionalWriter` (CONTRACT_STORAGE). A writer that
terminates after advancing the pointer and before persisting its manifest
leaves an *abandoned commit* (CONTRACT_CORE §Head Pointer). The history
remains linear, but the next snapshot's `ParentSnapshotID` names an ID that
has no manifest.

### Optimistic Concurrency (CAS)

Lode detects concurrent commits and returns `ErrSnapshotConflict`.

The commit detects concurrent writers in three steps:
- At commit time, the writer records the expected parent snapshot ID.
- The commit writes the latest pointer with `CompareAndSwap`.
- If another writer committed after this writer read `Latest()`, the pointer
  content differs from the expected parent, and the commit returns
  `ErrSnapshotConflict`.

A caller retries a commit manually in four steps:
1. Receive `ErrSnapshotConflict`.
2. Call `Latest()` again to obtain the current head.
3. Merge or reconstruct state against the new head.
4. Commit again.

Data files are immutable and already persisted, so the cost of a retry is
one manifest write plus one pointer swap.

Lode also supports automatic retry, and the default configuration performs
no retries.
`WithRetry(n)` enables a bounded number of automatic retries within the
commit path. On `ErrSnapshotConflict`, the writer:
1. Waits for an exponential backoff interval with jitter.
2. Calls `Latest()` to refresh the in-memory CAS cache.
3. Resolves the parent again and assigns it to the manifest (retaining the
   same snapshot ID).
4. Retries the pointer CAS.

Lode writes data files once and retries only the manifest and the pointer.
The writer retains the snapshot ID because a Dataset's data paths contain
it, so any data file the write persisted is valid only under that ID. A
write that persisted no data file, such as an empty batch, retains the ID
as well. A Volume retry draws a new ID because its block paths do not
contain it (CONTRACT_VOLUME §Optimistic Concurrency (CAS), V1_API_DECISIONS
D10c). If the writer exhausts all retries, it returns `ErrSnapshotConflict`
exactly as a writer without retries does.

The retry options are `WithRetry(n)`, `WithRetryBaseDelay(d)`,
`WithRetryMaxDelay(d)`, `WithRetryJitter(j)`, and `WithRetryObserver(fn)`.
By default, the writer performs zero retries and uses a 10 ms base delay, a
2 s maximum delay, full jitter, and no observer.

A caller can register an observer for retries.
`WithRetryObserver(fn)` registers a callback that the writer calls on each
retry.
- The writer calls `fn` exactly once per retry attempt, after it receives
  `ErrSnapshotConflict` and before the backoff interval.
- `fn` receives the retry attempt number, counted from 1.
- The writer never calls `fn` for the initial attempt, so the number of
  calls equals the number of retries performed.
- The writer calls `fn` synchronously on the committing goroutine, and the
  callback MUST NOT block.
- `fn` has no effect on retry decisions or results.
- The constructor rejects a `nil` `fn`.

CAS requires no activation:
- CAS is always enabled, because every store implements
  `ConditionalWriter`.
- CAS requires no configuration.

CAS remains compatible with single-writer use:
- Single-writer workflows are unaffected. CAS succeeds on every commit
  when no concurrent writer is active.
- Callers may still coordinate externally, but CAS does not require it.

### Concurrent Blob Write Safety

A caller may safely issue `Store.Put` calls to distinct paths concurrently.
Each data file has a unique path within its snapshot, and the writer never
modifies a data file after persisting it.

However, each `Write` or `StreamWrite` call creates a new snapshot and
updates the latest pointer. **The serialization point is the pointer update,
not the data write.**

CAS detects concurrent snapshot creation and returns
`ErrSnapshotConflict`, and the caller retries with the new parent. Consumers
that must write several blob files as part of a single logical operation
should either serialize snapshot creation (one `Write` or `StreamWrite` per
file, in sequence) or handle `ErrSnapshotConflict` by retrying.

### Storage-Level Concurrency for Streaming Writes

Streaming writes (`Blobs.StreamWrite`, `Records.StreamWrite`) that produce large
payloads may use the storage adapter's multipart (chunked) upload path.

Backends that support conditional completion, such as S3 with
`If-None-Match`, provide two guarantees:
- Both the atomic and the multipart paths check atomically that the upload
  overwrites no existing object.
- The caller needs no coordination beyond the adapter.

Backends that do not support conditional completion carry a risk and an
obligation for the caller:
- The multipart path has a time-of-check-to-time-of-use (TOCTOU) interval
  between the preliminary check and completion.
- Callers MUST therefore either ensure single-writer semantics for each
  object path or use external coordination (distributed locks, queues, or
  similar mechanisms).

[`CONTRACT_STORAGE.md`](CONTRACT_STORAGE.md) defines adapter-specific
thresholds and guarantees.

---

## Error Semantics

Implementations MUST use the following error sentinels where applicable:

- `ErrNoSnapshots` for an empty history
- `ErrNotFound` for a missing snapshot ID
- `ErrPathExists` for an attempt to write to an existing path
- `ErrCodecNotStreamable` when the codec that `Records.StreamWrite` uses is
  not a `StreamingCodec`
- `ErrNilIterator` when a caller passes a `nil` iterator to
  `Records.StreamWrite`
- `ErrPartitioningNotSupported` when a caller invokes `Records.StreamWrite`
  on a partitioned layout
- `ErrInvalidArgument` for metadata that does not encode as a JSON object
- `ErrSnapshotConflict` when another writer committed after this writer
  resolved its parent, or when the pointer names a snapshot whose settings no
  manifest records (§Settings)
- `ErrSettingsMismatch` when the writer's settings differ from the
  dataset's (§Settings)

[`CONTRACT_ERRORS.md`](CONTRACT_ERRORS.md) defines the streaming-specific
error taxonomy and the handling guidance.

---

## Complexity Bounds

[CONTRACT_COMPLEXITY.md](CONTRACT_COMPLEXITY.md) provides complete
definitions and a glossary of variables.

### Parent Resolution

- In-memory cache (within a process): **0 store calls**.
- Persistent pointer (cold start): **1 store call** (1 `Get`). The
  snapshot that the pointer names is the parent even without a persisted
  manifest, because it may belong to a pending commit. Adopting
  any other parent would fork the history (CONTRACT_CORE §Head Pointer).
- Scan fallback (absent pointer, for backward compatibility): **O(N) through
  List**. The commit then creates the pointer.

Parent resolution MUST NOT use List when a valid pointer exists.

### Settings Check

Each run of the check costs the following store calls:
- Against the manifest that the pointer names: **1 `Get`**.
- While a first commit is pending: **2 `Get` calls** (the canonical
  manifest, then the staged copy).
- When neither exists: the scan of `Latest`, **O(N) through List**, and
  1 List of the dataset's directory.
- Against an empty dataset: **1 List** of the dataset's directory, to
  detect a history persisted under another layout.
- Against a dataset written before format version 1.1.0 with a Hive
  partitioner: **1 `Get` per snapshot** followed from the head to the first
  snapshot with files. That walk occurs once per dataset, because the first
  commit after the walk records the keys.

### Write Operations

| Operation | Store Calls (warm) | Memory |
|-----------|-------------------|--------|
| `Write` (unpartitioned) | 4 fixed | O(R + encoded) |
| `Write` (P partitions) | 2P + 3 | O(R + encoded) |
| `Blobs.StreamWrite` | 4 fixed | O(1) streaming |
| `Records.StreamWrite` | 4 fixed | O(1) streaming |

A dataset's first commit adds **one `Put` and one `Delete`** for its staged
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

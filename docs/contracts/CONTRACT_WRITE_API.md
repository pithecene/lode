# Lode Write API Contract

This document defines the required semantics for the write-facing dataset API
(`*Records[T]`, `*Blobs`, and `Root.RemoveDataset`).

---

## Goals

1. Deterministic snapshot creation.
2. Explicit metadata on every write.
3. Strictly linear snapshot history.
4. Safe, immutable commits and fenced whole-dataset removal.

---

## Non-Goals

- Query execution or filtering.
- Background compaction.
- Distributed coordination, consensus, or external lock management.

---

## Write Semantics

`*Records[T]` writes records of type `T`, and `*Blobs` writes one raw binary
object per snapshot. Callers open both handles from a writable `*Root`
(`Root.Records`, `Root.Blobs`), and each handle verifies its settings against
the dataset's head when it opens (§Settings).

### Buffered Writes (`Records.Write`, `Blobs.Write`)

- `Records.Write(ctx, records []T, metadata any) (*Snapshot, error)` and
  `Blobs.Write(ctx, data []byte, metadata any) (*Snapshot, error)` MUST
  create a new immutable `*Snapshot` on success.
- `metadata` MUST encode as a JSON object via `NewMetadata` (a struct, a
  string-keyed map, `Metadata`, or `nil`, including typed-nil string maps and
  struct pointers, which persists as `{}`). Any other value MUST fail with
  `ErrInvalidArgument` before any store call.
- Each commit MUST reference the resolved head snapshot, if any, as
  `ParentSnapshotID` ([`CONTRACT_CORE.md`](CONTRACT_CORE.md) §Head Pointer).
- Writes MUST NOT mutate existing snapshots, manifests, or data objects.
- On `Records.Write`, when `codec` implements `StatisticalCodec[T]`, the
  writer MUST invoke `EncodeWithStats` and record the returned `*ObjectStats`
  on the corresponding `ObjectRef`.
- On `Blobs.Write`, `Manifest.RowCount` MUST be `1`.

### Settings

Every writer of a dataset MUST share the dataset's persisted settings
([`CONTRACT_CORE.md`](CONTRACT_CORE.md) §Manifest Requirements): codec name,
`Codec.Describe()`, compressor (`Uncompressed`, `Gzip`, `Zstd`, or custom),
checksum algorithm (`SHA256`, `CRC32C`, `MD5`, or custom), partitioner and
ordered Hive keys, and `WithFlatPaths()`.

- When a writer opens, and again before its first commit if not yet settled
  against a post-`1.0.0` manifest, it MUST check its settings against the
  first of the following manifests that exists:
  1. the canonical manifest of the snapshot named by `latest`,
  2. that snapshot's staged manifest (`<dataset dir>/staged/<id>.json`),
  3. that snapshot's canonical manifest read a second time (catching a first
     commit that persisted its canonical manifest and deleted its staged copy
     between reads 1 and 2), or
  4. the newest manifest selected by scan when the pointer is absent or none
     of reads 1–3 found a manifest.
- A writer opened with `WithRecordedComponents()` MUST, when a head exists,
  take its compressor and checksum from the manifest that this check selects,
  in place of those that `WithCompressor` and `WithChecksum` configure. It
  reconstructs a bundled component by name, uses a configured component whose
  name matches, and MUST otherwise fail to open with `ErrSettingsMismatch`. A
  manifest without a checksum gives the writer none. Reads through the handle
  still resolve each snapshot's recorded components against the configured
  ones. The option takes effect
  wherever it appears among the options, and a writer without it keeps the
  check of its configured components. It changes neither the codec nor the
  layout check. With no head, the configured components apply, and a writer
  that opened without a head keeps them: if another first writer commits
  other components before it, its commit fails with `ErrSettingsMismatch`
  under the rule for racing first writers.
- With no pointer and no manifest under the writer's layout, the writer lists
  the dataset prefix and MUST reject with `ErrSettingsMismatch` if a
  `manifest.json` exists under another layout's topology (preventing parallel
  histories in one dataset prefix). An object at a
  manifest path of a bundled layout, with a generated snapshot ID ([`CONTRACT_LAYOUT.md`](CONTRACT_LAYOUT.md)
  §Canonical Path Grammars) is such a manifest whatever it holds. At any other path the writer MUST read the first
  4 KiB, and an object without the top-level `schema_name` of a Lode manifest
  there is a caller's file and MUST be ignored. A read that fails for another
  reason than `ErrNotFound` MUST fail the operation. Otherwise, the dataset is empty and the
  first commit records the writer's settings.
- Any mismatch between the writer's settings and the checked manifest—including
  a format `1.1.0` manifest that recorded non-default custom directory or
  manifest names—MUST fail immediately with `ErrSettingsMismatch`, without
  retrying.
- If two first writers with conflicting settings race on `CompareAndSwap`,
  the losing writer refreshes the head, checks against the winner's manifest,
  and returns `ErrSettingsMismatch` regardless of retry configuration.
- For format `1.0.0` Hive manifests (which predate `partition_keys`), the
  writer MUST verify its Hive key names and order against the partition path
  of the first ancestor snapshot with objects, returning
  `ErrSettingsMismatch` on mismatch, cycle, or missing/expired ancestor.

### `Blobs.StreamWrite` Semantics

- `Blobs.StreamWrite(ctx, r io.Reader, metadata any) (*Snapshot, error)`
  streams a single binary payload from `r` and commits the snapshot in one
  call when `r` reaches `io.EOF`.
- If `r` is `nil` (including a typed-nil `io.Reader`), `StreamWrite` MUST
  return `ErrInvalidArgument` before any store call.
- `metadata` follows the same `NewMetadata` validation as `Write` (`nil`
  persists as `{}`).
- Streamed writes MUST write directly to the final object path in a single
  pass without mutating existing objects, setting `Manifest.RowCount` to `1`.
- When uncompressed (`Uncompressed()`), `StreamWrite` streams `r` directly to
  `Store.Put` (computing the writer's `Checksum`, configured or adopted, inline) without spawning a
  pipe or background goroutine. When compressed, bytes stream through the
  compressor into `Store.Put`, and an early `Store.Put` error unblocks the
  stream immediately and takes precedence over downstream pipe errors.
- If `r` returns an error, `ctx` is canceled, or storage fails before commit,
  Lode writes no manifest. If `Store.Put` succeeded and the commit failed
  before any manifest write was attempted, Lode attempts a best-effort
  `Delete` of the streamed object; if a manifest write failed with an
  indeterminate outcome, Lode retains the object so a potentially committed
  snapshot remains intact.

### `Records.StreamWrite` Semantics

- `Records.StreamWrite(ctx, records iter.Seq2[T, error], metadata any)
  (*Snapshot, error)` consumes records one at a time from `records` and
  encodes them into a single data object.
- If `records` is `nil`, `StreamWrite` MUST return `ErrInvalidArgument`
  before any store call.
- If the codec does not implement `StreamingCodec[T]`, `StreamWrite` MUST
  return `ErrCodecNotStreamable`.
- If the dataset layout is partitioned, `StreamWrite` MUST return
  `ErrPartitioningNotSupported`.
- `Manifest.RowCount` MUST equal the total number of records consumed.
- When the writer computes a checksum, which `WithChecksum` configures or
  `WithRecordedComponents()` adopts (§Settings), the checksum is computed over
  the stored bytes during streaming and recorded on the `ObjectRef`.
- When the `StreamEncoder[T]` implements `StatisticalStreamEncoder`, the
  writer MUST collect `ObjectStats()` after `encoder.Close()` and record it
  on `ObjectRef.Stats`.
- On iterator error, encoder error, or storage error, no manifest is written;
  early `Store.Put` failures take precedence over subsequent pipe or close
  errors, and partial object cleanup follows the same rule as
  `Blobs.StreamWrite`.

### Timestamp Computation

- When `T` implements `Timestamped`, `Records.Write` and
  `Records.StreamWrite` MUST compute `MinTimestamp` and `MaxTimestamp` by
  calling `Timestamp()` on each written record.
- When `T` does not implement `Timestamped`, both fields MUST remain `nil`.

### Empty and Removed Dataset Behavior

- On an empty dataset (not removed): `Latest()` returns `ErrNoSnapshots`,
  `Snapshots()` returns an empty slice without error, and `Snapshot(id)`
  returns `ErrNotFound`.
- On a removed dataset (`latest` holding `lode:removed`): opening a handle or
  view, `Latest()`, `Snapshots()`, listings, retention, and commits MUST
  return `ErrRemoved`.

---

## Dataset Removal (`Root.RemoveDataset`)

`Root.RemoveDataset(ctx, id, opts...)` permanently removes dataset `id` from
the root's store:

1. Validate `id` as a single canonical path segment (`ErrInvalidArgument`
   before any store call) and resolve retry `DatasetOption` values.
2. Replace `<dataset dir>/latest` with `lode:removed` via `CompareAndSwap`
   against the observed pointer content (creating the pointer when absent,
   and treating an already-fenced `lode:removed` pointer as idempotent).
   Conflicting commits are retried per `WithRetry*`; if retries are spent,
   `RemoveDataset` returns `ErrSnapshotConflict` having deleted nothing.
3. If the previous pointer named a generated snapshot ID, verify that a
   committed manifest exists for that ID (under either unpartitioned or Hive
   grammar). If no manifest exists yet, the fenced head belongs to a pending
   or abandoned commit: `RemoveDataset` MUST return `ErrHeadUncommitted`
   leaving all objects intact behind the fence.
4. List `<dataset dir>/` and delete every object except `<dataset dir>/latest`
   (and its `.lock` companion on filesystem stores), ignoring `ErrNotFound`
   on individual deletes so a repeated `RemoveDataset` completes any
   interrupted cleanup.

The `latest` pointer holding `lode:removed` remains permanently as the fence
that prevents open handles or later calls from resurrecting or appending to
the removed dataset.

After attempting manifest publication, commits and `Sweep` repairs check the
fence and delete any manifests they may have persisted if fenced. A commit
that publishes successfully checks the fence with the caller's context. A
failed publication, a check prevented or interrupted by caller cancellation,
or a detected fence triggers reconciliation with a ten-second timeout
independent of caller cancellation. For `Sweep` repairs, reconciliation
always starts with the fence check. Any fence check and deletes during
reconciliation share that timeout.
When publication also failed, the error preserves both `ErrRemoved` and
the publication error, plus any cleanup error.

---

## Concurrency and Optimistic CAS

Snapshot history is strictly linear, enforced by `CompareAndSwap` on every
pointer update:
- `*Root` is safe for concurrent use by multiple goroutines.
- Individual `*Records[T]` and `*Blobs` writer handles are **not** safe for
  concurrent use across goroutines; concurrent writers in one process MUST
  open separate handles.
- On `ErrSnapshotConflict`, callers either refresh state via `Latest()` and
  re-commit manually, or configure `WithRetry(n)` (with
  `WithRetryBaseDelay`, `WithRetryMaxDelay`, `WithRetryJitter`, and
  `WithRetryObserver`) to retry manifest creation and pointer CAS
  automatically without rewriting already-persisted data objects.

---

## Complexity Bounds

See [`CONTRACT_COMPLEXITY.md`](CONTRACT_COMPLEXITY.md) for complete bounds.

| Operation | Store Calls (warm) | Memory |
|-----------|-------------------|--------|
| `Records.Write` (unpartitioned) | 3–4 fixed (`Put` data + `CompareAndSwap` + `Put` manifest) | O(R + encoded) |
| `Records.Write` (`P` partitions) | `2P + 2` to `2P + 3` | O(R + encoded) |
| `Blobs.Write` | 3–4 fixed | O(payload) |
| `Blobs.StreamWrite` | 3–4 fixed | O(1) streaming |
| `Records.StreamWrite` | 3–4 fixed | O(1) streaming |

Hot-path writes after open MUST NOT invoke `Store.List`.

---

## Prohibited Behaviors

- In-place mutation of existing data objects or manifests
- Inference or silent defaulting of caller metadata
- Branching histories or silent non-CAS pointer overwrites

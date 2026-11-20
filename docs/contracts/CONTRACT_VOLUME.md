# Lode Volume Model Contract

This document defines the contract for the `Volume` persistence paradigm in
package `lode/volume`.

---

## Goals

1. Represent sparse, resumable persistence of fixed-length byte address
   spaces explicitly.
2. Preserve manifest-driven commit visibility with cumulative,
   self-contained manifests.
3. Prevent staged write collisions across retries while maintaining read
   compatibility with earlier persisted block paths.

---

## Non-Goals

- Transport protocols, peer scheduling, or runtime orchestration.
- Implicit zero-filling of uncommitted gaps.
- Overlapping block overwrites or background compaction.

---

## Conceptual Model

- **Volume**: a fixed-length byte address space `[0..TotalLength)` identified
  by `volume.ID` (a single canonical path segment).
- **BlockRef**: a staged or committed byte range `(Offset, Length, Path,
  Checksum)`.
- **Manifest**: cumulative snapshot record (`SchemaName`
  `"lode-volume-manifest"`, `FormatVersion` `"1.0.0"`) containing the sorted,
  non-overlapping union of all committed `Blocks` up to and including the
  snapshot, `TotalLength`, `ParentSnapshotID`, `ChecksumAlgorithm`, and
  `Metadata` (`lode.Metadata`).
- **Snapshot**: pairs `volume.SnapshotID` (generated in `<instant>-<nonce>`
  form per [`CONTRACT_CORE.md`](CONTRACT_CORE.md) §Snapshot ID) with
  `*volume.Manifest`.

---

## Public API Surface

```go
type ID string

type SnapshotID string

type BlockRef struct {
    Offset   int64  `json:"offset"`
    Length   int64  `json:"length"`
    Path     string `json:"path"`
    Checksum string `json:"checksum,omitempty"`
}

type Manifest struct {
    SchemaName        string        `json:"schema_name"`
    FormatVersion     string        `json:"format_version"`
    VolumeID          ID            `json:"volume_id"`
    SnapshotID        SnapshotID    `json:"snapshot_id"`
    CreatedAt         time.Time     `json:"created_at"`
    Metadata          lode.Metadata `json:"metadata"`
    TotalLength       int64         `json:"total_length"`
    Blocks            []BlockRef    `json:"blocks"`
    ParentSnapshotID  SnapshotID    `json:"parent_snapshot_id,omitempty"`
    ChecksumAlgorithm string        `json:"checksum_algorithm,omitempty"`
}

type Snapshot struct {
    ID       SnapshotID
    Manifest *Manifest
}

type Writer interface {
    StageWriteAt(ctx context.Context, offset int64, r io.Reader) (BlockRef, error)
    Commit(ctx context.Context, blocks []BlockRef, metadata any) (*Snapshot, error)
}

type Reader interface {
    ReadAt(ctx context.Context, snapshotID SnapshotID, offset, length int64) ([]byte, error)
    ReaderAt(ctx context.Context, snapshotID SnapshotID) (SnapshotReaderAt, error)
    Latest(ctx context.Context) (*Snapshot, error)
    Snapshots(ctx context.Context) ([]*Snapshot, error)
    Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error)
}

type SnapshotReaderAt interface {
    io.ReaderAt
    Blocks() []BlockRef
}

type Volume interface {
    Writer
    Reader
    ID() ID
}

func New(id ID, storeFactory lode.Factory[lode.RangeStore], totalLength int64, opts ...Option) (Volume, error)
func Remove(ctx context.Context, id ID, storeFactory lode.Factory[lode.RangeStore], opts ...Option) error
func Head(ctx context.Context, id ID, storeFactory lode.Factory[lode.RangeStore]) (SnapshotID, error)
```

### Volume Options (`volume.Option`)

- `WithChecksum(c lode.Checksum)`: computes and records hex digests on staged
  blocks and records `ChecksumAlgorithm` in manifests (rejects `nil` with
  `lode.ErrInvalidArgument`).
- `WithRetry(n)`, `WithRetryBaseDelay(d)`, `WithRetryMaxDelay(d)`,
  `WithRetryJitter(j)`, `WithRetryObserver(fn)`: automatic CAS retry
  configuration for `Commit` and `Remove`, sharing Dataset defaults and
  validation.
- `nil` options passed to `New` or `Remove` MUST fail with
  `lode.ErrInvalidArgument` before calling `storeFactory`.

---

## Storage Layout and Nonced Block Paths

Volume uses a fixed internal storage layout:

```
volumes/<volume_id>/latest
volumes/<volume_id>/snapshots/<snapshot_id>/manifest.json
volumes/<volume_id>/staged/<snapshot_id>.json
volumes/<volume_id>/data/<offset>-<length>-<nonce>.bin
```

- `StageWriteAt` MUST generate a fresh 16-character lowercase hex nonce from
  a cryptographically secure random source for every staged block
  (`volumes/<volume_id>/data/<offset>-<length>-<nonce>.bin`). If a staging
  attempt or subsequent commit is abandoned, retrying `StageWriteAt` at the
  same `(offset, length)` writes to a distinct key and never fails with
  `lode.ErrPathExists`.
- Backward compatibility: `Commit` block validation and manifest validation
  MUST accept both nonced paths (`<offset>-<length>-<16hex>.bin`) and legacy
  pre-v1 unnonced paths (`<offset>-<length>.bin`) scoped under
  `volumes/<volume_id>/data/`. Any path outside `volumes/<volume_id>/data/`
  is rejected (`lode.ErrInvalidArgument` on `Commit`, `lode.ErrManifestInvalid`
  on manifest load).

---

## Write and Commit Semantics

- `StageWriteAt(ctx, offset, r)` validates `offset >= 0`, non-nil `r`
  (including typed-nil readers), non-empty payload, and
  `offset + length <= totalLength` (`lode.ErrInvalidArgument` otherwise),
  stages the block via `Store.Put`, and returns its `BlockRef`. Staged blocks
  are invisible to readers until committed.
- `Commit(ctx, blocks, metadata)` validates `metadata` via
  `lode.NewMetadata(metadata)` (`nil` persists as `{}`; non-object values
  return `lode.ErrInvalidArgument` before any store call), validates non-empty
  `blocks` within `[0, totalLength)`, resolves the parent (adopting a staged
  manifest at `volumes/<id>/staged/<parent>.json` if the parent commit is
  pending), verifies the writer's checksum algorithm matches the parent
  manifest's `ChecksumAlgorithm` (`lode.ErrSettingsMismatch` otherwise),
  merges `blocks` with the parent's cumulative blocks, rejects any overlap
  with `ErrOverlappingBlocks`, stages its own manifest, advances `latest` via
  `CompareAndSwap`, writes the canonical manifest, re-checks `latest` for
  `lode:removed` (deleting its newly written manifest and staged copy and
  returning `ErrRemoved` if fenced), and deletes the staged manifest
  ([`CONTRACT_CORE.md`](CONTRACT_CORE.md) §Head Pointer).
- Pending-commit completion: if `Commit` is retried with blocks that are
  already all present in a pending parent snapshot (whose pointer advanced
  before its canonical manifest write failed), `Commit` completes that
  pending parent from its staged copy and returns its `*Snapshot` (re-checking
  for `lode:removed` afterward). Once the parent's canonical manifest exists,
  committing with zero new blocks returns `lode.ErrInvalidArgument`.
- On CAS conflict with `WithRetry(n)`, `Commit` backs off, refreshes parent
  blocks, re-validates parent checksum compatibility and non-overlap
  (`lode.ErrSettingsMismatch` and `ErrOverlappingBlocks` abort immediately),
  and retries with a newly generated `SnapshotID`.

---

## Read Semantics

- `ReadAt(ctx, snapshotID, offset, length)` validates `offset >= 0`,
  `length > 0`, and `offset + length <= totalLength`
  (`lode.ErrInvalidArgument`), loads snapshot `snapshotID`, finds covering
  blocks in `O(log B + R)` time via binary search, returns `ErrRangeMissing`
  if any sub-range is uncovered (never zero-filling or returning partial
  bytes), and fetches covered slices via `RangeStore.ReadRange`.
- `ReaderAt(ctx, snapshotID)` loads and validates `snapshotID`'s manifest
  once and returns a concurrency-safe `SnapshotReaderAt` whose `ReadAt(p,
  off)` fetches block ranges only (returning `io.EOF` at or past
  `TotalLength` per `io.ReaderAt`) and whose `Blocks()` returns a defensive
  copy of the sorted `[]BlockRef`.

---

## Removal and Head Inspection

- `ErrRemoved` in package `volume` is an alias for `lode.ErrRemoved`
  (`volume.ErrRemoved == lode.ErrRemoved`).
- `Remove(ctx, id, storeFactory, opts...)`:
  1. Validates arguments (`lode.ErrInvalidArgument`) before calling
     `storeFactory`.
  2. Replaces `volumes/<id>/latest` with `lode:removed` via `CompareAndSwap`
     (retrying conflicts per `WithRetry*`, else returning
     `lode.ErrSnapshotConflict` with nothing deleted).
  3. Checks that any replaced generated snapshot ID has a canonical manifest
     (returning `lode.ErrHeadUncommitted` with objects intact behind the
     fence if pending).
  4. Deletes all manifests, staged manifests, and blocks under
     `volumes/<id>/`, leaving `volumes/<id>/latest` in place as the permanent
     fence so `Latest`, `Head`, and `Commit` return `ErrRemoved`.
- After advancing the head and attempting canonical manifest publication,
  a commit checks the removal fence and deletes its attempted manifest and
  staged copy if fenced. The check and deletes share a ten-second timeout
  independent of caller cancellation. When publication also failed, the
  error preserves both `ErrRemoved` and the publication error, plus any
  cleanup error.
- If a referenced block returns `lode.ErrNotFound`, `ReadAt` and
  `SnapshotReaderAt.ReadAt` check the removal fence and return `ErrRemoved`
  if the volume was removed.
- `Head(ctx, id, storeFactory)` reads `volumes/<id>/latest` without requiring
  `totalLength` and without writing: returns `SnapshotID` (including pending
  commits), `lode.ErrNoSnapshots` when empty, `ErrRemoved` when removed, or
  `lode.ErrHeadUncommitted` when the pointer is absent alongside existing
  manifests.

---

## Complexity Bounds

| Operation | Store Calls (warm) | Memory | CPU |
|-----------|-------------------|--------|-----|
| `StageWriteAt` | 1 `Put` | O(block) | O(block) |
| `Commit` | 6 fixed (`Get` parent + `Put` staged + CAS + `Put` canonical + `Get` pointer + `Delete` staged) | O(B) | O(B + K log K) |
| `ReadAt` | 1 `Get` + `R` `ReadRange` | O(L) | O(log B + R) |
| `ReaderAt` | 1 `Get` | O(B) | O(B) validation |
| `SnapshotReaderAt.ReadAt` | `R` `ReadRange` | O(L) | O(log B + R) |
| `Latest` | 2 `Get` calls | O(B) | O(1) |
| `Snapshot(id)` | 1 `Get` | O(B) | O(B) validation |
| `Snapshots` | 1 `List` + `S` `Get` calls | O(S × B_avg) | O(S × B_avg + S log S) |
| `Head` | 1 `Get` (+1 `List` only when pointer absent) | O(1) | O(1) |

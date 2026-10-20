# Lode Volume Model — Contract

This document defines the contract for the `Volume` persistence paradigm.
It is authoritative for all Volume implementations.

Status: **Active, experimental API** (v0.8.0; package `lode/volume` and experimental since D9)

---

## Goals

1. Represent sparse, resumable byte-range persistence explicitly.
2. Preserve manifest-driven commit semantics for partial materialization workflows.
3. Keep adapter behavior backend-agnostic and lifecycle semantics explicit.

---

## Non-goals

- Torrent protocol behavior (pieces, peers, trackers).
- Networking, scheduling, runtime orchestration, or execution policy.
- Hidden background mutation or inference of missing ranges.

---

## Conceptual Model

### Volume

A logical byte address space `[0..N)` identified by `ID` and `TotalLength`.

### Snapshot

An immutable commit that records the **cumulative** set of all verified
committed blocks across the volume's history. Each snapshot is self-contained:
its manifest is the sole authority for read visibility at that point in time.

### SnapshotID

A stable identifier for a committed Volume snapshot. Generated using Unix
nanoseconds (consistent with `DatasetSnapshotID`).

### BlockRef

A reference to a staged or committed byte block (offset, length, object path).
A block is not committed until included in a snapshot manifest.

Note: `BlockRef` is a Volume concept. The `DatasetReader` API uses
`ManifestRef` for dataset manifest locators.

### Volume Manifest

The authoritative description of committed blocks for a snapshot. Manifests
are **cumulative**: each manifest contains the full set of committed blocks,
not just the blocks added in that commit. This ensures every snapshot is
independently interpretable without chain traversal.

---

## Manifest Requirements

Volume manifests MUST include at minimum:
- schema name + version
- volume ID
- snapshot ID
- parent snapshot ID (when applicable)
- total length
- creation time
- explicit metadata object
- committed blocks list (**cumulative** — all blocks, not just new)

Each committed block MUST record:
- offset
- length
- immutable object reference/path
- optional checksum metadata when configured

Manifests are cumulative:
- Each manifest contains the full set of committed blocks across all
  prior commits, plus the new blocks added in this commit.
- A single manifest is sufficient to determine complete read visibility.
- No chain traversal is required to interpret a snapshot.

Absence is meaningful:
- Ranges not listed are uncommitted and MUST NOT be inferred.

---

## Write Lifecycle

Volume write lifecycle is explicit:

1. **Stage**: Data is written to the final storage path. Staged data is not
   visible to readers (no manifest references it). Staged data files use the
   path `volumes/<volume_id>/data/<offset>-<length>.bin`.
2. **Verify**: Caller/runtime validates data externally (e.g., hash check).
3. **Commit**: Verified blocks are recorded in a new immutable snapshot
   manifest alongside all previously committed blocks (cumulative).

Rules:
- Unverified or uncommitted blocks MUST NOT appear in committed manifests.
- Commit visibility is manifest-driven, same as Dataset.
- Snapshots are immutable once committed.
- Overlapping committed blocks within a snapshot are invalid and MUST return
  `ErrOverlappingBlocks`. Overlap validation applies to the full cumulative
  block set (existing + new), not just new blocks.
- Commit MUST include at least one new block. Empty commits (no new blocks)
  are invalid.

### Staged Data Lifecycle

Staged data is written directly to the final object path. Manifest-driven
visibility means uncommitted data files are invisible to readers — identical
to Dataset's partial-write behavior.

- Abandoned staged data (never committed) is harmless but may remain in storage.
- Cleanup of orphaned staged data is the caller's responsibility.
- No Volume-level Abort API is required.
- No implicit background cleanup is permitted.

### Resume Semantics

Resume is explicit and caller-driven.

- `Volume.Latest(ctx)` returns the most recent committed snapshot, which
  contains the cumulative set of all committed blocks.
- The caller determines which ranges are missing and re-stages them.
- Volume does NOT discover or recover uncommitted staged data.
- No hidden inference or data directory scanning is permitted.

---

## Read Semantics

Volume reads are range-first:
- `ReadAt(snapshotID, offset, length)` succeeds only if the entire requested
  range is covered by committed blocks.

Required behavior:
- If any sub-range is missing, return an explicit missing-range error.
- No zero-fill fallback.
- No partial-success ambiguity for committed read paths.
- Reads against a snapshot MUST reflect exactly the manifest state at commit time.

---

## Public API Surface

Volume is **experimental** (`docs/V1_API_DECISIONS.md` D9): it lives in
package `lode/volume` and is outside the v1.0 compatibility promise until it
passes its own stability gate (`docs/V1_READINESS.md` §4). This contract
binds its behavior in every release; the API shape may change between
releases. The persisted format (Storage Layout, Manifest Requirements) is
not experimental: data written by any release stays readable.

The public Volume API is explicit, minimal, and composed from `Writer` +
`Reader` sub-interfaces (package `volume`; `lode.` names are the shared
types it uses):

```go
// ID uniquely identifies a volume.
type ID string

// SnapshotID uniquely identifies an immutable volume snapshot.
type SnapshotID string

// BlockRef references a staged or committed byte block within a volume.
type BlockRef struct {
    Offset   int64
    Length   int64
    Path     string
    Checksum string // populated when WithChecksum is configured
}

// Snapshot represents an immutable point-in-time view of a volume.
type Snapshot struct {
    ID       SnapshotID
    Manifest *Manifest
}

// Writer defines the write surface for a volume.
type Writer interface {
    // StageWriteAt writes data at an offset and returns a block handle.
    // Staged data is not visible until Commit is called.
    StageWriteAt(ctx context.Context, offset int64, r io.Reader) (BlockRef, error)

    // Commit records the provided blocks into a new immutable snapshot.
    // The resulting manifest is cumulative: it includes all previously committed
    // blocks plus the new blocks. Commit MUST include at least one new block.
    Commit(ctx context.Context, blocks []BlockRef, metadata lode.Metadata) (*Snapshot, error)
}

// Reader defines the read surface for a volume.
type Reader interface {
    // ReadAt reads a fully committed range from a snapshot.
    ReadAt(ctx context.Context, snapshotID SnapshotID, offset, length int64) ([]byte, error)

    // Latest returns the most recently committed snapshot.
    // Returns lode.ErrNoSnapshots if no snapshots exist.
    Latest(ctx context.Context) (*Snapshot, error)

    // Snapshots lists all committed snapshots.
    Snapshots(ctx context.Context) ([]*Snapshot, error)

    // Snapshot retrieves a specific snapshot by ID.
    // Returns lode.ErrNotFound if the snapshot does not exist.
    Snapshot(ctx context.Context, id SnapshotID) (*Snapshot, error)
}

// Volume composes the full read/write surface for a sparse byte space.
type Volume interface {
    Writer
    Reader
    ID() ID
}

// New creates a volume with a fixed total length.
func New(id ID, storeFactory lode.StoreFactory, totalLength int64, opts ...Option) (Volume, error)
```

### Volume Options

Volume has its own `Option` type; Dataset and DatasetReader options are a
different type and are rejected at compile time. Options:

- `WithChecksum(c lode.Checksum)` — opt-in integrity checksums on staged blocks.
- `WithRetryCount(n)` — bounded automatic CAS retry on commit conflict.
- `WithRetryBaseDelay(d)`, `WithRetryMaxDelay(d)`, `WithRetryJitter(j)` —
  backoff tuning for CAS retry.
- `WithRetryObserver(fn)` — per-retry callback for observability; same
  semantics as Dataset (see `CONTRACT_WRITE_API.md` §Retry observer).

Retry options share Dataset's defaults and validation: an out-of-range value
returns `lode.ErrInvalidArgument` with the same message.

### API Constraints

- `StageWriteAt` MUST NOT create snapshot visibility.
- `Commit` MUST be manifest-driven and immutable.
- `Commit` MUST include at least one new block.
- `Commit` MUST validate no overlaps in the full cumulative block set.
- `ReadAt` MUST return `volume.ErrRangeMissing` if any sub-range is uncommitted.
- `Reader` methods parallel Dataset's read surface.
- `Volume` composes `Writer` + `Reader`.

---

## Error Semantics

- Missing committed range MUST return `volume.ErrRangeMissing`.
- Overlapping blocks at Commit MUST return `volume.ErrOverlappingBlocks`.
- Empty block list at Commit MUST return an error.
- `nil` metadata at Commit MUST be coalesced to empty (`lode.Metadata{}`).
- Range reads MUST NOT return partial data without error.
- `Latest` on empty volume MUST return `lode.ErrNoSnapshots`.
- `Snapshot` for unknown ID MUST return `lode.ErrNotFound`.
- Stale parent at Commit MUST return `lode.ErrSnapshotConflict` when the store
  implements `lode.ConditionalWriter`.

---

## Concurrency

Snapshot history for a Volume is linear.

### Volume Write Concurrency Matrix

| Pattern | Writers | Blocks | History | Status |
|---------|---------|--------|---------|--------|
| Single writer | 1 process | Non-overlapping blocks | Linear (guaranteed) | ✅ Supported |
| Multi-process, serialized | N processes, external coordination | Non-overlapping blocks | Linear (caller-enforced) | ✅ Supported (caller owns coordination) |
| Multi-process, uncoordinated | N processes, no coordination | Non-overlapping blocks | Linear (CAS-enforced) | ✅ Safe (CAS) |

### Optimistic Concurrency (CAS)

When the storage adapter implements `ConditionalWriter`, Volume detects
concurrent commits and returns `ErrSnapshotConflict`.

The mechanism is identical to Dataset CAS (see `CONTRACT_WRITE_API.md`
§Optimistic Concurrency):
- Commit captures expected parent at resolve time.
- Pointer update uses `CompareAndSwap` when available.
- Stale parent → `ErrSnapshotConflict`.
- Fallback to Delete+Put when `ConditionalWriter` is not implemented.

**Volume CAS retry merges naturally:**
- Non-overlapping blocks from concurrent writers can be merged on retry
  without conflict: re-read `Latest()`, union new blocks with the updated
  cumulative manifest, re-commit.
- Overlapping blocks remain invalid regardless of CAS (see `ErrOverlappingBlocks`).

**Automatic retry (opt-in):**

`WithRetryCount(n)` enables bounded automatic retry within `Commit`.
On `ErrSnapshotConflict`, the commit:
1. Sleeps with jittered exponential backoff.
2. Calls `Latest()` to refresh the in-memory CAS cache.
3. Re-resolves the parent and re-merges blocks against the new parent.
4. Validates no overlaps in the re-merged cumulative set.
5. Retries the pointer CAS with a fresh snapshot ID.

`ErrOverlappingBlocks` terminates the retry loop immediately (non-retryable).
If `WithRetryObserver(fn)` is configured, `fn` is called once per retry
before step 1.
If all retries are exhausted, `ErrSnapshotConflict` is returned as usual.

**Single-writer compatibility:**
- Single-writer workflows are unaffected.
- External coordination remains valid but is no longer required when
  using a CAS-capable store.

---

## Adapter Obligations

No new storage engine type is required.

Existing adapter capabilities remain sufficient:
- immutable object writes
- list/exists/get/delete behavior
- range reads

Adapters MUST preserve manifest-defined truth semantics.

## Storage Layout

Volume uses a **fixed internal layout**. This is not configurable.
The `Layout` abstraction is a Dataset-specific concept and does not apply
to Volume.

```
volumes/<volume_id>/snapshots/<snapshot_id>/manifest.json
volumes/<volume_id>/data/<offset>-<length>.bin
```

- Manifest paths follow the same commit-visibility pattern as Dataset.
- Data files are named by byte range for debuggability.
- This layout is an implementation detail, not part of the public API.

---

## Dataset Boundary

`Dataset` and `Volume` are distinct containers over one snapshot-history core
(the snapshot chain, latest pointer and CAS commit), with distinct semantics:

- Dataset streaming: atomic construction of complete objects
- Volume streaming: incremental commitment of partial truth
- Neither is a wrapper or special case of the other

If a workflow requires sparse, resumable, out-of-order, or range-verified truth,
it belongs to `Volume`, not `Dataset`.

Design discipline:
- Adapters remain minimal; lifecycle semantics stay in Lode core.
- Runtime-aware policy (scheduling, peer management, execution strategy)
  belongs in consuming systems, not in Lode.

---

## Complexity Bounds

See [CONTRACT_COMPLEXITY.md](CONTRACT_COMPLEXITY.md) for full definitions and variable glossary.

### Write Operations

| Operation | Store Calls (warm) | Memory | CPU |
|-----------|-------------------|--------|-----|
| `StageWriteAt` | 1 Put | O(block) | O(block) |
| `Commit` | 4 fixed | O(B) manifest | O(B + K log K) |

Cumulative manifest size is O(B). This is inherent in the cumulative design.

### Read Operations

| Operation | Store Calls | Memory | CPU |
|-----------|-------------|--------|-----|
| `ReadAt` | 1 Get + R ReadRange | O(L) | **O(log B + R)** |
| `Latest` | 2 (pointer + manifest) | O(B) | O(1) |
| `Snapshot(id)` | 1 Get | O(B) | O(B) validation |
| `Snapshots` | 1 List + S Gets | O(S × B_avg) | O(S × B_avg + S log S) |

Block lookup MUST be O(log B + R) where R is covering blocks.
Implementations MUST NOT perform per-read sorted-ness checks.
Sort verification MUST occur at manifest load time, not at read time.

`Snapshots()` cost grows with snapshot count × cumulative block count.
Callers MUST NOT use `Snapshots()` on hot paths.

---

## Prohibited Behaviors

- Inferring committed blocks not present in the manifest.
- Treating staged/unverified data as committed truth.
- Implicit background compaction or mutation of committed blocks.

---

## Completeness

A Volume is considered complete when the union of committed blocks covers
the full address space `[0..N)`. Completeness is derived from committed blocks,
not stored as a separate flag.

---

## Invariants

The following invariants MUST hold:

1. A snapshot's manifest is the sole authority for read visibility.
2. Each manifest is self-contained (cumulative blocks, no chain traversal needed).
3. No data may be readable unless referenced by a committed snapshot.
4. All committed blocks are immutable.
5. Committed blocks within a snapshot MUST NOT overlap.
6. Snapshot history for a Volume is linear.
7. Completeness is derived solely from committed blocks.

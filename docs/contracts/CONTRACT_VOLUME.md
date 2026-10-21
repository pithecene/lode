# Lode Volume Model — Contract

This document defines the contract for the `Volume` persistence paradigm.
It is authoritative for all Volume implementations.

Status: **Active, experimental API** (v0.8.0; package `lode/volume` and experimental since D9)

---

## Goals

1. Represent sparse, resumable persistence of byte ranges explicitly.
2. Preserve manifest-driven commit semantics for workflows that materialize
   data partially.
3. Keep adapter behavior independent of the backend and lifecycle semantics
   explicit.

---

## Non-Goals

- BitTorrent protocol behavior (pieces, peers, trackers).
- Networking, scheduling, runtime orchestration, or execution policy.
- Concealed background mutation, or inference of missing ranges.

---

## Conceptual Model

### Volume

A logical byte address space `[0..N)`, identified by `ID` and
`TotalLength`.

### Snapshot

An immutable commit that records the **cumulative** set of all verified,
committed blocks across the volume's history. Each snapshot is
self-contained: its manifest is the sole authority for read visibility at
that point in time.

### SnapshotID

A stable identifier for a committed Volume snapshot, generated in the same
form as `DatasetSnapshotID` (CONTRACT_CORE §Snapshot ID).

### BlockRef

A reference to a staged or committed byte block (offset, length, and object
path). A block is not committed until a snapshot manifest includes it.

Note: `BlockRef` is a Volume concept. The `DatasetReader` API uses
`ManifestRef` for dataset manifest locators.

### Volume Manifest

The authoritative description of a snapshot's committed blocks. Manifests
are **cumulative**: each manifest contains the complete set of committed
blocks, not only the blocks added in that commit, so every snapshot can be
interpreted independently, without traversing the chain.

---

## Manifest Requirements

Volume manifests MUST include at least:
- the schema name and version
- the volume ID
- the snapshot ID
- the parent snapshot ID, when applicable
- the total length
- the creation time
- an explicit metadata object
- the list of committed blocks (**cumulative**: all blocks, not only the new
  ones)

Each committed block MUST record:
- its offset
- its length
- an immutable object reference or path
- checksum metadata, when configured

Manifests are cumulative:
- Each manifest contains the complete set of committed blocks from all
  prior commits, plus the blocks added in this commit.
- A single manifest suffices to determine complete read visibility.
- Interpreting a snapshot requires no traversal of the chain.

Absence is meaningful:
- Ranges that are not listed are uncommitted and MUST NOT be inferred.

---

## Write Lifecycle

The Volume write lifecycle is explicit:

1. **Stage**: data is written to its final storage path. Staged data is not
   visible to readers, because no manifest references it. Staged data files
   use the path `volumes/<volume_id>/data/<offset>-<length>.bin`.
2. **Verify**: the caller or runtime validates the data externally (for
   example, by verifying a hash).
3. **Commit**: verified blocks are recorded in a new immutable snapshot
   manifest, together with all previously committed blocks (cumulatively).

Rules:
- Unverified or uncommitted blocks MUST NOT appear in committed manifests.
- Commit visibility is manifest-driven, as for Datasets.
- Snapshots are immutable once committed.
- Overlapping committed blocks within a snapshot are invalid, and `Commit`
  MUST return `ErrOverlappingBlocks` for them. Overlap validation applies to
  the complete cumulative set of blocks (existing and new), not only to the
  new blocks.
- A commit MUST include at least one new block. An empty commit (one without
  new blocks) is invalid.

### Staged Data Lifecycle

Staged data is written directly to the final object path. Because
visibility is manifest-driven, uncommitted data files are invisible to
readers, exactly as for a Dataset's partial writes.

- Abandoned staged data (data that is never committed) is harmless but may
  remain in storage.
- Removing orphaned staged data is the caller's responsibility.
- No Volume-level `Abort` API is required.
- No implicit background removal is permitted.

### Resumption Semantics

Resumption is explicit and initiated by the caller.

- `Volume.Latest(ctx)` returns the most recent committed snapshot, which
  contains the cumulative set of all committed blocks.
- The caller determines which ranges are missing and stages them again.
- Volume does NOT discover or recover uncommitted staged data.
- Concealed inference and scanning of the data directory are not
  permitted.

---

## Read Semantics

Volume reads are organized by range:
- `ReadAt(snapshotID, offset, length)` succeeds only if committed blocks
  cover the entire requested range.

Required behavior:
- If any part of the range is missing, return an explicit missing-range
  error.
- Missing bytes are never filled with zeros.
- A committed read path never succeeds partially.
- A read of a snapshot MUST reflect exactly the state of its manifest at
  commit time.

---

## Public API Surface

Volume is **experimental** (`docs/V1_API_DECISIONS.md` D9). It resides in
the package `lode/volume` and is excluded from the v1.0 compatibility
commitment until it passes its own stability gate (`docs/V1_READINESS.md`
§4). This contract governs its behavior in every release, but the shape of
the API may change between releases. The persisted format (§Storage Layout,
§Manifest Requirements) is not experimental: data written by any release
remains readable.

The public Volume API is explicit and minimal, and it is composed of the
`Writer` and `Reader` interfaces (in the package `volume`; names prefixed
with `lode.` denote the shared types that it uses):

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
different type and are rejected at compile time. The options are:

- `WithChecksum(c lode.Checksum)`: integrity checksums on staged blocks
  (disabled by default).
- `WithRetryCount(n)`: a bounded number of automatic CAS retries after a
  commit conflict.
- `WithRetryBaseDelay(d)`, `WithRetryMaxDelay(d)`, `WithRetryJitter(j)`:
  configuration of the backoff between CAS retries.
- `WithRetryObserver(fn)`: a callback invoked on each retry, with the same
  semantics as for a Dataset (see `CONTRACT_WRITE_API.md` §Optimistic
  Concurrency (CAS)).

Retry options share the Dataset defaults and validation: an out-of-range
value returns `lode.ErrInvalidArgument` with the same message.

### API Constraints

- `StageWriteAt` MUST NOT make anything visible in a snapshot.
- `Commit` MUST be manifest-driven and immutable.
- `Commit` MUST include at least one new block.
- `Commit` MUST verify that no blocks overlap in the complete cumulative
  set.
- `ReadAt` MUST return `volume.ErrRangeMissing` if any part of the range is
  uncommitted.
- The `Reader` methods correspond to the Dataset read surface.
- `Volume` composes `Writer` and `Reader`.

---

## Error Semantics

- A missing committed range MUST produce `volume.ErrRangeMissing`.
- Overlapping blocks at `Commit` MUST produce `volume.ErrOverlappingBlocks`.
- An empty block list at `Commit` MUST produce an error.
- `nil` metadata at `Commit` MUST be converted to empty metadata
  (`lode.Metadata{}`).
- Range reads MUST NOT return partial data without an error.
- `Latest` on an empty volume MUST return `lode.ErrNoSnapshots`.
- `Snapshot` for an unknown ID MUST return `lode.ErrNotFound`.
- An obsolete parent at `Commit` MUST produce `lode.ErrSnapshotConflict`
  when the store implements `lode.ConditionalWriter`.
- When the pointer names a snapshot whose manifest has not been persisted
  (a pending or abandoned commit, CONTRACT_CORE §Head Pointer), Commit MUST
  return `lode.ErrSnapshotConflict`, because the parent's blocks cannot be
  merged without its manifest. If retries are configured, Commit retries.
  After an abandoned commit, every Volume commit returns
  `lode.ErrSnapshotConflict` until the pointer names a snapshot whose
  manifest exists: either the missing manifest is persisted, or an operator
  sets the pointer to an earlier snapshot.

---

## Concurrency

The snapshot history of a Volume is linear.

### Volume Write Concurrency Matrix

| Pattern | Writers | Blocks | History | Status |
|---------|---------|--------|---------|--------|
| Single writer | 1 process | Non-overlapping blocks | Linear (guaranteed) | ✅ Supported |
| Multiple processes, serialized | N processes, external coordination | Non-overlapping blocks | Linear (enforced by the caller) | ✅ Supported (the caller provides coordination) |
| Multiple processes, uncoordinated, store implements `ConditionalWriter` | N processes, no coordination | Non-overlapping blocks | Linear (enforced by CAS) | ✅ Supported; after an abandoned commit, commits return `ErrSnapshotConflict` until the pointer names a snapshot whose manifest exists (§Error Semantics) |
| Multiple processes, uncoordinated, store without `ConditionalWriter` | N processes, no coordination | Non-overlapping blocks | Undefined: concurrent Delete+Put commits can fork the history and omit blocks from the head | ❌ Unsafe |

### Optimistic Concurrency (CAS)

When the storage adapter implements `ConditionalWriter`, Volume detects
concurrent commits and returns `ErrSnapshotConflict`.

The mechanism is identical to Dataset CAS (see `CONTRACT_WRITE_API.md`
§Optimistic Concurrency (CAS)):
- `Commit` records the expected parent when it resolves the parent.
- The pointer is updated with `CompareAndSwap` when available.
- An obsolete parent produces `ErrSnapshotConflict`.
- When `ConditionalWriter` is not implemented, the commit uses Delete+Put.

**A Volume CAS retry merges blocks without conflict:**
- Non-overlapping blocks from concurrent writers can be merged on retry:
  call `Latest()` again, add the new blocks to the updated cumulative
  manifest, and commit again.
- Overlapping blocks remain invalid regardless of CAS (see
  `ErrOverlappingBlocks`).

**Automatic retry (disabled by default):**

`WithRetryCount(n)` enables a bounded number of automatic retries within
`Commit`. On `ErrSnapshotConflict`, the commit:
1. Waits for an exponential backoff interval with jitter.
2. Calls `Latest()` to refresh the in-memory CAS cache.
3. Resolves the parent again and merges the blocks against the new parent.
4. Verifies that no blocks overlap in the merged cumulative set.
5. Retries the pointer CAS with a new snapshot ID.

`ErrOverlappingBlocks` terminates the retry loop immediately, because it is
not retryable. If `WithRetryObserver(fn)` is configured, `fn` is called once
per retry, before step 1. If all retries are exhausted,
`ErrSnapshotConflict` is returned as it would be without retries.

**Single-writer compatibility:**
- Single-writer workflows are unaffected.
- External coordination remains valid but is no longer required with a
  store that supports CAS.

---

## Adapter Obligations

No new type of storage engine is required.

The existing adapter capabilities remain sufficient:
- immutable object writes;
- `List`, `Exists`, `Get`, and `Delete`; and
- range reads.

Adapters MUST preserve the semantics under which the manifest defines what
is committed.

## Storage Layout

Volume uses a **fixed internal layout**, which is not configurable. The
`Layout` abstraction is specific to Datasets and does not apply to Volume.

```
volumes/<volume_id>/snapshots/<snapshot_id>/manifest.json
volumes/<volume_id>/data/<offset>-<length>.bin
```

- Manifest paths follow the same commit-visibility pattern as for a
  Dataset.
- Data files are named by byte range to facilitate debugging.
- This layout is an implementation detail, not part of the public API.

---

## Dataset Boundary

`Dataset` and `Volume` are distinct containers built on one snapshot-history
core (the snapshot chain, the latest pointer, and the CAS commit), and they
have distinct semantics:

- Dataset streaming constructs complete objects atomically.
- Volume streaming commits partial data incrementally.
- Neither is a wrapper around, or a special case of, the other.

A workflow that requires sparse, resumable, out-of-order, or range-verified
data belongs to `Volume`, not to `Dataset`.

Design discipline:
- Adapters remain minimal; lifecycle semantics remain in the Lode core.
- Policy that depends on the runtime (scheduling, peer management, execution
  strategy) belongs in consuming systems, not in Lode.

---

## Complexity Bounds

See [CONTRACT_COMPLEXITY.md](CONTRACT_COMPLEXITY.md) for complete definitions
and a glossary of variables.

### Write Operations

| Operation | Store Calls (warm) | Memory | CPU |
|-----------|-------------------|--------|-----|
| `StageWriteAt` | 1 Put | O(block) | O(block) |
| `Commit` | 4 fixed | O(B) manifest | O(B + K log K) |

The size of the cumulative manifest is O(B), a cost inherent in the
cumulative design.

### Read Operations

| Operation | Store Calls | Memory | CPU |
|-----------|-------------|--------|-----|
| `ReadAt` | 1 Get + R ReadRange | O(L) | **O(log B + R)** |
| `Latest` | 2 (pointer + manifest) | O(B) | O(1) |
| `Snapshot(id)` | 1 Get | O(B) | O(B) validation |
| `Snapshots` | 1 List + S Gets | O(S × B_avg) | O(S × B_avg + S log S) |

Block lookup MUST be O(log B + R), where R is the number of covering
blocks. Implementations MUST NOT verify the sort order on each read; sort
verification MUST occur when the manifest is loaded, not when it is read.

The cost of `Snapshots()` grows with the product of the number of snapshots
and the cumulative number of blocks. Callers MUST NOT use `Snapshots()` on
hot paths.

---

## Prohibited Behaviors

- Inferring committed blocks that are absent from the manifest.
- Treating staged or unverified data as committed.
- Implicit background compaction or mutation of committed blocks.

---

## Completeness

A Volume is complete when the union of its committed blocks covers the
entire address space `[0..N)`. Completeness is derived from the committed
blocks, not stored as a separate flag.

---

## Invariants

The following invariants MUST hold:

1. A snapshot's manifest is the sole authority for read visibility.
2. Each manifest is self-contained (its blocks are cumulative, so no
   traversal of the chain is needed).
3. No data is readable unless a committed snapshot references it.
4. All committed blocks are immutable.
5. Committed blocks within a snapshot MUST NOT overlap.
6. The snapshot history of a Volume is linear.
7. Completeness is derived solely from committed blocks.

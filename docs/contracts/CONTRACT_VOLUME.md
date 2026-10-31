# Lode Volume Model Contract

This document defines the contract for the `Volume` persistence paradigm.
It is authoritative for all Volume implementations.

---

## Goals

1. Represent sparse, resumable persistence of byte ranges explicitly.
2. Preserve manifest-driven commit semantics for workflows that materialize
   data partially.
3. Keep adapter behavior independent of the backend, and make lifecycle
   semantics explicit.

---

## Non-Goals

- BitTorrent protocol behavior (pieces, peers, trackers).
- Networking, scheduling, runtime orchestration, or execution policy.
- Concealed background mutation, or inference of missing ranges.

---

## Conceptual Model

### Volume

A Volume is a logical byte address space `[0..N)`. Its `ID` and
`TotalLength` identify it.

### Snapshot

A snapshot is an immutable commit that records the **cumulative** set of
all verified, committed blocks across the volume's history. Each snapshot is
self-contained: its manifest alone determines which data a reader of that
snapshot can read.

### SnapshotID

A `SnapshotID` is a stable identifier for a committed Volume snapshot.
Volume generates it in the same form as `DatasetSnapshotID` (CONTRACT_CORE
§Snapshot ID).

### BlockRef

A `BlockRef` references a staged or committed byte block by offset, length,
and object path. A block is not committed until a snapshot manifest includes it.

`BlockRef` is a Volume type. The dataset read API uses
`ManifestRef` for dataset manifest locators.

### Volume Manifest

A Volume manifest is the authoritative description of a snapshot's
committed blocks. Manifests are **cumulative**: each manifest contains the
complete set of committed blocks, not only the blocks that its own commit
added, so a reader can interpret any snapshot without traversing the
chain.

---

## Manifest Requirements

Volume manifests MUST include at least:
- the schema name and version
- the volume ID
- the snapshot ID
- the parent snapshot ID, except in the first snapshot
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
- Each manifest contains every block from earlier commits, plus the blocks
  that its own commit adds.
- A single manifest suffices to determine complete read visibility.
- Interpreting a snapshot requires no traversal of the chain.

Absence from the manifest is meaningful:
- An implementation MUST treat any range that the manifest omits as
  uncommitted and MUST NOT infer its contents.

---

## Write Lifecycle

The Volume write lifecycle consists of three explicit steps:

1. **Stage**: the caller writes data to its final storage path. Staged data is not
   visible to readers, because no manifest references it. Staged data files
   use the path `volumes/<volume_id>/data/<offset>-<length>.bin`.
2. **Verify**: the caller or runtime validates the data externally (for
   example, by verifying a hash).
3. **Commit**: Volume records the verified blocks in a new immutable
   snapshot manifest, together with all previously committed blocks.

The following rules govern the write lifecycle:
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

The caller writes staged data directly to the final object path. Because
visibility is manifest-driven, uncommitted data files are invisible to
readers, exactly as for a Dataset's partial writes.

- Abandoned staged data, which no commit references, is harmless but may
  remain in storage.
- The caller removes abandoned staged data.
- This contract requires no Volume-level `Abort` API.
- Volume performs no implicit background removal.

### Resumption Semantics

The caller initiates resumption explicitly.

- `Volume.Latest(ctx)` returns the most recent committed snapshot, which
  contains the cumulative set of all committed blocks.
- The caller determines which ranges are missing and stages them again.
- Volume does NOT discover or recover uncommitted staged data.
- Volume MUST NOT infer uncommitted ranges or scan the data directory.

---

## Read Semantics

Volume serves reads by byte range:
- `ReadAt(snapshotID, offset, length)` succeeds only if committed blocks
  cover the entire requested range.

A range read MUST behave as follows:
- If any part of the range is missing, the read returns an explicit
  missing-range error.
- A read never fills missing bytes with zeros.
- A read of committed data never succeeds partially.
- A read of a snapshot MUST reflect exactly the state of its manifest at
  commit time.

---

## Public API Surface

Volume resides in the package `lode/volume`. This contract governs its
behavior in every release. Every release reads the data that an earlier
release persisted, because the storage layout and the manifest format
(§Storage Layout, §Manifest Requirements) are stable persisted formats.

The public Volume API is explicit and minimal and comprises the `Writer` and
`Reader` interfaces in the package `volume`. Names prefixed with `lode.`
denote the shared types of that API:

```go
// ID uniquely identifies a volume. It is one path segment
// (CONTRACT_STORAGE §Names in Keys).
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
    Commit(ctx context.Context, blocks []BlockRef, metadata Metadata) (*Snapshot, error)
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
func New(id ID, storeFactory lode.Factory[lode.RangeStore], totalLength int64, opts ...Option) (Volume, error)
```

### Volume Options

Volume has its own `Option` type. Dataset options (`DatasetOption`,
`ViewOption`, and `RootOption`) have different types, so the compiler
rejects them as Volume options. Volume provides the following options:

- `WithChecksum(c lode.Checksum)`: integrity checksums on staged blocks
  (disabled by default).
- `WithRetryCount(n)`: a bounded number of automatic CAS retries after a
  commit conflict.
- `WithRetryBaseDelay(d)`, `WithRetryMaxDelay(d)`, `WithRetryJitter(j)`:
  the backoff interval between CAS retries.
- `WithRetryObserver(fn)`: a callback invoked on each retry, with the same
  semantics as for a Dataset (see [`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md) §Optimistic
  Concurrency (CAS)).

Retry options share the Dataset defaults and validation: an out-of-range
value produces `lode.ErrInvalidArgument` with the same message as for a
Dataset.

### API Constraints

- `StageWriteAt` MUST NOT make any block visible in a snapshot.
- `Commit` MUST make blocks visible only through the manifest and MUST
  produce an immutable snapshot.
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
- `Commit` MUST convert `nil` metadata to empty metadata
  (`volume.Metadata{}`).
- Range reads MUST NOT return partial data without an error.
- `Latest` on an empty volume MUST return `lode.ErrNoSnapshots`.
- `Snapshot` for an unknown ID MUST return `lode.ErrNotFound`.
- An obsolete parent at `Commit` MUST produce `lode.ErrSnapshotConflict`.
- Commit MUST persist and remove a staged copy of its manifest as
  CONTRACT_CORE §Head Pointer specifies.
- The head pointer can name the snapshot of a pending or abandoned commit,
  and that snapshot lacks a canonical manifest (CONTRACT_CORE §Head
  Pointer). Commit MUST then load that snapshot from its staged copy, adopt
  it as the parent, and merge its blocks. An abandoned commit therefore
  does not prevent later commits.
- The pointer can name a snapshot that has neither a canonical manifest
  nor a staged copy. This state arises when a release earlier than v0.11.0
  persisted the commit or when an operator edited the pointer manually.
  Commit MUST then return `lode.ErrSnapshotConflict`, because it cannot
  merge the parent's blocks. If the caller configures retries, Commit
  retries. Commits return `lode.ErrSnapshotConflict` until the pointer
  names a snapshot that has a manifest: either an operator persists the
  missing manifest, or an operator sets the pointer to an earlier
  snapshot. A `Volume` value that caches an earlier head continues to
  expect that head's pointer content. After the repair, the caller MUST
  therefore call `Latest` or open the volume again, unless the caller
  configures retries (each retry refreshes the head).
- A removed Volume (§Removal) MUST produce `volume.ErrRemoved` from
  `Latest` and `Commit`.
- `Latest`, `Snapshot`, and `Snapshots` never read staged copies. An
  abandoned commit has no canonical manifest, so no listing includes it, and
  its blocks become visible through the next commit, which merges them. That
  commit's `ParentSnapshotID` names the abandoned snapshot.

---

## Concurrency

The snapshot history of a Volume is linear.

### Volume Write Concurrency Matrix

| Pattern | Writers | Blocks | History | Supported |
|---------|---------|--------|---------|-----------|
| Single writer | 1 process | Non-overlapping blocks | Linear (guaranteed) | Yes |
| Multiple processes, serialized | N processes, external coordination | Non-overlapping blocks | Linear (enforced by the caller) | Yes, with coordination that the caller provides |
| Multiple processes, uncoordinated | N processes, no coordination | Non-overlapping blocks | Linear (enforced by CAS) | Yes, because a later commit adopts an abandoned commit from its staged manifest (§Error Semantics) |
| Multiple writers in one process, store wrapped with `lode.SingleProcess` | N goroutines, each with its own `Volume`, sharing one wrapped store | Non-overlapping blocks | Linear (enforced by the process lock) | Yes |

### Optimistic Concurrency (CAS)

Volume detects concurrent commits and returns `ErrSnapshotConflict`.

Volume detects conflicts as a Dataset does (see
[`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md) §Optimistic Concurrency
(CAS)). The two differ only in how a retry assigns the snapshot ID.
Conflict detection consists of three steps:
- `Commit` records the expected parent when it resolves the parent.
- `Commit` updates the pointer with `CompareAndSwap`.
- An obsolete parent produces `ErrSnapshotConflict`.

A Volume CAS retry merges non-overlapping blocks:
- On retry, the caller merges non-overlapping blocks from concurrent
  writers by calling `Latest()` again, adding the new blocks to the updated
  cumulative manifest, and committing again.
- Overlapping blocks remain invalid regardless of CAS (see
  `ErrOverlappingBlocks`).

Volume also supports automatic retry, which is disabled by default.
`WithRetryCount(n)` enables a bounded number of automatic retries within
`Commit`. On `ErrSnapshotConflict`, the commit:
1. Waits for an exponential backoff interval with jitter.
2. Calls `Latest()` to refresh the in-memory CAS cache.
3. Resolves the parent again and merges the blocks against the new parent.
4. Verifies that no blocks overlap in the merged cumulative set.
5. Retries the pointer CAS with a new snapshot ID.

A Volume retry may draw a new snapshot ID because block paths do not contain
it (`volumes/<id>/data/<offset>-<length>.bin`). A Dataset retry retains its
ID because its data paths contain the ID (V1_API_DECISIONS D10c).

`ErrOverlappingBlocks` terminates the retry loop immediately, because it is
not retryable. If the caller configures `WithRetryObserver(fn)`, `Commit`
calls `fn` once per retry, before step 1. When `Commit` exhausts all
retries, it returns `ErrSnapshotConflict` as it does without retries.

Optimistic concurrency preserves compatibility with single-writer workflows:
- Single-writer workflows are unaffected.
- External coordination remains valid but is not required.

---

## Adapter Obligations

Volume requires no new type of storage engine.

The capabilities of a `lode.RangeStore` are sufficient:
- immutable object writes
- `List`, `Exists`, `Get`, and `Delete`
- range reads (`RangeReader`, CONTRACT_STORAGE §Range Reads), which `New`
  requires through its parameter type
- `CompareAndSwap` (CONTRACT_STORAGE §ConditionalWriter Capability)

Adapters MUST preserve the rule that the manifest alone defines the
committed blocks.

## Storage Layout

Volume uses a **fixed internal layout**, which is not configurable. The
`Layout` abstraction is specific to Datasets and does not apply to Volume.

```
volumes/<volume_id>/snapshots/<snapshot_id>/manifest.json
volumes/<volume_id>/staged/<snapshot_id>.json
volumes/<volume_id>/data/<offset>-<length>.bin
```

- Manifest paths follow the same commit-visibility pattern as for a
  Dataset.
- A staged manifest lies outside the snapshots prefix, so no snapshot
  listing includes it (§Error Semantics). A staged manifest that remains
  after a failed commit is harmless, like abandoned staged data.
- Volume names data files by byte range to facilitate debugging.
- This layout is not part of the public API, but it is a stable persisted
  format (§Public API Surface).

---

## Removal

A Volume is removed whole, by `Remove(ctx, id, factory, opts...)`. A
Volume has no expiry of single snapshots, because its snapshots are
cumulative (CONTRACT_RETENTION §Non-Goals).

`Remove` MUST proceed in this order:

1. It replaces the head pointer with the removal marker `lode:removed`
   through `CompareAndSwap` against the content it read, and creates the
   pointer with the marker when the Volume has none. The marker is not a
   snapshot ID, so no commit can parent on it. A commit that races the
   swap makes it conflict. `Remove` retries the swap as a commit retries
   (§Optimistic Concurrency), and returns `lode.ErrSnapshotConflict` once
   the retries are spent, having deleted nothing.
2. It deletes every other object of the Volume: manifests, staged
   manifests and blocks.

The pointer stays. It is the fence that keeps the Volume removed:

- Once the pointer holds the marker, `Latest` and `Commit` on every handle
  of the Volume MUST return `ErrRemoved`. A handle that remembers an
  earlier head commits without reading the pointer, so its swap is where
  it meets the marker. A commit whose swap conflicts MUST therefore read
  the pointer again, and MUST return `ErrRemoved` when it holds the
  marker, whether or not the caller configured retries. A writer that
  held the Volume open through the removal never advances the pointer.
- A store may report an object the removal deletes as missing
  (CONTRACT_STORAGE §Delete), as when two removals overlap. `Remove` MUST
  treat it as deleted.
- `Remove` MUST NOT delete the pointer, and removing a removed Volume
  deletes what remains, so a second call completes an interrupted one.
  A block staged into a removed Volume is an abandoned staged object,
  which the next `Remove` deletes.
- A removed Volume therefore keeps one small object, as an expired
  Dataset snapshot keeps its tombstone.

`Remove` checks its arguments before it creates the store: a Volume ID that
is not one canonical path segment yields `lode.ErrInvalidArgument`
(CONTRACT_STORAGE §Names in Keys), so a removal never reaches another
Volume's objects.

---

## Dataset Boundary

`Dataset` and `Volume` are distinct containers built on one snapshot-history
core (the snapshot chain, the latest pointer, and the CAS commit), and they
have distinct semantics:

- Dataset streaming constructs complete objects atomically.
- Volume streaming commits partial data incrementally.
- Neither container wraps the other or forms a special case of it.

A workflow that requires sparse, resumable, out-of-order, or range-verified
data belongs to `Volume`, not to `Dataset`.

Two design principles maintain this boundary:
- Adapters remain minimal, and lifecycle semantics remain in the Lode core.
- Policy that depends on the runtime (scheduling, peer management, execution
  strategy) belongs in consuming systems, not in Lode.

---

## Complexity Bounds

[CONTRACT_COMPLEXITY.md](CONTRACT_COMPLEXITY.md) gives complete definitions
and a glossary of variables.

### Write Operations

| Operation | Store Calls (warm) | Memory | CPU |
|-----------|-------------------|--------|-----|
| `StageWriteAt` | 1 Put | O(block) | O(block) |
| `Commit` | 5 fixed | O(B) manifest | O(B + K log K) |

The size of the cumulative manifest is O(B), a cost inherent in the
cumulative design.

### Read Operations

| Operation | Store Calls | Memory | CPU |
|-----------|-------------|--------|-----|
| `ReadAt` | 1 `Get` + R `ReadRange` | O(L) | **O(log B + R)** |
| `Latest` | 2 (pointer + manifest) | O(B) | O(1) |
| `Snapshot(id)` | 1 `Get` | O(B) | O(B) validation |
| `Snapshots` | 1 `List` + S `Get` calls | O(S × B_avg) | O(S × B_avg + S log S) |

Block lookup MUST be O(log B + R), where R is the number of covering
blocks. Implementations MUST verify the sort order when they load the
manifest and MUST NOT verify it on each range read.

The cost of `Snapshots()` grows with the snapshot count multiplied by the
cumulative block count. Callers MUST NOT use `Snapshots()` on
hot paths.

---

## Prohibited Behaviors

- Inferring committed blocks that are absent from the manifest.
- Treating staged or unverified data as committed.
- Compacting or mutating committed blocks implicitly in the background.

---

## Completeness

A Volume is complete when the union of its committed blocks covers the
entire address space `[0..N)`. Implementations derive completeness from the
committed blocks and do not store it as a separate flag.

---

## Invariants

The following invariants MUST hold:

1. A snapshot's manifest is the sole authority for read visibility.
2. Each manifest is self-contained: its blocks are cumulative, so a reader
   need not traverse the chain.
3. No data is readable unless a committed snapshot references it.
4. All committed blocks are immutable.
5. Committed blocks within a snapshot MUST NOT overlap.
6. The snapshot history of a Volume is linear.
7. Implementations derive completeness solely from committed blocks.

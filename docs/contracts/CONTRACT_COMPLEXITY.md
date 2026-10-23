# Lode Complexity Bounds — Contract

Status: Active (v0.8). All known violations have been resolved.

---

## Principle

Every public method MUST have a documented, bounded cost. A cost that
exceeds the documented bound is a defect. Hot-path operations MUST NOT
scale with the size of a dataset's or volume's history.

---

## Definitions

### Cost Dimensions

- Store calls: network round trips (Get, Put, List, Exists, Delete,
  ReadRange)
- Memory: peak allocation relative to the size of the input or output
- CPU: the algorithmic complexity class

### Variables

| Variable | Meaning |
|----------|---------|
| S | Committed snapshots in a dataset or volume |
| B | Committed blocks in a volume snapshot (cumulative) |
| K | New blocks in a single commit |
| F | Files in a dataset snapshot |
| R | Records in a write or read |
| P | Partitions in a dataset write |
| M | Manifests that match a listing query |
| N | Keys returned by `Store.List` |
| L | Length of the requested byte range |

### Path Classification

- Hot path: incurred on every read or write. A cost of O(S), O(B), or O(M)
  is a defect.
- Cold path: explicit listing (`Snapshots()`, `ListDatasets`). The cost is
  documented, and its scaling is acknowledged.
- Degraded path: a fallback for backward compatibility (a scan when the
  pointer is absent). The cost is documented, and the next commit restores
  the pointer.

---

## Store Interface

| Method | Calls | Memory | Notes |
|--------|-------|--------|-------|
| Put | 1 | O(payload) | Atomic; the multipart path adds a preliminary check |
| Get | 1 | O(1) stream | Returns an `io.ReadCloser` |
| Exists | 1 | O(1) | |
| Delete | 1 | O(1) | |
| ReadRange | 1 | O(L) | |
| ReaderAt | 1 to open + 1 per call | O(L) per call | |
| List | ⌈N/page⌉ | O(N) | Fully materialized (a constraint of the interface) |

---

## Dataset — Write Operations

| Operation | Store Calls (warm) | Store Calls (cold) | Memory |
|-----------|-------------------|--------------------|--------|
| Write (no partitions) | 4 fixed | scan fallback (below) | O(R + encoded) |
| Write (P partitions) | 2P + 3 | scan fallback (below) | O(R + encoded) |
| Blobs.StreamWrite | 4 fixed | scan fallback (below) | O(1) streaming |
| Records.StreamWrite | 4 fixed | scan fallback (below) | O(1) streaming |

Parent resolution:
- In-memory cache: 0 calls
- Pointer: 1 call (2 calls for a Volume, which also loads the parent's manifest)
- Scan fallback (degraded): O(N) through List, plus, for a Dataset, 1
  tombstone Get per snapshot scanned (and 1 Exists of the canonical manifest
  for each scanned snapshot that has a tombstone), from the newest snapshot
  until one is found that is not expired. The scan creates a missing pointer
  but never replaces an existing one (CONTRACT_CORE §Head Pointer).

Parent resolution MUST NOT use List when a valid pointer exists.

Exceptions, each incurred once (CONTRACT_WRITE_API §Settings):
- A dataset's first commit adds 1 Put and 1 Delete for its staged manifest.
- A writer's settings check, before its first commit, costs 1 Get of the
  manifest that the pointer names, 2 Gets while a first commit is pending,
  or the scan fallback and 1 List of the dataset's directory when neither
  manifest exists. Against an empty dataset it costs 1 List of the
  dataset's directory.
- The first write to a Hive dataset whose manifests predate format version
  1.1.0 adds 1 Get per snapshot followed from the head to the first
  snapshot with files.

---

## Dataset — Read Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| Latest (warm) | Hot | 2 (pointer + manifest) | O(manifest) | O(1) |
| Latest (cold) | Degraded | 1 List + 1 Get + 1 tombstone Get per snapshot scanned (+1 canonical-manifest Exists per scanned snapshot that has a tombstone) | O(N) | O(N) |
| Snapshot(id) | Hot | 1 Get | O(manifest) | O(1) |
| Snapshot(id) fallback | Degraded | 1 Get (canonical miss) + 1 tombstone Get + 1 List + scan | O(N) | O(N) |
| Snapshots() | Cold | 1 List + S Gets | O(S × manifest) | O(S × B_avg + S log S) |
| Read(id) | Hot | 1 + F Gets | O(R_total) | O(R_total) |

---

## Opening a Handle

| Operation | Store Calls |
|-----------|-------------|
| `Root.Records`, `Root.Blobs` (warm: the pointer names a manifest) | 1 Get of the pointer + 1 Get of the manifest |
| `Root.Records`, `Root.Blobs` (empty dataset) | 1 Get of the pointer + the scan fallback + 1 List of the dataset's directory |
| `ReadRoot.Records`, `ReadRoot.Blobs` (warm) | 1 Get of the pointer + at most 2 Gets of the manifest |
| `ReadRoot.Records`, `ReadRoot.Blobs` (no pointer) | 1 Get of the pointer + the scan fallback of each grammar |

A view does not know whether the dataset is partitioned until it has read a
manifest, so it reads the canonical manifest under the snapshots directory,
then under the segments directory; at most one exists. Opening writes
nothing. A writer's open check is the settings check of
CONTRACT_WRITE_API §Settings Check; once settled, commits keep the bounds
above.

---

## Dataset Views and Handles — Enumeration

| Operation | Path | Store Calls | Memory |
|-----------|------|-------------|--------|
| ListDatasets (on a root) | Cold | 1 List | O(N) |
| ListManifests | Cold | 1 List + M Gets (validation) | O(N + M × manifest) |
| ListPartitions | Cold | 1 List + M Gets | O(N + M × manifest) |
| GetManifest | Hot | 1 Get | O(manifest) |
| OpenObject | Hot | 1 Get | O(1) stream |

`ListManifests` MUST extract snapshot IDs from paths; manifest validation is
required by CONTRACT_ERRORS.md. `ListPartitions` MUST NOT deserialize any
manifest twice. On layouts that model partitions, `Prefix` reduces N and M
to the matching subtree. `StartAfter` and `Limit` are applied after
enumeration, so they bound the result set but not the store calls. Sorting
adds O(N log N) CPU and no store calls.

---

## Dataset — Retention

| Operation | Path | Store Calls | Memory |
|-----------|------|-------------|--------|
| Expire (K selected) | Cold | 1 List + (K + 1) pointer Gets (each + at most 1 Exists, + 1 List when the head has no canonical manifest) + K manifest Gets + K tombstone Gets + 1 Get per manifest path of each selected snapshot + K tombstone writes (each + at most 1 read-back Get) + Σ(manifests + files) Deletes | O(N + K × manifest) |
| Sweep (T tombstones, D objects deleted, manifests included) | Cold | 2 Lists + 1 pointer Get + T Gets + 1 Get per manifest path of a tombstoned snapshot with anything left + ≤ 1 Exists per manifest path of each unlisted-manifest group past grace + D Deletes | O(N + Σ tombstone inventories + one snapshot's manifest copies) |
| Snapshot(id), Read(id) expired | Hot miss | +1 Get (tombstone) | O(1) |

`Expire` with `Partition` lists only that partition's subtree; otherwise N
comprises the whole snapshot prefix. Without a pointer, retention lists the
whole snapshot prefix even for a `Partition` request, in order to
distinguish an empty dataset from one that it must refuse. Selection by
`Before` reads snapshot IDs from paths and loads no manifest beyond the K
selected. The complete bounds and their derivation are given in
`CONTRACT_RETENTION.md` §Complexity Bounds.

---

## Volume — Write Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| StageWriteAt | Hot | 1 Put | O(block) | O(block) |
| Commit (warm) | Hot | 5 fixed | O(B) manifest | O(B + K log K) |
| Commit (cold) | Degraded | +1 List | +O(N) | +O(N) |

A warm commit makes one Get of the parent's manifest, one Put of the staged
manifest, one pointer update, one Put of the canonical manifest, and one
Delete of the staged manifest (CONTRACT_VOLUME §Error Semantics).

The size of the cumulative manifest grows by O(B) per commit, a cost
inherent in the cumulative design.

---

## Volume — Read Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| ReadAt | Hot | 1 Get + R ReadRange | O(L) | O(log B + R) |
| Latest (warm) | Hot | 2 | O(B) | O(1) |
| Latest (cold) | Degraded | 1 List + 1 Get | O(N + B) | O(N) |
| Snapshots() | Cold | 1 List + S Gets | O(S × B_avg) | O(S × B_avg + S log S) |
| Snapshot(id) | Hot | 1 Get | O(B) | O(B) |

The block lookup in `ReadAt` MUST be O(log B + R). No read may verify the
sort order of blocks.

---

## Resolved Violations

All known violations have been resolved. Four were corrected in code, and
three were reclassified as inherent costs.

| ID | Operation | Resolution |
|----|-----------|------------|
| CX-1 | Sort verification in `findCoveringBlocks` | Corrected: blocks are sorted at load time in `validateVolumeManifest` |
| CX-2 | Validation Gets in `ListManifests` | Reclassified: required by CONTRACT_ERRORS.md |
| CX-3 | Repeated deserialization in `ListPartitions` | Corrected: a single pass (1 List + M Gets) |
| CX-4 | Degraded fallback of `Snapshot(id)` | Reclassified: a documented degraded path, not a hot-path violation |
| CX-5 | Memory of a partitioned `Write` | Reclassified: O(R) interface aliases, not O(3R) copies |
| CX-6 | Sort verification in `Volume.Snapshots` | Corrected: sorting at load time removes the redundant verification |
| CX-7 | `Walk` versus `WalkDir` in `fsStore.List` | Corrected: `filepath.WalkDir` avoids an `os.Stat` per entry |

### Reclassification Notes

**CX-2** (validation Gets in `ListManifests`): CONTRACT_ERRORS.md requires
that `ListManifests` return an error, rather than omit the manifest, when
manifest validation fails, so the M Gets for validation are mandated by
contract. Snapshot IDs are already extracted from paths; the Gets serve
validation alone.

**CX-4** (degraded fallback of `Snapshot(id)`): this contract classifies the
fallback as a degraded path. The implementation first attempts the
canonical path (an O(1) Get) and scans only for data written before
canonical manifests were introduced. This behavior preserves backward
compatibility and is not a hot-path violation.

**CX-5** (memory of a partitioned `Write`): `partitionRecords` creates
interface aliases of records, not copies. Peak memory is O(R) interface
values plus the encoded output of each partition. The references in the
partition map occupy 16 bytes per record, which is negligible relative to
the record data.

---

## Design Invariant

> Every public method has a documented cost.
> A cost that exceeds the documented bound is a defect, not a trade-off.

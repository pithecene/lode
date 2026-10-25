# Lode Complexity Bounds Contract

---

## Bounded Cost of Public Methods

Every public method MUST have a documented, bounded cost. A cost that
exceeds the documented bound is a defect. Hot-path operations MUST NOT
scale with the size of a dataset's or volume's history.

---

## Definitions

### Cost Dimensions

- Store calls: network round trips (`Get`, `Put`, `List`, `Exists`, `Delete`,
  `ReadRange`)
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
- Cold path: explicit listing (`Snapshots()`, `ListDatasets`). This
  contract documents the cost and acknowledges its scaling.
- Degraded path: a fallback for backward compatibility (a scan when the
  pointer is absent). This contract documents the cost, and the next commit
  restores the pointer.

---

## Store Interface

| Method | Calls | Memory | Notes |
|--------|-------|--------|-------|
| `Put` | 1 | O(payload) | Atomic, and the multipart path adds a preliminary check |
| `Get` | 1 | O(1) stream | Returns an `io.ReadCloser` |
| `Exists` | 1 | O(1) | |
| `Delete` | 1 | O(1) | |
| `ReadRange` | 1 | O(L) | |
| `ReaderAt` | 1 to open + 1 per call | O(L) per call | |
| `List` | ⌈N/page⌉ | O(N) | Fully materialized (a constraint of the interface) |

---

## Dataset Write Operations

| Operation | Store Calls (warm) | Store Calls (cold) | Memory |
|-----------|-------------------|--------------------|--------|
| Write (no partitions) | 4 fixed | scan fallback (below) | O(R + encoded) |
| Write (P partitions) | 2P + 3 | scan fallback (below) | O(R + encoded) |
| Blobs.StreamWrite | 4 fixed | scan fallback (below) | O(1) streaming |
| Records.StreamWrite | 4 fixed | scan fallback (below) | O(1) streaming |

Parent resolution incurs the following store calls:
- In-memory cache: 0 calls
- Pointer: 1 call (2 calls for a Volume, which also loads the parent's manifest)
- Scan fallback (degraded): O(N) through `List`. For a Dataset, the scan
  adds 1 tombstone `Get` per snapshot scanned, and 1 `Exists` of the
  canonical manifest for each scanned snapshot that has a tombstone. It
  proceeds from the newest snapshot until it finds one that is not
  expired. The scan creates a missing pointer but never replaces an
  existing one (CONTRACT_CORE §Head Pointer).

Parent resolution MUST NOT use `List` when a valid pointer exists.

The following exceptions each occur once (CONTRACT_WRITE_API §Settings):
- A dataset's first commit adds 1 `Put` and 1 `Delete` for its staged manifest.
- A writer's settings check, before its first commit, costs 1 `Get` of the
  manifest that the pointer names, or 2 `Get` calls while a first commit is
  pending. When neither the manifest that the pointer names nor the staged
  manifest exists, the check costs the scan fallback and 1 `List` of the
  dataset's directory. Against an empty dataset, it costs 1 `List` of the
  dataset's directory.
- The first write to a Hive dataset whose manifests predate format version
  1.1.0 follows the snapshot chain from the head to the first snapshot with
  files. That write adds 1 `Get` per snapshot followed.

---

## Dataset Read Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| Latest (warm) | Hot | 2 (pointer + manifest) | O(manifest) | O(1) |
| Latest (cold) | Degraded | 1 `List` + 1 `Get` + 1 tombstone `Get` per snapshot scanned (+1 canonical-manifest `Exists` per scanned snapshot that has a tombstone) | O(N) | O(N) |
| Snapshot(id) | Hot | 1 `Get` | O(manifest) | O(1) |
| Snapshot(id) fallback | Degraded | 1 `Get` (canonical miss) + 1 tombstone `Get` + 1 `List` + scan | O(N) | O(N) |
| Snapshots() | Cold | 1 `List` + S `Get` calls | O(S × manifest) | O(S × B_avg + S log S) |
| Read(id) | Hot | 1 + F `Get` calls | O(R_total) | O(R_total) |

---

## Opening a Handle

| Operation | Store Calls |
|-----------|-------------|
| `Root.Records`, `Root.Blobs` (warm: the pointer names a manifest) | 1 `Get` of the pointer + 1 `Get` of the manifest |
| `Root.Records`, `Root.Blobs` (empty dataset) | 1 `Get` of the pointer + the scan fallback + 1 `List` of the dataset's directory |
| `ReadRoot.Records`, `ReadRoot.Blobs` (warm) | 1 `Get` of the pointer + at most 2 `Get` calls of the manifest |
| `ReadRoot.Records`, `ReadRoot.Blobs` (no pointer) | 1 `Get` of the pointer + the scan fallback of each grammar |

A view cannot determine whether the dataset is partitioned before it reads
a manifest. It therefore reads the canonical manifest under the snapshots
directory first and under the segments directory second. At most one
exists. Opening writes nothing. A writer's open check is the settings check
of CONTRACT_WRITE_API §Settings Check. After that check completes, commits
maintain the bounds above.

---

## Enumeration by Dataset Views and Handles

| Operation | Path | Store Calls | Memory |
|-----------|------|-------------|--------|
| ListDatasets (on a root) | Cold | 1 `List` | O(N) |
| ListManifests | Cold | 1 `List` + M `Get` calls (validation) | O(N + M × manifest) |
| ListPartitions | Cold | 1 `List` + M `Get` calls | O(N + M × manifest) |
| GetManifest | Hot | 1 `Get` | O(manifest) |
| OpenObject | Hot | 1 `Get` | O(1) stream |

`ListManifests` MUST extract snapshot IDs from paths, and CONTRACT_ERRORS.md
requires manifest validation. `ListPartitions` MUST NOT deserialize any
manifest twice. On layouts that model partitions, `Prefix` reduces N and M
to the matching subtree. The listing applies `StartAfter` and `Limit` after
enumeration, so they bound the result set but not the store calls. Sorting
adds O(N log N) CPU and no store calls.

---

## Dataset Retention

| Operation | Path | Store Calls | Memory |
|-----------|------|-------------|--------|
| Expire (K selected) | Cold | The sum of the terms below | O(N + K × manifest) |
| Sweep (T tombstones, D objects deleted, manifests included) | Cold | The sum of the terms below | O(N + Σ tombstone inventories + one snapshot's manifest copies) |
| Snapshot(id), Read(id) expired | Hot miss | +1 `Get` (tombstone) | O(1) |

The store calls of `Expire`, with K snapshots selected, are the sum of
these terms:
- 1 `List`
- K + 1 pointer `Get` calls, each with at most 1 `Exists`, and with 1
  `List` when the head has no canonical manifest
- K manifest `Get` calls and K tombstone `Get` calls
- 1 `Get` per manifest path of each selected snapshot
- K tombstone writes, each with at most 1 read-back `Get`
- Σ(manifests + files) `Delete` calls

The store calls of `Sweep`, with T tombstones and D objects deleted
(manifests included), are the sum of these terms:
- 2 `List` calls, 1 pointer `Get`, and T `Get` calls
- 1 `Get` per manifest path of each tombstoned snapshot that retains
  objects
- at most 1 `Exists` per manifest path of each unlisted-manifest group
  older than the grace period
- D `Delete` calls
- 1 `Get`, 1 `Put` per missing partition copy, and 1 tombstone `Get` for
  each unindexed snapshot older than the grace period

`Expire` with `Partition` lists only that partition's subtree. Otherwise, N
counts every key under the snapshot prefix. Without a pointer, retention
lists the whole snapshot prefix even for a `Partition` request. The full
listing distinguishes an empty dataset from a dataset that retention must
refuse. Selection by `Before` reads snapshot IDs from paths and loads no
manifest beyond the K selected.
[`CONTRACT_RETENTION.md`](CONTRACT_RETENTION.md) §Complexity Bounds gives
the complete bounds and their derivation.

---

## Volume Write Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| StageWriteAt | Hot | 1 `Put` | O(block) | O(block) |
| Commit (warm) | Hot | 5 fixed | O(B) manifest | O(B + K log K) |
| Commit (cold) | Degraded | +1 `List` | +O(N) | +O(N) |

A warm commit makes five store calls (CONTRACT_VOLUME §Error Semantics).
They are one `Get` of the parent's manifest, one `Put` of the staged
manifest, one pointer update, one `Put` of the canonical manifest, and one
`Delete` of the staged manifest.

The size of the cumulative manifest grows by O(B) per commit, a cost
inherent in the cumulative design.

---

## Volume Read Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| ReadAt | Hot | 1 `Get` + R `ReadRange` | O(L) | O(log B + R) |
| Latest (warm) | Hot | 2 | O(B) | O(1) |
| Latest (cold) | Degraded | 1 `List` + 1 `Get` | O(N + B) | O(N) |
| Snapshots() | Cold | 1 `List` + S `Get` calls | O(S × B_avg) | O(S × B_avg + S log S) |
| Snapshot(id) | Hot | 1 `Get` | O(B) | O(B) |

The block lookup in `ReadAt` MUST be O(log B + R). No read may verify the
sort order of blocks.

---

## Design Invariant

> Every public method has a documented cost.
> A cost that exceeds the documented bound is a defect, not a trade-off.

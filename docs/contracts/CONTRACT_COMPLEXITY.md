# Lode Complexity Bounds Contract

---

## Bounded Cost of Public Methods

Every public method MUST have a documented, bounded cost. Exceeding a
documented bound is a defect. Hot-path operations MUST NOT scale with the
depth of a dataset's or volume's snapshot history.

---

## Definitions

### Cost Dimensions
- **Store calls**: backend round trips (`Get`, `Put`, `List`, `ListPrefixes`,
  `Exists`, `Delete`, `ReadRange`, `CompareAndSwap`)
- **Memory**: peak heap allocation relative to payload, manifest, or listing
  size
- **CPU**: algorithmic complexity class

### Variables

| Variable | Meaning |
|----------|---------|
| `S` | Committed snapshots in a dataset or volume |
| `D` | Candidate dataset child prefixes under `datasets/` |
| `C` | Canonical-only Hive snapshots lacking partition manifests |
| `B` | Cumulative committed blocks in a volume snapshot |
| `K` | New blocks in a volume commit (or selected snapshots in `Expire`) |
| `F` | Data objects in a dataset snapshot |
| `R` | Records in a write or read (or covering blocks in `ReadAt`) |
| `P` | Touched partitions in a dataset write |
| `M` | Manifests yielded by `Manifests` |
| `N` | Keys returned by `Store.List` |
| `L` | Byte length of a requested range |

### Path Classification
- **Hot path**: steady-state reads and writes (`Write`, `StreamWrite`,
  `Latest`, `Snapshot`, `Read`, `OpenObject`, `ReaderAt`, `StageWriteAt`,
  `Commit`, `ReadAt`). Costs of `O(S)`, `O(B)` linear scans on lookup, or
  unbounded `List` calls are defects.
- **Cold path**: explicit enumerations and lifecycle maintenance
  (`ListDatasets`, `ListPartitions`, `Manifests`, `Snapshots`, `Expire`,
  `Sweep`, `RemoveDataset`, `volume.Remove`).
- **Degraded path**: backward-compatibility or crash-recovery fallback when
  `latest` or a canonical manifest is absent.

---

## Store Interface

| Method | Calls | Memory | Notes |
|--------|-------|--------|-------|
| `Put` | 1 (atomic) or `2 + ⌈size/partSize⌉` (multipart) | O(1) spooled (`FS`, `s3.Store`) or O(payload) (`Memory`) | Atomic no-overwrite |
| `Get` | 1 | O(1) stream | Returns `io.ReadCloser` |
| `Exists` | 1 | O(1) | |
| `Delete` | 1 | O(1) | Idempotent |
| `CompareAndSwap` | 1–2 | O(pointer) | 1 conditional `PutObject` on S3 when `ETag` is cached or `expected == ""` |
| `ReadRange` | 1 | O(min(L, available)) | |
| `ReaderAt` | 1 to open + 1 per `ReadAt` | O(L) per read | |
| `List` | `⌈N/page⌉` | O(N) | Materializes all keys matching prefix |
| `ListPrefixes` | `⌈D/page⌉` | O(D) | Shallow delimiter listing (`PrefixLister`) |

---

## Opening a Dataset Handle or View

| Operation | Store Calls |
|-----------|-------------|
| `Root.Records`, `Root.Blobs` (warm: pointer names committed manifest) | 1 `Get` (pointer) + 1 `Get` (manifest) |
| `Root.Records`, `Root.Blobs` (empty dataset) | 1 `Get` (pointer) + scan fallback + 1 `List` (foreign-layout check) |
| `ReadRoot.Records`, `ReadRoot.Blobs` (warm, without `WithLayout`) | 1 `Get` (pointer) + at most 2 `Get` calls (probing `snapshots/` then `segments/`) |
| `ReadRoot.Records`, `ReadRoot.Blobs` with `WithLayout(l)` | 1 `Get` (pointer) + 1 `Get` (manifest in `l`'s directory; +1 `Exists` on miss) |
| `ReadRoot.Records`, `ReadRoot.Blobs` with `WithAnchor(id)` | At most 2 `Get` calls for `id`'s canonical manifest (1 `Get` when combined with `WithLayout`), 1 pointer `Get` to check the removal fence, and retains a private manifest copy |

Opening a handle or view writes nothing to storage.

---

## Dataset Write Operations

| Operation | Store Calls (warm) | Store Calls (cold) | Memory |
|-----------|-------------------|--------------------|--------|
| `Records.Write` (unpartitioned) | 3–4 fixed (`Put` data + CAS + `Put` manifest) | scan fallback | O(R + encoded) |
| `Records.Write` (`P` partitions) | `2P + 2` to `2P + 3` (`P` data `Put` + CAS + `1 + P` manifest `Put`) | scan fallback | O(R + encoded) |
| `Blobs.Write` | 3–4 fixed | scan fallback | O(payload) |
| `Blobs.StreamWrite` | 3–4 fixed | scan fallback | O(1) streaming |
| `Records.StreamWrite` | 3–4 fixed | scan fallback | O(1) streaming |

One-time cold/first-commit additions:
- A dataset's first commit (or commits on a writer that adopted a pending
  first commit before any canonical manifest exists) adds `1 Put + 1 Delete`
  for the staged manifest in `staged/`.
- Hot-path commits after open MUST NOT call `Store.List`.

---

## Dataset Read Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| `Latest` (warm) | Hot | 2 `Get` calls (pointer + manifest) | O(manifest) | O(1) |
| `Latest` (degraded scan) | Degraded | 1 `List` + 1 `Get` + tombstone checks per scanned candidate | O(N) | O(N) |
| `Snapshot(id)` | Hot | 2 `Get` calls (1 pointer `Get` and 1 `Exists` for a live canonical anchor) | O(manifest) | O(1) |
| `Snapshot(id)` fallback | Degraded | 2 `Get` calls + 1 tombstone `Get` + 1 `List` + 1 `Get` | O(N) | O(N) |
| `Read(id)` | Hot | snapshot lookup + `F` object reads | O(R_total) | O(R_total) |
| `Snapshots()` | Cold | 1 `List` + `1 + S` `Get` calls | O(S × manifest) | O(S log S) |
| `OpenObject` | Hot | 1 `Get` | O(1) stream | O(1) |
| `ReaderAt` | Hot | 1 to open + 1 per `ReadAt` | O(L) per read | O(1) |

---

## Enumeration and Removal

| Operation | Path | Store Calls | Memory |
|-----------|------|-------------|--------|
| `ListDatasets` (with `PrefixLister`) | Cold | 1 `ListPrefixes` + `D` pointer `Get` calls + at most `2D` `Exists` calls (per-dataset `List` only when pointerless or pending) | O(D) |
| `ListDatasets` (without `PrefixLister`) | Cold | 1 `List` + `D` pointer `Get` calls | O(N) |
| `ListPartitions` (Hive, fully indexed) | Cold | 1 `List` + 1 pointer `Get` | O(N) |
| `ListPartitions` (Hive, `C` canonical-only) | Cold | 1 `List` + `1 + C` `Get` calls | O(N + manifest) |
| `Manifests` (`M` yielded) | Cold | 1 `List` + `1 + M` `Get` calls | O(N + manifest) |
| `Root.RemoveDataset` | Cold | 1 `Get` + 1 `CompareAndSwap` + at most 2 `Exists` + 1 `List` + `N` `Delete` calls | O(N) |

- `ListPartitions` derives partition paths directly from listed
  partition-manifest keys, reading manifests only for unindexed canonical-only
  snapshots (`C = 0` in steady state).
- `Manifests` loads and validates only the `M` manifests yielded within the
  `(StartAfter, Limit)` window.

---

## Dataset Retention

| Operation | Path | Store Calls | Memory |
|-----------|------|-------------|--------|
| `Expire` (`K` selected) | Cold | 1 `List` + `(K + 1)` pointer checks + `O(K)` manifest/tombstone reads and writes + object/manifest `Delete` calls | O(N + K × manifest) |
| `Sweep` (`T` tombstones, `D_del` deleted) | Cold | 2 `List` + 1 pointer `Get` + `T` tombstone `Get` + settling/orphan/reindex checks + `D_del` `Delete` calls | O(N + Σ tombstone inventories + 1 snapshot's manifests) |
| `Snapshot(id)` / `Read(id)` on expired ID | Hot miss | +1 `Get` (tombstone) | O(1) |

---

## Volume Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| `StageWriteAt` | Hot | 1 `Put` | O(block) | O(block) |
| `Commit` (warm) | Hot | 5 fixed (`Get` parent + `Put` staged + CAS + `Put` canonical + `Delete` staged) | O(B) | O(B + K log K) |
| `Commit` (cold) | Degraded | +1 `List` | +O(N) | +O(N) |
| `ReadAt` | Hot | 1 `Get` + `R` `ReadRange` | O(L) | O(log B + R) |
| `ReaderAt` | Hot | 1 `Get` | O(B) | O(B) |
| `SnapshotReaderAt.ReadAt` | Hot | `R` `ReadRange` | O(L) | O(log B + R) |
| `Latest` (warm) | Hot | 2 `Get` calls | O(B) | O(1) |
| `Snapshot(id)` | Hot | 1 `Get` | O(B) | O(B) |
| `Snapshots()` | Cold | 1 `List` + `1 + S` `Get` calls | O(S × B_avg) | O(S × B_avg + S log S) |
| `Head` | Hot | 1 `Get` (+1 `List` only when pointer absent) | O(1) | O(1) |
| `Remove` | Cold | 1 `Get` + 1 `CompareAndSwap` + at most 1 `Exists` + 1 `List` + `N` `Delete` calls | O(N) | O(N) |

---

## Design Invariant

> **Every public method has a documented cost.
> A cost that exceeds the documented bound is a defect, not a trade-off.**

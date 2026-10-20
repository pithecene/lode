# Lode Complexity Bounds — Contract

Status: Active (v0.8). All known violations resolved.

---

## Principle

Every public method MUST have documented, bounded cost.
Cost that exceeds the documented bound is a bug.
Hot-path operations MUST NOT scale with dataset/volume history size.

---

## Definitions

### Cost dimensions

- Store calls: network round-trips (Get, Put, List, Exists, Delete, ReadRange)
- Memory: peak allocation relative to input/output size
- CPU: algorithmic complexity class

### Variables

| Variable | Meaning |
|----------|---------|
| S | Committed snapshots in dataset or volume |
| B | Committed blocks in a volume snapshot (cumulative) |
| K | New blocks in a single commit |
| F | Files in a dataset snapshot |
| R | Records in a write or read |
| P | Partitions in a dataset write |
| M | Manifests matching a listing query |
| N | Keys returned by Store.List |
| L | Requested byte range length |

### Path classification

- Hot path: per-read, per-write. O(S), O(B), O(M) is a bug.
- Cold path: explicit listing (Snapshots(), ListDatasets). Documented cost; acknowledged scaling.
- Degraded path: backward-compat fallback (scan when pointer missing). Documented, self-healing.

---

## Store Interface

| Method | Calls | Memory | Notes |
|--------|-------|--------|-------|
| Put | 1 | O(payload) | Atomic; multipart adds preflight |
| Get | 1 | O(1) stream | Returns io.ReadCloser |
| Exists | 1 | O(1) | |
| Delete | 1 | O(1) | |
| ReadRange | 1 | O(L) | |
| ReaderAt | 1 setup + 1/call | O(L) per call | |
| List | ⌈N/page⌉ | O(N) | Full materialization (interface constraint) |

---

## Dataset — Write Operations

| Operation | Store Calls (warm) | Store Calls (cold) | Memory |
|-----------|-------------------|--------------------|--------|
| Write (no partitions) | 4 fixed | +1 List (scan) | O(R + encoded) |
| Write (P partitions) | 2P + 3 | +1 List (scan) | O(R + encoded) |
| StreamWrite | 4 fixed | +1 List (scan) | O(1) streaming |
| StreamWriteRecords | 4 fixed | +1 List (scan) | O(1) streaming |

Parent resolution:
- In-memory cache: 0 calls
- Pointer + Exists: 2 calls
- Scan fallback (degraded): O(N) via List. Self-heals.

Parent resolution MUST NOT use List when a valid pointer exists.

---

## Dataset — Read Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| Latest (warm) | Hot | 2 (pointer + manifest) | O(manifest) | O(1) |
| Latest (cold) | Degraded | 1 List + 1 Get | O(N) | O(N) |
| Snapshot(id) | Hot | 1 Get | O(manifest) | O(1) |
| Snapshot(id) fallback | Degraded | 1 List + scan | O(N) | O(N) |
| Snapshots() | Cold | 1 List + S Gets | O(S × manifest) | O(S × B_avg + S log S) |
| Read(id) | Hot | 1 + F Gets | O(R_total) | O(R_total) |

---

## DatasetReader — Enumeration

| Operation | Path | Store Calls | Memory |
|-----------|------|-------------|--------|
| ListDatasets | Cold | 1 List | O(N) |
| ListManifests | Cold | 1 List + M Gets (validation) | O(N + M × manifest) |
| ListPartitions | Cold | 1 List + M Gets | O(N + M × manifest) |
| GetManifest | Hot | 1 Get | O(manifest) |
| OpenObject | Hot | 1 Get | O(1) stream |

ListManifests MUST extract snapshot IDs from paths. Manifest validation is required per CONTRACT_ERRORS.md.
ListPartitions MUST NOT double-deserialize manifests.
`Prefix` narrows N and M to the matching subtree on partition-aware layouts.
`StartAfter` and `Limit` are applied after enumeration: they bound the result
set, not the store calls. Sorting adds O(N log N) CPU, no store calls.

---

## Dataset — Retention

| Operation | Path | Store Calls | Memory |
|-----------|------|-------------|--------|
| Expire (K selected) | Cold | 1 List + (K + 1) pointer Gets (each + at most 1 Exists, + 1 List when the head has no canonical manifest) + K manifest Gets + K tombstone Gets + 1 Get per manifest copy of an already-tombstoned snapshot + K tombstone writes (each + at most 1 read-back Get) + Σ(manifests + files) Deletes | O(N + K × manifest) |
| Sweep (T tombstones, D objects deleted, manifests included) | Cold | 2 Lists + 1 pointer Get + T Gets + 1 Get per remaining manifest copy of a tombstoned snapshot + D Deletes | O(N + Σ tombstone inventories) |
| Snapshot(id), Read(id) expired | Hot miss | +1 Get (tombstone) | O(1) |

`Expire` with `Partition` lists only that partition subtree; otherwise N is
the whole snapshot prefix. Selection by `Before` reads snapshot IDs from
paths and loads no manifest beyond the K selected. Full bounds and their
derivation are in `CONTRACT_RETENTION.md` §Complexity Bounds.

---

## Volume — Write Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| StageWriteAt | Hot | 1 Put | O(block) | O(block) |
| Commit (warm) | Hot | 4 fixed | O(B) manifest | O(B + K log K) |
| Commit (cold) | Degraded | +1 List | +O(N) | +O(N) |

Cumulative manifest size grows O(B) per commit. This is inherent in the cumulative design.

---

## Volume — Read Operations

| Operation | Path | Store Calls | Memory | CPU |
|-----------|------|-------------|--------|-----|
| ReadAt | Hot | 1 Get + R ReadRange | O(L) | O(log B + R) |
| Latest (warm) | Hot | 2 | O(B) | O(1) |
| Latest (cold) | Degraded | 1 List + 1 Get | O(N + B) | O(N) |
| Snapshots() | Cold | 1 List + S Gets | O(S × B_avg) | O(S × B_avg + S log S) |
| Snapshot(id) | Hot | 1 Get | O(B) | O(B) |

ReadAt block lookup MUST be O(log B + R). Per-read sort checks MUST NOT exist.

---

## Resolved Violations

All known violations have been addressed. Four were fixed in code; three were reclassified as inherent costs.

| ID | Operation | Resolution |
|----|-----------|------------|
| CX-1 | `findCoveringBlocks` sort check | Fixed: sort at load time in `validateVolumeManifest` |
| CX-2 | `ListManifests` validation Gets | Reclassified: contractually required by CONTRACT_ERRORS.md |
| CX-3 | `ListPartitions` double deser | Fixed: single-pass (1 List + M Gets) |
| CX-4 | `Snapshot(id)` degraded fallback | Reclassified: documented degraded path, not hot-path violation |
| CX-5 | `Write` partitioned memory | Reclassified: O(R interface aliases), not O(3R copies) |
| CX-6 | `Volume.Snapshots` sort checks | Fixed: sort at load time eliminates redundant checks |
| CX-7 | `fsStore.List` Walk vs WalkDir | Fixed: `filepath.WalkDir` avoids per-entry `os.Stat` |

### Reclassification Notes

**CX-2** (`ListManifests` validation Gets): CONTRACT_ERRORS.md requires that `ListManifests`
returns errors (not skips) when manifest validation fails. The M Gets for validation are
contractually mandated. Snapshot IDs are already extracted from paths; the Gets are purely
for validation.

**CX-4** (`Snapshot(id)` degraded fallback): The complexity contract classifies this as a
`Degraded` path. Code tries the canonical path first (O(1) Get), and only falls back to
scan for pre-canonical data. This is backward-compat behavior, not a hot-path violation.

**CX-5** (Write partitioned memory): `partitionRecords` creates interface aliases (not copies)
of records. Peak memory is O(R interface values) + O(encoded per partition). The partition map
references are 16 bytes per record — negligible relative to actual record data.

---

## Design Invariant

> Every public method has a documented cost.
> Cost that exceeds the documented bound is a bug, not a tradeoff.

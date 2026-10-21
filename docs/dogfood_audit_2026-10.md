# Dogfood Audit — 2026-10 (quarry, private-A)

Full-history, read-only audit of the one production Lode dataset in the
dogfooding registry. This file is **evidence**, not a contract: it records
what was measured. `V1_READINESS.md` holds the verdicts drawn from it.

Privacy: `private-A` is a redacted alias (see `V1_READINESS.md` §Dogfooding
Registry). This file reports Lode-level structure and counts only — no
partition values, bucket names, endpoints, or record contents.

---

## Scope and Method

| Item | Value |
|------|-------|
| Audit date | 2026-10-19 → 2026-10-20 (UTC) |
| Dataset | `quarry` (Quarry's default dataset ID), Quarry's Hive layout (five partition keys), JSONL codec |
| Backend | Cloudflare R2 via the S3 API |
| Writer | Quarry runtime embedded in `private-A` (see version timeline) |
| Reader used by the audit | Lode **v0.9.0**, the version `private-A` runs today (Quarry 0.13.4), with Quarry 0.13.4's dataset options |
| Access | `ListObjectsV2` and `GetObject` only; every Lode handle wrapped in a store that refuses `Put`/`Delete` |
| Coverage | Every object under `datasets/quarry/`; every manifest decoded; every data file downloaded and checked. The stray test dataset (Inventory) was counted in the history pass only and is excluded from every total |
| Cost | ~0.5M Class B + ~1k Class A operations (inside R2's free tier) |

Checks per data file: stored size = manifest `SizeBytes`; every line parses
as JSON. Row count: the JSONL codec records no per-file `Stats` in v0.9.0, so
line count was compared with `Manifest.RowCount` for single-file snapshots
only; files in multi-file snapshots were checked for size and JSON validity. Content checksums could not be
checked: no file carries one (Quarry does not enable `WithChecksum`).

---

## Writer Version Timeline (private-A)

Derived from the consumer's deployed Quarry runtime image, mapped to the Lode
version each Quarry release pins.

| From (UTC) | Quarry runtime | Lode | Concurrency control |
|------------|----------------|------|---------------------|
| 2026-02-07 | 0.5 – 0.7.3 | ≤ v0.7.3 | none |
| 2026-02-12 | 0.7.3 → 0.11.0 | v0.7.3 – v0.7.4 | none (persistent pointer from v0.7.3) |
| 2026-03-08 03:36 | 0.12.0 | v0.8.0 | CAS on the latest pointer |
| 2026-03-08 18:33 | 0.12.1 → 0.13.4 | v0.9.0 | CAS + `WithRetryCount(3)` |
| 2026-03-26 → today | 0.13.4 | v0.9.0 | CAS + retry |

`private-A` has not taken Lode v0.9.1 or v0.10.0. Quarry itself is on
v0.10.0 (Quarry v0.13.6, 2026-10-19).

---

## Inventory

| Measure | Value |
|---------|------:|
| First / last snapshot | 2026-02-07 / 2026-10-10 |
| Unique snapshots | 96,478 |
| Manifest objects | 233,815 (1–5 per snapshot; see F2) |
| Data objects | 191,261 (3.85 GB) |
| — referenced by a manifest | 143,159 (1.78 GB) |
| — referenced by no manifest | 48,102 (2.07 GB; see F5) |
| Partitions | 32,830 |
| Runs (distinct Quarry run IDs) | 10,858 |
| Events received / persisted (Quarry metrics) | 4,756,412 / 4,728,932 |
| Latest pointer | valid; names the newest snapshot |
| Other datasets in the bucket | 1 stray test dataset (1 snapshot) |

Snapshots by month (unique, by `CreatedAt`):

| 2026-02 | 2026-03 | 2026-04 | 2026-05 | 2026-06 | 2026-07 | 2026-08 | 2026-09 | 2026-10 |
|--------:|--------:|--------:|--------:|--------:|--------:|--------:|--------:|--------:|
| 58,698 | 17,668 | 0 | 1,010 | 9,597 | 2,402 | 3,505 | 3,251 | 347 |

April is empty: the consumer's pipeline did not run. Writes after
2026-10-10 had not resumed at audit time.

---

## Findings

Each finding is tagged with how it bears on v1.0:
**PASS** (evidence for a criterion), **BLOCKER** (must be resolved or
renegotiated), **LIMITATION** (accept and document), **CONSUMER** (not a
Lode concern; recorded for context).

### F1 — No corruption detected in anything Lode wrote · PASS (as observed)

| Check | Result |
|-------|-------:|
| Manifests that fail to decode | 0 of 233,815 |
| Schema / codec / compressor | `lode-manifest@1.0.0` / `jsonl` / `noop` on all |
| Manifest ID ≠ path ID | 0 |
| Referenced files missing | 0 |
| Size mismatches (manifest vs stored vs downloaded) | 0 |
| Row-count mismatches (single-file snapshots) | 0 |
| Unparseable JSONL lines | 0 |
| `GetObject` errors | 0 |

Every file reference (390,402 across manifest copies) was downloaded and
checked. Limit: with no checksums, a bit flip that keeps the line count and
JSON validity would not be caught.

### F2 — Hive writes several copies of each manifest · PASS (format note)

233,815 manifest objects describe 96,478 snapshots. For a partitioned write,
Lode v0.9.0's Hive layout writes one canonical manifest
(`segments/<id>/manifest.json`, for `Snapshot(id)` lookups) plus one copy in
each touched partition: *k* + 1 objects for *k* partitions. The histogram
below spans every writer since 2026-02 and is not split by Lode version, so
it shows this dataset's history, not v0.9.0's format alone. Manifest objects
per snapshot: 1 object 5,815; 2
objects 50,991; 3 objects 33,946; 4 objects 4,450; 5 objects 1,276. All copies of a
snapshot are byte-identical (0 divergent). This is the layout working as
designed, but it is part of the on-disk format v1.0 freezes, and it multiplies
manifest storage and listing cost by ~2.4× across this history.

### F3 — History forked under CAS once · BLOCKER (investigate)

1,113 snapshots have more than one child, leaving 1,157 abandoned branch
heads. 1,112 of these forks predate the consumer's adoption of CAS
(2026-03-08 03:36 UTC). Forking is expected there: the consumer ran
concurrent writers on a Lode without concurrency control.

**One fork happened under Lode v0.9.0 with CAS + retry:**

```text
parent 1787681028908031434  committed 2026-08-25T18:03:50.039Z
├─ child 1787681028905717428  committed 18:03:51.762Z  (different run)
└─ child 1787681093846346313  committed 18:09:07.367Z  (different run)
```

Both children continue their own branch; one branch ends as an abandoned head.
The pointer-swap CAS should make this impossible.

Evidence from the same fork: the first child's ID **precedes** its parent's
ID. Snapshot IDs are taken at write start (`time.Now().UnixNano()`) and
commits land in a different order, so ID order is not commit order. The
latest-by-scan fallback (`history.go` `scan`, unchanged in v0.10.0) assumes
"the lexicographically largest ID is the latest". If a writer reaches the
scan fallback (pointer names a manifest not yet visible: the
pointer-before-manifest window), it can choose a non-head parent while the
CAS expectation still matches the pointer. This is a **candidate** mechanism
consistent with the data. It is not reproduced. Tracked in
[lode#208](https://github.com/pithecene/lode/issues/208).

**Update 2026-10-20 (root cause).** The window above lasts milliseconds; the
second child committed 5 minutes after the first, so the window cannot
explain it. A read-only replay of all 34,642 CAS-era snapshots found 290
commits from a fresh writer (no in-memory head) made while the head's ID was
smaller than an older snapshot's. 289 parented on the head; only this one took
the larger ID, so v0.9.0 did not scan systematically (a read-only probe of
the pointer path on R2 also succeeds).

Proven: the commit took the scan's answer (largest ID, the head's parent)
while its CAS matched the pointer. In v0.9.0 that happens only when a read of
a valid head fails and is read as a stale pointer, in one of two ways:

1. The forking writer's existence check on the head failed, so it scanned
   while its CAS still expected the pointer's content.
2. Another writer's retry refresh (`Latest`) failed to read the head, scanned,
   and rewound the pointer to the parent; the forking writer then legitimately
   built on it. That writer's own commit must then have failed, since the
   parent has no third child.

Inferred: which of the two. The store error itself is not recorded (Quarry
does not classify write failures, F7). Both paths are reproduced and fixed;
see `V1_READINESS.md` §10. The pointer-to-manifest window and a non-atomic CAS
are both ruled out, because both need the two children's commits to overlap
and they were 5 minutes apart.

### F4 — Two snapshots reference a parent that does not exist · LIMITATION (historical)

Both are from 2026-02-12, when the consumer ran Lode v0.7.3. The missing parent
is in no dataset in the bucket. Not seen since.

### F5 — Unreferenced data objects are 54% of data bytes · LIMITATION

48,102 data objects (2.07 GB) are referenced by no manifest. Only objects
Lode's write path stores (`segments/<id>/data/`) are counted. Caller-managed
files that `Store.Put` writes outside that path (Quarry's sidecars, tracked in
snapshot `Metadata`) are classified separately, and none remain in the dataset:

| 2026-02 | 2026-03 | 2026-05 – 2026-10 |
|--------:|--------:|------------------:|
| 15,526 (70 MB) | 31,592 (1.98 GB) | 984 (17 MB) |

The audit establishes only that no manifest references them; it did not
determine how each one arose. Known ways to leave one behind: a write whose
commit fails after its data object is stored (cleanup is best-effort and does
not cover every write path), or a `Put` that succeeds remotely while its
response is lost (`CONTRACT_STORAGE.md`).
Lode has no reference-aware sweep that finds or removes them later (the
`Store` API's `List`/`Delete` are not reference-aware). lode#174 covers this. The
March volume is not attributed (the audit did not join orphans to Quarry run
outcomes). Since May the rate is low (~160 objects/month).

### F6 — The read API's cold path takes hours at this scale · LIMITATION

Lode v0.9.0, run from one client against R2:

| Call | Result | Wall time |
|------|--------|----------:|
| `Dataset.Latest` | pointer target | 131 ms |
| `Dataset.Read(latest)` | 1 record | 130 ms |
| `ListDatasets` | 2 datasets (incl. the stray test dataset) | 3 m 39 s |
| `ListManifests` | 96,472 snapshots | 2 h 05 m |
| `ListPartitions` | 32,830 partitions | 2 h 40 m |

This is within `CONTRACT_COMPLEXITY.md` (1 List + M Gets, made one at a
time). For the Hive layout the List covers the whole dataset prefix: 425,077
keys (233,815 manifests, 191,261 data objects, the pointer). The reader skips canonical
copies and already-seen IDs, so M is about one `Get` per returned snapshot
(~96k). But in practice no consumer can use the cold path at
this size. `private-A` does not read through `DatasetReader` (F8).

Open discrepancy: `ListManifests` returned 96,472 snapshots; manifest
decoding found 96,478. Six snapshots are missing from Lode's own listing.
Candidate, not verified: v0.9.0 `ListManifests` skips canonical manifests in
partition-aware layouts, so a snapshot with no partition copy (a write
with no partition key) is never listed.

### F7 — Write outcomes recorded by Quarry · CONSUMER (with a Lode-relevant gap)

Quarry writes a metrics record per run process (20,081 records):

| Period | `lode_write_success` | `lode_write_failure` | Failure rate |
|--------|---------------------:|---------------------:|-------------:|
| 2026-02 | 98,700 | 15,402 | 13.5% |
| 2026-03 | 24,558 | 1,220 | 4.7% |
| 2026-05 – 2026-10 | 28,569 | 526 | 1.8% |

`lode_write_retry` is 0 everywhere because Quarry 0.13.4 does not populate it
(populated from Quarry 0.13.5 / Lode v0.9.1). So **this dataset has no
evidence about how often CAS retries happen**. Quarry does not classify write
failures, so the 1.8% cannot be split between storage errors, exhausted CAS
retries, and caller errors. 27,480 events were received but neither persisted
nor counted as dropped. That is a Quarry accounting gap, not a Lode one.

### F8 — Lode's on-disk paths have a production dependent · LIMITATION (D8)

Lode-level fact only: in `private-A`, data is read back by Lode's on-disk
path grammar, not through Lode's read API. A change to that grammar (the
`segments/` / `data/` names or the Hive encoding) would break it with no API
change. That makes `V1_API_DECISIONS.md` D8 (on-disk path compatibility
policy) a real dependency, not a hypothetical one.

---

## Open Questions

1. ~~F3 ([#208](https://github.com/pithecene/lode/issues/208)): reproduce the scan-fallback fork (two writers, one landing in the
   pointer-before-manifest window) and decide whether `scan` must order by
   commit (e.g. `CreatedAt` or the parent chain) instead of ID.~~ Resolved
   2026-10-20: root cause in F3's update. The window mechanism is reproduced
   separately and left to the commit-protocol decision.
2. F6: confirm whether the six snapshots `ListManifests` does not return are
   canonical-only.
3. F5: which March runs left 1.98 GB unreferenced (join orphans to run
   outcomes).
4. F7: classify `lode_write_failure` (Quarry would have to record the error
   class).

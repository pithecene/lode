# V1_READINESS.md — v1.0 Release Criteria

This document defines the **non-negotiable criteria** that must all pass
before Lode can be tagged v1.0.

Stability is proven by **usage and time**, not design review.

---

## Gate Rule

- Every criterion below must be checked off with recorded evidence.
- Evidence must reference a real downstream project (or redacted alias), real data, and a real observer.
- No criterion may be waived — if it cannot be met, it must be renegotiated and this document updated.
- The v1.0 tag is blocked until all criteria pass, except §4: it is the Volume stability gate (`V1_API_DECISIONS.md` D9), which gates Volume leaving experimental, not the v1.0 tag.

---

## Current Assessment (2026-10-22)

Based on the full-history audit of the production dataset
([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md)):

- **Holds:** no corruption detected and zero manifest decode failures across 96,478
  snapshots over 8 months on R2; `Manifest` format stable from 2026-02-06
  until the unreleased format 1.1.0 (D11), which adds three fields and
  reads every 1.0.0 manifest.
- **Blocker, addressed in v0.11.0 and awaiting verification:** one history
  fork under CAS + retry (2026-08-25, §10). The cause is identified: a failed
  read of a valid head was treated as a stale pointer, and the scan fallback
  then selected the head's parent, because snapshot ID order is not commit
  order. v0.11.0 removes every path to that outcome
  ([#208](https://github.com/pithecene/lode/issues/208)); the criterion
  remains unmet until v0.11.0 is observed in production.
- **Not yet met, time-gated:** API and sentinel freeze windows reopened by
  v0.10.0 and again by v0.11.0, which moves Volume out of `lode`, requires
  `CompareAndSwap` of every `Store`, and adds two sentinels (§1, §5); the
  earliest pass of both is 2026-11-19. The unreleased surface of
  `V1_API_DECISIONS.md` D11 replaces the construction and record API and
  restarts both windows when it is released. D5, D6, and D10b open; D1–D3
  and D8 decided by D11; D4, D7, D10a, D10c, and D10f accepted.
- **Not yet met, evidence missing:** native AWS S3 (§6), steady-state snapshot
  reads (§3), checksum-backed corruption checks (no production file carries a
  checksum, §10).
- **Renegotiated (D9):** Volume is experimental for v1.0. Volume dogfooding
  (§4) and `VolumeManifest` stability (§2) gate Volume leaving experimental,
  not the v1.0 tag. The boundary has shipped: package `lode/volume`, marked
  experimental in `PUBLIC_API.md` and `CONTRACT_VOLUME.md` (D9).
- **Accepted limitations (GC and performance are out of scope):** 2.07 GB of
  unreferenced data objects, which `Dataset.Sweep` (v0.11.0, lode#174) can now
  reclaim; `ListManifests` and
  `ListPartitions` take hours at this size. As a result the consumer reads the
  on-disk paths directly, which makes D8 a real dependency.

---

## Dogfooding Registry

Each downstream project that integrates Lode gets a subsection here.
Use the template to add new projects as they begin integration.

**Privacy rule:** Only public projects are listed by name. Private projects
use a redacted alias (e.g. `private-A`, `private-B`) and must not reveal
internal project names, repo URLs, domain-specific details, or
implementation specifics that could identify the project or its purpose.
Usage profiles and evidence summaries should describe Lode-level behavior
only (e.g. "Dataset round-trip over S3"), not the consumer's architecture
or data model. Aggregate usage counts (snapshots, runs, events,
partitions, bytes) are allowed: they show scale without revealing purpose.
If a linked issue lives in a private tracker, reference it as `(private)`
instead of by number.

### quarry

- **Integration date:** 2026-02-10
- **Contact:** @pithecene
- **Usage profile:** Dataset
- **Storage backend:** AWS S3 (S3 adapter targeting Cloudflare R2)
- **Status:** Active — Lode v0.10.0 since Quarry v0.13.6 (2026-10-19); ~8 months of production operation through `private-A`
- **Visibility:** Public
- **Last audited:** 2026-10-19

| Criterion | Validated | Date | Notes |
|-----------|:---------:|------|-------|
| Dataset write round-trip | ✅ | 2026-02-23 | Hive-partitioned writes and reads over S3-compatible backend at >100k object scale |
| Volume write round-trip | N/A | — | Dataset-only integration (no Volume API referenced in Quarry's Lode client) |
| Error sentinels observed | ✅ | 2026-03-22 | All 3 sentinels validated: `ErrNoSnapshots` (production), `ErrPathExists` (integration test), `ErrNotFound` (integration test). `quarry/lode/sentinel_test.go` still present at v0.13.6. |
| API friction (none = pass) | ⚠️ | 2026-10-19 | Upgrades v0.9.0 → v0.9.1 → v0.10.0 needed no call-site changes beyond adopting new options (v0.10.0 bump was dependency-only; Quarry matches on sentinels, so v0.10.0 error-text changes were behavior-neutral; Quarry CI green on v0.10.0). Open friction: no retention/delete primitive (lode#174, filed from this workload) gates Quarry's planned `gc`; Quarry's v0.14 run reader still pages `Store.List` client-side (no storage-native continuation). Earlier friction resolved: sidecar inventory via snapshot `Metadata["sidecar_files"]` (Quarry v0.13.4); CAS via `WithRetryCount(3)` + `WithRetryObserver` (Quarry v0.13.5, Lode v0.9.1, lode#169); listing prefix/cursor (lode#175, v0.10.0). |

**Lode APIs exercised (Quarry v0.13.6):** `NewDataset`, `WithHiveLayout`,
`WithCodec(NewJSONLCodec())`, `WithRetryCount`, `WithRetryObserver`,
`Dataset.Write`/`Latest`/`Snapshots`/`Read` (metrics and sidecar queries),
`Store.Put` (sidecar files), S3 adapter, `NewMemory`/`NewFSFactory` (tests),
`ErrNoSnapshots`, `ErrNotFound`, `ErrPathExists`, `ErrSnapshotConflict`.

### private-A

- **Integration date:** 2026-02-10 (indirect — writes through Quarry's embedded Lode)
- **Contact:** @pithecene
- **Usage profile:** Dataset
- **Storage backend:** Cloudflare R2 via the S3 adapter
- **Status:** Active production since 2026-02-07; runs Quarry 0.13.4 → Lode v0.9.0 (CAS + retry since 2026-03-08; not on v0.9.1/v0.10.0)
- **Visibility:** Private (alias — see privacy rule above)
- **Last audited:** 2026-10-20 — full-history read-only audit, [`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md)

**Scale (audit, 2026-10-20):** 96,478 snapshots over 8 months, 10,858 runs,
4.73M events persisted, 143k referenced data objects (1.78 GB), 32,830
partitions, one dataset on R2.

| Criterion | Validated | Date | Notes |
|-----------|:---------:|------|-------|
| Dataset write round-trip | ⚠️ | 2026-10-20 | Writes: ✅ no corruption detected across all 96,478 snapshots (audit F1). Reads: Lode's reader is not exercised in production (F8). Lode's reader was exercised by the audit: `Latest`/`Read` in ~130 ms; `ListManifests`/`ListPartitions` took 2–3 h (F6). |
| Volume write round-trip | N/A | — | Dataset-only |
| Error sentinels observed | N/A | — | Sentinels surface inside Quarry; covered by the quarry entry |
| API friction (none = pass) | ❌ | 2026-10-20 | (1) One history fork under CAS (F3). (2) 48k unreferenced data objects, 2.07 GB, and no reference-aware sweep in Lode to find or remove them (F5, lode#174). (3) Cold listing unusable at this size (F6), so the consumer couples to the path grammar (F8, `V1_API_DECISIONS.md` D8). The earlier sidecar-manifest friction no longer applies to this consumer. |

<!--
### <project-name or redacted alias>

- **Integration date:** TBD
- **Contact:** TBD
- **Usage profile:** Dataset / Volume / Both
- **Storage backend:** <backend>
- **Status:** Not started
- **Visibility:** Public / Private (if private, use alias — see privacy rule above)

| Criterion | Validated | Date | Notes |
|-----------|:---------:|------|-------|
| Dataset write round-trip | | | |
| Volume write round-trip | | | |
| Error sentinels observed | | | |
| API friction (none = pass) | | | |
-->

---

## Criteria

### 1. API Surface Freeze

- [ ] No public type, function, or method signature in the v1.0-promised surface has changed for 4+ weeks after first downstream integration (experimental Volume excluded, D9)

> **Evidence:** Window open. Last release change: v0.10.0 (2026-10-19) added `DatasetListOptions.StartAfter`, `PartitionListOptions.Prefix`/`StartAfter`, `ManifestListOptions.StartAfter`. Unreleased: D9 removes the Volume types, `NewVolume` and `ErrOptionNotValidForVolume` from `lode` (moved to experimental `lode/volume`), which restarts the window when it is released. Quarry took v0.10.0 with no call-site changes and uses no Volume API.
> Date: 2026-10-20 | Observer: @pithecene | Project: lode, quarry
>
> **Evidence:** Window restarted by v0.11.0 (2026-10-22). `Store` is now `ObjectStore` together with `ConditionalWriter`, so every store must implement `CompareAndSwap`; `SingleProcess` is added; `Dataset` gains `Expire` and `Sweep` with `ExpireRequest`, `ExpireReport`, `SweepRequest`, and `SweepReport`; D9 removes the Volume types, `NewVolume`, and `ErrOptionNotValidForVolume` from `lode`; and the module path becomes `github.com/pithecene/lode`. Earliest pass: 2026-11-19.
> Date: 2026-10-22 | Observer: @pithecene | Project: lode
>
> **Evidence:** Unreleased: D11 replaces the construction and record surface. `NewDataset`, `NewDatasetReader`, and the store factories give way to `Open`, `OpenReadOnly`, `FS`, `Memory`, and `Use`; records are typed (`Records[T]`, `Codec[T]`), blobs have their own handle (`Blobs`), metadata is any JSON object, and options are typed by target. The window restarts when it is released.
> Date: 2026-10-22 | Observer: @pithecene | Project: lode, Quarry
> Summary: Not met; window restarts on every API change.
> Issue: lode#175

- [ ] No new sentinel errors added to the v1.0-promised surface for 4+ weeks after first downstream integration (experimental Volume excluded, D9; freeze window reset: v0.11.0 added `ErrSnapshotExpired` and `ErrHeadUncommitted`)

> **Evidence:** Window open. v0.10.0 (2026-10-19) added `ErrInvalidArgument` and `ErrPartitionPrefixNotSupported`. Earliest pass: 2026-11-16.
> Date: 2026-10-20 | Observer: @pithecene | Project: lode
>
> **Evidence:** Unreleased: D11 adds `ErrSettingsMismatch` and `ErrReadOnly` and removes `ErrCodecConfigured`, `ErrOptionNotValidForDataset`, and `ErrOptionNotValidForDatasetReader`. The window restarts when it is released.
> Date: 2026-10-22 | Observer: @pithecene | Project: lode
>
> **Evidence:** Window restarted by v0.11.0 (2026-10-22), which adds `ErrSnapshotExpired` and `ErrHeadUncommitted` for snapshot expiry. Earliest pass: 2026-11-19.
> Date: 2026-10-22 | Observer: @pithecene | Project: lode
> Summary: Not met.
> Issue: —

- [ ] Every candidate in `docs/V1_API_DECISIONS.md` is marked accepted (and shipped) or rejected; none is left open

> **Evidence:** D1–D8 all Open. D9 accepted and shipped: Volume is experimental in package `lode/volume`, and `PUBLIC_API.md` / `CONTRACT_VOLUME.md` say so. Dogfooding now bears on D8: `private-A` reads the on-disk path grammar directly (audit F8), so the path-compatibility policy has a real consumer.
> Date: 2026-10-20 | Observer: @pithecene | Project: lode, private-A
>
> **Evidence:** D10a–D10c, D10f, and D11 Open. D10d accepted and shipped: every `Store` implements `CompareAndSwap`. D10e superseded by #260: a Volume commit adopts an abandoned one from its staged manifest. D10 follows from the history fork investigated in #208 (audit F3); D11 follows from Quarry's call sites (three constructions of one dataset, a second store value for sidecar files, and untyped records).
> Date: 2026-10-22 | Observer: @pithecene | Project: lode, Quarry
>
> **Evidence:** D11 accepted (owner decision, 2026-10-22) and implemented, unreleased: the persisted format in #279 and the surface in the pull request that follows it. D11 decides D1–D3, and D8 is accepted as D11 revised it: the names within a dataset's paths are root options, and the default names, the flat paths, and the path grammar are persisted format that no release changes. D4–D7, D10a–D10c, and D10f remain open.
> Date: 2026-10-22 | Observer: @pithecene | Project: lode
>
> **Evidence:** D11 is on `main`, unreleased: the format in #279, the surface in #280, and two refactors in #282 and #283. D10a, D10c, and D10f accepted (owner decision, 2026-10-23); they change contract text only. D7 accepted and implemented: the S3 test double is no longer exported. D4 accepted and implemented: partition paths are a `PartitionPath`. The 2026-10-20 entry above predates D11: D1–D3 and D8 are no longer open.
> Date: 2026-10-23 | Observer: @pithecene | Project: lode
> Summary: Not met; D5, D6, and D10b open.
> Issue: —

- [ ] `PUBLIC_API.md` matches exported symbols in `lode/api.go` with zero discrepancies

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Option surface of the v1.0-promised API has not expanded since v0.9.1 (experimental Volume options excluded, D9; freeze window reset: v0.9.1 added WithRetryObserver; v0.9.0 added WithRetryCount/BaseDelay/MaxDelay/Jitter, ErrOptionNotValidForVolume; removed VolumeOption, WithVolumeChecksum)

> **Evidence:** No option added in v0.10.0. Window since v0.9.1 (2026-10-18) passes 4 weeks on 2026-11-15. Both downstream-driven additions (`WithRetryCount`, `WithRetryObserver`) came from Quarry requests (lode#163, lode#169).
> Date: 2026-10-20 | Observer: @pithecene | Project: lode, quarry
> Summary: On track; not yet met.
> Issue: lode#169
>
> **Evidence:** Unreleased: D11 types options by target (`RootOption`, `DatasetOption`, `ViewOption`, `ComponentOption`), removes `WithCodec` and `WithHiveLayout` (the codec is a parameter; `WithLayout` takes `Hive(...)`), renames `WithRetryCount` to `WithRetry`, and adds the path-name options `WithDatasetsDir`, `WithSnapshotsDir`, `WithPartitionsDir`, `WithSegmentsDir`, `WithManifestFile`, `WithDataDir`, `WithExpiredDir`, `WithStagedDir`, and `WithFlatPaths`. The window restarts when it is released.
> Date: 2026-10-22 | Observer: @pithecene | Project: lode
> Summary: Not met once D11 is released.

### 2. Manifest Format Freeze

- [ ] `Manifest` struct fields unchanged for 4+ weeks

> **Evidence:** `Manifest` fields unchanged since 2026-02-06 (#94); `FileRef` fields unchanged since 2026-02-06 (#103, which added `FileRef.Stats`). All 233,815 production manifest objects are `lode-manifest@1.0.0` and decode cleanly ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md) F1).
> Date: 2026-10-20 | Observer: @pithecene | Project: lode, private-A
> Summary: Met; >8 months stable, 96,478 snapshots in production.
> Issue: —
>
> **Evidence:** Unreleased: manifest format 1.1.0 (#279, D11) adds `CodecDescription`, `PartitionKeys`, and `Paths`, so that every setting a writer must share is persisted. Every 1.0.0 manifest remains readable. The window restarts when it is released.
> Date: 2026-10-22 | Observer: @pithecene | Project: lode
> Summary: Not met once format 1.1.0 is released.
> Issue: —

- [x] ~~`VolumeManifest` struct fields unchanged for 4+ weeks~~ Reclassified: Volume stability gate (`V1_API_DECISIONS.md` D9)

> **Evidence:** Fields unchanged in source since 2026-02-06 (#94). No downstream Volume data (§4). Volume is experimental for v1.0 (D9), so the v1.0 tag does not freeze the `volume.Manifest` type; persisted Volume manifests stay readable (D8). This criterion moves to the gate for Volume leaving experimental.
> Date: 2026-10-20 | Observer: @pithecene | Project: lode
> Summary: Not a v1.0 blocker. Still unmet for Volume's own gate: no consumer writes Volumes.
> Issue: —

- [ ] Owner sign-off on manifest JSON schema as v1.0 wire format

> **Evidence:** Inputs for the sign-off from the audit: Hive layout writes one byte-identical manifest copy per touched partition (~2.4× objects in production, F2); no production file carries a checksum (F1). Both are part of what v1.0 freezes.
> Date: 2026-10-20 | Observer: @pithecene | Project: private-A
> Summary: Pending owner decision.
> Issue: —

### 3. Dataset Dogfooding

- [x] At least one consumer writing Dataset snapshots to S3 for 2+ weeks

> **Evidence:** `private-A` (through Quarry) has written to R2 from 2026-02-07 to 2026-10-10: 96,478 snapshots, 10,858 runs, 4.73M events ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md)).
> Date: 2026-10-20 | Observer: @pithecene | Project: private-A, quarry
> Summary: Met; ~8 months.
> Issue: —

- [ ] At least one consumer reading Dataset snapshots back (round-trip) for 2+ weeks

> **Evidence:** In production, `private-A` data is read back by Lode's on-disk path grammar, not through Lode's read API (audit F8). Quarry reads through Lode only for metrics and sidecar queries. The audit exercised Lode's reader once over the full dataset: `Latest`/`Read` ~130 ms, `ListManifests` 2 h 05 m, `ListPartitions` 2 h 40 m (F6).
> Date: 2026-10-20 | Observer: @pithecene | Project: private-A, quarry
> Summary: Not met. Reading data objects by path skips snapshots, so it is not a snapshot round-trip. Quarry's `Dataset.Read`/`Latest` calls (metrics and sidecar queries) have no evidence of 2+ weeks of production use.
> Issue: —

- [x] At least 100 Dataset snapshots committed and read with no corruption

> **Evidence:** 96,478 snapshots; every manifest decoded and every referenced data file downloaded: 0 missing, 0 size mismatches, 0 unparseable lines, 0 row-count mismatches (row counts checkable for single-file snapshots only) ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md) F1).
> Date: 2026-10-20 | Observer: @pithecene | Project: private-A
> Summary: Met by ~965×.
> Issue: —

- [ ] No Dataset API changes required to support integration

> **Evidence:** Integration drove three additive API changes: CAS retry options (v0.9.0, lode#163), `WithRetryObserver` (v0.9.1, lode#169), listing `Prefix`/`StartAfter` (v0.10.0, lode#175). None broke Quarry. lode#174 (retention) is open from the same workload, but GC is out of scope for v1.0.
> Date: 2026-10-20 | Observer: @pithecene | Project: quarry
> Summary: Not met as worded: the API grew to support the integration, additively. Re-word to "no breaking changes", or accept that the window resets.
> Issue: lode#174

### 4. Volume Dogfooding — Volume stability gate (not a v1.0 blocker)

Volume is experimental for v1.0 (`V1_API_DECISIONS.md` D9). The criteria in
this section gate Volume leaving experimental, not the v1.0 tag. They stay
unchanged so evidence lands in one place when a consumer appears. The
nearest candidate is pithecene/quarry#255.

- [ ] At least one consumer using StageWriteAt + Commit with real data

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] At least one consumer using ReadAt on committed ranges

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] At least 3 Volume snapshots committed with cumulative manifests in a real workflow

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] No Volume API changes required to support integration

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Volume writes/reads on AWS S3 during dogfooding with no adapter errors (moved from §6, D9)

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Zero silent data corruption in Volume data during dogfooding, verified with checksums (moved from §10, D9)

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] `VolumeManifest` struct fields unchanged for 4+ weeks with real downstream data (moved from §2, D9)

> **Evidence:** Fields unchanged in source since 2026-02-06 (#94). No downstream Volume data.
> Date: 2026-10-20 | Observer: @pithecene | Project: lode
> Summary: Not met.
> Issue: —

### 5. Error Path Validation

- [x] ErrNoSnapshots observed and handled correctly by a downstream consumer

> **Evidence:** Quarry handles `ErrNoSnapshots` on cold start (empty dataset, no committed snapshots).
> Date: 2026-02-23 | Observer: @pithecene | Project: quarry
> Summary: Sentinel correctly returned and handled. Latest-pointer self-healing scan fallback exercised on first access.
> Issue: (private)

- [x] ErrPathExists observed in real storage confirming immutability enforcement

> **Evidence:** Not observed in production — validated via targeted integration test (`TestPutFile_ErrPathExists_DoubleWrite` in `quarry/lode/sentinel_test.go`). Double-write via `LodeClient.PutFile()` to memory store confirms `lode.ErrPathExists` returned and classified as `StorageError` with `ErrPathExists` kind. Exercises Quarry's actual sidecar file write path.
> Date: 2026-03-22 | Observer: @pithecene | Project: quarry
> Summary: Immutability enforcement confirmed via integration test exercising Quarry's production write path against Lode's storage layer.
> Issue: (private)

- [x] ErrNotFound observed and handled correctly for a missing snapshot lookup

> **Evidence:** Not observed in production — validated via targeted integration test (`TestQueryLatestMetrics_ErrNotFound_StaleSnapshotID` in `quarry/lode/sentinel_test.go`). Fabricated snapshot ID passed to `ds.Read()` confirms `lode.ErrNotFound` returned. Exercises the exact code path in `QueryLatestMetrics` where a snapshot could be GC'd between list and read.
> Date: 2026-03-22 | Observer: @pithecene | Project: quarry
> Summary: Missing-snapshot sentinel confirmed via integration test exercising Quarry's stale-snapshot-ID recovery path.
> Issue: (private)

### 6. S3 Backend Verification

- [ ] AWS S3 used for Dataset writes/reads during dogfooding with no adapter errors (Volume half moved to §4, D9)

> **Evidence:** R2 only. Reads: 0 errors over ~0.5M `GetObject` calls and the full listing during the audit. Writes: Quarry recorded `lode_write_failure` at 1.8% of writes since May (F7), unclassified, so adapter errors cannot be ruled in or out. Native AWS S3 not exercised.
> Date: 2026-10-20 | Observer: @pithecene | Project: private-A
> Summary: Partial. Needs AWS S3 and classified write failures.
> Issue: —

- [ ] Non-AWS backend caveat documented in README and PUBLIC_API.md as known v1.0 limitation

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 7. Known Limitations Accepted

- [ ] Concurrency model documented in README + PUBLIC_API.md + contracts (single-writer when store lacks `ConditionalWriter`; CAS-based optimistic concurrency when available)

> **Evidence:** CAS optimistic concurrency shipped in v0.8.0 (#135). Contracts, PUBLIC_API.md, and README document `ConditionalWriter`, `ErrSnapshotConflict`, and the adapter CAS capability matrix. PR #156 added the adapter CAS capability table to PUBLIC_API.md.
> Date: 2026-02-25 | Observer: @pithecene | Project: lode
> Summary: Concurrency model fully documented across README, PUBLIC_API.md, CONTRACT_WRITE_API.md, and CONTRACT_VOLUME.md. Adapter CAS capability matrix in PUBLIC_API.md.
> Issue: #135

- [ ] Context cancellation cleanup nondeterminism documented as best-effort, no correctness impact

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Deferred test gaps (ITER-LIFECYCLE, COMP-WRAPPER-ORDER, ERR-RANGE-NOT-SUPPORTED) accepted as non-blocking

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 8. Documentation Completeness

- [ ] README "Status" section updated to v1.0 language

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] All 13 contract files reviewed — no "planned"/"draft"/"future" language for shipped features

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [x] All examples pass (`task examples`)

> **Evidence:** `task examples` passes all 10 examples (default_layout, hive_layout, blob_upload, manifest_driven, stream_write_records, parquet, volume_sparse, vector_artifacts, optimistic_concurrency, s3). Verified during v0.8.0 release and audit sweep PR.
> Date: 2026-02-25 | Observer: @pithecene | Project: lode
> Summary: All examples compile and run successfully. `internal/testutil` dependency removed in audit sweep (#157).

- [ ] CHANGELOG includes v1.0 entry summarizing stability commitment

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 9. Blob File Completeness

- [x] ~~Blob files written alongside structured data within a snapshot are enumerable without prefix-scanning storage~~ Reclassified: out of scope (see below)

> **Evidence:** Reclassified as out-of-scope. Files written via `Store.Put()` directly — bypassing the Dataset write path — are not tracked by manifests by design. The manifest describes files Lode wrote; caller-managed sidecar files are the caller's responsibility. Correct patterns: use `StreamWrite` to write blobs through Lode, or track file inventories in snapshot `Metadata`. See `PUBLIC_API.md` §Sidecar Files and Store Access.
> Date: 2026-03-22 | Observer: @pithecene | Project: lode
> Summary: Design analysis confirmed this is a caller-side usage pattern, not a Lode gap. Manifest self-description integrity preserved.
> Issue: —

- [x] ~~Consumers can verify which blob files were successfully persisted for a given commit~~ Reclassified: out of scope (see above)

> **Evidence:** Same reclassification. Blobs written through Lode's write path (`Write`, `StreamWrite`, `StreamWriteRecords`) are tracked in `Manifest.Files` and verifiable. Blobs written outside Lode are outside the manifest's scope.
> Date: 2026-03-22 | Observer: @pithecene | Project: lode
> Summary: Completeness verification is available for all files written through Lode's API. Files bypassing the API are the caller's concern.
> Issue: —

### 10. Data Correctness

- [ ] Zero silent data corruption in Dataset data during dogfooding (Volume half moved to §4, D9)

> **Evidence:** Full-history verification of 96,478 snapshots: every referenced data file matches its manifest size and parses as JSONL; row counts match where the manifest records them per file (single-file snapshots) ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md) F1). Limit: no file carries a checksum, so byte-level flips that keep size and JSON validity are not excluded.
> Date: 2026-10-20 | Observer: @pithecene | Project: private-A
> Summary: Partial. No corruption detected by size, row-count and parse checks (Dataset). Silent corruption cannot be ruled out until a consumer writes with `WithChecksum`.
> Issue: —

- [x] Zero manifest deserialization failures on same-version data

> **Evidence:** 0 of 233,815 manifest objects fail to decode; all `lode-manifest@1.0.0`, written across Lode releases up to v0.9.0 and read by v0.9.0 ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md) F1).
> Date: 2026-10-20 | Observer: @pithecene | Project: private-A
> Summary: Met.
> Issue: —

- [ ] Zero snapshot history inconsistencies (forked history, missing parent, duplicate ID)

> **Evidence:** 0 duplicate IDs. 2 missing parents (2026-02-12, Lode v0.7.3). 1,113 forks, of which 1,112 predate the consumer's CAS adoption (2026-03-08) and are expected without concurrency control. **1 fork happened under Lode v0.9.0 CAS + retry (2026-08-25)** ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md) F3). The same fork shows a child whose ID precedes its parent's: ID order is not commit order, which breaks the assumption in the latest-by-scan fallback (`history.go` `scan`, unchanged in v0.10.0).
>
> **Root cause (2026-10-20):** a read-only replay of the 34,642 snapshots committed since CAS adoption shows that the forking commit adopted the snapshot with the largest ID, the head's parent, while its CAS matched the pointer; 289 comparable commits adopted the head. In v0.9.0 that outcome requires a failed read of a valid head to be treated as a stale pointer, either by the forking writer or by another writer's retry refresh, which then replaced the pointer. Which of the two occurred is inferred, not observed, because the store error was not recorded (audit F3). v0.11.0 removes both paths and returns any failed read other than not-found to the caller.
> Date: 2026-10-20 | Observer: @pithecene | Project: private-A
> Summary: Not met. The cause is identified and removed in v0.11.0; the criterion requires a clean observation period on v0.11.0.
> Issue: [#208](https://github.com/pithecene/lode/issues/208)

---

## Out of Scope for v1.0

The following are explicitly **not blockers** for the v1.0 release:

- Content-addressable storage
- Zarr codec support
- New codecs beyond Parquet
- Performance benchmarking or optimization
- New storage adapters beyond S3
- Compaction or garbage collection
- Distributed coordination (consensus, lock management)
- Query planning or execution
- Volume API and `VolumeManifest` stability: Volume is experimental for
  v1.0 (`V1_API_DECISIONS.md` D9; §4 is its own gate)

These may become goals for v1.1+ but must not gate the initial stability release.

---

## References

- `PUBLIC_API.md` — public API shape and defaults
- `docs/V1_API_DECISIONS.md` — breaking API candidates held for v1.0
- `docs/dogfood_audit_2026-10.md` — full-history production audit (evidence for §2, §3, §6, §10)
- `docs/contracts/CONTRACT_CORE.md` — core model invariants
- `docs/contracts/CONTRACT_WRITE_API.md` — write API semantics
- `docs/contracts/CONTRACT_READ_API.md` — read API contract
- `docs/contracts/CONTRACT_VOLUME.md` — Volume persistence model
- `docs/contracts/CONTRACT_ERRORS.md` — error taxonomy
- `docs/contracts/CONTRACT_TEST_MATRIX.md` — contract-to-test traceability
- `docs/contracts/CONTRACT_STORAGE.md` — storage adapter obligations

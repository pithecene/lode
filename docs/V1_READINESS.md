# V1_READINESS.md — v1.0 Release Criteria

This document defines the **non-negotiable criteria** that must all pass
before Lode can be tagged v1.0.

Stability is proven by **usage and time**, not design review.

---

## Gate Rule

- Every criterion below must be checked off with recorded evidence.
- Evidence must reference a real downstream project (or redacted alias), real data, and a real observer.
- No criterion may be waived — if it cannot be met, it must be renegotiated and this document updated.
- The v1.0 tag is blocked until all criteria pass.

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
or data model. If a linked issue lives in a private tracker, reference it
as `(private)` instead of by number.

### quarry

- **Integration date:** 2026-02-10
- **Contact:** @pithecene-io
- **Usage profile:** Dataset
- **Storage backend:** AWS S3 (S3 adapter targeting Cloudflare R2)
- **Status:** In progress (~2 weeks production operation)

| Criterion | Validated | Date | Notes |
|-----------|:---------:|------|-------|
| Dataset write round-trip | ✅ | 2026-02-23 | Hive-partitioned writes and reads over S3-compatible backend at >100k object scale |
| Volume write round-trip | N/A | — | Dataset-only integration |
| Error sentinels observed | ✅ | 2026-03-22 | All 3 sentinels validated: `ErrNoSnapshots` (production), `ErrPathExists` (integration test), `ErrNotFound` (integration test) |
| API friction (none = pass) | ✅ | 2026-03-22 | All friction resolved. Blob completeness (§9): Quarry tracks sidecar files in snapshot `Metadata["sidecar_files"]` (v0.13.4); caller responsibility, not a Lode gap. CAS: resolved for CAS-capable stores (R2) via `WithRetryCount(3)` (v0.12.1); non-CAS store guidance documented in `PUBLIC_API.md` §Adapter CAS Support. |

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

- [ ] No public type, function, or method signature has changed for 4+ weeks after first downstream integration

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] No new sentinel errors added for 4+ weeks after first downstream integration (freeze window reset: v0.10.0 added `ErrInvalidArgument`)

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Every candidate in `docs/V1_API_DECISIONS.md` is marked accepted (and shipped) or rejected; none is left open

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] `PUBLIC_API.md` matches exported symbols in `lode/api.go` with zero discrepancies

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Option surface has not expanded since v0.9.1 (freeze window reset: v0.9.1 added WithRetryObserver; v0.9.0 added WithRetryCount/BaseDelay/MaxDelay/Jitter, ErrOptionNotValidForVolume; removed VolumeOption, WithVolumeChecksum)

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 2. Manifest Format Freeze

- [ ] `Manifest` struct fields unchanged for 4+ weeks

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] `VolumeManifest` struct fields unchanged for 4+ weeks

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

- [ ] Owner sign-off on manifest JSON schema as v1.0 wire format

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 3. Dataset Dogfooding

- [ ] At least one consumer writing Dataset snapshots to S3 for 2+ weeks

> **Evidence:** Quarry production pipeline writing Hive-partitioned Dataset snapshots to S3-compatible backend (R2) since 2026-02-10. 15 days elapsed as of 2026-02-25; threshold met.
> Date: 2026-02-25 | Observer: @pithecene-io | Project: quarry
> Summary: Hive-partitioned event and blob storage, deterministic segment layout confirmed idempotent across re-runs. 2-week threshold met.
> Issue: (private)

- [ ] At least one consumer reading Dataset snapshots back (round-trip) for 2+ weeks

> **Evidence:** Quarry reads Dataset snapshots via prefix listing and segment data files. Full round-trip confirmed but read path requires prefix scan + post-filter (no segment enumeration shortcut).
> Date: 2026-02-23 | Observer: @pithecene-io | Project: quarry
> Summary: Partial — reads work but rely on prefix listing rather than manifest-driven discovery for blob files.
> Issue: (private)

- [ ] At least 100 Dataset snapshots committed and read with no corruption

> **Evidence:** _not yet recorded — snapshot count not yet confirmed_
> Date: — | Observer: — | Project: quarry
> Summary: No corruption observed during dogfooding period; exact count pending.
> Issue: (private)

- [ ] No Dataset API changes required to support integration

> **Evidence:** No API changes required during integration. Two performance fixes (v0.7.1 O(n²) write, v0.7.3 latest pointer) were internal improvements, not API changes.
> Date: 2026-02-23 | Observer: @pithecene-io | Project: quarry
> Summary: API surface stable throughout integration. Consumer workarounds address missing features (blob manifest), not API defects.
> Issue: (private)

### 4. Volume Dogfooding

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

### 5. Error Path Validation

- [x] ErrNoSnapshots observed and handled correctly by a downstream consumer

> **Evidence:** Quarry handles `ErrNoSnapshots` on cold start (empty dataset, no committed snapshots).
> Date: 2026-02-23 | Observer: @pithecene-io | Project: quarry
> Summary: Sentinel correctly returned and handled. Latest-pointer self-healing scan fallback exercised on first access.
> Issue: (private)

- [x] ErrPathExists observed in real storage confirming immutability enforcement

> **Evidence:** Not observed in production — validated via targeted integration test (`TestPutFile_ErrPathExists_DoubleWrite` in `quarry/lode/sentinel_test.go`). Double-write via `LodeClient.PutFile()` to memory store confirms `lode.ErrPathExists` returned and classified as `StorageError` with `ErrPathExists` kind. Exercises Quarry's actual sidecar file write path.
> Date: 2026-03-22 | Observer: @pithecene-io | Project: quarry
> Summary: Immutability enforcement confirmed via integration test exercising Quarry's production write path against Lode's storage layer.
> Issue: (private)

- [x] ErrNotFound observed and handled correctly for a missing snapshot lookup

> **Evidence:** Not observed in production — validated via targeted integration test (`TestQueryLatestMetrics_ErrNotFound_StaleSnapshotID` in `quarry/lode/sentinel_test.go`). Fabricated snapshot ID passed to `ds.Read()` confirms `lode.ErrNotFound` returned. Exercises the exact code path in `QueryLatestMetrics` where a snapshot could be GC'd between list and read.
> Date: 2026-03-22 | Observer: @pithecene-io | Project: quarry
> Summary: Missing-snapshot sentinel confirmed via integration test exercising Quarry's stale-snapshot-ID recovery path.
> Issue: (private)

### 6. S3 Backend Verification

- [ ] AWS S3 used for Dataset and Volume writes/reads during dogfooding with no adapter errors

> **Evidence:** Quarry uses the S3 adapter targeting Cloudflare R2 (S3-compatible). Dataset writes/reads confirmed working at >100k object scale. No adapter errors observed. Volume not exercised (Dataset-only integration).
> Date: 2026-02-23 | Observer: @pithecene-io | Project: quarry
> Summary: Partial — S3 adapter verified on R2 for Dataset path. Volume and native AWS S3 not yet exercised.
> Issue: (private)

- [ ] Non-AWS backend caveat documented in README and PUBLIC_API.md as known v1.0 limitation

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 7. Known Limitations Accepted

- [ ] Concurrency model documented in README + PUBLIC_API.md + contracts (single-writer when store lacks `ConditionalWriter`; CAS-based optimistic concurrency when available)

> **Evidence:** CAS optimistic concurrency shipped in v0.8.0 (#135). Contracts, PUBLIC_API.md, and README document `ConditionalWriter`, `ErrSnapshotConflict`, and the adapter CAS capability matrix. PR #156 added the adapter CAS capability table to PUBLIC_API.md.
> Date: 2026-02-25 | Observer: @pithecene-io | Project: lode
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
> Date: 2026-02-25 | Observer: @pithecene-io | Project: lode
> Summary: All examples compile and run successfully. `internal/testutil` dependency removed in audit sweep (#157).

- [ ] CHANGELOG includes v1.0 entry summarizing stability commitment

> **Evidence:** _not yet recorded_
> Date: — | Observer: — | Project: —
> Summary: —
> Issue: #___

### 9. Blob File Completeness

- [x] ~~Blob files written alongside structured data within a snapshot are enumerable without prefix-scanning storage~~ Reclassified: out of scope (see below)

> **Evidence:** Reclassified as out-of-scope. Files written via `Store.Put()` directly — bypassing the Dataset write path — are not tracked by manifests by design. The manifest describes files Lode wrote; caller-managed sidecar files are the caller's responsibility. Correct patterns: use `StreamWrite` to write blobs through Lode, or track file inventories in snapshot `Metadata`. See `PUBLIC_API.md` §Sidecar Files and Store Access.
> Date: 2026-03-22 | Observer: @pithecene-io | Project: lode
> Summary: Design analysis confirmed this is a caller-side usage pattern, not a Lode gap. Manifest self-description integrity preserved.
> Issue: —

- [x] ~~Consumers can verify which blob files were successfully persisted for a given commit~~ Reclassified: out of scope (see above)

> **Evidence:** Same reclassification. Blobs written through Lode's write path (`Write`, `StreamWrite`, `StreamWriteRecords`) are tracked in `Manifest.Files` and verifiable. Blobs written outside Lode are outside the manifest's scope.
> Date: 2026-03-22 | Observer: @pithecene-io | Project: lode
> Summary: Completeness verification is available for all files written through Lode's API. Files bypassing the API are the caller's concern.
> Issue: —

### 10. Data Correctness

- [ ] Zero silent data corruption during dogfooding (Dataset or Volume)

> **Evidence:** No corruption observed during ~2 weeks of Quarry production pipeline operation on R2.
> Date: 2026-02-23 | Observer: @pithecene-io | Project: quarry
> Summary: Partial — no corruption observed but formal verification (checksum audit) not yet performed.
> Issue: (private)

- [ ] Zero manifest deserialization failures on same-version data

> **Evidence:** No manifest deserialization failures observed during Quarry dogfooding.
> Date: 2026-02-23 | Observer: @pithecene-io | Project: quarry
> Summary: Partial — no failures observed; systematic audit not yet performed.
> Issue: (private)

- [ ] Zero snapshot history inconsistencies (forked history, missing parent, duplicate ID)

> **Evidence:** Persistent latest pointer (v0.7.3) resolved prior cold-start scan issue. No history inconsistencies observed since.
> Date: 2026-02-23 | Observer: @pithecene-io | Project: quarry
> Summary: Partial — no inconsistencies observed; O(n²) parent resolution and latest-pointer bugs (pre-v0.7.3) could have caused issues in earlier versions.
> Issue: (private)

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

These may become goals for v1.1+ but must not gate the initial stability release.

---

## References

- `PUBLIC_API.md` — public API shape and defaults
- `docs/V1_API_DECISIONS.md` — breaking API candidates held for v1.0
- `docs/contracts/CONTRACT_CORE.md` — core model invariants
- `docs/contracts/CONTRACT_WRITE_API.md` — write API semantics
- `docs/contracts/CONTRACT_READ_API.md` — read API contract
- `docs/contracts/CONTRACT_VOLUME.md` — Volume persistence model
- `docs/contracts/CONTRACT_ERRORS.md` — error taxonomy
- `docs/contracts/CONTRACT_TEST_MATRIX.md` — contract-to-test traceability
- `docs/contracts/CONTRACT_STORAGE.md` — storage adapter obligations

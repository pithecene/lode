# Release Criteria for v1.0

This document defines the **non-negotiable criteria** that must all pass
before Lode receives the v1.0 tag.

**Usage and time** prove stability, not design review.

---

## Gate Rule

- Each criterion in the Criteria section needs recorded evidence and a checkmark before it passes.
- Evidence must reference a real downstream project (or redacted alias), real data, and a real observer.
- No one may waive a criterion. A criterion that cannot pass requires renegotiation and an update to this document.
- Every criterion blocks the v1.0 tag until it passes, except §4: it is the Volume stability gate (`V1_API_DECISIONS.md` D9), which gates Volume leaving experimental, not the v1.0 tag.

---

## Current Assessment (2026-11-25)

- **Releases:** Quarry v0.20.2 pins Lode v0.18.5. Both released on 2026-11-25. Recording these versions does not change the status of any readiness criterion. Native AWS S3 qualification and two weeks of production Dataset snapshot reads remain open.
- **Holds:** The [October audit](dogfood_audit_2026-10.md) detected no corruption and zero manifest decode failures across 96,478 snapshots over 8 months on R2. Manifest format `1.2.0` reads `1.0.0` and `1.1.0` datasets with standard internal names.
- **Blocks (§10):** One history fork happened under CAS + retry on 2026-08-25. v0.11.0 removes its cause ([#208](https://github.com/pithecene/lode/issues/208)), and the criterion stays unmet until production observation covers v0.11.0 or later.
- **Blocks (evidence missing):** No evidence exists yet for native AWS S3 (§6), steady-state snapshot reads (§3), or checksum-backed corruption checks in production (§10).
- **Blocks (pending):** Three items remain open: the owner sign-off on the wire format (§2), the README status (§8), and the v1.0 CHANGELOG entry (§8). §3's "no Dataset API changes" and §1's "option surface has not expanded since v0.9.1" fail as worded.
- **Time-gated:** v0.18.0 released the D12 unification on 2026-11-20 and restarted the sentinel and manifest format (`1.2.0`) freeze windows, which pass on 2026-12-18 absent another reset. v0.18.3 released `WithRecordedComponents()` (lode#419) on 2026-11-21 and restarted the API surface window, which passes on 2026-12-19 (§1, §2).
- **Renegotiated (D9):** Volume is experimental for v1.0. Volume dogfooding (§4) and `volume.Manifest` stability (§2) gate Volume leaving experimental, not the v1.0 tag.
- **Reclassified (§9):** Blob file completeness is out of scope, because the manifest tracks only the files that Lode writes.

---

## Dogfooding Registry

Each downstream project that integrates Lode receives a subsection here.
Use the template to add new projects as they begin integration.

**Privacy rule:** The registry lists only public projects by name. Private projects
use a redacted alias (e.g. `private-A`, `private-B`) and must not reveal
internal project names, repo URLs, domain-specific details, or
implementation specifics that could identify the project or its purpose.
Usage profiles and evidence summaries should describe Lode-level behavior
only (e.g. "Dataset round-trip over S3"), not the consumer's architecture
or data model. Contributors may report aggregate usage counts (snapshots, runs, events,
partitions, bytes): they show scale without revealing purpose.
If a linked issue lives in a private tracker, reference it as `(private)`
instead of by number.

### Quarry

- **Integration date:** 2026-02-10
- **Contact:** @pithecene
- **Usage profile:** Dataset in the audited deployment. Quarry v0.20.2 also writes artifacts as Volumes.
- **Storage backend:** AWS S3 (S3 adapter targeting Cloudflare R2)
- **Status:** Active. Quarry v0.20.2 (2026-11-25) pins Lode v0.18.5. At the audit on 2026-10-19, the production deployment ran Quarry 0.13.4 with Lode v0.9.0 (see private-A).
- **Visibility:** Public
- **Last audited:** 2026-10-19

| Criterion | Validated | Date | Notes |
|-----------|:---------:|------|-------|
| Dataset write round-trip | ✅ | 2026-02-23 | Hive-partitioned writes and reads over S3-compatible backend at >100k object scale |
| Volume write round-trip | N/A | — | The audited deployment writes no Volumes |
| Error sentinels observed | ✅ | 2026-03-22 | `ErrNoSnapshots` in production, `ErrPathExists` and `ErrNotFound` in integration tests (`quarry/lode/sentinel_test.go`) |
| API friction (none = pass) | ⚠️ | 2026-10-19 | Open at the audit: no retention or delete primitive (lode#174) for Quarry's planned `gc`. Quarry's v0.14 run reader pages `Store.List` client-side. |

**Lode APIs exercised (Quarry v0.13.6):** `NewDataset`, `WithHiveLayout`,
`WithCodec(NewJSONLCodec())`, `WithRetryCount`, `WithRetryObserver`,
`Dataset.Write`/`Latest`/`Snapshots`/`Read` (metrics and sidecar queries),
`Store.Put` (sidecar files), S3 adapter, `NewMemory`/`NewFSFactory` (tests),
`ErrNoSnapshots`, `ErrNotFound`, `ErrPathExists`, `ErrSnapshotConflict`.

### private-A

- **Integration date:** 2026-02-10 (indirect: writes through Quarry's embedded Lode)
- **Contact:** @pithecene
- **Usage profile:** Dataset
- **Storage backend:** Cloudflare R2 via the S3 adapter
- **Status:** Active production since 2026-02-07. Runs Quarry 0.13.4 with Lode v0.9.0 (CAS + retry since 2026-03-08).
- **Visibility:** Private (alias under the privacy rule)
- **Last audited:** 2026-10-20: full-history read-only audit, [`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md)

**Scale (audit, 2026-10-20):** 96,478 snapshots over 8 months, 10,858 runs,
4.73M events persisted, 143k referenced data objects (1.78 GB), 32,830
partitions, one dataset on R2.

| Criterion | Validated | Date | Notes |
|-----------|:---------:|------|-------|
| Dataset write round-trip | ⚠️ | 2026-10-20 | Writes show no corruption across all 96,478 snapshots (audit F1). Production does not read through Lode (F8). |
| Volume write round-trip | N/A | — | Dataset-only |
| Error sentinels observed | N/A | — | Sentinels surface inside Quarry, and the quarry entry covers them |
| API friction (none = pass) | ❌ | 2026-10-20 | One history fork under CAS (F3). 48k unreferenced data objects, 2.07 GB (F5, lode#174). Cold listing was unusable at this size (F6), so the consumer couples to the path grammar (F8, `V1_API_DECISIONS.md` D8). |

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

In these criteria, @pithecene observed every recorded evidence entry.

### 1. API Surface Freeze

- [ ] No public type, function, or method signature in the v1.0-promised surface has changed for 4+ weeks after first downstream integration (experimental Volume excluded, D9)
  - **Status:** Time-gated. v0.18.3 released the option `WithRecordedComponents()` (lode#419, requested by Quarry) on 2026-11-21 and restarted the window, which passes on 2026-12-19. lode#436 adds the field `ManifestListOptions.Prefetch` for Panthalassa's raw inventory (panthalassa#1489), so the release that ships it restarts the window again.
  - **Evidence:** v0.18.3 adds the dataset option `WithRecordedComponents()`, which Quarry's zstd and SHA-256 writers (quarry#424) needed. No existing signature changes. (2026-11-21, lode)
  - **Evidence:** lode#436 adds `ManifestListOptions.Prefetch`, a bounded prefetch of manifest loads. No existing signature changes, and the manifest format stays the same. (2026-11-26, lode)
  - **Issue:** lode#175

- [ ] No new sentinel errors added to the v1.0-promised surface for 4+ weeks after first downstream integration (experimental Volume excluded, D9. Freeze window reset: v0.11.0 added `ErrSnapshotExpired` and `ErrHeadUncommitted`)
  - **Status:** Time-gated. v0.18.0 released the D12 sentinels on 2026-11-20 and restarted the window, which passes on 2026-12-18.
  - **Evidence:** v0.18.0 (D12) adds `ErrChecksumMismatch`, `ErrNotPartitioned`, and `ErrRemoved` (shared with `volume.ErrRemoved`). It removes `ErrNoManifests`, `ErrNilIterator`, and `ErrPartitionPrefixNotSupported`. (2026-11-20, lode)
  - **Issue:** none

- [x] Every candidate in `docs/plans/V1_API_DECISIONS.md` carries the mark accepted (and shipped) or rejected, and none stays open
  - **Status:** Met. Every decision through D12 has the status accepted or superseded, and none is open.
  - **Evidence:** D12 has the status accepted and implemented. It unifies the v1.0.0 surface and format `1.2.0` across `lode`, `lode/s3`, and `lode/volume`. (2026-11-18, lode)
  - **Issue:** none

- [x] `PUBLIC_API.md` matches exported symbols in `lode/api.go` with zero discrepancies
  - **Status:** Met. `internal/apidoc` enforces the parity in both directions.
  - **Evidence:** `PUBLIC_API.md` documents every exported top-level identifier of `lode`, `lode/s3`, and `lode/volume` and names no removed identifier. `TestEveryExportIsDocumented`, `TestEveryQualifiedNameExists`, and `TestEveryContractIsIndexedAndTypesExist` check it, updated in lockstep with D12. (2026-11-18, lode)
  - **Issue:** none

- [ ] Option surface of the v1.0-promised API has not expanded since v0.9.1 (experimental Volume options excluded, D9. Freeze window reset: v0.9.1 added WithRetryObserver, and v0.9.0 added WithRetryCount/BaseDelay/MaxDelay/Jitter and ErrOptionNotValidForVolume and removed VolumeOption and WithVolumeChecksum)
  - **Status:** Not met as worded. D12 (v0.18.0) and v0.18.3, which released `WithRecordedComponents()` (lode#419, requested by Quarry) on 2026-11-21, expanded the option surface after v0.9.1. Read as four weeks without another option change, the window passes on 2026-12-19, and that reading needs the renegotiation that the Gate Rule requires. The manifest format is unchanged by it.
  - **Evidence:** v0.18.3 adds one public option, the dataset option `WithRecordedComponents()`. `WithChecksum` keeps its signature, and a writer or view that opens with a checksum without a name now fails. (2026-11-21, lode)
  - **Issue:** none

### 2. Manifest Format Freeze

- [ ] `Manifest` struct fields unchanged for 4+ weeks
  - **Status:** Time-gated. v0.18.0 released manifest format `1.2.0` (D12) on 2026-11-20, so the window passes on 2026-12-18.
  - **Evidence:** Format `1.2.0`, released in v0.18.0, replaces `files` (`[]FileRef`) with `objects` (`[]ObjectRef`) and `paths` (`ManifestPaths`) with `flat` (`bool`). It reads formats `1.0.0` and `1.1.0` in place for standard or flat layouts with standard internal names, and datasets with custom internal names require [migration](../../PUBLIC_API.md#upgrading-a-dataset-with-custom-internal-names). (2026-11-20, lode)
  - **Issue:** none

- [x] ~~`VolumeManifest` struct fields unchanged for 4+ weeks~~ Reclassified: Volume stability gate (`V1_API_DECISIONS.md` D9)
  - **Status:** Reclassified. Volume is experimental for v1.0 (D9), so this criterion gates Volume leaving experimental and stays unmet there because no consumer has documented Volume use with real data.
  - **Evidence:** No consumer has documented downstream Volume data (§4). Persisted Volume manifests stay readable (D8). (2026-10-20, lode)
  - **Issue:** none

- [ ] Owner sign-off on manifest JSON schema as v1.0 wire format
  - **Status:** Not met. The owner decision is pending.
  - **Evidence:** The audit found that the Hive layout writes one byte-identical manifest copy per touched partition (~2.4× objects in production, F2). No production file carries a checksum (F1). (2026-10-20, private-A)
  - **Issue:** none

### 3. Dataset Dogfooding

- [x] At least one consumer writing Dataset snapshots to S3 for 2+ weeks
  - **Status:** Met for about 8 months.
  - **Evidence:** `private-A` (through Quarry) wrote to R2 from 2026-02-07 to 2026-10-10: 96,478 snapshots, 10,858 runs, and 4.73M events ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md)). (2026-10-20, private-A and quarry)
  - **Issue:** none

- [ ] At least one consumer reading Dataset snapshots back (round-trip) for 2+ weeks
  - **Status:** Not met. Production reads data objects by path, which skips snapshots, and Quarry's Lode reads have no evidence of 2+ weeks of production use.
  - **Evidence:** `private-A` reads its data back through Lode's on-disk path grammar, not Lode's read API (audit F8). (2026-10-20, private-A) Quarry v0.20.2 reads Dataset records through `Manifests` and `OpenDecompressed`, and no observer has documented two weeks of Quarry reading Dataset snapshots in production. (2026-11-25, quarry)
  - **Issue:** none

- [x] At least 100 Dataset snapshots committed and read with no corruption
  - **Status:** Met by about 965×.
  - **Evidence:** Every manifest of 96,478 snapshots decoded, and every referenced data file downloaded with 0 missing, 0 size mismatches, 0 unparseable lines, and 0 row-count mismatches ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md) F1). Row counts are checkable for single-file snapshots only. (2026-10-20, private-A)
  - **Issue:** none

- [ ] No Dataset API changes required to support integration
  - **Status:** Not met as worded. The API grew additively to support the integration, so the criterion needs rewording to "no breaking changes" or acceptance that the window resets.
  - **Evidence:** Quarry's zstd and SHA-256 writers (quarry#424) required the dataset option `WithRecordedComponents()` (lode#419, released in v0.18.3), an addition that breaks no caller. (2026-11-21, quarry)
  - **Issue:** lode#174

### 4. Volume Dogfooding (Volume Stability Gate, Not a v1.0 Blocker)

Volume is experimental for v1.0 (`V1_API_DECISIONS.md` D9). These criteria
govern Volume leaving experimental and do not block the v1.0 tag. Quarry v0.20.2
includes a Volume writer, but this record contains no evidence of its use with
real data. The criteria of this section remain unmet.

- [ ] At least one consumer using StageWriteAt + Commit with real data
  - **Status:** Not met. This record holds no qualifying evidence from Volume use with real data.
  - **Evidence:** None recorded.
  - **Issue:** none

- [ ] At least one consumer using ReadAt on committed ranges
  - **Status:** Not met. This record holds no qualifying evidence from Volume use with real data.
  - **Evidence:** None recorded.
  - **Issue:** none

- [ ] At least 3 Volume snapshots committed with cumulative manifests in a real workflow
  - **Status:** Not met. This record holds no qualifying evidence from Volume use with real data.
  - **Evidence:** None recorded.
  - **Issue:** none

- [ ] No Volume API changes required to support integration
  - **Status:** Not met. This record holds no qualifying evidence from Volume use with real data.
  - **Evidence:** None recorded.
  - **Issue:** none

- [ ] Volume writes/reads on AWS S3 during dogfooding with no adapter errors (moved from §6, D9)
  - **Status:** Not met. This record holds no qualifying evidence from Volume use with real data.
  - **Evidence:** None recorded.
  - **Issue:** none

- [ ] Zero silent data corruption in Volume data during dogfooding, verified with checksums (moved from §10, D9)
  - **Status:** Not met. This record holds no qualifying evidence from Volume use with real data.
  - **Evidence:** None recorded.
  - **Issue:** none

- [ ] `VolumeManifest` struct fields unchanged for 4+ weeks with real downstream data (moved from §2, D9)
  - **Status:** Not met. No consumer has documented downstream Volume data.
  - **Evidence:** This record holds no qualifying evidence from Volume use with real data. (2026-10-20, lode)
  - **Issue:** none

### 5. Error Path Validation

- [x] ErrNoSnapshots observed and handled correctly by a downstream consumer
  - **Status:** Met in production.
  - **Evidence:** Quarry handles `ErrNoSnapshots` on cold start against an empty dataset. The latest-pointer scan fallback ran on first access. (2026-02-23, quarry)
  - **Issue:** (private)

- [x] ErrPathExists observed in real storage confirming immutability enforcement
  - **Status:** Met by an integration test on Quarry's production write path.
  - **Evidence:** `TestPutFile_ErrPathExists_DoubleWrite` in `quarry/lode/sentinel_test.go` writes one sidecar file twice through `LodeClient.PutFile()` and receives `lode.ErrPathExists`, classified as a `StorageError`. (2026-03-22, quarry)
  - **Issue:** (private)

- [x] ErrNotFound observed and handled correctly for a missing snapshot lookup
  - **Status:** Met by an integration test on Quarry's stale-snapshot recovery path.
  - **Evidence:** `TestQueryLatestMetrics_ErrNotFound_StaleSnapshotID` in `quarry/lode/sentinel_test.go` passes a fabricated snapshot ID to `ds.Read()` and receives `lode.ErrNotFound`. (2026-03-22, quarry)
  - **Issue:** (private)

### 6. S3 Backend Verification

- [ ] AWS S3 used for Dataset writes/reads during dogfooding with no adapter errors (Volume half moved to §4, D9)
  - **Status:** Not met. Dogfooding exercises only R2, and write failures are unclassified.
  - **Evidence:** Reads showed 0 errors over ~0.5M `GetObject` calls and the full listing during the audit. Quarry recorded `lode_write_failure` at 1.8% of writes since May without classification (F7), so the evidence neither rules adapter errors in nor rules them out. (2026-10-20, private-A)
  - **Issue:** none

- [x] Non-AWS backend caveat documented in README and PUBLIC_API.md as known v1.0 limitation
  - **Status:** Met.
  - **Evidence:** `PUBLIC_API.md` §Other S3-Compatible Services states the consistency and conditional-write requirements, names the services that `TestIntegration_ConditionalWrites` verifies (LocalStack, RustFS) and Cloudflare R2 in production, and states that SeaweedFS requires 4.29 or later. README names the limitation and links to that section. (2026-10-24, lode)
  - **Issue:** none

### 7. Known Limitations Accepted

- [x] Concurrency model documented in README + PUBLIC_API.md + contracts (single-writer when store lacks `ConditionalWriter`, CAS-based optimistic concurrency when available)
  - **Status:** Met for the model of v0.11.0 and later.
  - **Evidence:** `Store` requires `ConditionalWriter`, and `SingleProcess` wraps a backend without a conditional write for writers in one process. `PUBLIC_API.md` §Concurrency and CAS, CONTRACT_STORAGE, CONTRACT_WRITE_API, and CONTRACT_VOLUME state that model, and README links to `PUBLIC_API.md`. (2026-10-24, lode)
  - **Issue:** none

- [x] Context cancellation cleanup nondeterminism documented as best-effort, no correctness impact
  - **Status:** Met.
  - **Evidence:** `PUBLIC_API.md` §Cleanup Behavior states that removal of partial objects is best-effort and timing-dependent, and that an unreferenced object belongs to no snapshot. (2026-10-24, lode)
  - **Issue:** none

- [x] Deferred test gaps (ITER-LIFECYCLE, COMP-WRAPPER-ORDER, ERR-RANGE-NOT-SUPPORTED) accepted as non-blocking
  - **Status:** Met. A test closes one gap, and two remain accepted.
  - **Evidence:** ITER-LIFECYCLE and COMP-WRAPPER-ORDER cover `ObjectIterator` and store composition, which are outside the v1.0 surface. `TestRangeReader_IsOptional/ReaderAt_without_range_reads` closes ERR-RANGE-NOT-SUPPORTED. (2026-10-24, lode)
  - **Issue:** none

### 8. Documentation Completeness

- [ ] README "Status" section updated to v1.0 language
  - **Status:** Not met.
  - **Evidence:** None recorded.
  - **Issue:** none

- [x] All twelve contract files reviewed: no "planned"/"draft"/"future" language for shipped features
  - **Status:** Met.
  - **Evidence:** All twelve contract files in `docs/contracts/` state invariants only and reflect the D12 surface and format `1.2.0`. `TestEveryContractIsIndexedAndTypesExist` in `internal/apidoc` checks that `docs/ARCH_INDEX.md` indexes each contract and that each contract declares only live exported types. (2026-11-18, lode)
  - **Issue:** none

- [x] All examples pass (`task examples`)
  - **Status:** Met.
  - **Evidence:** `task examples` passes all 12 programs it runs: `default_layout`, `hive_layout`, `vessel_positions`, `blob_upload`, `manifest_driven`, `stream_write_records`, `parquet`, `optimistic_concurrency`, `volume_sparse`, `depth_grid`, `vector_artifacts`, and `retention`. (2026-11-21, lode)
  - **Issue:** none

- [ ] CHANGELOG includes v1.0 entry summarizing stability commitment
  - **Status:** Not met.
  - **Evidence:** None recorded.
  - **Issue:** none

### 9. Blob File Completeness

- [x] ~~Blob files written alongside structured data within a snapshot are enumerable without prefix-scanning storage~~ Reclassified: out of scope (see its Status)
  - **Status:** Reclassified out of scope. Manifests list the blobs of committed snapshots, and caller-managed sidecar files are the caller's responsibility.
  - **Evidence:** Files written through `Store.Put()` bypass the Dataset write path. Callers write blobs through `StreamWrite` or track file inventories in snapshot metadata (`PUBLIC_API.md` §Sidecar Files and Store Access). (2026-03-22, lode)
  - **Issue:** none

- [x] ~~Consumers can verify which blob files were successfully persisted for a given commit~~ Reclassified: out of scope (see the enumeration criterion)
  - **Status:** Reclassified out of scope. Manifests list the blobs of committed snapshots, and caller-managed sidecar files are the caller's responsibility.
  - **Evidence:** Each committed snapshot's manifest lists the blobs of that commit, so a caller can enumerate them. A blob whose commit failed, and whose best-effort delete also failed, appears in no committed snapshot's manifest, and blobs written outside Lode are outside its scope. (2026-03-22, lode)
  - **Issue:** none

### 10. Data Correctness

- [ ] Zero silent data corruption in Dataset data during dogfooding (Volume half moved to §4, D9)
  - **Status:** Not met. Size, row-count, and parse checks found no corruption, but those checks cannot rule out silent corruption.
  - **Evidence:** Every referenced data file of 96,478 snapshots matches its manifest size and parses as JSONL ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md) F1). No file carries a checksum. (2026-10-20, private-A)
  - **Issue:** none

- [x] Zero manifest deserialization failures on same-version data
  - **Status:** Met.
  - **Evidence:** 0 of 233,815 manifest objects fail to decode. All are `lode-manifest@1.0.0`, written by releases up to v0.9.0 and read by v0.9.0 ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md) F1). (2026-10-20, private-A)
  - **Issue:** none

- [ ] Zero snapshot history inconsistencies (forked history, missing parent, duplicate ID)
  - **Status:** Not met. One fork happened under v0.9.0 CAS + retry, and v0.11.0 removes its cause, so the criterion needs a clean observation period on v0.11.0 or later.
  - **Evidence:** The audit found 0 duplicate IDs, 2 missing parents (Lode v0.7.3), and 1,113 forks, of which 1,112 predate CAS adoption and one happened under CAS on 2026-08-25 ([`dogfood_audit_2026-10.md`](dogfood_audit_2026-10.md) F3). Lode treated a failed read of a valid head as a stale pointer, and the scan fallback then selected the head's parent, because snapshot ID order is not commit order. (2026-10-20, private-A)
  - **Issue:** [#208](https://github.com/pithecene/lode/issues/208)

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
  v1.0 (`V1_API_DECISIONS.md` D9, and §4 is its own gate)

These may become goals for v1.1+ but must not gate the initial stability release.

---

## References

- `PUBLIC_API.md`: public API shape and defaults
- `docs/plans/V1_API_DECISIONS.md`: breaking API candidates held for v1.0
- `docs/plans/dogfood_audit_2026-10.md`: full-history production audit (evidence for §2, §3, §6, §10)
- `docs/contracts/CONTRACT_CORE.md`: core model invariants
- `docs/contracts/CONTRACT_WRITE_API.md`: write API semantics
- `docs/contracts/CONTRACT_READ_API.md`: read API contract
- `docs/contracts/CONTRACT_VOLUME.md`: Volume persistence model
- `docs/contracts/CONTRACT_ERRORS.md`: error taxonomy
- `docs/contracts/CONTRACT_STORAGE.md`: storage adapter obligations

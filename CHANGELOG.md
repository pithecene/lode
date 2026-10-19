# Changelog

All notable changes to Lode will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

- **Listing filters and cursors**: `PartitionListOptions.Prefix` narrows `ListPartitions` to a partition-path prefix (a storage prefix on Hive layouts); `PartitionListOptions.StartAfter` and `ManifestListOptions.StartAfter` resume a listing after a previous result. ([#175](https://github.com/pithecene-io/lode/issues/175))
- **`ErrPartitionPrefixNotSupported`**: Sentinel for `Prefix` on a layout without partitions.

### Changed

- **Deterministic listing order**: `ListPartitions` returns lexicographic `Path` order and `ListManifests` returns `(Partition, ID)` order on every store. Previously both followed the store's unspecified `List` order, and a multi-partition snapshot's `ManifestRef.Partition` could differ between calls. `Limit` now bounds the result set after sorting rather than stopping enumeration early; store-call bounds are unchanged.

### Fixed

- **Filesystem `List` prefix semantics**: `NewFS` treated the prefix as a directory, so a partial final token (`day=2024-01`) listed nothing while the memory and S3 stores matched it as a string prefix. All stores now agree; `CONTRACT_STORAGE.md` §List states the string-prefix rule.
- **Stream writes no longer block on a fast `Put` failure**: when the store's `Put` returned before draining the stream (for example `ErrPathExists`), `StreamWrite` and `StreamWriteRecords` blocked forever on the pipe. The read end is now closed with `Put`'s error, so `Write`, `Commit`, and `StreamWriteRecords` fail with it instead. When that error is `ErrPathExists`, cleanup leaves the existing object alone: it was written by another writer and data files are immutable. Found by mutation testing.

---

## [0.9.1] - 2026-10-18

### Added

- **Retry observer**: `WithRetryObserver(fn)` registers a callback invoked synchronously once per CAS retry attempt (1-indexed, before backoff) for Dataset and Volume. Enables callers to populate retry metrics such as `lode_write_retry_total`. ([#171](https://github.com/pithecene-io/lode/pull/171), closes [#169](https://github.com/pithecene-io/lode/issues/169))

### Security

- **AWS SDK EventStream DoS**: Bump `github.com/aws/aws-sdk-go-v2/service/s3` to v1.97.3 and `aws/protocol/eventstream` to v1.7.8, the first patched versions for the EventStream decoder panic advisory. Transitive `aws-sdk-go-v2` and `smithy-go` patch bumps follow via MVS. ([#170](https://github.com/pithecene-io/lode/pull/170))

### Upgrade Notes

- **Additive only**: No API removals or behavior changes. `WithRetryObserver` is opt-in and inert unless `WithRetryCount(n > 0)` is configured.
- **No data migration required**: Storage layout and manifest format are unchanged.

---

## [0.9.0] - 2026-03-08

### Added

- **Opt-in CAS retry**: `WithRetryCount(n)` enables automatic bounded retry on `ErrSnapshotConflict` within the commit path for both Dataset and Volume. Data files are written once; only the manifest re-parent and pointer CAS are retried. ([#164](https://github.com/pithecene-io/lode/pull/164), closes [#163](https://github.com/pithecene-io/lode/issues/163))
- **Backoff customization**: `WithRetryBaseDelay(d)`, `WithRetryMaxDelay(d)`, `WithRetryJitter(j)` for full control over jittered exponential backoff. Defaults: 10ms base, 2s max, full jitter.
- **`ErrOptionNotValidForVolume`**: Sentinel error for options that do not apply to Volume construction.

### Changed

- **Unified `Option` interface**: `VolumeOption` function type replaced by the shared `Option` interface (already used by Dataset and DatasetReader). `NewVolume` now takes `...Option`. Options that do not apply to a given constructor return an error at construction time. ([#164](https://github.com/pithecene-io/lode/pull/164))
- **`WithVolumeChecksum` → `WithChecksum`**: Volume block checksums now use the same `WithChecksum(c)` option as Dataset file checksums.

### Breaking Changes

- **`VolumeOption` type removed**: Replace `lode.VolumeOption` with `lode.Option`.
- **`WithVolumeChecksum` removed**: Replace `lode.WithVolumeChecksum(c)` with `lode.WithChecksum(c)`.

### Upgrade Notes

- **Search-and-replace migration**:
  - `lode.VolumeOption` → `lode.Option`
  - `lode.WithVolumeChecksum(` → `lode.WithChecksum(`
- **Retry is opt-in**: Default behavior (0 retries) is unchanged. Existing callers see no difference unless they configure `WithRetryCount`.
- **No data migration required**: Storage layout and manifest format are unchanged.

### References

- [CONTRACT_WRITE_API.md](docs/contracts/CONTRACT_WRITE_API.md) — Automatic retry semantics
- [CONTRACT_VOLUME.md](docs/contracts/CONTRACT_VOLUME.md) — Unified Option interface, Volume retry
- [CONTRACT_ERRORS.md](docs/contracts/CONTRACT_ERRORS.md) — Automatic retry guidance for ErrSnapshotConflict

---

## [0.8.0] - 2026-02-24

### Added

- **CAS optimistic concurrency**: `ConditionalWriter` interface and `CompareAndSwap` method on FS, Memory, and S3 stores. Dataset and Volume commit paths use CAS when available, falling back to Delete+Put for single-writer stores. `ErrSnapshotConflict` sentinel for conflict detection and retry. ([#134](https://github.com/pithecene-io/lode/pull/134), [#135](https://github.com/pithecene-io/lode/pull/135))
- **Optimistic concurrency example**: `examples/optimistic_concurrency/` demonstrates CAS conflict detection and retry pattern. ([#135](https://github.com/pithecene-io/lode/pull/135))
- **Vector artifact pipeline example**: `examples/vector_artifacts/` demonstrates embeddings, indices, and active pointers. ([#138](https://github.com/pithecene-io/lode/pull/138))
- **S3 adapter promoted from experimental**: Documentation, ARCH_INDEX, and README updated. ([#154](https://github.com/pithecene-io/lode/pull/154))
- **AI governance**: `CLAUDE.md` repository constitution and `ai/skills/` repo-local skill definitions. ([#136](https://github.com/pithecene-io/lode/pull/136))
- **Race detection in CI**: `-race` testing added as a parallel CI job. ([#141](https://github.com/pithecene-io/lode/pull/141))
- **Test hardening**: Store coverage (Exists, List, path safety), sentinel error messages, and Parquet type conversion. ([#145](https://github.com/pithecene-io/lode/pull/145), [#146](https://github.com/pithecene-io/lode/pull/146))

### Fixed

- **Windows build**: Extract `fsStore.CompareAndSwap` into build-constrained files. Panic replaced with error return in `buildFieldNode`. Metadata docs corrected to reflect nil coalescing. S3 test cleanup lifecycle fixed. ([#149](https://github.com/pithecene-io/lode/pull/149))
- **Example metadata docs, S3 connectivity detection, `.gitignore` anchoring, and CI race scope**. ([#150](https://github.com/pithecene-io/lode/pull/150))

### Changed

- **Table-driven manifest validation**: `validateVolumeManifest` refactored from sequential checks to validation table. ([#143](https://github.com/pithecene-io/lode/pull/143))
- **StreamWriteRecords cleanup helper**: Deduplicated 4 cleanup sequences into `cleanupStreamWrite()`. ([#144](https://github.com/pithecene-io/lode/pull/144))
- **S3 test context**: Replaced `context.Background()` with `t.Context()`/`b.Context()` in S3 integration test cleanup. ([#142](https://github.com/pithecene-io/lode/pull/142))
- **CI workflows hardened** with explicit least-privilege permissions. ([#151](https://github.com/pithecene-io/lode/pull/151), [#152](https://github.com/pithecene-io/lode/pull/152), [#153](https://github.com/pithecene-io/lode/pull/153))

### Upgrade Notes

- **CAS is always-on**: When the store implements `ConditionalWriter`, CAS is used automatically. No configuration required.
- **Retry on conflict**: Callers that write concurrently should handle `ErrSnapshotConflict` by re-reading `Latest()`, merging state, and re-committing.
- **No migration required**: Existing data and stores work without modification. Stores without `ConditionalWriter` retain the existing Delete+Put single-writer path.

---

## [0.7.4] - 2026-02-12

### Added

- **Complexity bounds contract**: `CONTRACT_COMPLEXITY.md` documents the cost of every public method in store calls, memory, and CPU. Operations are classified as hot-path (per-read/write), cold-path (explicit listing), or degraded (backward-compat fallback). Cost that exceeds the documented bound is a bug. ([#129](https://github.com/pithecene-io/lode/pull/129))
- **Cross-references**: `CONTRACT_WRITE_API.md`, `CONTRACT_READ_API.md`, `CONTRACT_VOLUME.md`, and `CONTRACT_STORAGE.md` now link to `CONTRACT_COMPLEXITY.md` for cost definitions. ([#129](https://github.com/pithecene-io/lode/pull/129))

### Fixed

- **CX-1: Per-read sort check in `findCoveringBlocks`**: Blocks are now sorted once at load time in `validateVolumeManifest` instead of checked on every read. ([#130](https://github.com/pithecene-io/lode/pull/130))
- **CX-3: Double deserialization in `ListPartitions`**: Reduced from 2 Gets per manifest to 1 Get (single-pass extraction). ([#130](https://github.com/pithecene-io/lode/pull/130))
- **CX-6: Redundant sort checks in `Volume.Snapshots`**: Eliminated by the same load-time sort fix as CX-1. ([#130](https://github.com/pithecene-io/lode/pull/130))
- **CX-7: `fsStore.List` used `filepath.Walk`**: Replaced with `filepath.WalkDir` to avoid per-entry `os.Stat` calls. ([#130](https://github.com/pithecene-io/lode/pull/130))

### Changed

- **`fmt.Errorf` → `errors.New`**: 10 instances in `volume.go` where no formatting verbs were used, per AGENTS.md code style rules. ([#131](https://github.com/pithecene-io/lode/pull/131))
- **Extracted `resolveParent` method**: Parent resolution cascade (cache → pointer → scan) moved from inline in `Commit` to a dedicated method for readability. ([#131](https://github.com/pithecene-io/lode/pull/131))
- **Extracted `ensureBlocksSortedByOffset` helper**: Deduplicated the check-then-sort pattern in `validateVolumeManifest`. ([#131](https://github.com/pithecene-io/lode/pull/131))

### Upgrade Notes

- No API changes; all improvements are internal
- No migration required; existing data is compatible without modification
- Safe to upgrade from v0.7.3

### References

- [docs/contracts/CONTRACT_COMPLEXITY.md](docs/contracts/CONTRACT_COMPLEXITY.md) — Complexity bounds contract
- [docs/contracts/CONTRACT_TEST_MATRIX.md](docs/contracts/CONTRACT_TEST_MATRIX.md) — Contract-to-test traceability

---

## [0.7.3] - 2026-02-12

### Fixed

- **O(N) cold-start `Latest()` on remote stores**: `Latest()` previously downloaded every manifest in the dataset to find the most recent snapshot. On Cloudflare R2 with ~5,800 manifests this took ~30 minutes. A persistent `latest` pointer file now provides O(1) resolution: 1 Get (pointer) + 1 Get (manifest). ([#127](https://github.com/pithecene-io/lode/pull/127), closes [#118](https://github.com/pithecene-io/lode/issues/118), [#119](https://github.com/pithecene-io/lode/issues/119))
- **O(N) `Snapshot(ctx, id)` on HiveLayout**: `Snapshot()` scanned all partition directories to find a manifest matching the given ID. A canonical manifest is now written at the non-partitioned path for O(1) lookup, with scan fallback for pre-v0.7.3 data. ([#127](https://github.com/pithecene-io/lode/pull/127), closes [#121](https://github.com/pithecene-io/lode/issues/121))
- **O(N) `latestByScan` downloaded ALL manifests**: Even the scan fallback path loaded every manifest. It now sorts paths lexicographically and loads only the last one: 1 List + 1 Get. ([#127](https://github.com/pithecene-io/lode/pull/127), closes [#122](https://github.com/pithecene-io/lode/issues/122))
- **O(B) `Volume.ReadAt` block lookup**: `findCoveringBlocks` used linear scan over all blocks. Now uses binary search on sorted blocks for O(log B) start-index lookup. Blocks are sorted at commit time via a merge-insert algorithm. ([#127](https://github.com/pithecene-io/lode/pull/127), closes [#123](https://github.com/pithecene-io/lode/issues/123))
- **O(N) `ListManifests` / `ListPartitions`**: Both methods downloaded every manifest to extract IDs and partition paths. Snapshot IDs and partition paths are now extracted from store listing paths. ([#127](https://github.com/pithecene-io/lode/pull/127), closes [#124](https://github.com/pithecene-io/lode/issues/124), [#125](https://github.com/pithecene-io/lode/issues/125))
- **O(N log N) `validateNoOverlaps` on pre-sorted blocks**: Skips redundant sort when blocks are already sorted (the common case after `mergeBlocks`). ([#127](https://github.com/pithecene-io/lode/pull/127), closes [#126](https://github.com/pithecene-io/lode/issues/126))

### Added

- **Persistent latest pointer**: Datasets and Volumes write a `latest` file containing the most recent snapshot ID. Cold-start `Latest()` reads this pointer (1 Get) instead of scanning all manifests. The pointer is written before the manifest (pointer-before-manifest protocol) to ensure cross-process correctness.
- **Pointer-before-manifest protocol**: Pointer write failure aborts the commit — no manifest is written. A pointer ahead of reality is harmless (Exists verification falls through to scan). This eliminates stale-but-existing pointer bugs across process restarts.
- **Canonical manifest for HiveLayout**: `Snapshot(ctx, id)` is now O(1) via a canonical manifest at the non-partitioned path, alongside the partition-specific copies.
- **Binary search for Volume block lookup**: Blocks are stored sorted by offset in the manifest. `findCoveringBlocks` uses `sort.Search` for O(log B) lookup instead of O(B) linear scan.
- **Merge-insert for cumulative blocks**: `mergeBlocks` merges pre-sorted existing blocks with new blocks in O(N + K log K) instead of re-sorting the entire set.
- **Benchmarks**: `BenchmarkFindCoveringBlocks` (O(log B) at 10–10K blocks), `BenchmarkMergeBlocks` (O(N + K log K) at 10–10K existing blocks)

### Upgrade Notes

- **No API changes**: All improvements are internal and transparent
- **No migration required**: Pre-v0.7.3 datasets and volumes work without modification
- **Automatic self-healing**: On first write after upgrade, the `latest` pointer is created. On first `Snapshot(ctx, id)` for HiveLayout, scan fallback works for pre-v0.7.3 data; the canonical manifest is created on the next write.
- **Safe to upgrade from v0.7.2**

### References

- [docs/BENCHMARKS.md](docs/BENCHMARKS.md) — Benchmark inventory and results
- [docs/contracts/CONTRACT_TEST_MATRIX.md](docs/contracts/CONTRACT_TEST_MATRIX.md) — Performance test coverage

---

## [0.7.2] - 2026-02-09

### Changed

- **Go module path**: Renamed from `github.com/justapithecus/lode` to `github.com/pithecene-io/lode` across all source, tests, examples, documentation, and tooling configuration

### Upgrade Notes

- **Import path migration**: Update all import paths in your codebase:
  - `github.com/justapithecus/lode/lode` → `github.com/pithecene-io/lode/lode`
  - `github.com/justapithecus/lode/lode/s3` → `github.com/pithecene-io/lode/lode/s3`
- **go.mod**: Update `require` directive to `github.com/pithecene-io/lode`
- No behavior changes; all APIs, semantics, and contracts are unchanged

---

## [0.7.1] - 2026-02-09

### Fixed

- **O(n²) write degradation on remote stores**: `Write()`, `StreamWrite()`, and `StreamWriteRecords()` called `Latest()` on every invocation, scanning all manifests via `store.List` + N×`store.Get`. On remote stores (S3, R2), this caused sequential writes to degrade quadratically. A 40-write burst that should complete in <1s took ~40 minutes. Parent snapshot ID is now cached after each successful write; only the first write (cold start) falls back to `Latest()`. ([#109](https://github.com/pithecene-io/lode/pull/109), closes [#108](https://github.com/pithecene-io/lode/issues/108))

### Added

- **Sequential write benchmarks**: `BenchmarkDataset_SequentialWrites` (wall-clock with simulated latency) and `BenchmarkDataset_SequentialWrites_StoreCallCount` (correctness assertion) guard against parent-resolution regressions

### Upgrade Notes

- No API changes; transparent internal optimization
- All write paths (`Write`, `StreamWrite`, `StreamWriteRecords`) benefit automatically
- Safe to upgrade from v0.7.0

---

## [0.7.0] - 2026-02-07

### Added

- **Per-File Column Statistics**: New `StatisticalCodec` and `StatisticalStreamEncoder` interfaces allow any codec to report per-file column stats (min, max, null count, distinct count) persisted on `FileRef`
- **Parquet Statistics**: Parquet codec implements `StatisticalCodec`, reporting column-level min/max/null count for all orderable types (int32, int64, float32, float64, string, timestamp)
- **New Public Types**: `FileStats`, `ColumnStats` on the public API surface

### Changed

- **Nil Metadata Coalescing**: `Write`, `StreamWrite`, `StreamWriteRecords`, and `Volume.Commit` now coalesce nil metadata to `Metadata{}` instead of returning an error

### Upgrade Notes

- Callers that previously passed `Metadata{}` solely to avoid nil errors can now pass `nil` safely
- Callers that relied on nil metadata returning an error should remove that expectation
- Per-file stats are opt-in: only codecs implementing `StatisticalCodec` produce them; manifests without stats remain valid

### References

- [CONTRACT_PARQUET.md](docs/contracts/CONTRACT_PARQUET.md) — Updated with per-file statistics spec
- [CONTRACT_WRITE_API.md](docs/contracts/CONTRACT_WRITE_API.md) — Updated with stats collection and nil coalescing semantics
- [CONTRACT_CORE.md](docs/contracts/CONTRACT_CORE.md) — Updated metadata rules

---

## [0.6.0] - 2026-02-07

### Added

- **Volume Persistence Paradigm**: `NewVolume`, `StageWriteAt`, `Commit`, `ReadAt`, `Latest`, `Snapshots`, `Snapshot` — sparse, resumable byte-range persistence as a coequal paradigm alongside Dataset
- **Volume Type Definitions**: `VolumeID`, `VolumeSnapshotID`, `BlockRef`, `VolumeManifest`, `VolumeSnapshot`, `VolumeOption`, `WithVolumeChecksum`
- **Volume Error Sentinels**: `ErrRangeMissing`, `ErrOverlappingBlocks`
- **Volume Example**: `examples/volume_sparse/` demonstrating stage → commit → read, ErrRangeMissing, incremental commits, resume pattern
- **Volume Test Suite**: Comprehensive contract-driven tests covering construction, staging, commit, read, validation, overlap detection, resume, cumulative manifests, and fault injection
- **Volume Contract**: `CONTRACT_VOLUME.md` — normative contract for Volume persistence model
- **Overflow-Safe Arithmetic**: Volume bounds checks use overflow-safe arithmetic to prevent silent wrapping on extreme int64 values

### Changed

- **Dataset Rename Pass (DD-1/DD-7)**: Public API types renamed for clarity now that Volume types coexist:
  - `SnapshotID` → `DatasetSnapshotID`
  - `Snapshot` → `DatasetSnapshot`
  - `Reader` → `DatasetReader`
  - `NewReader` → `NewDatasetReader`
  - `SegmentRef` → `ManifestRef`
  - `SegmentListOptions` → `ManifestListOptions`
  - `ListSegments` → `ListManifests`
  - `ErrOptionNotValidForReader` → `ErrOptionNotValidForDatasetReader`

### Breaking Changes

- **Public API renames**: All types and functions listed above are renamed without compatibility aliases. This is intentional — the renames disambiguate Dataset-specific types from the new Volume type definitions.

### Upgrade Notes

- **Search-and-replace migration**: The renames are mechanical. Apply these replacements across your codebase:
  - `lode.NewReader(` → `lode.NewDatasetReader(`
  - `lode.Reader` → `lode.DatasetReader`
  - `lode.SnapshotID` → `lode.DatasetSnapshotID`
  - `lode.Snapshot` → `lode.DatasetSnapshot`
  - `lode.SegmentRef` → `lode.ManifestRef`
  - `lode.SegmentListOptions` → `lode.ManifestListOptions`
  - `.ListSegments(` → `.ListManifests(`
  - `lode.ErrOptionNotValidForReader` → `lode.ErrOptionNotValidForDatasetReader`
- **No behavior changes intended**: All existing functionality is preserved under the new names.

### References

- [CONTRACT_VOLUME.md](docs/contracts/CONTRACT_VOLUME.md) — Volume persistence contract

---

## [0.5.0] - 2026-02-06

### Added

- **Parquet Codec**: `NewParquetCodec(schema, opts...)` for columnar storage with schema-explicit encoding
- **Parquet Example**: `examples/parquet/` demonstrating schema-typed columnar storage
- **Parquet Types**: `ParquetSchema`, `ParquetField`, and `ParquetType` constants for all primitive types
- **Parquet Compression**: `WithParquetCompression()` option for Snappy/Gzip internal compression
- **Parquet Error Sentinels**: `ErrSchemaViolation` and `ErrInvalidFormat` for precise error handling

### Breaking Changes

- **NewParquetCodec Signature**: Returns `(Codec, error)` instead of `Codec` to enable schema validation at construction time. All callsites must handle the error return.

### Upgrade Notes

- **Parquet codec is non-streaming**: Parquet requires a footer referencing all row groups, so `StreamWriteRecords` returns `ErrCodecNotStreamable`. Use `Dataset.Write` for Parquet encoding.
- **Compression layering**: When using Parquet codec, set Lode's compressor to `NewNoOpCompressor()`. Parquet has internal compression; double compression wastes CPU.
- **Schema validation**: Invalid schemas (bad types, empty names, duplicates) now return errors at construction time rather than encoding time.

### References

- [CONTRACT_PARQUET.md](docs/contracts/CONTRACT_PARQUET.md) — Parquet codec contract
- [CONTRACT_ERRORS.md](docs/contracts/CONTRACT_ERRORS.md) — Updated with Parquet error semantics

---

## [0.4.1] - 2026-02-05

### Changed

- **S3 Backend Compatibility Matrix**: Added documentation for conditional multipart completion support across S3-compatible backends; verified on AWS S3, untested on MinIO/LocalStack/R2

### Fixed

- **S3 Multipart Atomic No-Overwrite**: Large uploads (>5GB) now use conditional completion (`If-None-Match` on `CompleteMultipartUpload`) for atomic no-overwrite guarantee, closing the TOCTOU window that existed in v0.2.0

### Known Limitations

- **S3-compatible backend caveat**: Atomic no-overwrite for large uploads (>5GB) is verified on AWS S3; assumed but untested for other S3-compatible backends (MinIO, LocalStack, R2). If your backend does not support `If-None-Match` on `CompleteMultipartUpload`, large uploads may fail or lose atomicity. See `lode/s3` package docs for the backend support matrix.

### References

- [PUBLIC_API.md](PUBLIC_API.md) — User-facing API documentation
- [docs/contracts/](docs/contracts/) — Normative contract specifications

---

## [0.4.0] - 2026-02-05

### Added

- **Zstd Compressor**: `NewZstdCompressor()` for higher compression ratio and faster decompression than gzip
- **Example Convention Contract**: `CONTRACT_EXAMPLES.md` defines normative callsite conventions
- **Agent Convention Rules**: `AGENTS.md` now includes API/example convention rules with stop triggers

### Changed

- **StreamWriteRecords Parameter Order**: Reordered from `(ctx, metadata, records)` to `(ctx, records, metadata)` for ergonomic consistency with iterator-first patterns
- **Example Variable Naming**: Normalized `snapshot` variable naming across all examples

### Breaking Changes

- **StreamWriteRecords Signature**: Parameter order changed from `StreamWriteRecords(ctx, metadata, records)` to `StreamWriteRecords(ctx, records, metadata)`. Update callsites accordingly.

### Known Limitations

- **G3-4 (Residual Risk)**: Context cancellation cleanup behavior remains nondeterministic due to
  timing characteristics of storage adapters. Deterministic abort paths are tested; context
  cancellation semantics documented as best-effort. See `docs/contracts/CONTRACT_TEST_MATRIX.md` for details.

### Upgrade Notes

- **StreamWriteRecords migration**: Update all `StreamWriteRecords` callsites to use the new parameter order: `ds.StreamWriteRecords(ctx, iter, metadata)` instead of `ds.StreamWriteRecords(ctx, metadata, iter)`

### References

- [PUBLIC_API.md](PUBLIC_API.md) — User-facing API documentation
- [docs/contracts/CONTRACT_EXAMPLES.md](docs/contracts/CONTRACT_EXAMPLES.md) — Example convention contract
- [docs/contracts/](docs/contracts/) — Normative contract specifications

---

## [0.3.0] - 2026-02-04

### Added

- **README Quick Start**: 5-minute guide with blob, records, and streaming examples
- **Write API Decision Table**: Clear guidance for choosing Write/StreamWrite/StreamWriteRecords
- **Guarantees Table**: Explicit commitments vs non-goals in README
- **Gotchas Section**: Common pitfalls documented in README
- **Examples Index**: One-line purpose + run commands for all examples
- **Option Applicability Matrix**: Dataset vs Reader option compatibility in PUBLIC_API.md
- **Sentinel Error Table**: Complete error reference with `errors.Is()` guidance
- **Streaming Constraints Table**: Clear codec/partitioning rules for streaming APIs
- **Contract Cross-Links**: All PUBLIC_API.md claims now traceable to contracts
- **Streaming Failure Tests**: G3-1..G3-3 hardening tests for commit/abort/error semantics

### Changed

- **CHANGELOG Format**: Adopted Keep a Changelog structured format
- **PUBLIC_API.md**: Reorganized with Safety Guarantees section and error handling guidelines

### Fixed

- N/A

### Breaking Changes

- None

### Known Limitations

- **G3-4 (Residual Risk)**: Context cancellation cleanup behavior is nondeterministic due to
  timing characteristics of storage adapters. Deterministic abort paths are tested; context
  cancellation semantics documented as best-effort. See `docs/contracts/CONTRACT_TEST_MATRIX.md` for details.

### Next

Post-v0.3.0 improvements planned:

- **Bootstrap pattern documentation**: "Ensure storage exists before constructing dataset/reader"
  as an explicit callsite pattern in examples and docs.
- **Optional helper utilities**: Bootstrap helpers (e.g., `EnsureStorageExists`) may be added
  outside core write/read semantics, in examples, docs, or a separate helpers package.
- **Markdown snippet CI**: Lint extracted code snippets from README and PUBLIC_API.md.

### Upgrade Notes

- No API changes; documentation and test coverage improvements only
- Safe to upgrade from v0.2.0

### References

- [PUBLIC_API.md](PUBLIC_API.md) — Enhanced with option matrix and error guidance
- [docs/contracts/CONTRACT_TEST_MATRIX.md](docs/contracts/CONTRACT_TEST_MATRIX.md) — Test coverage traceability
- [docs/contracts/](docs/contracts/) — Normative contract specifications

---

## [0.2.0] - 2026-02-03

### Added

- **S3 Adapter**: Now part of public API under `lode/s3`
- **S3 Documentation**: Examples for AWS S3, MinIO, LocalStack, and Cloudflare R2
- **CHANGELOG.md**: Added for release tracking

### Changed

- **License**: Project is now licensed under Apache 2.0
- **README**: Now lists supported backends and license

### Fixed

- N/A

### Breaking Changes

- None

### Known Limitations

- Single-writer semantics required (no concurrent writer conflict resolution)
- Large uploads (>5GB on S3) have TOCTOU window for no-overwrite guarantee
- Cleanup of partial objects is best-effort, not guaranteed

### Upgrade Notes

- S3 adapter moved from `internal/` to `lode/s3`; update import paths if using experimental adapter

### References

- [PUBLIC_API.md](PUBLIC_API.md) — S3 adapter documentation
- [examples/s3/](examples/s3/) — S3 usage examples

---

## [0.1.0] - 2026-02-03

### Added

- **Public API**: Datasets and readers with immutable snapshots, manifests, and explicit metadata
- **Layout System**: Default, Hive, and Flat layouts with dataset enumeration and partition-pruning
- **Storage Adapters**: Filesystem (`NewFSFactory`) and in-memory (`NewMemoryFactory`)
- **S3 Adapter**: Experimental adapter under `internal/` (promoted to public in v0.2.0)
- **Codecs**: JSONL codec for structured records
- **Compression**: Gzip compressor and no-op default
- **Range Reads**: `ReadRange` and `ReaderAt` for partial object access
- **Examples**: Default layout, Hive layout, manifest-driven discovery, blob upload, S3 experimental

### Changed

- N/A (initial release)

### Fixed

- N/A (initial release)

### Breaking Changes

- None (initial release)

### Known Limitations

1. **Single-writer only**: Concurrent writes to the same dataset may corrupt history
2. **No query execution**: Lode structures data; query engines consume it
3. **No background compaction**: Callers control snapshot lifecycle
4. **No automatic cleanup**: Partial objects from failed writes may remain
5. **Manifest-driven only**: Data files without manifests are not discovered

### Upgrade Notes

**Runtime Requirements:**
- Go 1.25 or later

**Storage:**
- Filesystem paths must exist before use; Lode does not create directories
- S3 buckets must exist before use; Lode does not create buckets

### References

- [PUBLIC_API.md](PUBLIC_API.md) — Public API documentation
- [docs/contracts/](docs/contracts/) — Normative contract specifications
- [examples/](examples/) — Runnable usage examples

---

[Unreleased]: https://github.com/pithecene-io/lode/compare/v0.9.1...HEAD
[0.9.1]: https://github.com/pithecene-io/lode/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/pithecene-io/lode/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/pithecene-io/lode/compare/v0.7.4...v0.8.0
[0.7.4]: https://github.com/pithecene-io/lode/compare/v0.7.3...v0.7.4
[0.7.3]: https://github.com/pithecene-io/lode/compare/v0.7.2...v0.7.3
[0.7.2]: https://github.com/pithecene-io/lode/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/pithecene-io/lode/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/pithecene-io/lode/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/pithecene-io/lode/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/pithecene-io/lode/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/pithecene-io/lode/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/pithecene-io/lode/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/pithecene-io/lode/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/pithecene-io/lode/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/pithecene-io/lode/releases/tag/v0.1.0

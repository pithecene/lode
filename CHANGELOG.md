# Changelog

All notable changes to Lode will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Breaking

- **`Store` requires `CompareAndSwap`**: `Store` is now `ObjectStore` (the object operations) together with `ConditionalWriter`, so every commit advances the pointer through CAS. The Delete+Put pointer update for stores without `ConditionalWriter`, which could fork a history under concurrent writers without notice, is removed, and so is the non-atomic tombstone creation. A custom adapter without `CompareAndSwap` no longer compiles as a `Store`: implement `CompareAndSwap`, or, when every writer runs in one process, wrap the adapter's `ObjectStore` with `lode.SingleProcess`. The filesystem store on platforms without a file lock, such as Plan 9, returns an error matching `errors.ErrUnsupported` from `CompareAndSwap`, so commits there fail. `ErrSnapshotConflict` can now be returned by every store.
- **Go 1.27.3 is the minimum Go version**: `go.mod` declares `go 1.27.3`, so a module that requires Lode builds with Go 1.27.3 or later. Go 1.25, the previous minimum, no longer receives security fixes, and 1.27.3 includes the security fixes of 1.27.2 (the `go` command, `crypto/tls`, `html/template`, `net/http`, `net/textproto`, and `os`) and of 1.27.3 (`crypto/x509`). A program rebuilt with it rejects any certificate chain that passes through one of eight blocked eMudhra intermediate certificate authorities (CVE-2026-111943), which affects the S3 adapter only for an endpoint whose certificate chain includes one of them.
- **`Store.Get` must report absence as `ErrNotFound`** (custom store adapters only): CONTRACT_STORAGE formerly accepted "an equivalent error" for a missing path. Lode now relies on `errors.Is(err, ErrNotFound)` to distinguish an absent `latest` pointer or manifest from a failed read, so an adapter that reports absence with any other error causes `Latest` and commits to fail on a dataset without a pointer. The bundled filesystem, memory, and S3 adapters already comply.
- **Go module path**: the module path changes from `github.com/pithecene-io/lode` to `github.com/pithecene/lode`, matching the renamed GitHub organization. Every import must be updated, for example `github.com/pithecene-io/lode/lode` to `github.com/pithecene/lode/lode` and `github.com/pithecene-io/lode/lode/s3` to `github.com/pithecene/lode/lode/s3`, together with the `require` directive in `go.mod`. Releases up to v0.10.0 remain available under the former path. The package API, the on-disk format, and behavior are unchanged.
- **Volume moved to the experimental package `lode/volume`** (`V1_API_DECISIONS.md` D9). Volume is outside the v1.0 compatibility promise until it passes its own stability gate (`V1_READINESS.md` §4); `PUBLIC_API.md` §Compatibility Scope states what the commitment covers. Names omit the prefix that the package name now supplies:

  | Before (`lode`) | After (`volume`) |
  |---|---|
  | `NewVolume` | `New` |
  | `Volume`, `VolumeWriter`, `VolumeReader` | `Volume`, `Writer`, `Reader` |
  | `VolumeID`, `VolumeSnapshotID` | `ID`, `SnapshotID` |
  | `VolumeSnapshot`, `VolumeManifest` | `Snapshot`, `Manifest` |
  | `BlockRef` | `BlockRef` |
  | `ErrRangeMissing`, `ErrOverlappingBlocks` | `ErrRangeMissing`, `ErrOverlappingBlocks` |
  | `lode.WithChecksum`, `lode.WithRetry*` passed to `NewVolume` | `volume.WithChecksum`, `volume.WithRetry*` |

  Volume has its own `Option` type, so Dataset options no longer compile as Volume options, and `ErrOptionNotValidForVolume` has been removed. Behavior, error messages, the shared sentinels (`lode.ErrNotFound`, `ErrNoSnapshots`, `ErrSnapshotConflict`, `ErrInvalidArgument`, `ErrManifestInvalid`), the path grammar in storage, and the manifest JSON are unchanged, so Volumes written by earlier releases are read as before. Migration is mechanical: import `github.com/pithecene/lode/lode/volume` and rename according to the table.

### Added

- **`lode.SingleProcess(store)`**: adds `CompareAndSwap` to a store whose backend offers no conditional write, by serializing calls under a lock held by the process, so that writers in one process that share the returned value commit as they would with CAS. Reads through the returned value wait for a replacement in progress, so they never observe the replaced object as absent. It does not coordinate writers in other processes. A store that already implements `ConditionalWriter` is returned unchanged.
- **Optimistic concurrency for the filesystem store on Windows**: the filesystem store implements `ConditionalWriter` on Windows, locking the pointer's companion `.lock` file with `LockFileEx` as it does with `flock` on Unix, so several processes may write to one dataset or volume there without external coordination. `golang.org/x/sys` (v0.38.0, already in the module graph) is now a direct dependency.
- **Snapshot expiry**: `Dataset.Expire(ctx, ExpireRequest)` removes snapshots selected by `IDs`, by `Partition` (matched by whole segment; Hive layouts only), or by `Before` (the instant in the snapshot ID), and `DryRun` previews the result. The head is never expired and is reported in `ExpireReport.RetainedHead`; snapshots with files outside the selected partition are retained and reported in `Mixed`. Each expiry writes a tombstone first, so `Snapshot` and `Read` on an expired ID return the new `ErrSnapshotExpired` (which also matches `ErrNotFound`), and an interrupted expiry resumes on the next run. The new `ErrHeadUncommitted` reports a pending commit, or a dataset with snapshots but no `latest` pointer (one written before the pointer was introduced); any commit creates the pointer, after which retention proceeds. ([#174](https://github.com/pithecene/lode/issues/174))
- **Sweep of unreferenced objects**: `Dataset.Sweep(ctx, SweepRequest{Grace})` completes interrupted expiries and reclaims the data objects of snapshots that never committed (aborted streams) once their write began more than `Grace` ago, and `DryRun` previews the result. `Bytes` is the sum of the sizes recorded in expiry tombstones; objects without a recorded size are counted in `Unsized`. Reclamation operates per snapshot, never per `Files[]`, and objects outside the layout's data topology (sidecar files, the pointer, tombstones) are never modified. ([#174](https://github.com/pithecene/lode/issues/174))
- **Retention example**: `examples/retention/` expires runs by partition after a dry-run preview, shows the retained head, and runs `Sweep`.
- **`CONTRACT_RETENTION.md` is active**: snapshot expiry and the sweep of unreferenced objects are normative.
- **Security policy**: `SECURITY.md` states the supported versions and the private reporting channels (GitHub private vulnerability reporting, or contact@pithecene.sh), and the README and `LICENSE` name Pithecene LLC as the copyright holder, with that contact address.
- **Guides**: `docs/guides/` explains retention, snapshot IDs, and the storage and commit model (`concepts.md`), and `examples/README.md` indexes the examples.

### Changed

- **The JSON Lines decoder allocates in proportion to the file**: `Decode` formerly allocated a 64 KiB buffer for every file, whatever its size; it now lets the buffer grow from a small start to the longest line, with the same 10 MiB limit on a line. A read of a three-record file allocates 88 percent fewer bytes and takes 42 percent less time, and large files decode in the same time as before. `TestAllocationGates` now bounds the allocations and bytes of each Dataset and Volume hot path, so that an increase fails the test suite (docs/BENCHMARKS.md §Allocation Gates).
- **Writes partition records without copying them**: the partitioner now groups a batch of records itself. An unpartitioned layout returns the batch unchanged instead of appending each record to a new slice, and the Hive partitioner builds each partition key by concatenation instead of `fmt.Sprintf`. A write of three records allocates 4 percent fewer objects on an unpartitioned layout and 9 percent fewer on a Hive layout with two keys, and the Write allocation gate falls from 61 to 58 allocations.
- **Object paths are built in one allocation**: Dataset paths were joined with `path.Join`, which allocates its buffer and then the string; they are now built into a buffer sized in advance, with the same result, which `FuzzJoin` checks against `path.Join`. The history also computes the `latest` pointer path once instead of on every read and write of the pointer. A write of three records allocates 3 fewer objects, a read 1 fewer, and a Volume commit 1 fewer; the Write, Read, StreamWrite, StreamWriteRecords, and Volume Commit allocation gates are lowered accordingly.
- **The S3 adapter issues fewer requests per commit**: `CompareAndSwap` formerly read the `latest` pointer before every conditional write. It now creates the pointer with `If-None-Match: *` without reading it first, and when it replaces a pointer that it last replaced itself, it uses `If-Match` on the ETag that S3 returned for that write, again without reading the pointer. The first commit of a dataset therefore issues five requests instead of six, and each commit after the second through the same store issues three instead of four. The adapter records only the versions that it wrote by replacement, one per pointer, so objects that are created once, such as tombstones, leave no record; each record holds a digest of the content rather than the content, and the adapter holds at most 1,024 records. When the precondition proves wrong (the object exists, or another writer replaced it), the adapter reads the pointer and proceeds as before, so every outcome is unchanged (CONTRACT_STORAGE §ConditionalWriter Capability). `s3.MockS3Client` now derives an object's ETag from its content and returns it from `PutObject`, as S3 does.
- **`s3.MockS3Client` enforces the S3 part-number range**: `UploadPart` now returns an `InvalidArgument` API error for a part number outside 1–10,000, as S3 does. A multipart loop that fails to advance therefore fails promptly instead of accumulating parts without bound; such a mutant formerly exhausted the memory of the nightly mutation runner.
- **Unique snapshot IDs**: Dataset and Volume snapshot IDs now have the form `<instant>-<nonce>`, the Unix nanoseconds at which the write began followed by sixteen random hexadecimal digits, so a clock that repeats or is set back can no longer issue the same ID twice; retention relies on this to avoid acting on a reused ID. Compared as strings, IDs are still ordered by instant. Existing instant-only IDs remain valid; code that parsed an ID as an integer must treat it as an opaque string (CONTRACT_CORE §Snapshot ID).
- **Enumeration tolerates deletion**: `Snapshots`, the scan behind `Latest`, `ListManifests`, and `ListPartitions` omit a manifest deleted between listing and loading instead of failing. The scan behind `Latest` also passes over an expired snapshot and uses another manifest copy when one has been deleted.
- **Shared internals**: the snapshot-history core (the snapshot chain, the latest pointer, and the CAS commit loop) moved to `internal/history`, the retry configuration and backoff to `internal/retry`, the argument and manifest validation errors to `internal/validate`, and the fault-injection test store to `internal/testutil`. `ErrInvalidArgument` and `ErrManifestInvalid` are re-exported from `internal/validate` with the same values, the same static type `error`, and the same messages. Neither the API nor behavior changes; the move allows a second package to share these components in preparation for `V1_API_DECISIONS.md` D9.
- **The mutation gate covers the module**: `task mutate` and the nightly job now run gremlins over the whole module (excluding the examples and `internal/testutil`) rather than over `./lode` alone, so the relocated internals remain gated. `internal/history` has its own unit tests, because gremlins measures coverage per package.
- **Documentation style**: documentation follows *The Chicago Manual of Style* (see `CONTRIBUTING.md`), and the README is condensed to the introduction, Quick Start, core ideas, guarantees, and a map of the documentation.

### Fixed

- **An abandoned Volume commit no longer stalls the volume**: a commit advances the `latest` pointer before it persists its manifest, so a writer that terminated between the two left a pointer to a snapshot without a manifest. A Volume commit must merge its parent's blocks, so every later commit returned `ErrSnapshotConflict` until an operator repaired the pointer. A Volume commit now persists a staged copy of its manifest at `volumes/<volume_id>/staged/<snapshot_id>.json` before it advances the pointer, and removes the copy after it persists the canonical manifest; the next commit loads an abandoned parent from that copy and merges its blocks (CONTRACT_VOLUME §Error Semantics). A warm commit makes five store calls instead of three, and the Commit allocation gate rises from 127 to 139 allocations. `Latest` and `Snapshots` never read staged copies, so an abandoned commit's blocks become visible through the next commit. A pointer to a snapshot that has neither copy, such as one an earlier release abandoned, still returns `ErrSnapshotConflict`. The chaos harness now treats a settling Volume commit that conflicts as a violation.
- **The filesystem store rejects a leading `/` on Windows**: a path or listing prefix that begins with `/` is absolute and returns `ErrInvalidPath`. On Windows, `filepath.IsAbs` treats such a path as relative because it has no volume, so the store accepted it and resolved it beneath the root. Other platforms already rejected it.
- **S3 `CompareAndSwap` reports a concurrently deleted object as a conflict**: when another writer deleted the object between the read and the `If-Match` write, S3 answers with 404, which `CompareAndSwap` returned as a wrapped error. It now returns `ErrSnapshotConflict`, as the `ConditionalWriter` semantics require for an object that does not exist under a non-empty expectation, so the commit retries as after any other conflict.
- **The filesystem store publishes objects atomically**: `Put` created the file at its final path and then wrote its content, so a concurrent reader could open a manifest that existed but was still empty; a Volume commit resolving its parent then failed to decode it. The chaos harness found this with concurrent Volume writers. `Put` now writes under a temporary name and publishes the object with a hard link, which fails if the path exists, and `List` omits temporary names; CONTRACT_STORAGE §Put states that an object is visible only once complete. The filesystem store therefore requires a filesystem that supports hard links. Temporary names begin with the reserved prefix `.lode-tmp-`, and every store, not only the filesystem store, now rejects a key whose final segment begins with it (`ErrInvalidPath`), so that a key behaves alike on every backend.
- **A stream write no longer deletes the data of a visible snapshot**: when a manifest write reported an error after the pointer had advanced, `StreamWrite` and `StreamWriteRecords` deleted their data object, yet the manifest may have been stored (a lost response stores it), leaving a visible snapshot whose data was gone. The chaos harness found this under injected failures. The data object is now deleted only when the commit failed before any manifest write; otherwise it is retained, and `Dataset.Sweep` reclaims it if the commit did fail.
- **S3 integration tests run again**: MinIO no longer publishes its server image, so `task s3:up` could not start the integration services. RustFS 1.0.3, which honors the conditional writes on which the S3 adapter relies and applies `If-Match` atomically under concurrent writers, replaces MinIO beside LocalStack, and `task s3:up` now waits until both services are healthy.
- **History remains linear around a pending commit**: a commit advances the `latest` pointer before it persists its manifest, and the commit is *pending* between those two operations (CONTRACT_CORE §Head Pointer). Parent resolution formerly treated a pointer to a pending commit as stale and adopted the newest persisted manifest as parent, so the pending commit was excluded from the history. A new commit now adopts the snapshot that the pointer names as its parent, whether or not its manifest has been persisted, which preserves the linear history that CONTRACT_WRITE_API guarantees under CAS. Cold parent resolution now requires one `Get` instead of a `Get` and an `Exists`. A Volume commit, which merges its parent's blocks, returns `ErrSnapshotConflict` until the parent's manifest is persisted. If a writer terminates between advancing the pointer and persisting the manifest, the next snapshot's `ParentSnapshotID` names an ID without a manifest.
- **`Latest` no longer reverts the head**: when the `latest` pointer names a snapshot without a manifest, `Latest` still serves the newest committed snapshot from a scan but no longer replaces the pointer. Such a pointer may belong to a pending commit, and replacing it formerly removed that commit from the history; the next commit replaces an obsolete pointer through its CAS. `Latest` still creates a missing pointer from the scan result.
- **A failed read of the head is reported, not treated as absent** ([#208](https://github.com/pithecene/lode/issues/208)): when reading the `latest` pointer or the manifest that it names fails with any error other than not-found, `Latest`, `Write`, and Volume commits now return that error. They formerly treated it as an absent pointer or manifest and selected the largest snapshot ID, which names the head's parent when the head's write began first and committed second. `Latest` thus returned an earlier snapshot as the head, and a commit through a store without `ConditionalWriter` could fork the history; with CAS, the error appeared as `ErrSnapshotConflict`. The single history fork observed in production under CAS arose from such a failed read (`docs/dogfood_audit_2026-10.md` F3). The two preceding entries remove the paths through which that fork occurred; this entry removes the remaining paths.

### Security

- **Dependencies**: upgrade `github.com/klauspost/compress` from v1.18.3 to v1.18.7, which resolves the `s2` integer-overflow and out-of-bounds write advisory (GHSA-259r-337f-4rfw).

---

## [0.10.0] - 2026-10-19

### Added

- **Listing filters and cursors**: `PartitionListOptions.Prefix` narrows `ListPartitions` to a partition-path prefix (a storage prefix on Hive layouts); `PartitionListOptions.StartAfter` and `ManifestListOptions.StartAfter` resume a listing after a previous result. ([#175](https://github.com/pithecene/lode/issues/175))
- **`ErrPartitionPrefixNotSupported`**: Sentinel for `Prefix` on a layout without partitions.
- **`ErrInvalidArgument`**: One sentinel for every caller-supplied value Lode refuses — nil factories, nil layouts or compressors, out-of-range retry options, blob writes without exactly one `[]byte`, Volume offsets, lengths, blocks and commits. Previously these were ad-hoc errors matchable only by message; `errors.Is(err, lode.ErrInvalidArgument)` now classifies them and the message still names the value.
- **`DatasetListOptions.StartAfter`**: `ListDatasets` gains the cursor the other listings have; the last ID from a previous call resumes after it.
- **Mutation testing**: a nightly gremlins job gates `./lode` on test efficacy above 88% and mutant coverage above 95% (gremlins v0.6.0 fails a run that lands exactly on a threshold) (`.gremlins.yaml`; `task mutate` locally). The test hardening it drove is what surfaced the `Put`, stream-write and `ReadRange` fixes below.
- **`docs/V1_API_DECISIONS.md`**: the breaking API candidates held for v1.0 (`Write`/`WriteBlob` split, exported `Layout`, option applicability, opaque `PartitionPath`, optional `RangeReader`, error-prefix rule, the exported S3 mock, on-disk path policy), each recorded once with blast radius and a status. `V1_READINESS.md` §1 requires every one decided before the surface freeze.

### Changed

- **Deterministic listing order**: `ListDatasets` returns lexicographic ID order, `ListPartitions` returns lexicographic `Path` order and `ListManifests` returns `(Partition, ID)` order on every store. Previously both followed the store's unspecified `List` order, and a multi-partition snapshot's `ManifestRef.Partition` could differ between calls. `Limit` now bounds the result set after sorting rather than stopping enumeration early; store-call bounds are unchanged.
- **Argument error messages**: errors that now match `ErrInvalidArgument` read `lode: invalid argument: <value>`; the value text is unchanged, so substring checks keep working. `ErrInvalidPath`'s message is now `invalid path` — it was `invalid path: escapes storage root`, which was wrong for the `ReadRange` bounds and partition-prefix cases that return it.
- **Sentinel homes**: `ErrInvalidPath`, `ErrManifestInvalid` and the `ErrOptionNotValidFor*` sentinels are declared in `api.go` with the others. Identity under `errors.Is` and messages are unchanged.
- **Argument errors before store work**: `NewDataset`, `NewDatasetReader` and `NewVolume` apply and validate their options before calling the store factory, so an out-of-range option value is reported as `ErrInvalidArgument` (and an option that does not apply as its `ErrOptionNotValidFor*` sentinel, as before) even when the factory would fail; `Volume.ReadAt` checks every bound before loading the snapshot.
- **Internal restructuring (no API change)**: Dataset and Volume share one head-pointer protocol (`history`); the three layouts are one path grammar; every store adapter validates keys, prefixes and ranges through `internal/storepath`; the write paths stage into a `pendingSnapshot` and stream through one pipeline type. `dataset.go` is half its size. Behavior, on-disk paths, the public API and benchmarks are unchanged (allocations within +2 per write of the previous release; see `docs/BENCHMARKS.md`). Dataset error contexts are the same words but nest once more on the read path: a malformed canonical manifest makes `Dataset.Snapshot` fail with `lode: failed to get manifest: lode: failed to decode manifest: …` (previously `lode: failed to decode manifest: …`), and a failed compatibility scan reads `lode: failed to get manifest: lode: failed to list snapshots: …` (previously `lode: failed to list for snapshot search: …`). Four Volume error contexts now use the shared wording: `failed to list volume snapshots` → `failed to list snapshots`, `failed to read volume latest pointer` → `failed to read latest pointer`, `failed to load snapshot from pointer` → `failed to load latest snapshot`, `failed to load snapshot from cache` → `failed to load snapshot` (and a parent that cannot be loaded during a commit reads `failed to load parent snapshot`). `failed to write volume manifest`, `failed to marshal volume manifest`, `failed to get volume manifest` and `failed to decode volume manifest` are unchanged. Wrapped causes and `errors.Is` classification are unaffected.

### Fixed

- **Backslashes in keys**: every store rejects a key or prefix containing `\` with `ErrInvalidPath`. Previously the filesystem store rejected `..\x` only on Windows and the memory store only through `filepath`; the rule is now platform-independent and shared (`CONTRACT_STORAGE.md` §Put), and `s3.New` applies it to `Config.Prefix` too.
- **Filesystem `List` prefix semantics**: `NewFS` treated the prefix as a directory, so a partial final token (`day=2024-01`) listed nothing while the memory and S3 stores matched it as a string prefix. All stores now agree; `CONTRACT_STORAGE.md` §List states the string-prefix rule.
- **`Put` is all-or-nothing**: `NewFS` left a partial object behind when the supplied reader failed mid-copy; it now removes what it created. `CONTRACT_STORAGE.md` §Put makes the rule normative for every adapter: on error the path is left as it was, callers never clean up after a failed `Put`, and a path whose `Put` returned `ErrPathExists` belongs to another writer.
- **Stream writes no longer block on a fast `Put` failure**: when the store's `Put` returned before draining the stream (for example `ErrPathExists`), `StreamWrite` and `StreamWriteRecords` blocked forever on the pipe. The read end is now closed with `Put`'s error, so `Write`, `Commit`, and `StreamWriteRecords` fail with it instead. The writer never deletes after a failed `Put` — `Put` is all-or-nothing (above), so there is nothing of its own to remove and a path that returned `ErrPathExists` belongs to another writer. Found by mutation testing.
- **Filesystem `ReadRange` allocation**: `NewFS` allocated the full requested `length` before reading, so a valid range far beyond EOF (which the contract says returns the available bytes) could exhaust memory. The buffer is now bounded by the bytes the file can supply.

### Upgrade Notes

- **Additive API**: `ErrInvalidArgument`, `DatasetListOptions.StartAfter`, `ErrPartitionPrefixNotSupported`, `PartitionListOptions.Prefix`/`StartAfter`, `ManifestListOptions.StartAfter`. No removals; no new options (the option-surface freeze window is unaffected; the sentinel freeze window resets).
- **Message text**: errors classified as `ErrInvalidArgument` now begin `lode: invalid argument:`; `ErrInvalidPath` reads `invalid path`; four Volume read-path contexts dropped the word `volume` and two Dataset read-path failures nest one more context (both listed under Changed). Code matching with `errors.Is` is unaffected; substring matches on the value text still hold.
- **Listing order**: `ListDatasets`, `ListPartitions` and `ListManifests` are now sorted; `Limit` applies after sorting. Callers that depended on store order (undefined by contract) should re-check pagination.
- **Keys with backslashes** are now `ErrInvalidPath` on every store and in `s3.Config.Prefix`.
- **No data migration required**: storage layout and manifest format are unchanged.

---

## [0.9.1] - 2026-10-18

### Added

- **Retry observer**: `WithRetryObserver(fn)` registers a callback invoked synchronously once per CAS retry attempt (1-indexed, before backoff) for Dataset and Volume. Enables callers to populate retry metrics such as `lode_write_retry_total`. ([#171](https://github.com/pithecene/lode/pull/171), closes [#169](https://github.com/pithecene/lode/issues/169))

### Security

- **AWS SDK EventStream DoS**: Bump `github.com/aws/aws-sdk-go-v2/service/s3` to v1.97.3 and `aws/protocol/eventstream` to v1.7.8, the first patched versions for the EventStream decoder panic advisory. Transitive `aws-sdk-go-v2` and `smithy-go` patch bumps follow via MVS. ([#170](https://github.com/pithecene/lode/pull/170))

### Upgrade Notes

- **Additive only**: No API removals or behavior changes. `WithRetryObserver` is opt-in and inert unless `WithRetryCount(n > 0)` is configured.
- **No data migration required**: Storage layout and manifest format are unchanged.

---

## [0.9.0] - 2026-03-08

### Added

- **Opt-in CAS retry**: `WithRetryCount(n)` enables automatic bounded retry on `ErrSnapshotConflict` within the commit path for both Dataset and Volume. Data files are written once; only the manifest re-parent and pointer CAS are retried. ([#164](https://github.com/pithecene/lode/pull/164), closes [#163](https://github.com/pithecene/lode/issues/163))
- **Backoff customization**: `WithRetryBaseDelay(d)`, `WithRetryMaxDelay(d)`, `WithRetryJitter(j)` for full control over jittered exponential backoff. Defaults: 10ms base, 2s max, full jitter.
- **`ErrOptionNotValidForVolume`**: Sentinel error for options that do not apply to Volume construction.

### Changed

- **Unified `Option` interface**: `VolumeOption` function type replaced by the shared `Option` interface (already used by Dataset and DatasetReader). `NewVolume` now takes `...Option`. Options that do not apply to a given constructor return an error at construction time. ([#164](https://github.com/pithecene/lode/pull/164))
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

- **CAS optimistic concurrency**: `ConditionalWriter` interface and `CompareAndSwap` method on FS, Memory, and S3 stores. Dataset and Volume commit paths use CAS when available, falling back to Delete+Put for single-writer stores. `ErrSnapshotConflict` sentinel for conflict detection and retry. ([#134](https://github.com/pithecene/lode/pull/134), [#135](https://github.com/pithecene/lode/pull/135))
- **Optimistic concurrency example**: `examples/optimistic_concurrency/` demonstrates CAS conflict detection and retry pattern. ([#135](https://github.com/pithecene/lode/pull/135))
- **Vector artifact pipeline example**: `examples/vector_artifacts/` demonstrates embeddings, indices, and active pointers. ([#138](https://github.com/pithecene/lode/pull/138))
- **S3 adapter promoted from experimental**: Documentation, ARCH_INDEX, and README updated. ([#154](https://github.com/pithecene/lode/pull/154))
- **AI governance**: `CLAUDE.md` repository constitution and `ai/skills/` repo-local skill definitions. ([#136](https://github.com/pithecene/lode/pull/136))
- **Race detection in CI**: `-race` testing added as a parallel CI job. ([#141](https://github.com/pithecene/lode/pull/141))
- **Test hardening**: Store coverage (Exists, List, path safety), sentinel error messages, and Parquet type conversion. ([#145](https://github.com/pithecene/lode/pull/145), [#146](https://github.com/pithecene/lode/pull/146))

### Fixed

- **Windows build**: Extract `fsStore.CompareAndSwap` into build-constrained files. Panic replaced with error return in `buildFieldNode`. Metadata docs corrected to reflect nil coalescing. S3 test cleanup lifecycle fixed. ([#149](https://github.com/pithecene/lode/pull/149))
- **Example metadata docs, S3 connectivity detection, `.gitignore` anchoring, and CI race scope**. ([#150](https://github.com/pithecene/lode/pull/150))

### Changed

- **Table-driven manifest validation**: `validateVolumeManifest` refactored from sequential checks to validation table. ([#143](https://github.com/pithecene/lode/pull/143))
- **StreamWriteRecords cleanup helper**: Deduplicated 4 cleanup sequences into `cleanupStreamWrite()`. ([#144](https://github.com/pithecene/lode/pull/144))
- **S3 test context**: Replaced `context.Background()` with `t.Context()`/`b.Context()` in S3 integration test cleanup. ([#142](https://github.com/pithecene/lode/pull/142))
- **CI workflows hardened** with explicit least-privilege permissions. ([#151](https://github.com/pithecene/lode/pull/151), [#152](https://github.com/pithecene/lode/pull/152), [#153](https://github.com/pithecene/lode/pull/153))

### Upgrade Notes

- **CAS is always-on**: When the store implements `ConditionalWriter`, CAS is used automatically. No configuration required.
- **Retry on conflict**: Callers that write concurrently should handle `ErrSnapshotConflict` by re-reading `Latest()`, merging state, and re-committing.
- **No migration required**: Existing data and stores work without modification. Stores without `ConditionalWriter` retain the existing Delete+Put single-writer path.

---

## [0.7.4] - 2026-02-12

### Added

- **Complexity bounds contract**: `CONTRACT_COMPLEXITY.md` documents the cost of every public method in store calls, memory, and CPU. Operations are classified as hot-path (per-read/write), cold-path (explicit listing), or degraded (backward-compat fallback). Cost that exceeds the documented bound is a bug. ([#129](https://github.com/pithecene/lode/pull/129))
- **Cross-references**: `CONTRACT_WRITE_API.md`, `CONTRACT_READ_API.md`, `CONTRACT_VOLUME.md`, and `CONTRACT_STORAGE.md` now link to `CONTRACT_COMPLEXITY.md` for cost definitions. ([#129](https://github.com/pithecene/lode/pull/129))

### Fixed

- **CX-1: Per-read sort check in `findCoveringBlocks`**: Blocks are now sorted once at load time in `validateVolumeManifest` instead of checked on every read. ([#130](https://github.com/pithecene/lode/pull/130))
- **CX-3: Double deserialization in `ListPartitions`**: Reduced from 2 Gets per manifest to 1 Get (single-pass extraction). ([#130](https://github.com/pithecene/lode/pull/130))
- **CX-6: Redundant sort checks in `Volume.Snapshots`**: Eliminated by the same load-time sort fix as CX-1. ([#130](https://github.com/pithecene/lode/pull/130))
- **CX-7: `fsStore.List` used `filepath.Walk`**: Replaced with `filepath.WalkDir` to avoid per-entry `os.Stat` calls. ([#130](https://github.com/pithecene/lode/pull/130))

### Changed

- **`fmt.Errorf` → `errors.New`**: 10 instances in `volume.go` where no formatting verbs were used, per AGENTS.md code style rules. ([#131](https://github.com/pithecene/lode/pull/131))
- **Extracted `resolveParent` method**: Parent resolution cascade (cache → pointer → scan) moved from inline in `Commit` to a dedicated method for readability. ([#131](https://github.com/pithecene/lode/pull/131))
- **Extracted `ensureBlocksSortedByOffset` helper**: Deduplicated the check-then-sort pattern in `validateVolumeManifest`. ([#131](https://github.com/pithecene/lode/pull/131))

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

- **O(N) cold-start `Latest()` on remote stores**: `Latest()` previously downloaded every manifest in the dataset to find the most recent snapshot. On Cloudflare R2 with ~5,800 manifests this took ~30 minutes. A persistent `latest` pointer file now provides O(1) resolution: 1 Get (pointer) + 1 Get (manifest). ([#127](https://github.com/pithecene/lode/pull/127), closes [#118](https://github.com/pithecene/lode/issues/118), [#119](https://github.com/pithecene/lode/issues/119))
- **O(N) `Snapshot(ctx, id)` on HiveLayout**: `Snapshot()` scanned all partition directories to find a manifest matching the given ID. A canonical manifest is now written at the non-partitioned path for O(1) lookup, with scan fallback for pre-v0.7.3 data. ([#127](https://github.com/pithecene/lode/pull/127), closes [#121](https://github.com/pithecene/lode/issues/121))
- **O(N) `latestByScan` downloaded ALL manifests**: Even the scan fallback path loaded every manifest. It now sorts paths lexicographically and loads only the last one: 1 List + 1 Get. ([#127](https://github.com/pithecene/lode/pull/127), closes [#122](https://github.com/pithecene/lode/issues/122))
- **O(B) `Volume.ReadAt` block lookup**: `findCoveringBlocks` used linear scan over all blocks. Now uses binary search on sorted blocks for O(log B) start-index lookup. Blocks are sorted at commit time via a merge-insert algorithm. ([#127](https://github.com/pithecene/lode/pull/127), closes [#123](https://github.com/pithecene/lode/issues/123))
- **O(N) `ListManifests` / `ListPartitions`**: Both methods downloaded every manifest to extract IDs and partition paths. Snapshot IDs and partition paths are now extracted from store listing paths. ([#127](https://github.com/pithecene/lode/pull/127), closes [#124](https://github.com/pithecene/lode/issues/124), [#125](https://github.com/pithecene/lode/issues/125))
- **O(N log N) `validateNoOverlaps` on pre-sorted blocks**: Skips redundant sort when blocks are already sorted (the common case after `mergeBlocks`). ([#127](https://github.com/pithecene/lode/pull/127), closes [#126](https://github.com/pithecene/lode/issues/126))

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

- **O(n²) write degradation on remote stores**: `Write()`, `StreamWrite()`, and `StreamWriteRecords()` called `Latest()` on every invocation, scanning all manifests via `store.List` + N×`store.Get`. On remote stores (S3, R2), this caused sequential writes to degrade quadratically. A 40-write burst that should complete in <1s took ~40 minutes. Parent snapshot ID is now cached after each successful write; only the first write (cold start) falls back to `Latest()`. ([#109](https://github.com/pithecene/lode/pull/109), closes [#108](https://github.com/pithecene/lode/issues/108))

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

[Unreleased]: https://github.com/pithecene/lode/compare/v0.10.0...HEAD
[0.10.0]: https://github.com/pithecene/lode/compare/v0.9.1...v0.10.0
[0.9.1]: https://github.com/pithecene/lode/compare/v0.9.0...v0.9.1
[0.9.0]: https://github.com/pithecene/lode/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/pithecene/lode/compare/v0.7.4...v0.8.0
[0.7.4]: https://github.com/pithecene/lode/compare/v0.7.3...v0.7.4
[0.7.3]: https://github.com/pithecene/lode/compare/v0.7.2...v0.7.3
[0.7.2]: https://github.com/pithecene/lode/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/pithecene/lode/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/pithecene/lode/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/pithecene/lode/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/pithecene/lode/compare/v0.4.1...v0.5.0
[0.4.1]: https://github.com/pithecene/lode/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/pithecene/lode/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/pithecene/lode/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/pithecene/lode/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/pithecene/lode/releases/tag/v0.1.0

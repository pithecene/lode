# Changelog

All notable changes to Lode will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Breaking Changes

- **Every writer must share the dataset's settings**: before its first commit, a writer compares its codec and the codec's description, compressor, checksum, partitioner and Hive keys, and path names with those the dataset's head manifest records, and a writer whose settings differ is rejected with the new `ErrSettingsMismatch`. A dataset whose manifests predate this release has its Hive keys verified against its partition paths on the first write.
- **The construction and record surface is replaced**: a root opens datasets, records are typed by their codec, blobs have their own handle, and a reader opens a view that takes the layout, codec name, and compressor from the dataset's manifests. Persisted data is unchanged: a dataset written by an earlier release opens under the default path names.

  | Before | After |
  |---|---|
  | `NewDataset(id, factory, WithCodec(c), ...)` | `Open(factory).Records(ctx, id, c, ...)`, a `Records[T]` |
  | `NewDataset(id, factory)` (no codec) | `Open(factory).Blobs(ctx, id, ...)`, a `Blobs` |
  | `NewDatasetReader(factory)` | `OpenReadOnly(factory).Records(ctx, id, c)` or `.Blobs(ctx, id)`, a view |
  | `ds.Write(ctx, []any{...}, Metadata{...})` | `records.Write(ctx, []T{...}, metadata)` or `blobs.Write(ctx, []byte, metadata)` |
  | `ds.StreamWriteRecords(ctx, RecordIterator, ...)` | `records.StreamWrite(ctx, iter.Seq2[T, error], ...)` |
  | `ds.StreamWrite(ctx, ...)` | `blobs.StreamWrite(ctx, ...)` |
  | `DatasetReader.ListDatasets` | `Root.ListDatasets`, `ReadRoot.ListDatasets` |
  | `NewFSFactory`, `NewMemoryFactory`, `StoreFactory` | `FS`, `Memory`, `Factory[S]`, and `Use(store)` for a store already held |
  | `NewJSONLCodec()`, `NewParquetCodec(schema, ...)` | `JSONL[T]()`, `Parquet(schema, ...)`. An invalid Parquet schema fails when a handle opens |
  | `Codec`, `StreamingRecordCodec`, `RecordStreamEncoder` | `Codec[T]`, `StreamingCodec[T]`, `StreamEncoder[T]` |
  | `StatisticalStreamEncoder` | `StatisticalCodec`, implemented by codecs and by stream encoders |
  | `NewDefaultLayout()`, `NewHiveLayout(keys...)`, `WithHiveLayout(keys...)` | `WithLayout(Unpartitioned())`, `WithLayout(Hive(keys...).By(fn))` |
  | `NewFlatLayout()` | `Open(factory, WithFlatPaths())` |
  | `WithRetryCount` | `WithRetry` |
  | `Option` | `RootOption`, `DatasetOption`, `ViewOption`, and `ComponentOption` (`WithCompressor`, `WithChecksum`) |
  | `Metadata` (`map[string]any`) | any value that encodes as a JSON object, read back with `snap.Metadata.Decode(&v)` |
  | `snap.Manifest.Files` | `snap.Files` (`DatasetSnapshot` embeds `*Manifest`) |
  | `ObjectRef.Dataset` | removed: a handle's `OpenObject` reads within its own dataset |
  | `ErrCodecConfigured`, `ErrOptionNotValidForDataset`, `ErrOptionNotValidForDatasetReader` | removed: these misuses no longer compile |
- **`Codec` gains `Describe`**: a custom codec returns its configuration in a canonical form, or `""` when it has none.
- **Volume takes the new factory type**: `volume.New` takes a `lode.Factory[lode.RangeStore]`, and Volume metadata is `volume.Metadata`.
- **Range reads are an optional capability**: `ReadRange` and `ReaderAt` move from `ObjectStore` to the new `RangeReader` interface. An adapter that cannot read ranges omits them instead of returning `ErrRangeReadNotSupported`, and a handle's `ReaderAt` returns that error for such a store. `RangeStore` is a `Store` that reads ranges: `NewFS` and `NewMemory` return one, `FS`, `Memory`, and `s3.Factory` are factories of one, and `volume.New` takes one, so a store without range reads does not compile there. Code that called `ReadRange` on a `Store` it built holds a `RangeStore` instead.
- **Partition paths are typed**: the `ListManifests` parameter, `PartitionRef.Path`, `ManifestRef.Partition`, and `PartitionListOptions.StartAfter` are a `PartitionPath`, built with `ParsePartitionPath` (which returns `ErrInvalidPath` for a path that is not canonical) or taken from a listing. Pass `lode.PartitionPath{}` where `""` was passed, and call `String()` where a string is needed. A `PartitionPath` encodes as text, and therefore in JSON, as its string, and a listing reports a manifest that names a noncanonical partition as `ErrManifestInvalid`. `PartitionListOptions.Prefix` remains a string.
- **The S3 test double is no longer exported**: `s3.MockS3Client` and `s3.NewMockS3Client` are compiled only into the adapter's tests. A test that needs an in-memory S3 can copy the double or target an emulator such as LocalStack or MinIO.
- **A dataset ID must be one path segment**: an ID that is empty, `.` or `..`, contains `/` or a backslash, or begins with `.lode-tmp-` is rejected with `ErrInvalidArgument`. `=` remains permitted.

### Added

- **Manifest format 1.1.0**: the manifest records `codec_description`, `partition_keys`, and `paths`, so that every setting a writer must share is persisted.
- **Path names**: root options name the directories and files within a dataset's paths: `WithDatasetsDir`, `WithSnapshotsDir`, `WithPartitionsDir`, `WithSegmentsDir`, `WithManifestFile`, `WithDataDir`, `WithExpiredDir`, `WithStagedDir`, and `WithFlatPaths`. A root without them uses the default names.
- **S3 factory**: `s3.Factory` returns the factory of an S3 store, for `Open`.
- **`ErrReadOnly`**: reports a write through a read-only root's store.
- **Settings checked at open**: opening a writer checks its settings against the dataset's head, so a misconfigured writer fails when it opens rather than at its first commit.
- **`Sweep` restores partition indexes**: on a Hive dataset, a writer that terminated after its commit was complete but before persisting its partition manifests left a snapshot that listings filtered by partition, and `Expire` by `Partition`, could not find. `Sweep` now copies the canonical manifest to each missing partition path once the snapshot is older than the grace period, and reports it in the new `SweepReport.Reindexed`.
- **Staged first manifest**: a dataset's first commit stages its manifest at `<dataset dir>/staged/<snapshot_id>.json` before it advances the pointer and removes it afterward, so that a writer can check its settings against the commit while it is pending.

### Changed

- **Error messages name their source once**: a message begins with the prefix of the component in which the failure originated, and code that wraps a Lode error adds context without repeating a prefix, so a message no longer reads `lode: failed to write data: lode: …`. The filesystem store reports as `fs:` and the in-memory store as `memory:`, and the messages of `ErrSchemaViolation` and `ErrInvalidFormat` no longer begin with `parquet:`. Matching under `errors.Is` and `errors.As` is unchanged. Code that matches message text may need to change (CONTRACT_ERRORS §Error Messages).

### Upgrade Notes

- **Construction and records**: replace each name according to the table under Breaking Changes.
- **Writer settings**: open every writer of a dataset with the codec, compressor, checksum, partitioner, Hive keys, and path names that the dataset's head manifest records. A writer with other settings fails with `ErrSettingsMismatch`.
- **Custom codecs**: implement `Describe`, which returns the codec's configuration in a canonical form, or `""` when the codec has none.
- **Custom store adapters**: move `ReadRange` and `ReaderAt` to the `RangeReader` interface, or omit them when the backend cannot read ranges.
- **Volume**: pass a `lode.Factory[lode.RangeStore]` to `volume.New`, and use `volume.Metadata` for Volume metadata.
- **Partition paths**: pass `lode.PartitionPath{}` where `""` was passed, build a path with `ParsePartitionPath`, and call `String()` where a string is needed.
- **Dataset IDs**: use an ID of one path segment that does not begin with `.lode-tmp-`.
- **Tests that used the S3 double**: copy the double from the adapter's tests, or target an emulator such as LocalStack or MinIO.
- **Error text**: check code that matches message text against the new wording.
- **No data migration required**: a dataset written by an earlier release opens under the default path names.

### Fixed

- **Unfiltered Hive listings report every committed snapshot**: `ListManifests` and `ListPartitions` without a partition filter omitted a snapshot that had no partition manifest, either because its writer terminated after the commit was complete or because it had no files. Such a snapshot is now reported through its canonical manifest, under the dataset root. A listing filtered by partition still reflects only the partition manifests that exist.

---

## [0.11.0] - 2026-10-22

### Breaking Changes

- **Every `Store` must implement `CompareAndSwap`**: `Store` now combines `ObjectStore` with `ConditionalWriter`, and the Delete+Put pointer update, which could fork a history under concurrent writers, is removed. An adapter without a conditional write can be wrapped with `lode.SingleProcess` when every writer runs in one process and uses the same wrapped store. On platforms without file locks, such as Plan 9, the filesystem store's `CompareAndSwap` returns an error that matches `errors.ErrUnsupported`.
- **`Store.Get` must return `ErrNotFound` for a missing path**: Lode now distinguishes an absent object from a failed read. The bundled adapters already comply.
- **Go 1.27.3 is the minimum Go version**: Go 1.25 no longer receives security fixes.
- **The module path is `github.com/pithecene/lode`**: it replaces `github.com/pithecene-io/lode`, under which releases up to v0.10.0 remain available.
- **Volume moved to the experimental package `lode/volume`**: Volume is outside the v1.0 compatibility promise, and its names drop the prefix that the package name supplies.

  | Before (`lode`) | After (`volume`) |
  |---|---|
  | `NewVolume` | `New` |
  | `Volume`, `VolumeWriter`, `VolumeReader` | `Volume`, `Writer`, `Reader` |
  | `VolumeID`, `VolumeSnapshotID` | `ID`, `SnapshotID` |
  | `VolumeSnapshot`, `VolumeManifest` | `Snapshot`, `Manifest` |
  | `BlockRef`, `ErrRangeMissing`, `ErrOverlappingBlocks` | unchanged |
  | `lode.WithChecksum`, `lode.WithRetry*` | `volume.WithChecksum`, `volume.WithRetry*` |

  Volume has its own `Option` type, and `ErrOptionNotValidForVolume` is removed.

### Added

- **Snapshot expiry**: `Dataset.Expire` removes the snapshots selected by ID, by partition (on Hive layouts), or by age, and `DryRun` previews the result. The head is never expired, and a snapshot with files outside the selected partition is retained and reported in `ExpireReport.Mixed`. Reading an expired snapshot returns the new `ErrSnapshotExpired`, which also matches `ErrNotFound`. The new `ErrHeadUncommitted` reports that expiry cannot proceed because the head is uncommitted or because the dataset has snapshots but no `latest` pointer. Any commit creates the pointer. ([#174](https://github.com/pithecene/lode/issues/174))
- **Sweep of unreferenced objects**: `Dataset.Sweep` completes interrupted expiries and reclaims the data of snapshots that never committed once they are older than a grace period. ([#174](https://github.com/pithecene/lode/issues/174))
- **`lode.SingleProcess`**: adds `CompareAndSwap` to a store without a conditional write by serializing its calls under a lock in the process. Writers are serialized only if they use the same returned store, and writers in other processes are not coordinated.
- **Concurrent writers on Windows**: the filesystem store implements `CompareAndSwap` on Windows, so several processes may write to one dataset or volume.
- **Documentation**: `CONTRACT_RETENTION.md`, the guides in `docs/guides/`, the example `examples/retention/`, and a security policy in `SECURITY.md`.

### Changed

- **Fewer S3 requests per commit**: the first commit of a dataset issues five requests instead of six, and each commit after the second through the same store issues three instead of four.
- **Fewer allocations**: the JSON Lines decoder no longer allocates 64 KiB for every file, so a read of a small file allocates 88 percent fewer bytes, and writes and reads allocate fewer objects.
- **Snapshot IDs include a random suffix**: an ID has the form `<instant>-<nonce>`, so a clock that repeats can no longer issue the same ID twice. IDs remain ordered by instant, and earlier IDs remain valid.
- **Listings tolerate concurrent deletion**: `Snapshots`, `Latest`, `ListManifests`, and `ListPartitions` omit a manifest that is deleted during the listing instead of failing.
- **`s3.MockS3Client` behaves more like S3**: it returns an ETag derived from the content and rejects part numbers outside 1–10,000.

### Fixed

- **A history no longer forks around an uncommitted head** ([#208](https://github.com/pithecene/lode/issues/208)): a commit adopts the snapshot that the `latest` pointer names as its parent even before that snapshot's manifest is persisted, `Latest` no longer replaces the pointer, and a failed read of the head is returned instead of being treated as absent. These paths produced the one fork observed in production under CAS.
- **An abandoned Volume commit no longer stalls the volume**: a commit persists a staged copy of its manifest before it advances the pointer, so the next commit can adopt a parent whose writer terminated. Each commit makes two more store calls, to persist and then remove the staged copy.
- **The filesystem store publishes objects atomically**: a concurrent reader can no longer open an empty object. The filesystem store now requires hard links, and every store rejects a key whose final segment begins with `.lode-tmp-`.
- **A stream write retains the data of a snapshot that may be visible**: after a manifest write fails, the data object is deleted only if no manifest can have been stored. If the commit did fail, `Dataset.Sweep` reclaims the retained object. If the manifest was stored, the snapshot is live and its data remains.
- **S3 `CompareAndSwap` reports a concurrently deleted object as a conflict**: it returns `ErrSnapshotConflict`, which a commit configured with `WithRetryCount` retries.
- **The filesystem store rejects a path that begins with `/` on Windows**: it returns `ErrInvalidPath`, as on other platforms.

### Security

- **Dependencies**: `github.com/klauspost/compress` is upgraded to v1.18.7, which resolves GHSA-259r-337f-4rfw.
- **Go 1.27.3**: a program rebuilt with it includes the standard-library security fixes of Go 1.27.2 and 1.27.3.

### Upgrade Notes

- **Module path**: replace `github.com/pithecene-io/lode` with `github.com/pithecene/lode` in imports and in `go.mod`.
- **Custom store adapters**: implement `CompareAndSwap`, or wrap it with `lode.SingleProcess` when every writer runs in one process and uses the same wrapped store, and return `ErrNotFound` from `Get` for a missing path.
- **Volume**: import `github.com/pithecene/lode/lode/volume` and rename according to the table above.
- **Snapshot IDs**: code that parsed an ID as an integer must treat it as an opaque string.
- **No data migration required**: the storage layout and the manifest format are unchanged.

---

## [0.10.0] - 2026-10-19

### Added

- **Listing filters and cursors**: `PartitionListOptions.Prefix` narrows `ListPartitions` to a partition-path prefix, and `StartAfter` on `DatasetListOptions`, `PartitionListOptions`, and `ManifestListOptions` resumes a listing after a previous result. The new `ErrPartitionPrefixNotSupported` reports `Prefix` on a layout without partitions. ([#175](https://github.com/pithecene/lode/issues/175))
- **`ErrInvalidArgument`**: every caller-supplied value that Lode refuses, such as a nil factory, an out-of-range retry option, or a blob write without exactly one `[]byte`, returns an error that matches `ErrInvalidArgument`, and the message still names the value. Constructors validate their options before they call the store factory.

### Changed

- **Listings are sorted**: on every store, `ListDatasets` returns IDs in lexicographic order, `ListPartitions` returns paths in lexicographic order, and `ListManifests` returns references ordered by partition and then by ID. `Limit` applies after sorting.
- **Error messages**: errors that match `ErrInvalidArgument` begin `lode: invalid argument:`, and `ErrInvalidPath` reads `invalid path`. Two Dataset read failures add one level of context, and four Volume read failures omit the word `volume`, so that `failed to list volume snapshots` becomes `failed to list snapshots`. Classification with `errors.Is` is unchanged.

### Fixed

- **Backslashes in keys**: every store rejects a key or prefix that contains `\` with `ErrInvalidPath` on every platform, and `s3.New` applies the rule to `Config.Prefix`.
- **Filesystem listing by prefix**: the filesystem store treated a prefix as a directory, so a partial final segment such as `day=2024-01` listed nothing. It now matches a string prefix, as the other stores do.
- **A failed `Put` leaves no partial object**: the filesystem store left a partial object when the reader failed during the copy. It now removes the object.
- **Stream writes no longer hang on a failed `Put`**: when a store's `Put` failed before it read the whole stream, `StreamWrite` and `StreamWriteRecords` blocked indefinitely. They now return the error.
- **Filesystem `ReadRange` allocation**: a range that extends far beyond the end of a file no longer allocates the full requested length.

### Upgrade Notes

- **Error text**: code that matches error messages should be checked against the new wording. Matching with `errors.Is` is unaffected.
- **Pagination**: code that relied on the store's listing order should be checked, because listings are now sorted and `Limit` applies after sorting.
- **Backslashes**: keys and prefixes that contain `\` now return `ErrInvalidPath`.
- **No data migration required**: the storage layout and the manifest format are unchanged.

---

## [0.9.1] - 2026-10-18

### Added

- **Retry observer**: `WithRetryObserver(fn)` registers a function that Lode calls once for each commit retry, before the backoff, on Datasets and Volumes, for example to count retries in a metric. ([#171](https://github.com/pithecene/lode/pull/171), closes [#169](https://github.com/pithecene/lode/issues/169))

### Security

- **AWS SDK**: `github.com/aws/aws-sdk-go-v2/service/s3` is upgraded to v1.97.3 and `aws/protocol/eventstream` to v1.7.8, which resolve a denial-of-service advisory in the EventStream decoder. ([#170](https://github.com/pithecene/lode/pull/170))

### Upgrade Notes

- **No action required**: `WithRetryObserver` takes effect only when `WithRetryCount` is greater than zero, and the storage layout and the manifest format are unchanged.

---

## [0.9.0] - 2026-03-08

### Breaking Changes

- **`VolumeOption` is removed**: `NewVolume` takes the `Option` type that `NewDataset` and `NewDatasetReader` take, and an option that does not apply to a Volume returns the new `ErrOptionNotValidForVolume`. ([#164](https://github.com/pithecene/lode/pull/164))
- **`WithVolumeChecksum` is removed**: a Volume takes `WithChecksum`, as a Dataset does.

### Added

- **Retry on conflict**: `WithRetryCount(n)` retries a commit that fails with `ErrSnapshotConflict` up to `n` times, on Datasets and Volumes. Data files are written once. Only the manifest and the pointer update are retried. `WithRetryBaseDelay`, `WithRetryMaxDelay`, and `WithRetryJitter` configure the exponential backoff, whose defaults are a base delay of 10 ms, a maximum of 2 s, and full jitter. ([#164](https://github.com/pithecene/lode/pull/164), closes [#163](https://github.com/pithecene/lode/issues/163))

### Upgrade Notes

- **Renames**: replace `lode.VolumeOption` with `lode.Option` and `lode.WithVolumeChecksum` with `lode.WithChecksum`.
- **Retry is opt-in**: without `WithRetryCount`, a commit is not retried, as before.
- **No data migration required**: the storage layout and the manifest format are unchanged.

---

## [0.8.0] - 2026-02-24

### Added

- **Optimistic concurrency**: the new `ConditionalWriter` interface provides `CompareAndSwap`, which the memory and S3 stores implement, and the filesystem store implements on Unix. A Dataset or Volume commit advances the `latest` pointer through it when the store implements it, so a concurrent commit fails with the new `ErrSnapshotConflict` instead of replacing the head. Other stores update the pointer with `Delete` and `Put`, which supports one writer. ([#134](https://github.com/pithecene/lode/pull/134), [#135](https://github.com/pithecene/lode/pull/135))
- **Examples**: `examples/optimistic_concurrency/` shows conflict detection and retry, and `examples/vector_artifacts/` shows embeddings, indices, and active pointers. ([#135](https://github.com/pithecene/lode/pull/135), [#138](https://github.com/pithecene/lode/pull/138))

### Changed

- **The S3 adapter is no longer experimental**. ([#154](https://github.com/pithecene/lode/pull/154))

### Fixed

- **Windows builds**: the module builds on Windows, where the filesystem store does not implement `CompareAndSwap`. ([#149](https://github.com/pithecene/lode/pull/149))

### Upgrade Notes

- **No configuration required**: a commit uses `CompareAndSwap` whenever the store implements `ConditionalWriter`.
- **Concurrent writers**: handle `ErrSnapshotConflict` by reading `Latest`, merging, and committing again.
- **No data migration required**: the storage layout and the manifest format are unchanged.

---

## [0.7.4] - 2026-02-12

### Changed

- **Faster Volume reads and partition listings**: a Volume sorts its blocks once, when it loads a manifest, instead of on every read, `ListPartitions` reads each manifest once instead of twice, and the filesystem store lists without a `Stat` call per entry. ([#130](https://github.com/pithecene/lode/pull/130))
- **Documented costs**: `CONTRACT_COMPLEXITY.md` states the store calls, memory, and CPU of every public operation. ([#129](https://github.com/pithecene/lode/pull/129))

### Upgrade Notes

- **No action required**: the API, the storage layout, and the manifest format are unchanged.

---

## [0.7.3] - 2026-02-12

### Fixed

- **`Latest` no longer reads every manifest**: Datasets and Volumes persist a `latest` pointer that names the newest snapshot, so `Latest` makes two `Get` calls on a cold start. It formerly read every manifest, which took about thirty minutes for 5,800 manifests on Cloudflare R2. Without a pointer, `Latest` lists the manifests and reads only the newest. ([#127](https://github.com/pithecene/lode/pull/127), closes [#118](https://github.com/pithecene/lode/issues/118), [#119](https://github.com/pithecene/lode/issues/119), [#122](https://github.com/pithecene/lode/issues/122))
- **`Snapshot` no longer scans partitions on Hive layouts**: each commit also persists the manifest at a path outside the partitions, so `Snapshot` reads one object. Snapshots written before 0.7.3 are found by a scan. ([#127](https://github.com/pithecene/lode/pull/127), closes [#121](https://github.com/pithecene/lode/issues/121))
- **`ListManifests` and `ListPartitions` no longer read manifests**: they derive snapshot IDs and partition paths from the listed paths. ([#127](https://github.com/pithecene/lode/pull/127), closes [#124](https://github.com/pithecene/lode/issues/124), [#125](https://github.com/pithecene/lode/issues/125))
- **Faster Volume reads and commits**: `ReadAt` finds blocks by binary search, and `Commit` merges new blocks into the sorted blocks instead of sorting them all. ([#127](https://github.com/pithecene/lode/pull/127), closes [#123](https://github.com/pithecene/lode/issues/123), [#126](https://github.com/pithecene/lode/issues/126))

### Upgrade Notes

- **No action required**: datasets and volumes written by earlier releases are read as before. The first commit after the upgrade creates the `latest` pointer.

---

## [0.7.2] - 2026-02-09

### Breaking Changes

- **The module path is `github.com/pithecene-io/lode`**: it replaces `github.com/justapithecus/lode`.

### Upgrade Notes

- **Module path**: replace `github.com/justapithecus/lode` with `github.com/pithecene-io/lode` in imports and in `go.mod`. The API and behavior are unchanged.

---

## [0.7.1] - 2026-02-09

### Fixed

- **Sequential writes no longer slow down as a dataset grows**: `Write`, `StreamWrite`, and `StreamWriteRecords` read every manifest to find the parent snapshot on every call, so on a remote store each write took longer than the last, and forty writes took about forty minutes. A dataset now retains its last commit, so only its first write looks up the parent. ([#109](https://github.com/pithecene/lode/pull/109), closes [#108](https://github.com/pithecene/lode/issues/108))

### Upgrade Notes

- **No action required**: the API, the storage layout, and the manifest format are unchanged.

---

## [0.7.0] - 2026-02-07

### Added

- **Per-file column statistics**: a codec that implements the new `StatisticalCodec` or `StatisticalStreamEncoder` interface reports statistics for each file, which are persisted on its `FileRef` as the new `FileStats` and `ColumnStats` types: minimum, maximum, null count, and distinct count. The Parquet codec reports the minimum, maximum, and null count of every orderable column.

### Changed

- **Nil metadata is accepted**: `Write`, `StreamWrite`, `StreamWriteRecords`, and `Volume.Commit` treat nil metadata as empty instead of returning an error.

### Upgrade Notes

- **Nil metadata**: code that expected an error for nil metadata no longer receives one.
- **Statistics are optional**: manifests without statistics remain valid.

---

## [0.6.0] - 2026-02-07

### Breaking Changes

- **Dataset types are renamed**: the new names distinguish them from the Volume types, and no aliases are provided.

  | Before | After |
  |---|---|
  | `SnapshotID`, `Snapshot` | `DatasetSnapshotID`, `DatasetSnapshot` |
  | `Reader`, `NewReader` | `DatasetReader`, `NewDatasetReader` |
  | `SegmentRef`, `SegmentListOptions`, `ListSegments` | `ManifestRef`, `ManifestListOptions`, `ListManifests` |
  | `ErrOptionNotValidForReader` | `ErrOptionNotValidForDatasetReader` |

### Added

- **Volume**: a Volume persists sparse byte ranges of one object of fixed length. `NewVolume` creates one. `StageWriteAt` writes a block, `Commit` publishes staged blocks as a snapshot, and `ReadAt`, `Latest`, `Snapshots`, and `Snapshot` read them. `ReadAt` returns the new `ErrRangeMissing` for a range that committed blocks do not fully cover, and `Commit` returns `ErrOverlappingBlocks` for blocks that overlap. `CONTRACT_VOLUME.md` specifies the model, and `examples/volume_sparse/` demonstrates it.

### Upgrade Notes

- **Renames**: replace each name according to the table. Behavior is unchanged.

---

## [0.5.0] - 2026-02-06

### Added

- **Parquet codec**: `NewParquetCodec(schema, opts...)` encodes records as Parquet according to a `ParquetSchema` of `ParquetField` values, and returns an error for an invalid schema. `WithParquetCompression` selects Snappy, the default, Gzip, or no compression within the file. A record that lacks a required field or holds a value of the wrong type returns the new `ErrSchemaViolation`, and a malformed file returns `ErrInvalidFormat`. Fields that the schema does not name are ignored. `examples/parquet/` demonstrates the codec.

### Upgrade Notes

- **Parquet does not stream**: `StreamWriteRecords` returns `ErrCodecNotStreamable` with the Parquet codec, because a Parquet file ends with a footer that references every row group. `Write` encodes Parquet.
- **Compression**: with the Parquet codec, configure `NewNoOpCompressor()`, because Parquet compresses within the file.

---

## [0.4.1] - 2026-02-05

### Fixed

- **Large S3 uploads are atomic**: an upload larger than 5 GB completes with `If-None-Match`, so on a backend that honors it, the upload can no longer replace an object that a concurrent writer created after the existence check.

### Known Limitations

- **Other S3-compatible backends**: the conditional completion is verified on AWS S3 only. A backend that does not honor `If-None-Match` on `CompleteMultipartUpload` may reject large uploads or lose their atomicity. The `lode/s3` package documentation lists backend support.

---

## [0.4.0] - 2026-02-05

### Breaking Changes

- **`StreamWriteRecords` takes the records before the metadata**: the signature is `StreamWriteRecords(ctx, records, metadata)`.

### Added

- **Zstandard compression**: `NewZstdCompressor()`.

### Known Limitations

- **Cleanup after cancellation**: when the context of a stream write is canceled, removal of the partial object is best-effort.

### Upgrade Notes

- **Argument order**: exchange the last two arguments of every `StreamWriteRecords` call.

---

## [0.3.0] - 2026-02-04

### Breaking Changes

- **`Dataset` and `s3.API` gain methods**: `Dataset` adds `StreamWrite` and `StreamWriteRecords`, and `s3.API` adds `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, and `AbortMultipartUpload`, so a type that implements either interface must add them.

### Added

- **Streaming writes**: `Dataset.StreamWrite(ctx, metadata)` returns a `StreamWriter` that streams one blob into storage, and `Dataset.StreamWriteRecords(ctx, metadata, records)` encodes the records of a `RecordIterator` as they arrive, so the Dataset holds neither payload whole in memory. The memory store still keeps each object in memory. Streaming records requires a codec that implements `StreamingRecordCodec` and a layout without partitioning. The new sentinels `ErrCodecConfigured`, `ErrCodecNotStreamable`, `ErrNilIterator`, and `ErrPartitioningNotSupported` report misuse. ([#48](https://github.com/pithecene/lode/pull/48), [#49](https://github.com/pithecene/lode/pull/49), [#50](https://github.com/pithecene/lode/pull/50))
- **Checksums**: `WithChecksum(NewMD5Checksum())` records a checksum of each data file in the manifest. The `Checksum` and `HashWriter` interfaces support other algorithms.
- **Documentation**: the README gains a quick start and guidance on choosing among `Write`, `StreamWrite`, and `StreamWriteRecords`, and `PUBLIC_API.md` lists every sentinel error and the options that each constructor accepts.

### Changed

- **S3 uploads use constant memory**: `Put` spools the payload to a temporary file instead of reading it into memory, and an upload larger than 5 GB uses a multipart upload. ([#50](https://github.com/pithecene/lode/pull/50), [#51](https://github.com/pithecene/lode/pull/51))

### Known Limitations

- **Large S3 uploads**: an upload larger than 5 GB checks for an existing object before it begins, so a concurrent writer can create the object in the interval (resolved in 0.4.1).
- **Cleanup after cancellation**: when the context of a stream write is canceled, removal of the partial object is best-effort.

### Upgrade Notes

- **Calls**: the new APIs are additions, and existing calls are unchanged.
- **Implementations of the interfaces**: a type that implements `Dataset` adds `StreamWrite` and `StreamWriteRecords`, and a type that implements `s3.API` adds the four multipart methods.

---

## [0.2.0] - 2026-02-03

### Added

- **S3 adapter**: the package `lode/s3` stores datasets in AWS S3 and S3-compatible stores, with examples for AWS S3, MinIO, LocalStack, and Cloudflare R2.

### Changed

- **License**: Lode is licensed under the Apache License 2.0.

### Known Limitations

- **One writer per dataset**: concurrent writers are not coordinated.
- **Uploads are held in memory**: the S3 adapter reads each payload into memory, so an upload cannot exceed 5 GB, the limit of one `PutObject`.
- **Cleanup of partial objects is best-effort**.

### Upgrade Notes

- **Import path**: code that used the experimental adapter under `internal/` imports `lode/s3`.

---

## [0.1.0] - 2026-02-03

### Added

- **Datasets**: datasets and readers with immutable snapshots, manifests, and explicit metadata.
- **Layouts**: the default, Hive, and flat layouts, with dataset enumeration and partition pruning.
- **Stores**: a filesystem store (`NewFSFactory`) and an in-memory store (`NewMemoryFactory`), with range reads through `ReadRange` and `ReaderAt`.
- **Codecs and compression**: a JSON Lines codec, a Gzip compressor, and a no-op compressor, which is the default.
- **Examples**: the default layout, the Hive layout, manifest-driven discovery, and blob upload.

### Known Limitations

- **One writer per dataset**: concurrent writers to one dataset can corrupt its history.
- **Cleanup of partial objects**: objects from failed writes may remain.
- **Discovery through manifests only**: data files without a manifest are not found.

### Upgrade Notes

- **Requirements**: Go 1.25.6 or later. The filesystem root and an S3 bucket must exist before use. Lode creates directories beneath the root but neither the root nor the bucket.

---

[Unreleased]: https://github.com/pithecene/lode/compare/v0.11.0...HEAD
[0.11.0]: https://github.com/pithecene/lode/compare/v0.10.0...v0.11.0
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

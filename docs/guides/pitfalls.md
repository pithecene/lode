# Usage Pitfalls

This guide lists the rules of Lode's public API that most often surprise a
caller. [`PUBLIC_API.md`](../../PUBLIC_API.md) describes each API that these
rules concern, and the contracts state the rules normatively. A caller who
writes a dataset for the first time avoids the most common errors by reading
this guide before writing.

## Metadata and Checksums

- `nil` metadata (including typed-nil string maps and struct pointers)
  persists as `{}`. Metadata must encode as a JSON object (`ErrInvalidArgument`
  otherwise).
- A writer computes checksums and records them in manifests only when the
  caller configures `WithChecksum` (such as `lode.SHA256()` or
  `lode.CRC32C()`). When a snapshot manifest records a checksum algorithm,
  `Read` verifies the stored bytes automatically and returns
  `ErrChecksumMismatch` on corruption.

## Names, Prefixes, and Identifiers

- A dataset ID must be one canonical path segment, and it may contain `=`. A
  volume ID must be one path segment as well. Looking up a snapshot ID that
  is not one segment returns `ErrNotFound` (while passing a non-segment ID to
  `WithAnchor` returns `ErrInvalidArgument`).
- Internal directory and manifest names (`datasets`, `snapshots`,
  `partitions`, `segments`, `manifest.json`, `data`, `expired`, `staged`, and
  `latest`) are fixed constants. To place a root beneath a custom prefix in a
  bucket or directory, wrap the store factory with `lode.Prefixed`.
- A custom compressor's extension must keep `data<ext>` a single path segment
  without `/` or `\`.

## Partitions

- A Hive layout without `By` requires map records (`lode.D`, or `any` holding
  maps). Callers partition typed structs with `Hive(...).By(...)`, and
  opening the dataset rejects a `By` function written for another record
  type.
- `Hive` requires at least one key, and each key must be one path segment
  without `=`.
- A writer rejects a `Partition` map that names keys other than the layout's
  declared keys.
- A partition path is an opaque `PartitionPath`, which `ParsePartitionPath`
  constructs or `ListPartitions` returns. `ParsePartitionPath` returns
  `ErrInvalidPath` for a leading or trailing `/`, a `\`, or an empty, `.`, or
  `..` segment.
- The zero `PartitionPath{}` denotes the dataset root, so
  `Manifests(ctx, lode.PartitionPath{}, opts)` iterates over every committed
  snapshot.

## Listings

- `ListDatasets` returns an empty slice when no committed datasets exist
  (ignoring uncommitted or removed datasets), and returns
  `ErrDatasetsNotModeled` when the root uses `WithFlatPaths()`.
- `ListDatasets` orders results lexicographically by `DatasetID`,
  `ListPartitions` orders `[]PartitionPath` lexicographically by `String()`,
  and `Manifests` yields `LoadedManifest` values in `(Partition, ID)` order.
- `StartAfter` resumes strictly after a previous result in listing order
  (which is snapshot-start order rather than commit order). `Limit` bounds
  returned items; `Manifests` loads only the manifests it yields.
- On a Hive dataset, `ListPartitions` extracts partition paths directly from
  listed manifest keys without reading manifest files when the partition
  index is complete. A snapshot whose writer terminated before persisting its
  partition manifests appears under the dataset root in unfiltered listings
  until `Sweep` reindexes it.
- `PartitionListOptions.Prefix` is a raw path prefix (`day=2026-10` matches
  `day=2026-10-18`), whereas `ExpireRequest.Partition` matches whole path
  segments (`run_id=r1` never matches `run_id=r10`). Both return
  `ErrNotPartitioned` on an unpartitioned dataset and `ErrInvalidPath` for
  non-canonical fragments.

## Reads and Views

- `Read(ctx, id)` decompresses, decodes, and verifies recorded checksums,
  whereas `OpenObject(ctx, obj)` and `ReaderAt(ctx, obj)` return the raw
  stored bytes as persisted in storage (still compressed when a compressor is
  used, and without checksum verification).
- `ReaderAt` may return an `io.ReaderAt` that also implements `io.Closer`
  (such as `*os.File` on `FS`), which the caller closes when finished.
- `WithAnchor(id)` opens a view at snapshot `id` and retains its loaded
  manifest so `Snapshot(ctx, id)` or `Read(ctx, id)` issues zero extra
  manifest reads, while methods such as `Latest()` still consult live
  storage.

## Writes and Parquet

- Individual `*Records[T]`, `*Blobs`, and `volume.Volume` handles are not
  safe for concurrent use across goroutines; open separate handles from the
  shared `*Root` for concurrent writers.
- `Parquet[T](schema)` supports both `lode.D` and flat Go structs, but always
  requires an explicit `ParquetSchema` and does not support
  `Records.StreamWrite` (`ErrCodecNotStreamable`). Nullable struct fields
  require pointer types (or `[]byte` for `ParquetBytes`).
- Passing a `nil` `io.Reader` to `Blobs.StreamWrite` or a `nil` iterator to
  `Records.StreamWrite` returns `ErrInvalidArgument`.

# Usage Pitfalls

This guide lists the rules of Lode's public API that most often surprise a
caller. [`PUBLIC_API.md`](../../PUBLIC_API.md) describes each API that
these rules concern, and the contracts state the rules normatively. A
caller who writes a dataset for the first time benefits from reading this
guide beforehand.

## Metadata

- `nil` metadata persists as `{}`. Metadata must encode as a JSON object.
- A writer computes checksums and records them in manifests only when the
  caller configures a checksum.

## Names and Identifiers

- A dataset ID must be one path segment, and it may contain `=`. A volume
  ID must be one path segment as well. A snapshot ID that is not one
  segment names no snapshot (`ErrNotFound`).
- A custom compressor's extension must not contain `/` or a backslash,
  because a writer appends it to each data file's name.

## Partitions

- A Hive layout without `By` requires map records (`D`, or `any` holding
  maps). Typed records partition with `Hive(...).By(...)`, and opening the
  dataset rejects a `By` function for another record type.
- `Hive` requires at least one key, and each key must be one path segment
  without `=`.
- A partition that names keys other than the layout's fails the write.
- A partition path is a `PartitionPath`, which `ParsePartitionPath`
  constructs or a listing returns. `ParsePartitionPath` returns `ErrInvalidPath` for a
  leading `/`, a backslash, or an empty, `.`, or `..` segment.
- The zero `PartitionPath` is the dataset root, so
  `ListManifests(ctx, lode.PartitionPath{}, opts)` lists every snapshot. A
  `PartitionPath` encodes as text (and therefore in JSON) as its string,
  and decoding validates it.

## Listings

- `ListDatasets` returns `ErrNoManifests` when storage has objects but no
  manifests, and `ErrDatasetsNotModeled` for flat paths.
- `ListDatasets` orders results lexicographically by ID, and
  `ListPartitions` orders them lexicographically by `Path`. `ListManifests`
  orders results by `(Partition, ID)`.
- `StartAfter` resumes strictly after a previous result. `Limit` bounds the
  results, not the store calls.
- On a Hive dataset, a listing places a snapshot under the smallest
  partition whose manifest copy exists. A commit persists those copies
  after it is complete. A listing therefore places under the dataset root a
  snapshot without files, or one whose writer terminated before persisting
  the copies, and a listing filtered by partition omits that snapshot.
- `PartitionListOptions.Prefix` is a raw path prefix: `day=2026-10` matches
  `day=2026-10-18`. It requires a partitioned dataset, and a listing of an
  unpartitioned dataset returns `ErrPartitionPrefixNotSupported`.
- A prefix must be a canonical relative fragment, with no leading `/` and
  no `.`, `..`, or empty segment. A listing returns `ErrInvalidPath` for
  any other prefix. An empty result under a prefix is not an error.

## Reads

- `ReaderAt` may return an `io.ReaderAt` that also implements `io.Closer`,
  and the caller must close that reader after use.

## Writes

- Concurrent writes to the same dataset through separate handles are safe.
  Every store implements `ConditionalWriter`, so commits detect one another
  and return `ErrSnapshotConflict`
  ([`CONTRACT_WRITE_API.md`](../contracts/CONTRACT_WRITE_API.md)
  §Concurrent Blob Write Safety).
- `Records.StreamWrite` requires a codec that streams, and it does not
  support partitioning.
- An aborted stream leaves no snapshot, but partial objects may remain
  ([Safety Guarantees](../../PUBLIC_API.md#safety-guarantees)).

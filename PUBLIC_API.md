# Lode Public API

This document is an overview of Lode's public API for developers. It
describes the entry points, the defaults, and the form of configuration at
the call site.

---

## Scope

Lode defines the persistence structure: datasets, snapshots, manifests,
metadata, and safe write semantics.

Execution, scheduling, and query planning are outside its scope.

### Compatibility Scope

The v1.0 compatibility commitment covers the packages `lode` and `lode/s3`.
The package `lode/volume` is **experimental**, and the commitment omits that
package until the package passes its own stability gate
([`docs/plans/V1_API_DECISIONS.md`](docs/plans/V1_API_DECISIONS.md) D9 and
[`docs/plans/V1_READINESS.md`](docs/plans/V1_READINESS.md) §4). The
commitment covers persisted data in every package: no release renders
unreadable the data that an earlier release wrote (D8).

---

## Construction Overview

A caller opens Lode at a **root**, which consists of one store and the
names that its dataset paths use. A root opens **dataset handles**, typed by the records they hold, and
a read-only root opens **views**.

<!-- illustrative -->
```go
import (
    "github.com/pithecene/lode/lode"
)

root := lode.Open(lode.FS("/data"))

// Records of type Event, encoded as JSON Lines; T is inferred from the codec.
events, err := root.Records(ctx, "events", lode.JSONL[Event](),
    lode.WithLayout(lode.Hive("day").By(Event.Partition)),
    lode.WithRetry(3))
if err != nil { ... }
snap, err := events.Write(ctx, batch, RunInfo{RunID: runID})
got, err := events.Read(ctx, snap.ID) // []Event
var info RunInfo
err = snap.Metadata.Decode(&info)

// A view takes its layout and components from the dataset's manifests.
view, err := lode.OpenReadOnly(lode.FS("/data")).Records(ctx, "events", lode.JSONL[Event]())
```

### Roots

- `Open(factory, opts...) *Root` opens a root over a `Store`, which provides
  `CompareAndSwap`. `OpenReadOnly(factory, opts...) *ReadRoot` opens a root
  over any `ObjectStore`, and its views never write. A factory of a store
  without `CompareAndSwap` does not compile with `Open`.
- Each root constructor calls the factory once, and every dataset of the
  root shares the one store value. The root records a factory error or an
  invalid path option and returns it from every method, so a caller checks
  one error.
- Factories: `FS(root)`, `Memory()`, `Use(store)` (a store the caller
  holds, for sharing one value), and `s3.Factory(client, config)`.
  Constructing a factory performs no I/O.
- `Root.ListDatasets(ctx, opts)` and `ReadRoot.ListDatasets(ctx, opts)`
  list the datasets under the root's datasets directory.

### Path Names

Without options, a root uses the names every earlier release wrote:

```
datasets/<id>/snapshots/<snapshot>/manifest.json
datasets/<id>/snapshots/<snapshot>/data/<file>
datasets/<id>/partitions/<k=v>.../segments/<snapshot>/...   (Hive)
datasets/<id>/latest
```

Each name has one root option, which replaces that name only:
`WithDatasetsDir`, `WithSnapshotsDir`, `WithPartitionsDir`,
`WithSegmentsDir`, `WithManifestFile`, `WithDataDir`, `WithExpiredDir`, and
`WithStagedDir`. `WithFlatPaths()` places each dataset at the store root
and, without partitions, each snapshot directly under its dataset. A
caller cannot combine it with `WithDatasetsDir` or `WithSnapshotsDir`. Each name
must be one path segment without `=`, and the names must be distinct and
other than `latest`, the pointer's name, which is not configurable.

<!-- illustrative -->
```go
root := lode.Open(lode.FS("/data"),
    lode.WithDatasetsDir("tables"),
    lode.WithManifestFile("_manifest.json"))
```

The names, like the store, are part of a dataset's address. Each manifest
records them in its `ManifestPaths`, and Lode returns `ErrSettingsMismatch`
for any writer or view whose names differ from the dataset's.

### Record Datasets

`Root.Records[T](ctx, id, codec, opts...) (Records[T], error)` opens a
dataset of records of type `T`, which the codec fixes. `Records[T]` reads
(`Latest`, `Snapshot`, `Snapshots`, `Read`, and the listing methods),
writes (`Write`, `StreamWrite`), and retains through the `Retention`
interface (`Expire`, `Sweep`).
`ReadRoot.Records[T](ctx, id, codec, opts...)` opens a `RecordView[T]`,
which reads only.

### Blob Datasets

`Root.Blobs(ctx, id, opts...) (Blobs, error)` opens a dataset whose
snapshots each hold one blob: `Write(ctx, data []byte, metadata)` writes
one blob, `Read` returns `[]byte`, and `StreamWrite(ctx, metadata)` streams
one blob.
A blob has no fields on which a layout can partition, so its layout is
`Unpartitioned`.
`ReadRoot.Blobs(ctx, id, opts...)` opens a `BlobView`.

### Opening a Dataset

Opening a writer reads the dataset's head (one `Get` of the pointer and one
of its manifest) and checks the writer's settings against it. Those
settings are the codec and its description, the compressor, the checksum,
the layout and its keys, and the path names. Lode returns
`ErrSettingsMismatch` when the writer opens if the head's settings differ
from the writer's. If another writer commits conflicting settings after the
writer opens, Lode returns `ErrSettingsMismatch` at the writer's commit. Opening writes nothing.

Opening a view reads the head without creating a missing pointer, takes the
layout from the head's manifest, and returns `ErrNotFound` for a dataset
without snapshots. A view reads each snapshot with the compressor that
snapshot's manifest names.

### Volume (Experimental, Package `lode/volume`)

> **Experimental.** Volume resides in the package
> `github.com/pithecene/lode/lode/volume` and is excluded from the v1.0
> compatibility commitment until it passes its own stability gate
> ([`docs/plans/V1_READINESS.md`](docs/plans/V1_READINESS.md) §4, [`docs/plans/V1_API_DECISIONS.md`](docs/plans/V1_API_DECISIONS.md) D9). Its API may
> change in any release. Data that Volume has persisted remains readable,
> because the D8 commitment on persisted formats covers its path grammar and
> manifest format.

The constructor `volume.New(id, factory, totalLength, opts...)` creates a
sparse byte space, addressable by range, with manifest-driven commit
semantics. The constructor takes a `lode.Factory[lode.RangeStore]`, so a
call that passes a store without range reads does not compile. It returns a `volume.Volume`, whose write and read
methods form the `volume.Writer` and `volume.Reader` interfaces. The `id`
is a `volume.ID`. `Commit` returns a `volume.Snapshot`, which holds its
`volume.SnapshotID` and its `volume.Manifest`.

A Dataset constructs complete objects before committing, whereas a Volume
commits data incrementally as sparse byte ranges. A Volume suits a byte
object of known length that writers fill in pieces (in parallel, out of
order, or across restarts). Each committed range is immutable, and
committed ranges never overlap.

<!-- illustrative -->
```go
import (
    "github.com/pithecene/lode/lode"
    "github.com/pithecene/lode/lode/volume"
)

vol, _ := volume.New(
    "archive",
    lode.FS("/data"),
    10<<30, // 10 GB total length
)

// Stage a byte range
blk, _ := vol.StageWriteAt(ctx, 0, bytes.NewReader(chunk))

// Commit staged blocks into an immutable snapshot
snapshot, _ := vol.Commit(ctx, []volume.BlockRef{blk}, volume.Metadata{"source": "peer-1"})

// Read committed range
data, _ := vol.ReadAt(ctx, snapshot.ID, 0, int64(len(chunk)))

// Get latest snapshot (for resume)
latest, _ := vol.Latest(ctx)
```

Volume uses a fixed internal storage layout (`volumes/<id>/...`). Path
names, `Layout`, and settings recorded in manifests are specific to
datasets and do not apply to Volume, whose metadata is a `volume.Metadata`
map.

The package `volume` defines its own option type, `volume.Option`, so the
Dataset options of `lode` do not compile as Volume options. Volume accepts
the following options:
- `volume.WithChecksum(c)`: integrity checksums on staged blocks (disabled
  by default)
- `volume.WithRetryCount(n)`, `volume.WithRetryBaseDelay(d)`,
  `volume.WithRetryMaxDelay(d)`, `volume.WithRetryJitter(j)`,
  `volume.WithRetryObserver(fn)`:
  CAS retries after a commit conflict, with the same defaults and
  validation as in `lode`

*Contract reference: [`CONTRACT_VOLUME.md`](docs/contracts/CONTRACT_VOLUME.md)*

---

## Configuration Options

A caller may omit or combine options. Each supplied option replaces a
default component. An option type has one target, so an option that does not
apply to a handle does not compile with it.

| Option | Type | Root | Writer (`Records`, `Blobs`) | View | Notes |
|--------|------|:----:|:-------:|:----:|-------|
| `With*Dir`, `WithManifestFile`, `WithFlatPaths()` | `RootOption` | ✅ | | | Path names (§Path Names) |
| `WithLayout(l)` | `DatasetOption` | | ✅ | | `Unpartitioned()` (default) or `Hive(keys...)` |
| `WithCompressor(c)` | `ComponentOption` | | ✅ | ✅ | Writer: the compressor (default: NoOp). View: a custom compressor, matched by name |
| `WithChecksum(c)` | `ComponentOption` | | ✅ | ✅ | Writer: file checksums (default: none). View: a custom checksum, matched by name |
| `WithRetry(n)` | `DatasetOption` | | ✅ | | A bounded number of CAS retries after a conflict |
| `WithRetryBaseDelay(d)` | `DatasetOption` | | ✅ | | Base delay of the backoff (default: 10 ms) |
| `WithRetryMaxDelay(d)` | `DatasetOption` | | ✅ | | Maximum delay of the backoff (default: 2 s) |
| `WithRetryJitter(j)` | `DatasetOption` | | ✅ | | Jitter factor from 0.0 to 1.0 (default: 1.0) |
| `WithRetryObserver(fn)` | `DatasetOption` | | ✅ | | A callback invoked on each retry, for observability |

A view takes no layout or retry: the layout comes from the dataset's
manifests, and a view never commits. A view accepts a `ViewOption`, and
every `ComponentOption` is one. Lode reconstructs the bundled
compressors (`noop`, `gzip`, and `zstd`) from the names that manifests
record. The caller passes a custom compressor to the view with
`WithCompressor`. Volume has
its own option type (see §Volume).

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md), [`CONTRACT_READ_API.md`](docs/contracts/CONTRACT_READ_API.md)*

---

## Components and Constructors

Each configurable component has a public constructor. The public surface
includes a curated set of components:

**Store factories:**
- `FS(root)`: filesystem storage
- `Memory()`: in-memory storage (a new store per root)
- `Use(store)`: a store the caller holds
- `s3.Factory(client, config)`: S3-compatible storage (§S3 Storage Adapter)

The store types are `NewFS(root)`, `NewMemory()`, `s3.New(client,
config)`, and `SingleProcess(objectStore)`. `FS`, `Memory`, and `s3.Factory`
are factories of `RangeStore`, and `NewFS` and `NewMemory` each return a
`RangeStore`.

**Layouts:**
- `Unpartitioned()`: no partitions (the default)
- `Hive(keys...)`: a partition-first layout, of type `HiveLayout`, which
  requires map records (`D`) unless the caller supplies a typed function
  with `By`
- `Hive(keys...).By(func(T) Partition)`: partition values of typed records
  by key name, without reflection

**Compressors:**
- `NewNoOpCompressor()`: no compression (the default)
- `NewGzipCompressor()`: gzip compression
- `NewZstdCompressor()`: Zstandard compression (a higher ratio than gzip
  and faster decompression)
- `Compressor`: the interface of a custom compressor

**Codecs** (each returns the `Codec[T]` interface, so that the compiler
infers `T`):
- `JSONL[T]()`: the JSON Lines format, which streams and supports
  `Records.StreamWrite`, for records of any type `T`
- `Parquet(schema, opts...) Codec[D]`: the Apache Parquet columnar format
  for map records, which does not stream. Lode records an invalid-schema
  error and returns it when the dataset opens.

**Checksums:**
- `NewMD5Checksum()`: MD5 file checksums (disabled unless configured)
- `Checksum`, `HashWriter`: the interface of a custom checksum, whose
  `NewHasher` returns a `HashWriter` that accumulates a digest

**Record helpers:**
- `D`: an abbreviated alias for `map[string]any` at call sites and in
  examples
- `R(...)`: a helper that collects record literals into the `[]D` that
  `Records[D].Write` takes
- `Partition`: a record's partition values by key name, for `Hive(...).By`

**Identifiers, snapshots, and listings:**
- `DatasetID`, `DatasetSnapshotID`: the identifiers of a dataset and of a
  snapshot within it
- `DatasetSnapshot`: a committed snapshot, which embeds its `Manifest`
- `SnapshotReader`: the read surface that every dataset handle and view
  shares
- `ManifestRef`, `PartitionRef`: the references that `ListManifests` and
  `ListPartitions` return
- `PartitionPath`: a canonical partition path, which `ParsePartitionPath`
  constructs or a listing returns, and whose zero value names the dataset
  root
- `ObjectRef`: a reference to a data object, which `OpenObject` and
  `ReaderAt` take
- `DatasetListOptions`, `ManifestListOptions`, `PartitionListOptions`: the
  options of `ListDatasets`, `ListManifests`, and `ListPartitions`

**Interfaces:**
- `Codec[T]`, `StreamingCodec[T]`, `StreamEncoder[T]`: codecs and streaming
  encoders of records of type `T`. A custom codec implements `Describe`,
  which returns the codec's configuration in canonical form.
- `Timestamped`: an optional interface for records with timestamps (see
  below)
- `StatisticalCodec`: an optional interface of codecs and stream encoders
  that report per-file column statistics

**Types (per-file statistics):**
- `FileStats`: the row count and column statistics of a file
- `ColumnStats`: the minimum, maximum, null count, and distinct count of a
  column (the bundled Parquet codec does not compute the distinct count and
  reports zero)

**Range reads:**
- `RangeReader`: the optional range-read capability of a store, which a
  store that cannot read ranges does not implement
- `RangeStore`: a `Store` that is also a `RangeReader`, a type that the
  built-in stores and the S3 adapter implement
- `RangeReader.ReadRange(ctx, path, offset, length)`: reads a byte range
  from an object
- `RangeReader.ReaderAt(ctx, path)`: returns an `io.ReaderAt` for random
  access
- `ReaderAt(ctx, obj)` on a dataset handle or view: returns an
  `io.ReaderAt` for a data object, or `ErrRangeReadNotSupported` when the
  store does not implement `RangeReader`
- `volume.New`: a constructor that takes a factory of `RangeStore`, so that
  the compiler rejects a store that cannot read ranges

Range reads provide efficient access to columnar formats (Parquet footers),
block-indexed logs, and partial previews of artifacts.

---

## Timestamped Records

Records may implement the `Timestamped` interface, which enables manifests
to record minimum and maximum timestamps automatically:

<!-- illustrative -->
```go
type Timestamped interface {
    Timestamp() time.Time
}
```

When a `Records` handle writes records that implement this interface,
`Write` and `StreamWrite` compute `MinTimestamp` and `MaxTimestamp` from the
data and record them in the manifest. Query engines use these bounds to
prune by time range. Lode determines once, when the handle opens, whether the
record type implements the interface, so a type without the interface
incurs no cost per record.

Example:
<!-- illustrative -->
```go
type Event struct {
    ID        string
    EventTime time.Time
    Data      lode.D
}

func (e Event) Timestamp() time.Time { return e.EventTime }

events, _ := root.Records(ctx, "events", lode.JSONL[Event]())
// Write computes min/max from Event.Timestamp()
events.Write(ctx, []Event{{...}, {...}}, nil)
```

Records that do not implement `Timestamped` (including `lode.D`) leave the
manifest's timestamp fields `nil`. A `nil` value is valid and indicates
that timestamps do not apply to the snapshot.

---

## Parquet Codec

The Parquet codec provides columnar storage with an explicitly defined
schema.

### Construction

<!-- illustrative -->
```go
schema := lode.ParquetSchema{
    Fields: []lode.ParquetField{
        {Name: "id", Type: lode.ParquetInt64},
        {Name: "name", Type: lode.ParquetString},
        {Name: "score", Type: lode.ParquetFloat64, Nullable: true},
    },
}
// The default NoOp compressor avoids compressing twice. A schema error
// (invalid types, empty names, duplicates) is returned when the dataset opens.
ds, err := root.Records(ctx, "analytics", lode.Parquet(schema))
```

### Supported Types

Each field's `Type` is a `ParquetType`:

| Type | Go Input Types | Decoded Type |
|------|---------------|--------------|
| `ParquetInt32` | `int`, `int32`, `int64`, `float64` | `int32` |
| `ParquetInt64` | `int`, `int32`, `int64`, `float64` | `int64` |
| `ParquetFloat32` | `float32`, `float64` | `float32` |
| `ParquetFloat64` | `float32`, `float64` | `float64` |
| `ParquetString` | `string` | `string` |
| `ParquetBool` | `bool` | `bool` |
| `ParquetBytes` | `[]byte`, `string` | `[]byte` |
| `ParquetTimestamp` | `time.Time`, `string` (RFC 3339) | `time.Time` |

### Streaming Limitation

**The Parquet codec does not support streaming.**

A Parquet file ends with a footer that references every row group, so an
encoder cannot emit the file in one pass. `Records.StreamWrite` with a Parquet
codec returns `ErrCodecNotStreamable`.

A caller encodes Parquet records with `Records.Write`:

<!-- illustrative -->
```go
// Correct: use Write for Parquet.
snapshot, err := ds.Write(ctx, records, metadata)

// Incorrect: StreamWrite returns ErrCodecNotStreamable.
_, err := ds.StreamWrite(ctx, records, metadata)
// err is ErrCodecNotStreamable
```

### Compression Layering

Parquet compresses its output internally, so a second compressor adds cost
without benefit.

`Parquet` takes options of type `ParquetOption`. `WithParquetCompression`
selects the internal compression, a `ParquetCompression`:
`ParquetCompressionSnappy` (the default), `ParquetCompressionGzip`, or
`ParquetCompressionNone`.

| Configuration | Result |
|--------------|--------|
| Parquet + `NewNoOpCompressor()` | ✅ Correct: Parquet performs compression internally |
| Parquet + `NewGzipCompressor()` | ❌ Wasteful: the output is compressed twice for minimal benefit |
| Parquet + `WithParquetCompression(lode.ParquetCompressionSnappy)` | ✅ Correct: Parquet compresses internally with Snappy |

### Error Handling

| Error | Cause |
|-------|-------|
| `ErrSchemaViolation` | A missing required field, a type mismatch, or an invalid schema |
| `ErrInvalidFormat` | A malformed Parquet file or corrupt data |
| `ErrCodecNotStreamable` | `Records.StreamWrite` was called with the Parquet codec |

*Contract reference: [`CONTRACT_PARQUET.md`](docs/contracts/CONTRACT_PARQUET.md)*

### Per-File Statistics

The Parquet codec implements `StatisticalCodec` and automatically records
per-file column statistics on each `FileRef` during `Write`:

<!-- illustrative -->
```go
snap, _ := ds.Write(ctx, records, metadata)
for _, f := range snap.Files {
    if f.Stats != nil {
        fmt.Println("rows:", f.Stats.RowCount)
        for _, col := range f.Stats.Columns {
            fmt.Printf("  %s: min=%v max=%v nulls=%d\n",
                col.Name, col.Min, col.Max, col.NullCount)
        }
    }
}
```

The codec computes statistics from the Go record values during encoding.
Columns of ordered types (int32, int64, float32, float64, string,
timestamp) receive a minimum and a maximum, and every column receives a null count. Boolean and byte
columns have no minimum or maximum.

Codecs that do not implement `StatisticalCodec` (for example, JSONL)
produce no statistics: `FileRef.Stats` is `nil`, and Lode omits it from the
manifest's JSON.

---

## Metadata

`Write` and `StreamWrite` take metadata as any value that encodes as a JSON
object: a struct, a map such as `lode.D`, or `nil`, which persists as `{}`.
For a value that does not encode as an object, each method returns
`ErrInvalidArgument` before any store call.

`Manifest.Metadata` is a `Metadata`, which retains the persisted JSON and
decodes it on request: `snap.Metadata.Decode(&v)`. A struct target receives
its declared field types, and a map target receives JSON's types, with
numbers as `float64`. `Metadata.JSON()` returns the persisted object.

---

## Write APIs

`Records.Write(ctx, records, metadata)` creates a snapshot from records of
the handle's type, one data file per partition.

`Records.StreamWrite(ctx, records, metadata)` consumes an
`iter.Seq2[T, error]` and streams the records through a codec that streams
(a `StreamingCodec`) into one data file. An error the iterator yields
aborts the write.

`Blobs.Write(ctx, data, metadata)` creates a snapshot from one blob.

`Blobs.StreamWrite(ctx, metadata)` returns a `StreamWriter` for a
single-pass streaming write of one blob. `StreamWriter.Write` streams bytes,
`Commit` finalizes the write and returns a snapshot, and `Abort` discards
the write. A call to `Close` before `Commit` aborts the stream and creates
no snapshot. The row count of a blob snapshot is `1`.

### Streaming Constraints

| Constraint | `Blobs.StreamWrite` | `Records.StreamWrite` |
|------------|:-----------:|:------------------:|
| Codec | None (a blob dataset) | Required to stream (`StreamingCodec`) |
| Partitioning | Not applicable | ❌ Returns `ErrPartitioningNotSupported` |
| Row count | Always `1` | The number of records consumed |
| `nil` iterator | Not applicable | Returns `ErrNilIterator` |

`Records.StreamWrite` does not partition, because single-pass streaming
cannot buffer records to group them by key. Partitioned data requires
`Write`.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Blobs.StreamWrite Semantics, §Records.StreamWrite Semantics*

### Diagnosing Streaming Errors

| Error | API | Cause | Resolution |
|-------|-----|-------|------------|
| `ErrCodecNotStreamable` | `Records.StreamWrite` | The codec does not stream | Use a streaming codec (JSONL) or `Write` |
| `ErrNilIterator` | `Records.StreamWrite` | The iterator is `nil` | Supply an iterator |
| `ErrPartitioningNotSupported` | `Records.StreamWrite` | The layout is partitioned | Use `Write` for partitioned data |

**On failure:**
- Lode writes no manifest, so the snapshot does not exist.
- Partial data may remain in storage (its removal is best-effort).
- A caller tests for the sentinel errors with `errors.Is()`.

*Contract reference: [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md)*

---

## Usage Pitfalls

The [usage pitfalls guide](docs/guides/pitfalls.md) collects the rules of
this API that most often surprise a caller.

---

## Sidecar Files and Store Access

**A Lode manifest records only the files written through Lode's write
path.**

The manifest does not record files that a caller writes directly through
`Store.Put()`, bypassing the handles' `Write` and `StreamWrite`. Neither
`Snapshots()`, `Read()`, nor any other dataset API can discover them.

This behavior is deliberate. The manifest is authoritative because Lode
produced the files that it describes: Lode recorded their sizes, computed
their checksums (when configured), and applied the configured codec and
compressor. Files that Lode did not write cannot carry these guarantees.

### Correct Patterns for Binary Sidecar Files

| Pattern | When to Use | Completeness |
|---------|-------------|-------------|
| `Blobs.StreamWrite` per file | Large sidecar files that the caller streams once, each as its own snapshot of a blob dataset | Complete: `Snapshots()` enumerates every committed blob |
| `Blobs.Write` | Small blobs that fit in memory | Complete: the manifest records the file |
| File inventory in `Metadata` | The caller manages storage, and Lode records the inventory | Verified by the caller: paths are listed in metadata, and the caller confirms their existence |

### StreamWrite for Sidecar Blobs (Recommended)

<!-- illustrative -->
```go
// Each sidecar file receives its own snapshot of a blob dataset.
sidecars, _ := root.Blobs(ctx, "sidecars")
sw, _ := sidecars.StreamWrite(ctx, lode.D{"filename": "model.bin", "run_id": "r-123"})
io.Copy(sw, file)
snapshot, _ := sw.Commit(ctx)
// snapshot.Files[0] records the blob with its size and checksum.
```

### File Inventory in Metadata for Caller-Managed Storage

<!-- illustrative -->
```go
// The caller writes files outside Lode and records their paths in metadata.
store.Put(ctx, "custom/path/model.bin", modelReader)
store.Put(ctx, "custom/path/index.bin", indexReader)

snapshot, _ := ds.Write(ctx, records, Sidecars{
    Files: []string{"custom/path/model.bin", "custom/path/index.bin"},
})
// The caller can later decode snapshot.Metadata into Sidecars to
// enumerate the files, but must verify their existence independently.
```

A caller that writes files through `Store.Put()` without recording them in
metadata or in a blob dataset cannot expect the manifest to enumerate
them. Lode's on-disk paths form no interface for callers, so
a caller that needs its own files beside Lode's stores them under
a prefix it owns, or in a blob dataset.

---

## Choosing a Write API

- Use `Records.Write` for in-memory records, partitioned data, or codecs
  that do not stream.
- Use `Records.StreamWrite` for large streams of records with a codec that
  streams (without partitioning).
- Use `Blobs.Write` for one blob in memory, and `Blobs.StreamWrite` for a
  large blob that the caller streams once.

---

## Choosing a Compressor

| Compressor | Use When | Trade-offs |
|------------|----------|------------|
| `NewNoOpCompressor()` | The data is already compressed, or compression does not justify its cost | No CPU cost and no reduction in size |
| `NewGzipCompressor()` | Broad compatibility is required (gzip is universally supported) | A good ratio at moderate speed |
| `NewZstdCompressor()` | The best ratio or fast decompression is required | A better ratio than gzip and faster decompression |

**Notes:**
- Each manifest records the compressor, and a writer must use the
  dataset's compressor. A view reads each snapshot with the compressor its
  manifest names: a bundled one by name, or a custom one passed with
  `WithCompressor`.
- When a dataset has a codec, Lode compresses data after the codec encodes
  it.
- Streaming writes compress data as they stream it.

*Contract reference: [`CONTRACT_LAYOUT.md`](docs/contracts/CONTRACT_LAYOUT.md) §Compressor*

---

## Storage Prerequisites

**Storage must exist before a caller opens a dataset, volume, or view.**

Lode does not create storage infrastructure (directories or buckets), and
the omission is deliberate for three reasons:

- **No concealed side effects**: opening a handle does not modify external
  state.
- **Explicit provisioning**: callers control when and how they create
  storage.
- **Uniformity across backends**: the same pattern applies to the
  filesystem, S3, and other adapters.

### Storage Requirements by Backend

| Backend | Prerequisite | Example |
|---------|--------------|---------|
| Filesystem | The directory must exist | `mkdir -p /data/lode` |
| S3 | The bucket must exist | `aws s3 mb s3://my-bucket` |
| Memory | None (in process) | — |

### Provisioning Pattern

Provisioning helpers, if needed, belong outside Lode's core APIs:

<!-- illustrative -->
```go
// A wrapper that ensures storage exists before opening a root.
func OpenFS(dir string) (*lode.Root, error) {
    if err := os.MkdirAll(dir, 0755); err != nil {
        return nil, fmt.Errorf("create storage root: %w", err)
    }
    return lode.Open(lode.FS(dir)), nil
}
```

Because provisioning remains outside Lode, opening a root has no side
effects and behaves predictably.

*Contract reference: [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md)*

---

## Safety Guarantees

### Commit Semantics

**A persisted manifest signifies a complete commit.**

A snapshot becomes visible only after Lode persists its manifest. Data
files may exist before the manifest, but no read discovers them until the
manifest references them.

- `StreamWriter.Commit()` writes the manifest, and the snapshot is
  invisible until then.
- `StreamWriter.Abort()`, or `Close()` without `Commit()`, writes no
  manifest.
- If streaming fails, Lode writes no manifest, and the write has no visible
  effect.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Read-After-Write Visibility, [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §Commit Semantics*

### Concurrency and CAS

Lode uses optimistic concurrency control, implemented with compare-and-swap
(CAS), to detect conflicting writes. CAS availability depends on the storage
adapter:

#### Adapter CAS Support

| Adapter | CAS | Concurrent writers | What happens on conflict |
|---------|-----|--------------------|--------------------------|
| **Memory** | ✅ | Safe without coordination | `ErrSnapshotConflict`, after which the caller retries from `Latest()` |
| **S3** | ✅ | Safe without coordination | `ErrSnapshotConflict`, after which the caller retries from `Latest()` |
| **Filesystem (Unix and Windows)** | ✅ | Safe without coordination | `ErrSnapshotConflict`, after which the caller retries from `Latest()` |
| **Filesystem (other platforms, such as Plan 9 and WebAssembly)** | ❌ | Not supported | `CompareAndSwap` returns `errors.ErrUnsupported`, so every commit fails |
| **Custom adapter** | ✅ (required: `ConditionalWriter` is part of `Store`) | Safe without coordination | `ErrSnapshotConflict`, after which the caller retries from `Latest()` |
| **`SingleProcess` over a backend without CAS** | ✅ within one process | Safe for writers in this process that share the wrapped store | `ErrSnapshotConflict`, after which the caller retries from `Latest()` |

Lode always enables CAS and requires no configuration for it.

#### Concurrent Writers

Several **separate processes** (or separate handles or `Volume` values)
may write to the same dataset or volume safely without external
coordination. A single handle or `Volume` value is **not** safe for
concurrent use by several goroutines, but a `Root` is safe for such use.

A writer that terminates after advancing the `latest` pointer and before
persisting its manifest leaves an *abandoned commit*. The history remains
linear. The next Dataset snapshot names a parent that has no manifest. The
next Volume commit adopts the abandoned commit from its staged manifest and
merges its blocks (CONTRACT_CORE §Head Pointer, CONTRACT_VOLUME §Error
Semantics). Only a pointer that names a commit persisted by a release
earlier than v0.11.0, or a pointer edited by hand, blocks Volume commits
until an operator repairs it. After such a repair, a caller without
configured retries calls `Latest` on an existing `Volume` value, or opens
the volume again, before committing.

On a conflict, with manual retries:
1. Receive `ErrSnapshotConflict`.
2. Read the state again through `Latest()`.
3. Merge the changes into the new state, or reconstruct them, and then
   commit again.

With automatic retries, the caller configures `WithRetry(n)` when the
dataset opens. On a conflict, the commit path then refreshes its state and
assigns the manifest a new parent automatically, with an exponential
backoff and jitter between attempts. Lode writes data files once and
retries only the manifest and the pointer CAS. A caller configures the
backoff with `WithRetryBaseDelay`, `WithRetryMaxDelay`, and
`WithRetryJitter`. It observes retries (for example, to count them in a
metric) with `WithRetryObserver(fn)`, and Lode calls `fn(attempt)`
synchronously once per retry, before the backoff interval, with the attempt
number counted from 1.

Data files are immutable and already persisted, so the cost of a retry is
one manifest write plus one pointer swap.

#### Backends without a Conditional Write

`Store` requires `CompareAndSwap`. A backend that offers no conditional
write implements `lode.ObjectStore` (the object operations alone). When
every writer runs in one process, the caller wraps the backend once with
`lode.SingleProcess` and passes the result to every handle:

```go
store := lode.SingleProcess(customStore) // one value, shared by all writers
root := lode.Open(lode.Use(store))
```

`SingleProcess` implements `CompareAndSwap` with a lock held by the process,
so writers in that process commit as they would with CAS. Reads through the
returned value wait for a replacement in progress, so they never observe a
replaced object as absent. `SingleProcess` does not coordinate writers or
readers in other processes, nor a second `SingleProcess` around the same
backend. With
writers in several processes, such a backend requires external
coordination, such as locks, queues, or leader election. `SingleProcess`
returns unchanged any store that already implements `ConditionalWriter`.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Concurrency, [`CONTRACT_VOLUME.md`](docs/contracts/CONTRACT_VOLUME.md) §Concurrency, [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §ConditionalWriter Capability*

### Performance

Before Lode required CAS, a store without `ConditionalWriter` updated the
pointer with Delete+Put (two store calls). A single `CompareAndSwap` reduces
both latency and the number of allocations relative to that path.

The following benchmarks ran on the in-memory adapter, with twenty
sequential writes per iteration, on an AMD Ryzen 9 5900XT:

| Metric | Delete+Put | CAS (ConditionalWriter) | Delta |
|--------|------------|-------------------------|-------|
| ns/op  | 161,603    | 136,200                 | −16%  |
| B/op   | 110,587    | 98,710                  | −11%  |
| allocs | 740        | 720                     | −3%   |

### Large Upload Guarantees

S3 completes a single PUT atomically only for objects of at most 5 GB. For
a larger upload, the S3 adapter uses conditional completion
(`If-None-Match` on `CompleteMultipartUpload`) to provide the same atomic
guarantee against overwriting as for small uploads.

Both upload paths therefore refuse, atomically, to overwrite an existing
object:
- **Small uploads** (at most 5 GB): atomic through `PutObject` with
  `If-None-Match`
- **Large uploads** (above 5 GB): atomic through `CompleteMultipartUpload`
  with `If-None-Match`

The adapter also checks for an existing object first, so that an upload
to an existing key fails before the adapter sends any parts.

Backend compatibility limits the atomic guarantee for large uploads.

The atomic guarantee for large uploads (above 5 GB) depends on the
backend's support for `If-None-Match` on `CompleteMultipartUpload`:

| Backend | Conditional Multipart | Status |
|---------|----------------------|--------|
| Cloudflare R2 | Supported | ✅ Verified in production |
| AWS S3 | Supported | ✅ Expected to be compatible |
| RustFS | Supported | ⚠️ Test backend only |
| MinIO | Unknown | ⚠️ Not tested |
| LocalStack | Unknown | ⚠️ Test backend only |

Lode's integration tests use the backends marked ⚠️ only as test
environments. A deployment on a backend that the table does not list as
verified requires a test of large uploads (above 5 GB) before it relies on
the multipart path, or single-writer semantics in the application.

*Contract reference: [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §Put Upload Paths, [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Storage-Level Concurrency*

### Cleanup Behavior

**Lode removes partial objects on a best-effort basis and does not
guarantee their removal.**

On an abort, or on an error before commit:
- If the failure occurs before Lode writes any manifest, the snapshot does
  not exist.
- A failed `Put` leaves no object ([`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §Put is atomic),
  and the writer never deletes anything after a failed `Put`.
- When a stream's commit fails before any manifest write, Lode attempts to
  delete the stream's data object, which may remain in storage.
- If a manifest write returns an error, the store may nevertheless have
  persisted the manifest (when the network lost only the response), so the
  snapshot may be visible. Lode therefore retains the data object, and if the commit did
  fail, `Sweep` reclaims it.
- Callers should not rely on Lode to remove partial objects
  automatically.
  `Sweep` reclaims them once they are older than its grace period
  (§Retention).
- Removal uses an independent context, so that it can succeed even after the
  caller cancels its own context. Whether removal
  completes after a cancellation depends on the timing of the adapter, so
  the outcome can differ from one run to the next. The difference has no
  effect on correctness, because an object that no manifest references
  belongs to no snapshot.

A failure to delete a partial object does not create a snapshot.

*Contract reference: [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md) §Streaming API Errors, [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §Streaming Write Atomicity*

---

## Retention

Lode never removes anything on its own initiative. `Expire`, on a writer
handle (`Records` or `Blobs`), removes the snapshots that the caller
selects, and `Sweep` reclaims the residual data of interrupted writes and
expiries. Views cannot expire or sweep, because removal is a write. The
caller's policy determines which snapshots to retain, for how long, and
when to run either operation ([`CONTRACT_RETENTION.md`](docs/contracts/CONTRACT_RETENTION.md)).

<!-- illustrative -->
```go
// Preview, then remove, one run of a Hive dataset.
req := lode.ExpireRequest{Partition: "source=s/category=c/day=2026-10-19/run_id=r1", DryRun: true}
preview, err := ds.Expire(ctx, req)
// preview.Expired, preview.Objects, preview.Bytes

req.DryRun = false
report, err := ds.Expire(ctx, req)
if report.RetainedHead != "" {
    // The newest snapshot is the head and is retained; expire it after a newer commit.
}
```

Each field of `ExpireRequest` selects a set of snapshots, `Expire` removes
the intersection of those sets, and the caller must set at least one
field:

| Field | Selects |
|-------|---------|
| `IDs` | The named snapshots |
| `Partition` | Snapshots in this partition or beneath it, matched by whole segment (`run_id=r1` never matches `run_id=r10`), in Hive layouts only |
| `Before` | Snapshots whose write began before this instant (Lode derives the start time from the snapshot ID, so selection loads no manifest) |

- **Lode never expires the head.** When the selection includes the head,
  `Expire` retains the head and reports it in `ExpireReport.RetainedHead`
  without returning an error.
- **Lode retains mixed snapshots.** When `Partition` selects a snapshot that
  also has files outside the partition, `Expire` reports it in `Mixed`. Name it in
  `IDs` to expire it.
- **Lode records each expiry.** `Snapshot` and `Read` on an expired ID return
  `ErrSnapshotExpired`, which also matches `ErrNotFound`.
- **Expiry is resumable.** The report is non-nil on error and lists the
  work completed. The expiry's tombstone records the snapshot's files, so
  repeating the same `IDs` request completes the expiry from any point, as
  does `Sweep`. `Objects` and `Bytes` count what each run deleted, and the
  totals across runs sum correctly.
- **Lode deletes only what it wrote.** Objects that a caller placed beside
  the dataset through `Store.Put` (sidecar files) remain, because `Expire`
  never deletes them. The caller deletes them after their snapshots
  expire.
- **A read in progress may fail.** A `Read` that loaded a manifest before
  its expiry began can encounter `ErrNotFound` on a data file. Lode
  provides no read lease.

<!-- illustrative -->
```go
// From the caller's own scheduler: complete interrupted expiries, restore
// the partition index of interrupted commits, and reclaim objects of
// writes that never committed, once they are older than a day.
report, err := ds.Sweep(ctx, lode.SweepRequest{Grace: 24 * time.Hour})
// report.Settled, report.Reindexed, report.Objects, report.Bytes, report.Unsized, report.Orphans
```

`Sweep` returns a `SweepReport` of what it reclaimed, or of what it would
reclaim on a dry run.

- **`Sweep` restores a missing partition index.** On a Hive dataset, a
  commit persists its partition manifests after it persists the canonical
  manifest. A writer that terminates before that step leaves a snapshot
  that listings filtered by partition omit.
  `Sweep` copies the canonical manifest to each missing partition path once
  the snapshot is older than `Grace`, and reports it in `Reindexed`.

- **A grace period is required.** `Sweep` deletes an object of an
  uncommitted snapshot only when its write began longer ago than `Grace`,
  so it never reclaims a write in progress. `Grace` must exceed the
  duration of the longest write, and `Sweep` returns `ErrInvalidArgument`
  for a zero or negative value.
- **Reclamation operates per snapshot.** `Sweep` retains every object in a
  live snapshot's data prefix, whether or not its `Files[]` lists the
  object, and deletes all of them once the snapshot expires.
- **`Bytes` counts only recorded sizes.** `Bytes` is the sum of the
  sizes that an expiry's tombstone recorded for the objects deleted.
  Objects without a recorded size (orphaned objects, and objects that a
  snapshot's `Files[]` never listed) are counted in `Unsized`, and the report
  never estimates their sizes.
- **`Sweep` never modifies objects outside the data topology.** It neither
  reports nor deletes sidecar files, the pointer, or tombstones.

---

## Errors

Lode returns errors for invalid configuration, storage failures, and
missing objects, and the sentinel table below defines the meaning of each
error.

### Sentinel Errors

A caller checks for sentinel errors with `errors.Is()`:

<!-- illustrative -->
```go
snap, err := ds.Latest(ctx)
if errors.Is(err, lode.ErrNoSnapshots) {
    // The dataset exists but has no committed snapshots.
}
```

| Sentinel | Meaning | Typical Source |
|----------|----------|----------------|
| `ErrNotFound` | The object, path, or dataset does not exist | Storage, views |
| `ErrNoSnapshots` | The dataset or Volume has no committed snapshots | Writer handles, Volume |
| `ErrNoManifests` | Storage has objects but no valid manifests | `ListDatasets` |
| `ErrPathExists` | A write to an existing path (a violation of immutability) | Storage |
| `ErrInvalidPath` | The key escapes the root or is empty, the prefix or partition path is not canonical, or the `ReadRange` bounds are invalid | Storage, listing, `ParsePartitionPath` |
| `ErrInvalidArgument` | Lode rejects a value that the caller supplied (a `nil` factory, an invalid dataset ID or path name, metadata that is not a JSON object, an invalid offset, and so on) | Roots, handles, Volume |
| `ErrDatasetsNotModeled` | Flat paths do not support dataset enumeration | `ListDatasets` |
| `ErrPartitionPrefixNotSupported` | `Prefix` was specified on a dataset without partitions | `ListPartitions` |
| `ErrManifestInvalid` | A manifest fails validation | Views, listing |
| `ErrCodecNotStreamable` | `Records.StreamWrite` was called with a codec that does not stream | `Records` |
| `ErrNilIterator` | A `nil` iterator was supplied to `Records.StreamWrite` | `Records` |
| `ErrPartitioningNotSupported` | `Records.StreamWrite` was called on a partitioned dataset | `Records` |
| `ErrRangeReadNotSupported` | The store does not implement `RangeReader` | `ReaderAt` on a handle or view |
| `ErrSnapshotConflict` | Another writer committed after the parent was resolved (CAS) | Writer handles, Volume |
| `ErrSettingsMismatch` | The handle's codec, codec description, compressor, checksum, layout and partition keys, or path names differ from the dataset's | Writer handles, views |
| `ErrReadOnly` | A write (`Put`, `Delete`, or `CompareAndSwap`) was attempted through a read-only root's store | Views |
| `ErrSnapshotExpired` | `Expire` removed the snapshot, and the error also matches `ErrNotFound` | Handles |
| `ErrHeadUncommitted` | The latest pointer names a snapshot whose manifest has not been persisted, or is absent while snapshots exist | `Expire`, `Sweep` (absent pointer only) |
| `ErrSchemaViolation` | A record does not conform to the Parquet schema, or the schema is invalid | Parquet codec |
| `ErrInvalidFormat` | A Parquet file is malformed or corrupt | Parquet codec |

Volume (experimental) adds two sentinels in the package `volume`:
`volume.ErrRangeMissing` (a `ReadAt` range is not fully committed) and
`volume.ErrOverlappingBlocks` (committed blocks overlap in the cumulative
manifest). It also returns the shared sentinels above where they apply.

### Error Handling Guidelines

A caller may retry the following errors:
- Storage I/O errors (network failures, timeouts): the caller may retry.
- `ErrNotFound` during a race: the caller may retry if the store is
  eventually consistent.
- `ErrSnapshotConflict`: call `Latest()` again, merge state, and commit
  again (or use `WithRetry` for automatic retries).
- `ErrHeadUncommitted`: retry `Expire` or `Sweep` once the pending commit
  completes. A dataset that predates the pointer requires one commit, which
  creates the pointer.

A caller should not retry the following configuration and logic errors:
- `ErrDatasetsNotModeled`: list datasets under a root with a datasets
  directory.
- `ErrInvalidArgument`: correct the value that the message names.
- `ErrSettingsMismatch`: match the dataset's configuration of the setting
  that the message names.
- `ErrManifestInvalid`: the data is corrupt, so investigate its source.
- `ErrPathExists`: a logic error (an attempt to write the same path twice).

*Contract reference: [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md)*

---

## S3 Storage Adapter

The `lode/s3` package provides an S3-compatible storage adapter.

**Public API:**
- `s3.Factory(client, config)`: a factory for `lode.Open`, which records
  a configuration error instead of returning it
- `s3.New(client, config)`: a constructor that creates an `*s3.Store` from
  an AWS SDK client
- `s3.Config{Bucket, Prefix}`: the store's configuration
- `s3.API`: the subset of the AWS SDK client that the store calls, which
  the SDK's own client satisfies

The caller constructs the client directly with the AWS SDK, so Lode's API
surface remains minimal and the caller retains full control over
credentials, endpoints, and options.

### AWS S3

<!-- illustrative -->
```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

    "github.com/pithecene/lode/lode/s3"
)

cfg, _ := config.LoadDefaultConfig(ctx)
client := awss3.NewFromConfig(cfg)

root := lode.Open(s3.Factory(client, s3.Config{
    Bucket: "my-bucket",
    Prefix: "lode-data/",
}))
```

### LocalStack

<!-- illustrative -->
```go
import (
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/credentials"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

    "github.com/pithecene/lode/lode/s3"
)

cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(
        credentials.NewStaticCredentialsProvider("test", "test", ""),
    ),
)
client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
    o.BaseEndpoint = aws.String("http://localhost:4566")
    o.UsePathStyle = true
})

store, _ := s3.New(client, s3.Config{Bucket: "my-bucket"})
```

### MinIO

<!-- illustrative -->
```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(
        credentials.NewStaticCredentialsProvider("minioadmin", "minioadmin", ""),
    ),
)
client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
    o.BaseEndpoint = aws.String("http://localhost:9000")
    o.UsePathStyle = true
})

store, _ := s3.New(client, s3.Config{Bucket: "my-bucket"})
```

### Cloudflare R2

<!-- illustrative -->
```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("auto"),
    config.WithCredentialsProvider(
        credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
    ),
)
client := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
    o.BaseEndpoint = aws.String("https://" + accountID + ".r2.cloudflarestorage.com")
})

store, _ := s3.New(client, s3.Config{Bucket: "my-bucket"})
```

**Bucket requirement:** the bucket must exist before the first storage
operation, such as opening a dataset or view, listing datasets, or calling
a Volume method, because Lode does not create buckets.

### Other S3-Compatible Services

**Lode v1.0 verifies few S3-compatible services.** Its guarantees on such a
service rest on three behaviors of the service:

- strong read-after-write consistency
- `If-None-Match: *` on `PutObject` and on `CompleteMultipartUpload`,
  which refuses to replace an existing object, so that data files and
  manifests remain immutable
- `If-Match` on `PutObject`, applied atomically under concurrent writers,
  which detects a conflicting commit

AWS S3 provides all three. A service that accepts a conditional header
without enforcing it lets two writers replace each other's manifest or
pointer without an error, and that replacement forks the dataset's
history.

Lode's integration tests verify both conditional writes against LocalStack
and RustFS: sixteen writers race on `CompareAndSwap` and on `Put`, and
exactly one succeeds (`TestIntegration_ConditionalWrites`). Cloudflare R2
serves a production consumer. SeaweedFS requires version 4.29 or later.
Before 4.18 it accepts several concurrent conditional writes of one
object, and from 4.18 through 4.28 it stalls conditional writes under
contention. Lode verifies no other service. A caller who relies on one
first runs the same race against it and verifies the conditional
completion of a multipart upload, which Lode uses for objects larger than 5 GB.

---

## Examples

The examples under [`examples/`](examples/) use only the public API and
demonstrate roots, typed records, blob datasets, views, explicit
configuration, and typed metadata.

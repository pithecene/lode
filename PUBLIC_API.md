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
persisted-format commitment covers standard and flat layouts. Readers open
format 1.0.0 and 1.1.0 datasets with the standard internal names in place.
A pre-v1 dataset with custom internal names requires migration.

### Upgrading a dataset with custom internal names

Use the release that wrote the dataset and its original root options. This
includes a dataset that customized only the expiry directory through
`WithExpiredDir("tombstones")`. A store prefix selects the whole root.
Internal names describe paths within that root.

1. Pause writers and retention on the source. Keep a copy of the complete
   original root, including its pointer and tombstones.
2. With the old release, open the source using its original options. Open
   an empty destination under a separate root using standard names and the
   intended codec, compression, checksum, and partition settings.
3. Enumerate surviving snapshots and follow parent links to replay ancestors
   before their children. For each snapshot, read its records or blob and
   write the payload and original metadata to the destination. Record the
   old and new snapshot IDs in a durable mapping.
4. Open the destination with the new release. Compare the snapshot inventory,
   mapped parent links, metadata, and decoded records or blob contents before
   switching clients. Update external snapshot references using the mapping.

Replay creates new IDs, creation times, and object paths. Retain the original
root and reader for expired history, gaps in lineage, and references that
must preserve their historical identity.

---

## Construction Overview

A caller opens Lode at a **root**, which binds one store and its path
topology. A writable root opens concrete **dataset handles** (`*Records[T]`
and `*Blobs`), and a read-only root opens concrete **views**
(`*RecordView[T]` and `*BlobView`).

<!-- illustrative -->
```go
import (
    "github.com/pithecene/lode/lode"
)

root := lode.Open(lode.FS("/data"))

// Records of type Event, encoded as JSON Lines; T is inferred from the codec.
events, err := root.Records(ctx, "events", lode.JSONL[Event](),
    lode.WithLayout(lode.Hive("day").By(Event.Partition)),
    lode.WithChecksum(lode.SHA256()),
    lode.WithRetry(3))
if err != nil { ... }
snap, err := events.Write(ctx, batch, RunInfo{RunID: runID})
got, err := events.Read(ctx, snap.ID) // []Event
var info RunInfo
err = snap.Metadata.Decode(&info)

// A view takes its layout, compressor, and checksum from the dataset's manifests.
view, err := lode.OpenReadOnly(lode.FS("/data")).Records(ctx, "events", lode.JSONL[Event]())
```

### Roots

- `Open(factory, opts...) *Root` opens a root over a `Store`, which provides
  `CompareAndSwap` (`ConditionalWriter`). `OpenReadOnly(factory, opts...)
  *ReadRoot` opens a root over any `ObjectStore`, and its views never write.
  A factory of a store without `CompareAndSwap` does not compile with `Open`.
- Each root constructor calls its `Factory` once, and every dataset of the
  root shares that single store value. The root records a factory error or an
  invalid option and returns it from every method, so a caller checks one
  error when opening a dataset or listing datasets.
- Factories: `FS(root, opts...)`, `Memory()`, `Use(store)` (a store the
  caller already holds, for sharing one value), `Prefixed(factory, prefix)`,
  and `s3.Bucket(client, bucket, opts...)`. Constructing a factory performs
  no I/O.
- `Root.ListDatasets(ctx, opts)` and `ReadRoot.ListDatasets(ctx, opts)`
  return the committed `DatasetID` values in lexicographic order (and an
  empty slice when no committed datasets exist). When the store implements
  `PrefixLister`, `ListDatasets` discovers candidate dataset prefixes
  shallowly instead of scanning every object key.
- `Root.RemoveDataset(ctx, id, opts...)` permanently removes a dataset. It
  fences the dataset's `latest` pointer with `lode:removed` so that
  subsequent opens, reads of `Latest`, listings, and commits return
  `ErrRemoved`, and then deletes all manifests, staged manifests,
  tombstones, and data objects under the dataset's prefix.

### Path Topology

Without root options, Lode uses fixed internal path segments:

```
datasets/<id>/snapshots/<snapshot>/manifest.json
datasets/<id>/snapshots/<snapshot>/data/<file>
datasets/<id>/partitions/<k=v>.../segments/<snapshot>/...   (Hive)
datasets/<id>/latest
```

To place all Lode objects beneath a custom prefix in a bucket or directory,
wrap the store factory with `Prefixed`:

<!-- illustrative -->
```go
root := lode.Open(lode.Prefixed(s3.Bucket(client, "warehouse"), "prod/v1"))
```

`WithFlatPaths()`, the sole `RootOption`, omits the `datasets/` directory
and, on unpartitioned layouts, the `snapshots/` directory
(`<id>/<snapshot>/manifest.json`). Because flat paths have no common
`datasets/` prefix, `ListDatasets` returns `ErrDatasetsNotModeled` on a flat
root. Each manifest of format version `1.2.0` records whether the dataset
uses flat paths (`flat`), and Lode returns `ErrSettingsMismatch` when a
writer or view opens the dataset with a different `WithFlatPaths()` setting.

### Record Datasets

`Root.Records[T](ctx, id, codec, opts...) (*Records[T], error)` opens a
dataset of records of type `T`, inferred from `codec`. `*Records[T]` provides
reading (`ID`, `Latest`, `Snapshot`, `Snapshots`, `Read`, `ListPartitions`,
`Manifests`, `OpenObject`, `OpenDecompressed`, `ReaderAt`), writing (`Write`, `StreamWrite`), and
retention (`Expire`, `Sweep`).

`ReadRoot.Records[T](ctx, id, codec, opts...) (*RecordView[T], error)` opens
a read-only `*RecordView[T]`, which exposes the same read and listing
methods without write or retention operations.

### Blob Datasets

`Root.Blobs(ctx, id, opts...) (*Blobs, error)` opens a dataset whose
snapshots each hold one raw binary object: `Write(ctx, data []byte,
metadata)` commits an in-memory byte slice, `StreamWrite(ctx, r io.Reader,
metadata)` streams bytes from an `io.Reader` into a single snapshot in one
call, and `Read(ctx, id)` returns `[]byte`. A blob dataset has no fields on
which to partition, so its layout is always `Unpartitioned()`.

`ReadRoot.Blobs(ctx, id, opts...) (*BlobView, error)` opens a read-only
`*BlobView`.

### Opening a Dataset or View

Opening a writer handle reads the dataset's head (one `Get` of the `latest`
pointer and one of its manifest) and verifies the writer's settings against
it: the codec name and `Codec.Describe()` output, the compressor, the
checksum algorithm, the partitioner and Hive keys, and `WithFlatPaths()`.
Lode returns `ErrSettingsMismatch` at open time if any setting differs, or at
commit time if a concurrent first writer commits different settings first.
With `WithRecordedComponents()`, a writer of a dataset with a head takes the
head's compressor and checksum for writes, and `WithCompressor` and
`WithChecksum` only supply a custom component that the head names. Reads
resolve each snapshot's recorded components against those options
(CONTRACT_WRITE_API §Settings).
Opening a handle never writes to storage.

Opening a view resolves the head without creating a missing pointer, adopts
the layout from the head manifest, and returns `ErrNotFound` when the dataset
has no committed snapshots (or `ErrRemoved` when the dataset was removed). A
view reconstructs bundled compressors (`Uncompressed`, `Gzip`, `Zstd`) and
bundled checksums (`SHA256`, `CRC32C`, `MD5`) automatically from the names
recorded in each snapshot's manifest.

Two view options refine how a view opens:
- `WithLayout(l)` restricts manifest lookups to the directory of `l` (saving
  a probe on Hive datasets) and returns `ErrSettingsMismatch` if the opened
  manifest records another layout.
- `WithAnchor(id)` opens the view at snapshot `id` instead of the head,
  retaining a private copy of its validated manifest. Subsequent reads check
  the removal fence and canonical object existence before reusing that copy.

### Volume (Experimental, Package `lode/volume`)

> **Experimental.** Volume resides in the package
> `github.com/pithecene/lode/lode/volume` and is excluded from the v1.0
> compatibility commitment until it passes its own stability gate
> ([`docs/plans/V1_READINESS.md`](docs/plans/V1_READINESS.md) §4,
> [`docs/plans/V1_API_DECISIONS.md`](docs/plans/V1_API_DECISIONS.md) D9). Its
> API may change in any release. Data that Volume has persisted remains
> readable, because the D8 commitment on persisted formats covers its path
> grammar and manifest format.

The constructor `volume.New(id, factory, totalLength, opts...)` creates a
sparse byte address space with manifest-driven commit semantics. It accepts a
`lode.Factory[lode.RangeStore]`, so passing a store without range reads fails
at compile time. It returns a `volume.Volume`, which composes `volume.Writer`
and `volume.Reader`. Its identifier type is `volume.ID`. `Commit(ctx, blocks,
metadata)` accepts any JSON-object-shaped metadata value (or `nil` for `{}`)
and returns a `*volume.Snapshot`, pairing a `volume.SnapshotID` with a
`*volume.Manifest` (whose `Metadata` field has type `lode.Metadata`).
`ReaderAt(ctx, snapshotID)` loads a snapshot's manifest once and returns a
`volume.SnapshotReaderAt`, an `io.ReaderAt` whose reads fetch block data
only.

A dataset constructs complete objects before committing, whereas a volume
commits data incrementally as sparse, non-overlapping byte ranges. Staged
blocks are written to nonced keys
(`volumes/<id>/data/<offset>-<length>-<nonce>.bin`) so an interrupted staging
write never blocks a retry at the same byte range, while pre-v1 unnonced
block paths remain readable.

<!-- illustrative -->
```go
import (
    "github.com/pithecene/lode/lode"
    "github.com/pithecene/lode/lode/volume"
)

vol, _ := volume.New(
    "archive",
    lode.FS("/data"),
    10<<30, // 10 GiB total length
)

// Stage a byte range.
blk, _ := vol.StageWriteAt(ctx, 0, bytes.NewReader(chunk))

// Commit staged blocks and metadata into an immutable cumulative snapshot.
snapshot, _ := vol.Commit(ctx, []volume.BlockRef{blk}, lode.D{"source": "peer-1"})

// Read a committed range, or open a SnapshotReaderAt for repeated random reads.
data, _ := vol.ReadAt(ctx, snapshot.ID, 0, int64(len(chunk)))
ra, _ := vol.ReaderAt(ctx, snapshot.ID)
_ = ra.Blocks()

// Get the latest snapshot to resume.
latest, _ := vol.Latest(ctx)
```

`volume.Remove(ctx, id, factory, opts...)` permanently removes a volume
([`CONTRACT_VOLUME.md`](docs/contracts/CONTRACT_VOLUME.md) §Removal). It
replaces the head pointer with `lode:removed` so `Latest` and `Commit` on
every handle return `volume.ErrRemoved` (an alias for `lode.ErrRemoved`), and
then deletes the volume's manifests, staged manifests, and blocks. A racing
commit causes the pointer swap to conflict: `volume.WithRetry` retries the
swap, and otherwise `volume.Remove` returns `lode.ErrSnapshotConflict` having
deleted nothing. If the fenced head belongs to a pending commit without a
canonical manifest, `volume.Remove` returns `lode.ErrHeadUncommitted` with
objects intact behind the fence; calling `volume.Remove` again after the
commit finishes completes the deletion.

`volume.Head(ctx, id, factory)` returns the `volume.SnapshotID` that the head
pointer names without requiring `totalLength`
([`CONTRACT_VOLUME.md`](docs/contracts/CONTRACT_VOLUME.md) §Head). It never
writes and returns `lode.ErrNoSnapshots` when empty, `volume.ErrRemoved` when
removed, and `lode.ErrHeadUncommitted` when the pointer is absent alongside
existing manifests.

The package `volume` defines its own `volume.Option` interface:
- `volume.WithChecksum(c)`: integrity checksums on staged blocks (disabled by
  default)
- `volume.WithRetry(n)`, `volume.WithRetryBaseDelay(d)`,
  `volume.WithRetryMaxDelay(d)`, `volume.WithRetryJitter(j)`,
  `volume.WithRetryObserver(fn)`: automatic CAS retries on commit or removal
  conflicts, sharing the defaults and validation of `lode`

*Contract reference: [`CONTRACT_VOLUME.md`](docs/contracts/CONTRACT_VOLUME.md)*

---

## Configuration Options

A caller may omit or combine options. Each option type targets a specific
constructor, so inapplicable options fail to compile.

| Option | Type | Target | Notes |
|--------|------|--------|-------|
| `WithFlatPaths()` | `RootOption` | `Open`, `OpenReadOnly` | Places datasets at the store root (§Path Topology) |
| `WithSync()` | `FSOption` | `FS`, `NewFS` | Fsyncs temporary object and pointer files and parent directories on publish |
| `WithLayout(l)` | `ComponentOption` | Writer, View | Writer: `Unpartitioned()` (default) or `Hive(keys...)`. View: expected layout |
| `WithCompressor(c)` | `ComponentOption` | Writer, View | Writer: compressor (default: `Uncompressed()`), which `WithRecordedComponents()` overrides for a dataset with a head. View: custom compressor matched by name |
| `WithChecksum(c)` | `ComponentOption` | Writer, View | Writer: object checksums (default: none), which `WithRecordedComponents()` overrides for a dataset with a head. View: custom checksum matched by name |
| `WithRecordedComponents()` | `DatasetOption` | Writer | Takes the compressor and checksum that the head records, published or staged. `WithCompressor` and `WithChecksum` then set the components of a dataset without a head, and otherwise only supply a custom component that the head names for writes. Reads still resolve each snapshot's components against them |
| `WithRetry(n)` | `DatasetOption` | Writer, `RemoveDataset` | Maximum automatic CAS retries after `ErrSnapshotConflict` (default: 0) |
| `WithRetryBaseDelay(d)` | `DatasetOption` | Writer, `RemoveDataset` | Exponential backoff base delay (default: 10 ms) |
| `WithRetryMaxDelay(d)` | `DatasetOption` | Writer, `RemoveDataset` | Exponential backoff maximum delay (default: 2 s) |
| `WithRetryJitter(j)` | `DatasetOption` | Writer, `RemoveDataset` | Backoff jitter factor in `[0.0, 1.0]` (default: 1.0) |
| `WithRetryObserver(fn)` | `DatasetOption` | Writer, `RemoveDataset` | Synchronous callback `func(attempt int)` invoked before each retry backoff |
| `WithAnchor(id)` | `ViewOption` | View | Opens the view at snapshot `id` and retains its manifest (§Opening a Dataset or View) |

Every `ComponentOption` satisfies both `DatasetOption` and `ViewOption`.
`s3.Option` is reserved for S3 store options on `s3.New` and `s3.Bucket`, and
`volume.Option` configures `volume.New` and `volume.Remove`.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md), [`CONTRACT_READ_API.md`](docs/contracts/CONTRACT_READ_API.md)*

---

## Components and Constructors

**Store factories and constructors:**
- `FS(root, opts...)` and `NewFS(root, opts...)`: local filesystem
  `RangeStore` (accepting `FSOption` such as `WithSync()`)
- `Memory()` and `NewMemory()`: in-memory `RangeStore` (`Memory()` creates an
  isolated store per root; pass `Use(NewMemory())` to share one across roots)
- `Use(store)`: wraps an existing `ObjectStore` or `Store` as a `Factory`
- `Prefixed(factory, prefix)`: scopes every key of another `RangeStore`
  factory beneath `prefix`, preserving `PrefixLister` when supported
- `SingleProcess(objectStore)`: adds process-local `CompareAndSwap` to an
  `ObjectStore` that lacks conditional writes, preserving `RangeReader` and
  `PrefixLister`
- `s3.Bucket(client, bucket, opts...)` and `s3.New(client, bucket, opts...)`:
  S3-compatible `*s3.Store` implementing `RangeStore` and `PrefixLister`
  (§S3 Storage Adapter)

**Store interfaces:**
- `ObjectStore`: core object operations (`Put`, `Get`, `Exists`, `List`,
  `Delete`)
- `ConditionalWriter`: atomic `CompareAndSwap`
- `Store`: `ObjectStore` plus `ConditionalWriter`
- `PrefixLister`: optional shallow prefix enumeration (`ListPrefixes`) used
  by `ListDatasets`
- `RangeReader`: optional random-access capability (`ReadRange`, `ReaderAt`)
- `RangeStore`: `Store` plus `RangeReader`

**Layouts:**
- `Layout`: the dataset partitioning interface
- `Unpartitioned()`: unpartitioned layout (the default)
- `Hive(keys...)`: returns a `*HiveLayout` that partitions map records (`D`)
  by `key=value` directories
- `Hive(keys...).By(func(T) Partition)`: binds a typed partition extractor
  without reflection

**Compressors:**
- `Uncompressed()`: pass-through compressor with wire name `"noop"` (the
  default)
- `Gzip()`: gzip compression (`.gz`, wire name `"gzip"`)
- `Zstd()`: Zstandard compression (`.zst`, wire name `"zstd"`)
- `Compressor`: interface for custom stream compressors

**Codecs** (each returns `Codec[T]`, so `Root.Records` infers `T`):
- `JSONL[T]()`: JSON Lines codec for any record type `T`; implements
  `StreamingCodec[T]` for `Records.StreamWrite`
- `Parquet[T](schema, opts...)`: stateless Apache Parquet codec for `lode.D`
  maps or flat Go structs validated against an explicit `ParquetSchema`

**Checksums:**
- `SHA256()`: SHA-256 checksum (`"sha256"`, 64 hex characters)
- `CRC32C()`: Castagnoli CRC-32C checksum (`"crc32c"`, 8 hex characters)
- `MD5()`: MD5 checksum (`"md5"`, 32 hex characters)
- `Checksum` and `HashWriter`: interfaces for custom integrity digests

When a snapshot manifest records a checksum algorithm, `Records.Read`,
`RecordView.Read`, `Blobs.Read`, and `BlobView.Read` verify the stored bytes
of every read object automatically and return `ErrChecksumMismatch` if an
object's digest is missing or does not match. `OpenObject` and `ReaderAt`
return raw stored bytes without decompression or checksum verification, while
`OpenDecompressed(ctx, manifest, obj)` streams the decompressed payload and
verifies the checksum from the matching manifest entry at successful `io.EOF`.
Closing after reading a prefix leaves the remaining bytes unverified. Consumers
with a decoded-size limit must enforce their own budget and treat exceeding it
as an incomplete read. An absent inventory path returns `ErrInvalidArgument`.

**Record helpers and metadata:**
- `D`: alias for `map[string]any`
- `R(records...)`: builds a `[]D` slice from inline `D` literals for
  `Records[D].Write`
- `Partition`: `map[string]any` mapping Hive key names to partition values
- `Metadata` and `NewMetadata(v)`: raw JSON-object snapshot metadata, decoded
  on demand via `Metadata.Decode(&target)` or inspected via `Metadata.JSON()`

**Snapshots, manifests, and listings:**
- `DatasetID` and `SnapshotID`: dataset and snapshot identifiers
- `Snapshot`: committed dataset snapshot, embedding `*Manifest`
- `Manifest`: self-describing snapshot record (`Objects []ObjectRef`,
  `Metadata`, `RowCount`, settings, and lineage)
- `ObjectRef`: persisted data object reference (`Path`, `SizeBytes`,
  `Checksum`, and optional `*ObjectStats`), accepted directly by `OpenObject`
  and `ReaderAt`
- `ObjectStats` and `ColumnStats`: per-object row count and column min, max,
  null count, and optional distinct count
- `PartitionPath` and `ParsePartitionPath(s)`: canonical relative partition
  path (zero value denotes the dataset root), returned by `ListPartitions`
- `ManifestRef` and `LoadedManifest`: manifest locator `(ID, Partition)` and
  paired `(Ref, *Manifest)` yielded by `Manifests`
- `DatasetListOptions`, `PartitionListOptions`, and `ManifestListOptions`:
  pagination (`Limit`, `StartAfter`) and prefix options for `ListDatasets`,
  `ListPartitions`, and `Manifests`, and `ManifestListOptions.Prefetch`,
  the bound on manifests that `Manifests` loads ahead, concurrently

**Codec extension interfaces:**
- `Codec[T]`, `StreamingCodec[T]`, `StreamEncoder[T]`: batch and streaming
  record codecs
- `StatisticalCodec[T]`: stateless batch codec (`EncodeWithStats`) returning
  `*ObjectStats` per written object
- `StatisticalStreamEncoder`: optional interface on a `StreamEncoder[T]`
  reporting `ObjectStats()` after `Close`
- `Timestamped`: optional record interface for automatic `MinTimestamp` and
  `MaxTimestamp` tracking

---

## Timestamped Records

Records may implement `Timestamped` so manifests record minimum and maximum
event timestamps automatically:

<!-- illustrative -->
```go
type Timestamped interface {
    Timestamp() time.Time
}
```

When `T` implements `Timestamped`, `Records.Write` and `Records.StreamWrite`
compute `MinTimestamp` and `MaxTimestamp` across the written records. Lode
checks interface satisfaction once when the handle opens, so non-timestamped
record types incur zero per-record overhead.

<!-- illustrative -->
```go
type Event struct {
    ID        string
    EventTime time.Time
    Data      lode.D
}

func (e Event) Timestamp() time.Time { return e.EventTime }

events, _ := root.Records(ctx, "events", lode.JSONL[Event]())
events.Write(ctx, []Event{{...}, {...}}, nil)
```

---

## Parquet Codec

`Parquet[T](schema, opts...)` provides columnar storage for both typed flat
Go structs and `lode.D` map records against an explicit `ParquetSchema`. No
schema is inferred from Go types.

### Construction with Structs or Maps

Struct fields bind to `schema.Fields` by `parquet` struct tag name, else
`json` struct tag name, else exported Go field name (`parquet:"-"` or
`json:"-"` skips a field). Non-nullable fields require value types, and
nullable fields require pointer types (except `ParquetBytes`, which accepts
`[]byte` where `nil` encodes as null). Because `Codec.Describe()` depends
solely on `schema`, a dataset written with `Parquet[User](schema)` can also
be opened with `Parquet[lode.D](schema)`.

<!-- illustrative -->
```go
type User struct {
    ID    int64    `parquet:"id"`
    Name  string   `parquet:"name"`
    Score *float64 `parquet:"score"`
}

schema := lode.ParquetSchema{
    Fields: []lode.ParquetField{
        {Name: "id", Type: lode.ParquetInt64},
        {Name: "name", Type: lode.ParquetString},
        {Name: "score", Type: lode.ParquetFloat64, Nullable: true},
    },
}

// Validation errors (empty schema, bad types, missing or incompatible struct
// fields) match ErrSchemaViolation and surface when Records opens.
ds, err := root.Records(ctx, "users", lode.Parquet[User](schema))
```

### Supported Types

Each `ParquetField.Type` uses one of the `ParquetType` constants:
- Integer and float types: `ParquetInt32` (`int32`/`int`), `ParquetInt64` (`int64`/`int`), `ParquetFloat32` (`float32`), and `ParquetFloat64` (`float64`), using pointer fields on structs when `Nullable` is true.
- Text, binary, boolean, and temporal types: `ParquetString` (`string`), `ParquetBytes` (`[]byte`), `ParquetBool` (`bool`), and `ParquetTimestamp` (`time.Time` in UTC, or RFC 3339 strings in `D` maps). See `docs/contracts/CONTRACT_PARQUET.md` for full map coercion rules.

### Streaming Limitation and Compression Layering

Parquet files require a footer referencing every row group, so `Parquet[T]`
does not implement `StreamingCodec[T]`. Calling `Records.StreamWrite` with a
Parquet codec returns `ErrCodecNotStreamable`; use `Records.Write`.

Parquet compresses column chunks internally. Configure internal compression
with `ParquetOption` and `WithParquetCompression` using `ParquetCompression`
values: `ParquetCompressionSnappy` (default), `ParquetCompressionGzip`, or
`ParquetCompressionNone`. Keep Lode's outer compressor at its default
`Uncompressed()` to avoid double compression:

| Configuration | Result |
|--------------|--------|
| `Parquet[T](schema)` + `Uncompressed()` (default) | ✅ Correct: Parquet compresses column chunks internally |
| `Parquet[T](schema)` + `Gzip()` | ❌ Wasteful: compresses already-compressed Parquet output |
| `Parquet[T](schema, WithParquetCompression(ParquetCompressionSnappy))` | ✅ Correct: explicit Snappy internal compression |

### Per-Object Statistics

`Parquet[T]` implements `StatisticalCodec[T]` statelessly via
`EncodeWithStats`, recording an `*ObjectStats` on each `ObjectRef` during
`Records.Write`:

<!-- illustrative -->
```go
snap, _ := ds.Write(ctx, users, metadata)
for _, obj := range snap.Objects {
    if obj.Stats != nil {
        fmt.Println("rows:", obj.Stats.RowCount)
        for _, col := range obj.Stats.Columns {
            fmt.Printf("  %s: min=%v max=%v nulls=%d\n",
                col.Name, col.Min, col.Max, col.NullCount)
        }
    }
}
```

Ordered types (`ParquetInt32`, `ParquetInt64`, `ParquetFloat32`,
`ParquetFloat64`, `ParquetString`, `ParquetTimestamp`) record finite `Min`
and `Max` bounds (`NaN` and `±Inf` are omitted), and every column records
`NullCount`. The bundled Parquet codec leaves `DistinctCount` at zero.

*Contract reference: [`CONTRACT_PARQUET.md`](docs/contracts/CONTRACT_PARQUET.md)*

---

## Metadata

`Write`, `StreamWrite`, and `volume.Writer.Commit` accept metadata as any
value that encodes as a JSON object: a struct, a string-keyed map such as
`lode.D`, a `Metadata` value, or `nil` (including typed-nil string maps and
struct pointers), which persists as `{}`. Any value that does not encode as a
JSON object fails with `ErrInvalidArgument` before any store call.

`Manifest.Metadata` has type `Metadata` (constructible directly via
`NewMetadata(v)`). Call `snap.Metadata.Decode(&v)` to unmarshal into a typed
struct or map, or `snap.Metadata.JSON()` to obtain a copy of the raw JSON
object.

---

## Write APIs

- `Records.Write(ctx, records, metadata)` encodes `[]T` and commits one
  immutable `*Snapshot` (writing one data object per partition when Hive
  partitioning is configured).
- `Records.StreamWrite(ctx, records, metadata)` consumes an
  `iter.Seq2[T, error]` one record at a time through a `StreamingCodec[T]`
  into a single data object and commits when iteration finishes cleanly.
- `Blobs.Write(ctx, data, metadata)` commits one in-memory `[]byte` blob
  (`RowCount` is `1`).
- `Blobs.StreamWrite(ctx, r, metadata)` streams bytes from `r` (`io.Reader`)
  into one immutable blob object and commits when `r` reaches `io.EOF`
  (`RowCount` is `1`). When uncompressed, `r` streams directly to `Store.Put`
  without an intermediate `io.Pipe` or background goroutine.

### Streaming Constraints

| Constraint | `Blobs.StreamWrite` | `Records.StreamWrite` |
|------------|:-------------------:|:---------------------:|
| Input source | `io.Reader` | `iter.Seq2[T, error]` |
| `nil` source | Returns `ErrInvalidArgument` | Returns `ErrInvalidArgument` |
| Codec | None (raw bytes) | Must implement `StreamingCodec[T]` (else `ErrCodecNotStreamable`) |
| Partitioning | Not applicable (`Unpartitioned`) | Not supported (`ErrPartitioningNotSupported`) |
| Row count | Always `1` | Number of records consumed |

**On failure** (reader error, iterator error, context cancellation, or store
error):
- Lode writes no manifest, so no snapshot is created.
- Partial streamed objects are deleted on a best-effort basis before manifest
  writes, or reclaimed later by `Sweep`.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md), [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md)*

---

## Usage Pitfalls

The [usage pitfalls guide](docs/guides/pitfalls.md) collects the rules of
this API that most often surprise a caller.

---

## Sidecar Files and Store Access

**A Lode manifest records only the objects written through Lode's write
path.**

Objects written directly via `Store.Put` bypass dataset handles, so Lode
neither records them in `Manifest.Objects` nor discovers them through
`Snapshots`, `Manifests`, or `Read`.

### Correct Patterns for Binary Sidecar Files

| Pattern | When to Use | Completeness |
|---------|-------------|-------------|
| `Blobs.StreamWrite` per file | Large binary artifacts streamed once into a blob dataset | Complete: `Snapshots()` and `Manifests()` enumerate every committed blob |
| `Blobs.Write` | Small blobs already in memory | Complete: `Manifest.Objects` records size and checksum |
| Object inventory in `Metadata` | Caller-managed paths stored outside Lode's data prefix | Caller-verified: paths are persisted in `Metadata`, and the caller manages their lifecycle |

<!-- illustrative -->
```go
// Recommended: stream each binary artifact as a snapshot of a blob dataset.
sidecars, _ := root.Blobs(ctx, "sidecars", lode.WithChecksum(lode.SHA256()))
snap, _ := sidecars.StreamWrite(ctx, file, lode.D{"filename": "model.bin", "run_id": "r-123"})
// snap.Objects[0] records the object's path, size, and SHA-256 digest.
```

---

## Choosing a Write API and Compressor

**Write API:**
- Use `Records.Write` for in-memory batches, Hive-partitioned datasets, or
  non-streaming codecs such as `Parquet[T]`.
- Use `Records.StreamWrite` for large unpartitioned record streams with
  `JSONL[T]()`.
- Use `Blobs.Write` for in-memory blobs and `Blobs.StreamWrite` for streaming
  binary readers.

**Compressor:**

| Compressor | Wire Name | Use When |
|------------|-----------|----------|
| `Uncompressed()` | `"noop"` | Data is already compressed (such as Parquet or media blobs) or CPU cost outweighs size savings |
| `Gzip()` | `"gzip"` | Universal tool compatibility is required |
| `Zstd()` | `"zstd"` | Higher compression ratio and faster decompression are preferred |

*Contract reference: [`CONTRACT_LAYOUT.md`](docs/contracts/CONTRACT_LAYOUT.md) §Compressor*

---

## Storage Prerequisites

**Storage must exist before a caller opens a dataset, volume, or view.** Lode
does not create root directories or S3 buckets implicitly.

| Backend | Prerequisite | Example |
|---------|--------------|---------|
| Filesystem (`FS`, `NewFS`) | Root directory must exist on a filesystem supporting hard links | `mkdir -p /data/lode` |
| S3 (`s3.Bucket`, `s3.New`) | Bucket must exist | `aws s3 mb s3://my-bucket` |
| Memory (`Memory`, `NewMemory`) | None (in process) | — |

<!-- illustrative -->
```go
// Optional caller helper to provision local storage before opening a root.
func OpenFS(dir string) (*lode.Root, error) {
    if err := os.MkdirAll(dir, 0o755); err != nil {
        return nil, fmt.Errorf("create storage root: %w", err)
    }
    return lode.Open(lode.FS(dir, lode.WithSync())), nil
}
```

*Contract reference: [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md)*

---

## Safety Guarantees

### Commit Semantics and Durability

**A persisted canonical manifest signifies a complete commit.** Data objects
are written before the `latest` pointer advances and the manifest is
persisted, so readers never observe uncommitted data. On local filesystems,
`FS` publishes objects atomically with hard links and pointer updates with
advisory file locks and atomic renames; pass `WithSync()` when power-loss
durability (`fsync` on temporary files and parent directories) is required in
addition to crash consistency.

### Concurrency and CAS

Lode uses optimistic concurrency control via `CompareAndSwap`
(`ConditionalWriter`) on every commit, with no configuration required.

| Adapter | CAS | Concurrent Writers | Conflict Behavior |
|---------|-----|--------------------|-------------------|
| **Memory** | ✅ | Safe without coordination | `ErrSnapshotConflict`; refresh via `Latest()` or configure `WithRetry` |
| **S3** | ✅ | Safe without coordination | `ErrSnapshotConflict`; refresh via `Latest()` or configure `WithRetry` |
| **Filesystem (Unix and Windows)** | ✅ | Safe without coordination | `ErrSnapshotConflict`; refresh via `Latest()` or configure `WithRetry` |
| **Filesystem (Plan 9, WebAssembly)** | ❌ | Not supported | `CompareAndSwap` returns `errors.ErrUnsupported` |
| **`SingleProcess` over `ObjectStore`** | ✅ within one process | Safe for goroutines sharing one wrapped store | `ErrSnapshotConflict`; refresh via `Latest()` or configure `WithRetry` |

`Root` and `ReadRoot` are safe for concurrent use across goroutines. A single
`*Records[T]`, `*Blobs`, or `volume.Volume` writer handle is **not** safe for
concurrent use by multiple goroutines; open a separate handle per concurrent
writer.

```go
store := lode.SingleProcess(customObjectStore) // one value shared by all in-process writers
root := lode.Open(lode.Use(store))
```

*Contract reference: [`CONTRACT_CORE.md`](docs/contracts/CONTRACT_CORE.md) §Head Pointer, [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Concurrency, [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §ConditionalWriter Capability*

### Large Upload Guarantees

For S3 payloads at or below 5 GiB, `s3.Store.Put` uploads atomically via
`PutObject` with `If-None-Match: "*"`. Above 5 GiB (up to 5 TiB), it uses
`CompleteMultipartUpload` with `If-None-Match: "*"` plus a preflight
existence check:

| Backend | Conditional Multipart | Status |
|---------|----------------------|--------|
| Cloudflare R2 | Supported | ✅ Verified in production |
| AWS S3 | Supported | ✅ Expected to be compatible |
| Hetzner | Ignored | ⚠️ Best-effort preflight only (ensure single-writer above 5 GiB) |
| RustFS | Supported | ⚠️ Test backend only |
| MinIO | Unknown | ⚠️ Not tested |
| LocalStack | Unknown | ⚠️ Test backend only |

Note on SSE-KMS and SSE-C: on buckets encrypted with SSE-KMS or SSE-C,
single-part object `ETag` values are not MD5 digests of the plaintext
payload, so if the network drops a successful `PutObject` response and the
SDK retries, `Put` cannot confirm payload identity via `HeadObject` and
returns `ErrPathExists`.

### Cleanup Behavior

Lode removes partial streamed objects on a best-effort basis when a stream
fails before any manifest write. If a manifest write itself returns an
indeterminate error (such as a lost network response), Lode retains the data
object so a committed snapshot is never corrupted; `Sweep` reclaims
unreferenced objects once they exceed its grace period.

---

## Retention and Dataset Removal

Lode never deletes data in the background. On writer handles (`*Records[T]`
and `*Blobs`), `Expire(ctx, ExpireRequest)` removes selected historical
snapshots and returns an `*ExpireReport`, and `Sweep(ctx, SweepRequest)`
settles interrupted expiries, repairs missing Hive partition manifests,
reclaims orphaned data objects older than `Grace`, and returns a
`*SweepReport` ([`CONTRACT_RETENTION.md`](docs/contracts/CONTRACT_RETENTION.md)).

<!-- illustrative -->
```go
req := lode.ExpireRequest{Partition: "day=2026-10-19/run_id=r1", DryRun: true}
preview, err := ds.Expire(ctx, req)

req.DryRun = false
report, err := ds.Expire(ctx, req)
// report.Expired, report.Mixed, report.RetainedHead, report.Objects, report.Bytes, report.Skipped

sweepReport, err := ds.Sweep(ctx, lode.SweepRequest{Grace: 24 * time.Hour})
// sweepReport.Settled, sweepReport.Reindexed, sweepReport.Objects, sweepReport.Bytes,
// sweepReport.Unsized, sweepReport.Orphans, sweepReport.Unparseable
```

- `ExpireRequest` intersects its non-zero selectors (`IDs`, whole-segment
  `Partition`, and snapshot-ID timestamp `Before`); at least one selector is
  required.
- `Expire` never deletes a head snapshot that one of its reads of `latest`
  names (`ExpireReport.RetainedHead` names the last one it retained) and retains Hive snapshots that span outside
  `Partition` (reporting them in `ExpireReport.Mixed`).
- Reading or looking up an expired snapshot returns `ErrSnapshotExpired`,
  which also matches `ErrNotFound`.
- To remove an entire dataset—including its head snapshot—call
  `Root.RemoveDataset(ctx, id, opts...)`.

---

## Errors

Callers classify errors with `errors.Is()`:

<!-- illustrative -->
```go
snap, err := ds.Latest(ctx)
if errors.Is(err, lode.ErrNoSnapshots) {
    // The dataset has no committed snapshots yet.
}
```

| Sentinel | Meaning | Typical Source |
|----------|---------|----------------|
| `ErrNotFound` | Object, snapshot, or dataset does not exist | Storage, views, handles |
| `ErrNoSnapshots` | Dataset or volume has no committed snapshots | Writer handles, `volume` |
| `ErrPathExists` | Write attempted to an existing immutable path | Storage |
| `ErrInvalidPath` | Empty or escaping storage path, invalid partition path or prefix, or invalid `ReadRange` bounds | Storage, listing, `ParsePartitionPath`, `Expire` |
| `ErrInvalidArgument` | Rejected caller argument (`nil` factory, option, codec, reader, or iterator; invalid ID; non-object metadata; non-positive `Sweep` grace; out-of-range retry option) | Roots, handles, `volume` |
| `ErrDatasetsNotModeled` | `ListDatasets` called on a root configured with `WithFlatPaths()` | `Root.ListDatasets`, `ReadRoot.ListDatasets` |
| `ErrNotPartitioned` | Partition prefix or partition expiry requested on an unpartitioned dataset | `ListPartitions`, `Expire` |
| `ErrManifestInvalid` | Persisted manifest failed structural or scope validation | Views, handles, `volume` |
| `ErrChecksumMismatch` | Stored object bytes did not match the manifest's recorded checksum (or digest was missing) | `Read` on handles and views |
| `ErrCodecNotStreamable` | `Records.StreamWrite` called with a codec that does not implement `StreamingCodec` | `Records.StreamWrite` |
| `ErrPartitioningNotSupported` | `Records.StreamWrite` called on a partitioned layout | `Records.StreamWrite` |
| `ErrRangeReadNotSupported` | Store does not implement `RangeReader` | `ReaderAt` on handles and views |
| `ErrSnapshotConflict` | Concurrent writer advanced the `latest` pointer since parent resolution | Writer handles, `RemoveDataset`, `volume` |
| `ErrSettingsMismatch` | Handle or view settings differ from those recorded in the dataset's manifests | Opening handles/views, `Write`, `StreamWrite`, `Read` |
| `ErrReadOnly` | Mutation attempted through a `ReadRoot` store wrapper | Views |
| `ErrSnapshotExpired` | Snapshot was removed by `Expire` (also matches `ErrNotFound`) | `Snapshot`, `Read`, `WithAnchor` |
| `ErrRemoved` | Dataset or volume was removed via `Root.RemoveDataset` or `volume.Remove` | Handles, views, `volume` |
| `ErrHeadUncommitted` | Head pointer names a pending snapshot without a manifest, or pointer is absent alongside existing snapshots | `Expire`, `Sweep`, `RemoveDataset`, `volume.Remove`, `volume.Head` |
| `ErrSchemaViolation` | Invalid `ParquetSchema`, incompatible struct binding, or record violating schema constraints | `Parquet`, `Records` |
| `ErrInvalidFormat` | Malformed, truncated, or schema-incompatible Parquet file on decode | `Parquet` |

Package `lode/volume` (experimental) exports `volume.ErrRangeMissing`
(uncommitted byte range in `ReadAt`), `volume.ErrOverlappingBlocks`
(overlapping cumulative blocks in `Commit`), and `volume.ErrRemoved` (an
alias for `lode.ErrRemoved`).

*Contract reference: [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md)*

---

## S3 Storage Adapter

The `lode/s3` package adapts AWS S3 and compatible services to
`lode.RangeStore` and `lode.PrefixLister`.

**Public API:**
- `s3.Bucket(client, bucket, opts...)`: returns a
  `lode.Factory[lode.RangeStore]` for `lode.Open` and `lode.OpenReadOnly`
- `s3.New(client, bucket, opts...)`: constructs an `*s3.Store` directly
- `s3.Store`: the S3 store type
- `s3.Option`: option interface for `s3.New` and `s3.Bucket`
- `s3.API`: subset of the AWS SDK v2 S3 client interface required by
  `s3.Store`

Key prefixing is composed via `lode.Prefixed` rather than an S3-specific
configuration struct.

### AWS S3

<!-- illustrative -->
```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

    "github.com/pithecene/lode/lode"
    "github.com/pithecene/lode/lode/s3"
)

cfg, _ := config.LoadDefaultConfig(ctx)
client := awss3.NewFromConfig(cfg)

root := lode.Open(lode.Prefixed(s3.Bucket(client, "my-bucket"), "lode-data"))
```

### LocalStack, MinIO, and Cloudflare R2

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

store, _ := s3.New(client, "my-bucket")
```

For Cloudflare R2, use region `"auto"` and endpoint
`"https://" + accountID + ".r2.cloudflarestorage.com"`. The target bucket
must exist before the first storage operation.

### Other S3-Compatible Services

**Lode v1.0 verifies few S3-compatible services.** Its guarantees on such a
service rest on three behaviors of the service:

- strong read-after-write consistency,
- `If-None-Match: *` on `PutObject` and `CompleteMultipartUpload`, which
  prevents overwriting immutable data objects and manifests, and
- atomic `If-Match` on `PutObject` under concurrent writers, which detects
  conflicting pointer swaps.

AWS S3 provides all three. Integration tests race sixteen concurrent writers
on `CompareAndSwap` and `Put` against LocalStack and RustFS
(`TestIntegration_ConditionalWrites`). Cloudflare R2 serves a production
consumer. SeaweedFS requires version 4.29 or later (versions before 4.18
accept multiple concurrent conditional writes, and 4.18 through 4.28 stall
under contention). Verify conditional write races and multipart conditional
completion before deploying against any other S3-compatible service.

---

## Examples

The runnable programs under [`examples/`](examples/) use only the public API
and demonstrate roots, typed structs and map records, Parquet schemas, blob
streaming, Hive partitioning, retention, optimistic concurrency, S3 prefixing,
and sparse volumes.

# PUBLIC_API.md — Lode Public API

This document is an overview of Lode's public API for developers. It
describes the entry points, the defaults, and the form of configuration at
the call site.

---

## Scope

Lode concerns the structure of persistence: datasets, snapshots,
manifests, metadata, and safe write semantics.

Execution, scheduling, and query planning are outside its scope.

### Compatibility Scope

The v1.0 compatibility commitment covers the packages `lode` and `lode/s3`.
The package `lode/volume` is **experimental** and is excluded until it
passes its own stability gate (`docs/V1_API_DECISIONS.md` D9;
`docs/V1_READINESS.md` §4). Persisted data is covered in either case: no
release renders unreadable the data that an earlier release wrote (D8).

---

## Construction Overview

### Dataset

`NewDataset(id, storeFactory, opts...)` creates a dataset with a documented
default configuration. Options replace parts of that default set of
components.

Default components:
- Layout: DefaultLayout
- Partitioner: NoOp (through the layout)
- Compressor: NoOp
- Codec: none (raw blob storage)
- Checksum: none (disabled by default)

Example:
<!-- illustrative -->
```go
import (
    "github.com/pithecene/lode/lode"
)

ds, _ := lode.NewDataset(
    "mydata",
    lode.NewFSFactory("/data"),
    lode.WithCodec(lode.NewJSONLCodec()),  // Optional: for structured records
)
```

### DatasetReader

`NewDatasetReader(storeFactory, opts...)` creates a read façade.

Default behavior:
- Layout: DefaultLayout

Example:
<!-- illustrative -->
```go
reader, _ := lode.NewDatasetReader(
    lode.NewFSFactory("/data"),
)
// Or with Hive layout (preferred for partitioned data):
reader, _ := lode.NewDatasetReader(
    lode.NewFSFactory("/data"),
    lode.WithHiveLayout("day"),
)
```

### Volume (Experimental, Package `lode/volume`)

> **Experimental.** Volume resides in the package
> `github.com/pithecene/lode/lode/volume` and is excluded from the v1.0
> compatibility commitment until it passes its own stability gate
> (`docs/V1_READINESS.md` §4, `docs/V1_API_DECISIONS.md` D9). Its API may
> change in any release. Data that it has persisted remains readable,
> because its path grammar and manifest format are persisted formats (D8).

`volume.New(id, storeFactory, totalLength, opts...)` creates a sparse byte
space, addressable by range, with manifest-driven commit semantics.

A Dataset constructs complete objects before committing; a Volume commits
data incrementally as sparse byte ranges. A Volume suits a byte object of
known length, immutable once written, that is filled in pieces (in
parallel, out of order, or across restarts); committed ranges never
overlap.

<!-- illustrative -->
```go
import (
    "github.com/pithecene/lode/lode"
    "github.com/pithecene/lode/lode/volume"
)

vol, _ := volume.New(
    "archive",
    lode.NewFSFactory("/data"),
    10<<30, // 10 GB total length
)

// Stage a byte range
blk, _ := vol.StageWriteAt(ctx, 0, bytes.NewReader(chunk))

// Commit staged blocks into an immutable snapshot
snapshot, _ := vol.Commit(ctx, []volume.BlockRef{blk}, lode.Metadata{"source": "peer-1"})

// Read committed range
data, _ := vol.ReadAt(ctx, snapshot.ID, 0, int64(len(chunk)))

// Get latest snapshot (for resume)
latest, _ := vol.Latest(ctx)
```

Volume uses a fixed internal storage layout (`volumes/<id>/...`). The
`Layout` abstraction is specific to Datasets and does not apply to Volume.

**Volume options** (in the package `volume`, with a separate `volume.Option`
type; the Dataset options of `lode` do not compile as Volume options):
- `volume.WithChecksum(c)`: integrity checksums on staged blocks (disabled
  by default)
- `volume.WithRetryCount(n)`, `WithRetryBaseDelay(d)`,
  `WithRetryMaxDelay(d)`, `WithRetryJitter(j)`, `WithRetryObserver(fn)`:
  CAS retries after a commit conflict, with the same defaults and
  validation as in `lode`

*Contract reference: [`CONTRACT_VOLUME.md`](docs/contracts/CONTRACT_VOLUME.md)*

---

## Configuration Options

Options are optional and composable. When supplied, they replace default
components.

Options are validated at construction time. Supplying an option that does
not apply to the target (a dataset or a reader) returns an error.

### Option Applicability Matrix

| Option | Dataset | DatasetReader | Notes |
|--------|:-------:|:------:|-------|
| `WithHiveLayout(keys...)` | ✅ | ✅ | Preferred for the Hive layout |
| `WithLayout(layout)` | ✅ | ✅ | For any layout |
| `WithCompressor(c)` | ✅ | ❌ | Compression at write time |
| `WithCodec(c)` | ✅ | ❌ | Record encoding |
| `WithChecksum(c)` | ✅ | ❌ | File checksums |
| `WithRetryCount(n)` | ✅ | ❌ | A bounded number of CAS retries after a conflict |
| `WithRetryBaseDelay(d)` | ✅ | ❌ | Base delay of the backoff (default: 10 ms) |
| `WithRetryMaxDelay(d)` | ✅ | ❌ | Maximum delay of the backoff (default: 2 s) |
| `WithRetryJitter(j)` | ✅ | ❌ | Jitter factor from 0.0 to 1.0 (default: 1.0) |
| `WithRetryObserver(fn)` | ✅ | ❌ | A callback invoked on each retry, for observability |

Supplying an inapplicable option to `NewDatasetReader` returns an error at
construction time. Volume has its own option type (see §Volume).

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md), [`CONTRACT_READ_API.md`](docs/contracts/CONTRACT_READ_API.md)*

---

## Components and Constructors

Each configurable component has a public constructor. The public surface
includes a curated set of components:

**Storage adapters:**
- `NewFSFactory(root)`: filesystem storage
- `NewMemoryFactory()`: in-memory storage
- `s3.New(client, config)`: S3-compatible storage (see below)

**Layouts:**
- `NewDefaultLayout()`: the default layout, suitable for new users (used
  automatically)
- `NewHiveLayout(keys...) (layout, error)`: a partition-first layout
  (`WithHiveLayout` is preferred at call sites)
- `NewFlatLayout()`: a minimal flat layout

**Compressors:**
- `NewNoOpCompressor()`: no compression (the default)
- `NewGzipCompressor()`: gzip compression
- `NewZstdCompressor()`: Zstandard compression (a higher ratio and faster
  decompression)

**Codecs:**
- `NewJSONLCodec()`: the JSON Lines format (supports streaming)
- `NewParquetCodec(schema, opts...) (Codec, error)`: the Apache Parquet
  columnar format (does not support streaming)

**Checksums:**
- `NewMD5Checksum()`: MD5 file checksums (disabled unless configured)

Constructed components are passed to the construction of a dataset or a
reader.

**Record helpers:**
- `D`: an abbreviated alias for `map[string]any` at call sites and in
  examples
- `R(...)`: a helper that builds `[]any` from record literals

**Interfaces:**
- `Timestamped`: an optional interface for records with timestamps (see
  below)
- `StatisticalCodec`: an optional codec interface for per-file column
  statistics
- `StatisticalStreamEncoder`: an optional stream-encoder interface for
  per-file column statistics

**Types (per-file statistics):**
- `FileStats`: the row count and column statistics of a file
- `ColumnStats`: the minimum, maximum, null count, and distinct count of a
  column

**Range reads:**
- `Store.ReadRange(ctx, path, offset, length)`: reads a byte range from an
  object
- `Store.ReaderAt(ctx, path)`: returns an `io.ReaderAt` for random access
- `DatasetReader.ReaderAt(ctx, obj)`: returns an `io.ReaderAt` for a data
  object

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

When records implement this interface, `Dataset.Write` computes
`MinTimestamp` and `MaxTimestamp` from the data and records them in the
manifest, which allows query engines to prune by time range.

Example:
<!-- illustrative -->
```go
type Event struct {
    ID        string
    EventTime time.Time
    Data      lode.D
}

func (e Event) Timestamp() time.Time { return e.EventTime }

// Write computes min/max from Event.Timestamp()
ds.Write(ctx, []any{Event{...}, Event{...}}, lode.Metadata{})
```

Records that do not implement `Timestamped` (including `lode.D`) result in
`nil` timestamp fields in the manifest, which is valid and indicates that
timestamps do not apply to that snapshot.

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
codec, err := lode.NewParquetCodec(schema)
if err != nil {
    // Handle the schema validation error (invalid types, empty names, duplicates)
}

ds, _ := lode.NewDataset("analytics", factory,
    lode.WithCodec(codec),
    lode.WithCompressor(lode.NewNoOpCompressor()), // Important: avoid compressing twice
)
```

### Supported Types

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

Parquet files require a footer that references every row group, which
makes genuine streaming impossible. `StreamWriteRecords` with a Parquet
codec returns `ErrCodecNotStreamable`.

Use `Dataset.Write` for Parquet encoding:

<!-- illustrative -->
```go
// Correct: use Write for Parquet.
snapshot, err := ds.Write(ctx, records, metadata)

// Incorrect: StreamWriteRecords returns ErrCodecNotStreamable.
_, err := ds.StreamWriteRecords(ctx, iter, metadata)
// err is ErrCodecNotStreamable
```

### Compression Layering

**Important:** Parquet compresses internally. Do not compress the output a
second time.

| Configuration | Result |
|--------------|--------|
| Parquet + `NewNoOpCompressor()` | ✅ Correct: Parquet performs compression internally |
| Parquet + `NewGzipCompressor()` | ❌ Wasteful: the output is compressed twice for minimal benefit |
| Parquet + `WithParquetCompression(lode.ParquetCompressionSnappy)` | ✅ Correct: uses Parquet's internal Snappy compression |

### Error Handling

| Error | Cause |
|-------|-------|
| `ErrSchemaViolation` | A missing required field, a type mismatch, or an invalid schema |
| `ErrInvalidFormat` | A malformed Parquet file or corrupt data |
| `ErrCodecNotStreamable` | `StreamWriteRecords` was called with the Parquet codec |

*Contract reference: [`CONTRACT_PARQUET.md`](docs/contracts/CONTRACT_PARQUET.md)*

### Per-File Statistics

The Parquet codec implements `StatisticalCodec` and automatically records
per-file column statistics on each `FileRef` during `Write`:

<!-- illustrative -->
```go
snap, _ := ds.Write(ctx, records, metadata)
for _, f := range snap.Manifest.Files {
    if f.Stats != nil {
        fmt.Println("rows:", f.Stats.RowCount)
        for _, col := range f.Stats.Columns {
            fmt.Printf("  %s: min=%v max=%v nulls=%d\n",
                col.Name, col.Min, col.Max, col.NullCount)
        }
    }
}
```

Statistics are computed from the Go record values during encoding. Ordered
types (int32, int64, float32, float64, string, timestamp) receive a minimum
and a maximum, and every column receives a null count. Boolean and byte
columns have no minimum or maximum.

Codecs that do not implement `StatisticalCodec` (for example, JSONL)
produce no statistics: `FileRef.Stats` is `nil` and is omitted from the
manifest's JSON.

---

## Metadata

Writes always accept explicit metadata supplied by the caller. Empty
metadata is valid; `nil` metadata is converted to `Metadata{}`.

---

## Write APIs

`Dataset.Write(ctx, data, metadata)` creates a snapshot from in-memory data.

`Dataset.StreamWrite(ctx, metadata)` returns a `StreamWriter` for a
single-pass streaming write of one binary payload. `StreamWriter.Write`
streams bytes, `Commit` finalizes the write and returns a snapshot, and
`Abort` discards the write. If `Close` is called before `Commit`, the stream
is aborted and no snapshot is created. Streamed writes are limited to raw
blobs: no codec is applied, and the row count is `1`.

`Dataset.StreamWriteRecords(ctx, records, metadata)` consumes a pull-based
iterator of records and streams them through a codec that supports
streaming. If the configured codec does not support streaming,
`StreamWriteRecords` returns an error.

### Streaming Constraints

| Constraint | StreamWrite | StreamWriteRecords |
|------------|:-----------:|:------------------:|
| Codec permitted | ❌ Returns `ErrCodecConfigured` | ✅ Required (must support streaming) |
| Partitioning | ❌ Not supported | ❌ Returns `ErrPartitioningNotSupported` |
| Row count | Always `1` | The number of records consumed |
| `nil` iterator | Not applicable | Returns `ErrNilIterator` |

**Why partitioning is unavailable:** single-pass streaming cannot buffer
records in order to partition them by key. Use `Write` for partitioned
data.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §StreamWrite Semantics, §StreamWriteRecords Semantics*

### Diagnosing Streaming Errors

Common errors from the streaming APIs:

| Error | API | Cause | Resolution |
|-------|-----|-------|------------|
| `ErrCodecConfigured` | `StreamWrite` | A codec is configured | Remove the codec or use `Write` |
| `ErrCodecNotStreamable` | `StreamWriteRecords` | The codec does not support streaming | Use a streaming codec (JSONL) or `Write` |
| `ErrNilIterator` | `StreamWriteRecords` | The iterator is `nil` | Supply a valid `RecordIterator` |
| `ErrPartitioningNotSupported` | `StreamWriteRecords` | Partitioning is configured | Use `Write` for partitioned data |

**On failure:**
- No manifest is written, so the snapshot does not exist.
- Partial data may remain in storage (its removal is best-effort).
- Use `errors.Is()` to test for the sentinel errors (`ErrCodec*`,
  `ErrNil*`, `ErrPartitioning*`).

*Contract reference: [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md)*

---

## Usage Pitfalls (Important)

- `nil` metadata is converted to `Metadata{}` (pass `nil` or `Metadata{}`
  for empty metadata).
- Raw blob mode (without a codec) requires exactly one `[]byte` element in
  `Write`.
- Raw blob mode cannot use partitioning, because it has no record fields
  from which to extract keys.
- `WithHiveLayout` requires at least one partition key (validated when the
  option is applied).
- `ListDatasets` returns `ErrNoManifests` when storage has objects but no
  manifests.
- Layouts that do not model datasets (for example, the flat layout) return
  `ErrDatasetsNotModeled`.
- `ListDatasets` results are in lexicographic order of ID, `ListPartitions`
  results in lexicographic order of `Path`, and `ListManifests` results in
  order of `(Partition, ID)`. `StartAfter` resumes strictly after a previous
  result; `Limit` bounds the results, not the store calls.
- `PartitionListOptions.Prefix` is a raw path prefix (`day=2026-10` matches
  `day=2026-10-18`) and requires a layout that models partitions (otherwise
  `ErrPartitionPrefixNotSupported` is returned). It must be a canonical
  relative fragment, with no leading `/` and no `.`, `..`, or empty segment;
  otherwise `ErrInvalidPath` is returned. An empty result under a prefix is
  not an error.
- `ReaderAt` may return an `io.ReaderAt` that also implements `io.Closer`;
  close it after use.
- Checksums are computed and recorded in manifests only when configured.
- Concurrent `Write` and `StreamWrite` calls on the same dataset are safe
  only with a store that supports CAS (`ConditionalWriter`). Without CAS,
  serialize writes externally. See `CONTRACT_WRITE_API.md` §Concurrent Blob
  Write Safety.
- `StreamWrite` is valid only when no codec is configured; otherwise it
  returns an error.
- `StreamWriteRecords` requires a codec that supports streaming; otherwise
  it returns an error.
- `StreamWriteRecords` does not support partitioning, because single-pass
  streaming cannot partition records.
- An aborted stream leaves no snapshot, but partial objects may remain (see
  §Safety Guarantees).

---

## Sidecar Files and Store Access

**A Lode manifest records only the files written through Lode's write
path.**

Files written directly through `Store.Put()`, bypassing `Dataset.Write`,
`StreamWrite`, and `StreamWriteRecords`, are not recorded in the manifest
and cannot be discovered through `Snapshots()`, `Read()`, or any other
Dataset API.

This behavior is deliberate. The manifest is authoritative because Lode
produced the files that it describes: Lode knows their sizes, computed
their checksums (when configured), and applied the configured codec and
compressor. Files that Lode did not write cannot carry these guarantees.

### Correct Patterns for Binary Sidecar Files

| Pattern | When to Use | Completeness |
|---------|-------------|-------------|
| `StreamWrite` per file | Each sidecar file is its own snapshot, recorded by its manifest | Complete: `Snapshots()` enumerates every committed blob |
| `Write` with `[]byte` data | Small blobs that fit in memory | Complete: the manifest records the file |
| File inventory in `Metadata` | The caller manages storage, and Lode records the inventory | Verified by the caller: paths are listed in metadata, and the caller confirms their existence |

### StreamWrite for Sidecar Blobs (Recommended)

<!-- illustrative -->
```go
// Each sidecar file receives its own snapshot through StreamWrite.
sw, _ := ds.StreamWrite(ctx, lode.Metadata{"filename": "model.bin", "run_id": "r-123"})
io.Copy(sw, file)
snapshot, _ := sw.Commit(ctx)
// snapshot.Manifest.Files[0] records the blob with its size and checksum.
```

### File Inventory in Metadata (When Managing Storage Directly)

<!-- illustrative -->
```go
// The caller writes files outside Lode and records their paths in metadata.
store.Put(ctx, "custom/path/model.bin", modelReader)
store.Put(ctx, "custom/path/index.bin", indexReader)

snapshot, _ := ds.Write(ctx, records, lode.Metadata{
    "sidecar_files": []string{"custom/path/model.bin", "custom/path/index.bin"},
})
// The caller can later read snapshot.Manifest.Metadata["sidecar_files"]
// to enumerate the files, but must verify their existence independently.
```

**Pattern to avoid:** writing files through `Store.Put()` without recording
them in metadata or using `StreamWrite`, and then expecting the manifest to
enumerate them.

---

## Choosing a Write API

- Use `Write` for in-memory data, partitioned data, or codecs that do not
  support streaming.
- Use `StreamWrite` for large binary payloads that should be streamed once
  (without a codec).
- Use `StreamWriteRecords` for large streams of records with codecs that
  support streaming (without partitioning).

---

## Choosing a Compressor

| Compressor | Use When | Trade-offs |
|------------|----------|------------|
| `NewNoOpCompressor()` | The data is already compressed, or compression does not justify its cost | No CPU cost; no reduction in size |
| `NewGzipCompressor()` | Broad compatibility is required (gzip is universally supported) | A good ratio at moderate speed |
| `NewZstdCompressor()` | The best ratio or fast decompression is required | A better ratio than gzip and faster decompression |

**Notes:**
- The choice of compressor is recorded in manifests; readers must support
  the compressor used.
- Compression is applied after codec encoding, if any.
- Streaming writes (`StreamWrite`, `StreamWriteRecords`) compress data as
  they stream it.

*Contract reference: [`CONTRACT_LAYOUT.md`](docs/contracts/CONTRACT_LAYOUT.md) §Compressor*

---

## Storage Prerequisites

**Ensure that storage exists before constructing a dataset, volume, or
reader.**

Lode does not create storage infrastructure (directories, buckets). This
is deliberate:

- **No concealed side effects**: constructors do not modify external state.
- **Explicit provisioning**: callers control when and how storage is
  created.
- **Uniformity across backends**: the same pattern applies to the
  filesystem, S3, and other adapters.

### Required Setup

| Backend | Prerequisite | Example |
|---------|--------------|---------|
| Filesystem | The directory must exist | `mkdir -p /data/lode` |
| S3 | The bucket must exist | `aws s3 mb s3://my-bucket` |
| Memory | None (in process) | — |

### Provisioning Pattern

Provisioning helpers, if needed, belong outside Lode's core APIs:

<!-- illustrative -->
```go
// A wrapper that ensures storage exists before constructing the dataset.
func EnsureFSDataset(id, root string, opts ...lode.Option) (lode.Dataset, error) {
    if err := os.MkdirAll(root, 0755); err != nil {
        return nil, fmt.Errorf("create storage root: %w", err)
    }
    return lode.NewDataset(id, lode.NewFSFactory(root), opts...)
}
```

This arrangement keeps Lode's constructors free of side effects and
predictable.

*Contract reference: [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md)*

---

## Safety Guarantees

### Commit Semantics

**The presence of a manifest signifies that a commit is complete.**

A snapshot becomes visible only after its manifest has been written to
storage. Data files may exist before the manifest, but they cannot be
discovered until the manifest references them.

- `StreamWriter.Commit()` writes the manifest; the snapshot is invisible
  until then.
- `StreamWriter.Abort()`, or `Close()` without `Commit()`, ensures that no
  manifest is written.
- On an error during streaming, no manifest is written, and the write is
  as though it never occurred.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Read-After-Write Visibility, [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §Commit Semantics*

### Concurrency and CAS

Lode uses optimistic concurrency control (CAS) to detect conflicting
writes. Whether CAS is available depends on the storage adapter:

#### Adapter CAS Support

| Adapter | CAS | Concurrent writers | What happens on conflict |
|---------|-----|--------------------|--------------------------|
| **Memory** | ✅ | Safe without coordination | `ErrSnapshotConflict`; retry after `Latest()` |
| **S3** | ✅ | Safe without coordination | `ErrSnapshotConflict`; retry after `Latest()` |
| **Filesystem (Unix)** | ✅ | Safe without coordination | `ErrSnapshotConflict`; retry after `Latest()` |
| **Filesystem (other platforms)** | ❌ | A single writer only | Undefined; serialize writes externally |
| **Custom adapter** | Depends on the adapter | Implement `ConditionalWriter` for CAS | Uses Delete+Put without it |

CAS is always enabled when the adapter supports it, and no configuration
is required.

#### With CAS (Memory, S3, and the Filesystem on Unix)

Several **separate processes** (or separate `Dataset` or `Volume` values)
MAY write to the same dataset or volume safely without external
coordination. A single `Dataset` or `Volume` value is NOT safe for
concurrent use by several goroutines.

On a conflict, with manual retries:
1. Receive `ErrSnapshotConflict`.
2. Read the state again through `Latest()`.
3. Merge or reconstruct, and then commit again.

On a conflict, with automatic retries: configure `WithRetryCount(n)` at
construction time. On a conflict, the commit path refreshes its state and
assigns the manifest a new parent automatically, with an exponential
backoff and jitter between attempts. Data files are written once; only the
manifest and the pointer CAS are retried. Configure the backoff with
`WithRetryBaseDelay`, `WithRetryMaxDelay`, and `WithRetryJitter`. Observe
retries (for example, to count them in a metric) with
`WithRetryObserver(fn)`: `fn(attempt)` is called synchronously once per
retry, before the backoff interval, with the attempt number counted from 1.

Data files are immutable and already persisted, so the cost of a retry is
one manifest write plus one pointer swap.

#### Without CAS (the Filesystem on Other Platforms, and Custom Adapters without `ConditionalWriter`)

Callers MUST ensure that at most one writer is active per dataset or volume
at any time. External coordination (locks, queues, leader election) is the
caller's responsibility; without it, concurrent writers may corrupt the
snapshot history.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Concurrency, [`CONTRACT_VOLUME.md`](docs/contracts/CONTRACT_VOLUME.md) §Concurrency, [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §ConditionalWriter Capability*

### Performance

CAS replaces Delete+Put (two store calls) with a single `CompareAndSwap`,
which reduces both latency and the number of allocations. Stores without
`ConditionalWriter` retain the Delete+Put path unchanged.

Benchmarks on the in-memory adapter (twenty sequential writes per
iteration, AMD Ryzen 9 5900XT):

| Metric | Delete+Put | CAS (ConditionalWriter) | Delta |
|--------|------------|-------------------------|-------|
| ns/op  | 161,603    | 136,200                 | −16%  |
| B/op   | 110,587    | 98,710                  | −11%  |
| allocs | 740        | 720                     | −3%   |

### Large Upload Guarantees

For uploads that exceed the storage adapter's atomic threshold (for
example, 5 GB for S3), the S3 adapter uses conditional completion
(`If-None-Match` on `CompleteMultipartUpload`) to provide the same atomic
guarantee against overwriting as for small uploads.

Both upload paths therefore guarantee atomically that nothing is
overwritten:
- **Small uploads** (at most 5 GB): atomic through `PutObject` with
  `If-None-Match`
- **Large uploads** (above 5 GB): atomic through `CompleteMultipartUpload`
  with `If-None-Match`

A preliminary existence check is retained so that an upload fails early,
before uploading parts for an object that already exists.

**Caveat Regarding Backend Compatibility:**

The atomic guarantee for large uploads (above 5 GB) depends on the
backend's support for `If-None-Match` on `CompleteMultipartUpload`:

| Backend | Conditional Multipart | Status |
|---------|----------------------|--------|
| Cloudflare R2 | Supported | ✅ Verified in production |
| AWS S3 | Supported | ✅ Expected to be compatible |
| RustFS | Supported | ⚠️ Test backend only |
| MinIO | Unknown | ⚠️ Not tested |
| LocalStack | Unknown | ⚠️ Test backend only |

The backends marked ⚠️ are used for integration testing only. Before
deploying against a backend that is not listed as verified, test large
uploads (above 5 GB) before relying on the multipart path, or ensure
single-writer semantics in the application.

*Contract reference: [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §Put Upload Paths, [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Storage-Level Concurrency*

### Cleanup Behavior

**The removal of partial objects is best-effort, not guaranteed.**

On an abort, or on an error before commit:
- No manifest is written, so the snapshot does not exist.
- A failed `Put` leaves no object (`CONTRACT_STORAGE.md` §Put is atomic),
  and the writer never deletes anything after a failed `Put`.
- A data object that was fully written but whose commit failed is deleted
  on a best-effort basis and may remain in storage.
- Callers should not rely on the automatic removal of partial objects;
  `Dataset.Sweep` reclaims them once they are older than its grace period
  (§Retention).
- Removal uses an independent context, to maximize the likelihood of
  success even if the caller's context was canceled.

A failure to delete a partial object does not create a snapshot.

*Contract reference: [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md) §Streaming API Errors, [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §Streaming Write Atomicity*

---

## Retention

Lode never removes anything on its own initiative. `Dataset.Expire`
removes the snapshots that the caller selects, and `Dataset.Sweep`
reclaims the residual data of interrupted writes and expiries. Which
snapshots to retain, for how long, and when to run either operation are
matters of the caller's policy (`CONTRACT_RETENTION.md`).

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

The fields of `ExpireRequest` intersect, and at least one of them must be
set:

| Field | Selects |
|-------|---------|
| `IDs` | The named snapshots |
| `Partition` | Snapshots in this partition or beneath it, matched by whole segment (`run_id=r1` never matches `run_id=r10`); Hive layouts only |
| `Before` | Snapshots whose write began before this instant (derived from the snapshot ID, so selection loads no manifest) |

- **The head is never expired.** When the selection includes the head, it
  is retained and reported in `ExpireReport.RetainedHead`; this is not an
  error.
- **Mixed snapshots are retained.** A snapshot selected by `Partition` that
  also has files outside the partition is reported in `Mixed`. Name it in
  `IDs` to expire it.
- **Expiry is recorded.** `Snapshot` and `Read` on an expired ID return
  `ErrSnapshotExpired`, which also matches `ErrNotFound`.
- **Expiry is resumable.** The report is non-nil on error and lists the
  work completed. The expiry's tombstone records the snapshot's files, so
  repeating the same `IDs` request completes the expiry from any point, as
  does `Sweep`. `Objects` and `Bytes` count what each run deleted, and the
  totals across runs sum correctly.
- **Lode deletes only what it wrote.** Objects that a caller placed beside
  the dataset through `Store.Put` (sidecar files) are never deleted by
  `Expire`; delete them after their snapshots have expired.
- **A read in progress may fail.** A `Read` that loaded a manifest before
  its expiry began can encounter `ErrNotFound` on a data file. Lode
  provides no read lease.

<!-- illustrative -->
```go
// From the caller's own scheduler: complete interrupted expiries and
// reclaim objects of writes that never committed, once they are older
// than a day.
report, err := ds.Sweep(ctx, lode.SweepRequest{Grace: 24 * time.Hour})
// report.Settled, report.Objects, report.Bytes, report.Unsized, report.Orphans
```

- **A grace period is required.** An object of a snapshot that never
  committed is deleted only once its write began longer ago than `Grace`,
  so a write in progress is never reclaimed. `Grace` must exceed the
  duration of the longest write; a zero or negative value returns
  `ErrInvalidArgument`.
- **Reclamation operates per snapshot.** Objects in a live snapshot's data
  prefix are retained whether or not its `Files[]` lists them; once the
  snapshot has expired, all of them are deleted.
- **Bytes are counted only where recorded.** `Bytes` is the sum of the
  sizes that an expiry's tombstone recorded for the objects deleted.
  Objects without a recorded size (orphaned objects, and objects that a
  snapshot's `Files[]` never listed) are counted in `Unsized` and never
  estimated.
- **Objects outside the data topology are never modified.** Sidecar files,
  the pointer, and tombstones are neither reported nor deleted.

---

## Errors

Errors are returned for invalid configuration, storage failures, and
missing objects. Their semantics are stable and documented.

### Sentinel Errors

Use `errors.Is()` to check for sentinel errors:

<!-- illustrative -->
```go
snap, err := ds.Latest(ctx)
if errors.Is(err, lode.ErrNoSnapshots) {
    // The dataset exists but has no committed snapshots.
}
```

| Sentinel | Meaning | Typical Source |
|----------|---------|----------------|
| `ErrNotFound` | The object or path does not exist | Storage, DatasetReader |
| `ErrNoSnapshots` | The Dataset or Volume has no committed snapshots | Dataset, Volume |
| `ErrNoManifests` | Storage has objects but no valid manifests | DatasetReader |
| `ErrPathExists` | A write to an existing path (a violation of immutability) | Storage |
| `ErrInvalidPath` | The key escapes the root or is empty, the prefix is not canonical, or the `ReadRange` bounds are invalid | Storage, DatasetReader |
| `ErrInvalidArgument` | A value supplied by the caller cannot be used (a `nil` factory, an invalid offset, an incorrectly shaped blob write, and so on) | Dataset, DatasetReader, Volume |
| `ErrDatasetsNotModeled` | The layout does not support dataset enumeration | DatasetReader |
| `ErrPartitionPrefixNotSupported` | `Prefix` was specified on a layout without partitions | DatasetReader |
| `ErrManifestInvalid` | A manifest fails validation | DatasetReader |
| `ErrCodecConfigured` | `StreamWrite` was called while a codec is configured | Dataset |
| `ErrCodecNotStreamable` | `StreamWriteRecords` was called with a codec that does not stream | Dataset |
| `ErrNilIterator` | A `nil` iterator was supplied to `StreamWriteRecords` | Dataset |
| `ErrOptionNotValidForDataset` | The option does not apply to a Dataset | Dataset |
| `ErrOptionNotValidForDatasetReader` | The option does not apply to a DatasetReader | DatasetReader |
| `ErrPartitioningNotSupported` | `StreamWriteRecords` was called with partitioning | Dataset |
| `ErrRangeReadNotSupported` | The store does not support range reads | Storage |
| `ErrSnapshotConflict` | Another writer committed after the parent was resolved (CAS) | Dataset, Volume |
| `ErrSnapshotExpired` | The snapshot was removed by `Expire`; the error also matches `ErrNotFound` | Dataset |
| `ErrHeadUncommitted` | The latest pointer names a snapshot whose manifest has not been persisted, or is absent while snapshots exist | Dataset.Expire, Dataset.Sweep (absent pointer only) |
| `ErrSchemaViolation` | A record does not conform to the Parquet schema | Parquet codec |
| `ErrInvalidFormat` | A Parquet file is malformed or corrupt | Parquet codec |

Volume (experimental) adds two sentinels in the package `volume`:
`volume.ErrRangeMissing` (a `ReadAt` range is not fully committed) and
`volume.ErrOverlappingBlocks` (committed blocks overlap in the cumulative
manifest). It also returns the shared sentinels above where they apply.

### Error Handling Guidelines

**Safe to retry:**
- Storage I/O errors (network failures, timeouts): the caller may retry.
- `ErrNotFound` during a race: the caller may retry if the store is
  eventually consistent.
- `ErrSnapshotConflict`: call `Latest()` again, merge state, and commit
  again (or use `WithRetryCount` for automatic retries).
- `ErrHeadUncommitted`: retry `Expire` or `Sweep` once the pending commit
  completes; a dataset written before the pointer was introduced requires
  one commit, which creates the pointer.

**Not to be retried (configuration or logic errors):**
- `ErrDatasetsNotModeled`: reconfigure with a different layout.
- `ErrInvalidArgument`: correct the call; the message names the value.
- `ErrManifestInvalid`: the data is corrupt; investigate its source.
- `ErrPathExists`: a logic error (an attempt to write the same path twice).
- A component mismatch: reconfigure the dataset or read a snapshot with
  matching components.

*Contract reference: [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md)*

---

## S3 Storage Adapter

The `lode/s3` package provides an S3-compatible storage adapter.

**Public API:**
- `s3.New(client, config)`: creates a store from an AWS SDK client
- `s3.Config{Bucket, Prefix}`: the store's configuration

The client is constructed with the AWS SDK directly. This keeps Lode's API
surface minimal while leaving the caller full control over credentials,
endpoints, and options.

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

store, _ := s3.New(client, s3.Config{
    Bucket: "my-bucket",
    Prefix: "lode-data/",
})
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

**Bucket requirement:** the bucket must exist before use; Lode does not
create buckets.

**Consistency:** AWS S3 provides strong read-after-write consistency.
Other backends may differ; consult their documentation.

---

## Examples

The examples under [`examples/`](examples/) use only the public API and
demonstrate the default components, explicit configuration, and the
required metadata.

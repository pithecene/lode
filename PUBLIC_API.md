# PUBLIC_API.md — Lode Public API

This document is a developer-facing overview of Lode’s public API.
It describes the entrypoints, defaults, and configuration shape used at
the callsite.

---

## Scope

Lode focuses on persistence structure:
datasets, snapshots, manifests, metadata, and safe write semantics.

Execution, scheduling, and query planning are out of scope.

---

## Construction Overview

### Dataset

`NewDataset(id, storeFactory, opts...)` creates a dataset with a documented
default configuration. Options override parts of that default bundle.

Default bundle:
- Layout: DefaultLayout
- Partitioner: NoOp (via layout)
- Compressor: NoOp
- Codec: none (raw blob storage)
- Checksum: none (opt-in)

Example:
<!-- illustrative -->
```go
import (
    "github.com/pithecene-io/lode/lode"
)

ds, _ := lode.NewDataset(
    "mydata",
    lode.NewFSFactory("/data"),
    lode.WithCodec(lode.NewJSONLCodec()),  // Optional: for structured records
)
```

### DatasetReader

`NewDatasetReader(storeFactory, opts...)` creates a read facade.

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

### Volume

`NewVolume(id, storeFactory, totalLength, opts...)` creates a sparse, range-addressable
byte space with manifest-driven commit semantics.

Volume is a coequal persistence paradigm alongside Dataset. Dataset builds
complete objects before committing; Volume commits truth incrementally via
sparse byte ranges.

<!-- illustrative -->
```go
vol, _ := lode.NewVolume(
    "archive",
    lode.NewFSFactory("/data"),
    10<<30, // 10 GB total length
)

// Stage a byte range
blk, _ := vol.StageWriteAt(ctx, 0, bytes.NewReader(chunk))

// Commit staged blocks into an immutable snapshot
snapshot, _ := vol.Commit(ctx, []lode.BlockRef{blk}, lode.Metadata{"source": "peer-1"})

// Read committed range
data, _ := vol.ReadAt(ctx, snapshot.ID, 0, int64(len(chunk)))

// Get latest snapshot (for resume)
latest, _ := vol.Latest(ctx)
```

Volume uses a fixed internal storage layout (`volumes/<id>/...`). The
`Layout` abstraction is Dataset-specific and does not apply to Volume.

**Volume options:**
- `WithChecksum(c)` — opt-in integrity checksums on staged blocks
- `WithRetryCount(n)` — opt-in automatic CAS retry on commit conflict

*Contract reference: [`CONTRACT_VOLUME.md`](docs/contracts/CONTRACT_VOLUME.md)*

---

## Configuration Options

Options are opt-in and composable. They override default components when
provided.

Options are validated at construction time. Passing an option that does
not apply to the target (dataset vs reader) returns an error.

### Option Applicability Matrix

| Option | Dataset | DatasetReader | Volume | Notes |
|--------|:-------:|:------:|:------:|-------|
| `WithHiveLayout(keys...)` | ✅ | ✅ | ❌ | Preferred for Hive layout |
| `WithLayout(layout)` | ✅ | ✅ | ❌ | For any layout |
| `WithCompressor(c)` | ✅ | ❌ | ❌ | Write-time compression |
| `WithCodec(c)` | ✅ | ❌ | ❌ | Record encoding |
| `WithChecksum(c)` | ✅ | ❌ | ✅ | File / block checksums |
| `WithRetryCount(n)` | ✅ | ❌ | ✅ | Bounded CAS retry on conflict |
| `WithRetryBaseDelay(d)` | ✅ | ❌ | ✅ | Backoff base delay (default: 10ms) |
| `WithRetryMaxDelay(d)` | ✅ | ❌ | ✅ | Backoff cap (default: 2s) |
| `WithRetryJitter(j)` | ✅ | ❌ | ✅ | Jitter factor 0.0–1.0 (default: 1.0) |
| `WithRetryObserver(fn)` | ✅ | ❌ | ✅ | Per-retry callback for observability |

Passing an inapplicable option to `NewDatasetReader` or `NewVolume` returns
an error at construction time.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md), [`CONTRACT_READ_API.md`](docs/contracts/CONTRACT_READ_API.md)*

---

## Components and Constructors

Each configurable component has a public constructor. The public surface
includes a curated set of components:

**Storage adapters:**
- `NewFSFactory(root)` - Filesystem storage
- `NewMemoryFactory()` - In-memory storage
- `s3.New(client, config)` - S3-compatible storage (see below)

**Layouts:**
- `NewDefaultLayout()` - Default novice-friendly layout (used automatically)
- `NewHiveLayout(keys...) (layout, error)` - Partition-first layout (prefer `WithHiveLayout` for fluent API)
- `NewFlatLayout()` - Minimal flat layout

**Compressors:**
- `NewNoOpCompressor()` - No compression (default)
- `NewGzipCompressor()` - Gzip compression
- `NewZstdCompressor()` - Zstd compression (higher ratio, faster decompression)

**Codecs:**
- `NewJSONLCodec()` - JSON Lines format (streaming-capable)
- `NewParquetCodec(schema, opts...) (Codec, error)` - Apache Parquet columnar format (non-streaming)

**Checksums:**
- `NewMD5Checksum()` - MD5 file checksums (opt-in)

Constructed components are intended to be passed into dataset or reader
construction.

**Record helpers:**
- `D` - Shorthand alias for `map[string]any` in callsites and examples
- `R(...)` - Convenience helper to build `[]any` from record literals

**Interfaces:**
- `Timestamped` - Optional interface for records with timestamps (see below)
- `StatisticalCodec` - Optional codec interface for per-file column statistics
- `StatisticalStreamEncoder` - Optional stream encoder interface for per-file column statistics

**Types (per-file statistics):**
- `FileStats` - Per-file row count and column statistics
- `ColumnStats` - Per-column min, max, null count, and distinct count

**Range read support:**
- `Store.ReadRange(ctx, path, offset, length)` - Read byte range from object
- `Store.ReaderAt(ctx, path)` - Get `io.ReaderAt` for random access
- `DatasetReader.ReaderAt(ctx, obj)` - Get `io.ReaderAt` for data object

Range reads enable efficient access to columnar formats (Parquet footers),
block-indexed logs, and partial artifact previews.

---

## Timestamped Records

Records can optionally implement the `Timestamped` interface to enable
automatic min/max timestamp tracking in manifests:

<!-- illustrative -->
```go
type Timestamped interface {
    Timestamp() time.Time
}
```

When records implement this interface, `Dataset.Write` computes
`MinTimestamp` and `MaxTimestamp` from the data and populates the manifest.
This enables time-range pruning by query engines.

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

Records that do not implement `Timestamped` (including `lode.D`)
result in `nil` timestamp fields in the manifest—this is valid and indicates
timestamps are not applicable for that snapshot.

---

## Parquet Codec

The Parquet codec provides columnar storage with explicit schema definition.

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
    // Handle schema validation error (bad types, empty names, duplicates)
}

ds, _ := lode.NewDataset("analytics", factory,
    lode.WithCodec(codec),
    lode.WithCompressor(lode.NewNoOpCompressor()), // Important: avoid double compression
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
| `ParquetTimestamp` | `time.Time`, `string` (RFC3339) | `time.Time` |

### Streaming Limitation

**Parquet codec does NOT support streaming.**

Parquet files require a footer that references all row groups, making true streaming
impossible. `StreamWriteRecords` with a Parquet codec returns `ErrCodecNotStreamable`.

Use `Dataset.Write` for Parquet encoding:

<!-- illustrative -->
```go
// Correct: use Write for Parquet
snapshot, err := ds.Write(ctx, records, metadata)

// Wrong: StreamWriteRecords returns ErrCodecNotStreamable
_, err := ds.StreamWriteRecords(ctx, iter, metadata)
// err is ErrCodecNotStreamable
```

### Compression Layering

**Important:** Parquet has internal compression. Do not double-compress.

| Configuration | Result |
|--------------|--------|
| Parquet + `NewNoOpCompressor()` | ✅ Correct: Parquet handles compression internally |
| Parquet + `NewGzipCompressor()` | ❌ Wasteful: double compression, minimal benefit |
| Parquet + `WithParquetCompression(lode.ParquetCompressionSnappy)` | ✅ Correct: uses Parquet's internal Snappy |

### Error Handling

| Error | Cause |
|-------|-------|
| `ErrSchemaViolation` | Missing required field, type mismatch, invalid schema |
| `ErrInvalidFormat` | Malformed Parquet file, corrupted data |
| `ErrCodecNotStreamable` | `StreamWriteRecords` called with Parquet codec |

*Contract reference: [`CONTRACT_PARQUET.md`](docs/contracts/CONTRACT_PARQUET.md)*

### Per-File Statistics

The Parquet codec implements `StatisticalCodec` and automatically populates
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

Statistics are computed from Go record values during encoding. Orderable types
(int32, int64, float32, float64, string, timestamp) get min/max. All columns
get null count. Boolean and bytes columns have no min/max.

Codecs that do not implement `StatisticalCodec` (e.g., JSONL) produce no stats —
`FileRef.Stats` is nil and omitted from the manifest JSON.

---

## Metadata

Writes always take explicit caller-supplied metadata. Empty metadata is
valid; nil metadata is coalesced to `Metadata{}`.

---

## Write APIs

`Dataset.Write(ctx, data, metadata)` creates a snapshot from in-memory data.

`Dataset.StreamWrite(ctx, metadata)` returns a `StreamWriter` for single-pass
streaming writes of a single binary payload. `StreamWriter.Write` streams bytes,
`Commit` finalizes and returns a snapshot, and `Abort` discards the write.
If `Close` is called before `Commit`, the stream is aborted and no snapshot is created.
Streamed writes are raw-blob only: codecs are not applied and row count is `1`.

`Dataset.StreamWriteRecords(ctx, records, metadata)` consumes a pull-based iterator
of records and streams them through a streaming-capable codec. If the configured
codec does not support streaming, `StreamWriteRecords` returns an error.

### Streaming Constraints

| Constraint | StreamWrite | StreamWriteRecords |
|------------|:-----------:|:------------------:|
| Codec allowed | ❌ Returns `ErrCodecConfigured` | ✅ Required (streaming-capable) |
| Partitioning | ❌ Not supported | ❌ Returns `ErrPartitioningNotSupported` |
| Row count | Always `1` | Equals records consumed |
| Nil iterator | N/A | Returns `ErrNilIterator` |

**Why no partitioning?** Single-pass streaming cannot buffer records to partition
by key. Use `Write` for partitioned data.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §StreamWrite Semantics, §StreamWriteRecords Semantics*

### Streaming Troubleshooting

Common errors when using streaming APIs:

| Error | API | Cause | Resolution |
|-------|-----|-------|------------|
| `ErrCodecConfigured` | `StreamWrite` | Codec is configured | Remove codec or use `Write` |
| `ErrCodecNotStreamable` | `StreamWriteRecords` | Codec doesn't support streaming | Use streaming codec (JSONL) or `Write` |
| `ErrNilIterator` | `StreamWriteRecords` | Iterator is nil | Provide valid `RecordIterator` |
| `ErrPartitioningNotSupported` | `StreamWriteRecords` | Partitioning configured | Use `Write` for partitioned data |
**On failure:**
- No manifest is written (snapshot does not exist)
- Partial data may remain in storage (best-effort cleanup)
- Use `errors.Is()` to check sentinel errors (`ErrCodec*`, `ErrNil*`, `ErrPartitioning*`)

*Contract reference: [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md)*

---

## Usage Gotchas (Important)

- `nil` metadata is coalesced to `Metadata{}` (pass `nil` or `Metadata{}` for empty).
- Raw blob mode (no codec) requires exactly one `[]byte` element in `Write`.
- Raw blob mode cannot use partitioning (no record fields to extract keys).
- `WithHiveLayout` requires at least one partition key (validated on apply).
- `ListDatasets` returns `ErrNoManifests` when storage has objects but no manifests.
- Layouts that do not model datasets (e.g., flat) return `ErrDatasetsNotModeled`.
- `ListDatasets` results are lexicographic by ID; `ListPartitions` results are lexicographic by `Path`; `ListManifests` results are ordered by `(Partition, ID)`. `StartAfter` resumes strictly after a previous result; `Limit` bounds results, not store calls.
- `PartitionListOptions.Prefix` is a raw path prefix (`day=2026-10` matches `day=2026-10-18`) and requires a partition-aware layout (`ErrPartitionPrefixNotSupported` otherwise). It must be a canonical relative fragment — no leading `/`, no `.`/`..`/empty segments — or `ErrInvalidPath` is returned. An empty result under a prefix is not an error.
- `ReaderAt` may return an `io.ReaderAt` that also implements `io.Closer`; close it when done.
- Checksums are computed and recorded in manifests only when configured.
- Concurrent `Write`/`StreamWrite` calls to the same dataset are safe only with a CAS-capable store (`ConditionalWriter`). Without CAS, serialize writes externally. See `CONTRACT_WRITE_API.md` §Concurrent Blob Write Safety.
- `StreamWrite` is only valid when no codec is configured; otherwise it returns an error.
- `StreamWriteRecords` requires a streaming-capable codec; otherwise it returns an error.
- `StreamWriteRecords` does not support partitioning (single-pass streaming cannot partition).
- Aborted streams leave no snapshot; partial objects may remain (see "Safety Guarantees").

---

## Sidecar Files and Store Access

**Lode's manifest tracks only files written through Lode's write path.**

Files written via `Store.Put()` directly — bypassing `Dataset.Write`,
`StreamWrite`, or `StreamWriteRecords` — are not recorded in the manifest
and are not discoverable via `Snapshots()`, `Read()`, or any Dataset API.

This is by design. The manifest is authoritative because Lode produced the
files it describes: it knows their size, computed their checksums (when
configured), and applied the configured codec and compressor. Files Lode
didn't write cannot carry these guarantees.

### Correct Patterns for Binary Sidecar Files

| Pattern | When to Use | Completeness |
|---------|-------------|-------------|
| `StreamWrite` per file | Each sidecar is its own snapshot; manifest tracks it | Full — `Snapshots()` enumerates all committed blobs |
| `Write` with `[]byte` data | Small blobs that fit in memory | Full — manifest tracks the file |
| File inventory in `Metadata` | Caller manages storage; Lode tracks the inventory | Caller-verified — paths listed in metadata, existence checked by caller |

### StreamWrite for Sidecar Blobs (Recommended)

<!-- illustrative -->
```go
// Each sidecar file gets its own snapshot via StreamWrite
sw, _ := ds.StreamWrite(ctx, lode.Metadata{"filename": "model.bin", "run_id": "r-123"})
io.Copy(sw, file)
snapshot, _ := sw.Commit(ctx)
// snapshot.Manifest.Files[0] tracks the blob with size and checksum
```

### File Inventory in Metadata (When Managing Storage Directly)

<!-- illustrative -->
```go
// Caller writes files outside Lode and records paths in metadata
store.Put(ctx, "custom/path/model.bin", modelReader)
store.Put(ctx, "custom/path/index.bin", indexReader)

snapshot, _ := ds.Write(ctx, records, lode.Metadata{
    "sidecar_files": []string{"custom/path/model.bin", "custom/path/index.bin"},
})
// Caller can later read snapshot.Manifest.Metadata["sidecar_files"]
// to enumerate files, but must verify existence independently
```

**Anti-pattern:** Writing files via `Store.Put()` without tracking them in
metadata or using `StreamWrite`, then expecting the manifest to enumerate them.

---

## Choosing a Write API

- Use `Write` for in-memory data, partitioned data, or codecs that do not support streaming.
- Use `StreamWrite` for large binary payloads that should be streamed once (no codec).
- Use `StreamWriteRecords` for large record streams with streaming-capable codecs (no partitioning).

---

## Choosing a Compressor

| Compressor | Use When | Trade-offs |
|------------|----------|------------|
| `NewNoOpCompressor()` | Data is already compressed, or compression overhead not justified | No CPU cost; no size reduction |
| `NewGzipCompressor()` | Broad compatibility required (gzip is universal) | Good ratio; moderate speed |
| `NewZstdCompressor()` | Best compression ratio or fast decompression needed | Better ratio than gzip; faster decompression |

**Notes:**
- Compressor choice is recorded in manifests; readers must support the compressor used
- Compression is applied after codec encoding (if any)
- Streaming writes (`StreamWrite`, `StreamWriteRecords`) apply compression on-the-fly

*Contract reference: [`CONTRACT_LAYOUT.md`](docs/contracts/CONTRACT_LAYOUT.md) §Compressor*

---

## Storage Prerequisites

**Ensure storage exists before constructing a dataset, volume, or reader.**

Lode does not create storage infrastructure (directories, buckets). This is intentional:

- **No hidden side effects**: Constructors do not modify external state
- **Explicit provisioning**: Callers control when and how storage is created
- **Backend symmetry**: Same pattern for filesystem, S3, and other adapters

### Required Setup

| Backend | Prerequisite | Example |
|---------|--------------|---------|
| Filesystem | Directory must exist | `mkdir -p /data/lode` |
| S3 | Bucket must exist | `aws s3 mb s3://my-bucket` |
| Memory | None (in-process) | — |

### Bootstrap Pattern

If provisioning helpers are needed, implement them outside Lode's core APIs:

<!-- illustrative -->
```go
// Wrapper that ensures storage exists before constructing dataset
func EnsureFSDataset(id, root string, opts ...lode.Option) (lode.Dataset, error) {
    if err := os.MkdirAll(root, 0755); err != nil {
        return nil, fmt.Errorf("create storage root: %w", err)
    }
    return lode.NewDataset(id, lode.NewFSFactory(root), opts...)
}
```

This keeps Lode's constructors pure and predictable.

*Contract reference: [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md)*

---

## Safety Guarantees

### Commit Semantics

**Manifest presence is the commit signal.**

A snapshot becomes visible only after its manifest is successfully written to storage.
Data files may exist before the manifest, but they are not discoverable until the
manifest references them.

- `StreamWriter.Commit()` writes the manifest; the snapshot is invisible until then.
- `StreamWriter.Abort()` or `Close()` without `Commit()` ensures no manifest is written.
- On error during streaming, no manifest is written (the write never "happened").

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Read-after-write Visibility, [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §Commit Semantics*

### Concurrency and CAS

Lode uses optimistic concurrency (CAS) to detect conflicting writes.
Whether CAS is available depends on which storage adapter you use:

#### Adapter CAS Support

| Adapter | CAS | Concurrent writers | What happens on conflict |
|---------|-----|--------------------|--------------------------|
| **Memory** | ✅ | Safe without coordination | `ErrSnapshotConflict` — retry with `Latest()` |
| **S3** | ✅ | Safe without coordination | `ErrSnapshotConflict` — retry with `Latest()` |
| **FS (Unix)** | ✅ | Safe without coordination | `ErrSnapshotConflict` — retry with `Latest()` |
| **FS (non-Unix)** | ❌ | Single-writer only | Undefined — serialize writes externally |
| **Custom adapter** | Depends | Implement `ConditionalWriter` for CAS | Falls back to Delete+Put without it |

CAS is always-on when the adapter supports it. No configuration required.

#### With CAS (Memory, S3, FS on Unix)

Multiple **separate process instances** (or separate `Dataset`/`Volume` values)
MAY safely write to the same dataset or volume without external coordination.
A single `Dataset` or `Volume` value is NOT safe for concurrent use by multiple
goroutines.

On conflict (manual retry):
1. Catch `ErrSnapshotConflict`
2. Re-read state via `Latest()`
3. Merge or rebuild, then re-commit

On conflict (automatic retry):
Configure `WithRetryCount(n)` at construction time. The commit path
automatically refreshes state and re-parents the manifest on conflict,
with jittered exponential backoff. Data files are written once — only
the manifest and pointer CAS are retried. Customize backoff with
`WithRetryBaseDelay`, `WithRetryMaxDelay`, and `WithRetryJitter`.
Observe retries (e.g. for a retry counter metric) with
`WithRetryObserver(fn)`: `fn(attempt)` is called synchronously once per
retry, before the backoff sleep, with a 1-indexed attempt number.

Data files are immutable and already persisted — retry cost is one manifest
write plus one pointer swap.

#### Without CAS (FS on non-Unix, custom adapters without `ConditionalWriter`)

Callers MUST ensure at most one writer is active per dataset or volume at any
time. External coordination (locks, queues, leader election) is the caller's
responsibility. Without this, concurrent writers may corrupt snapshot history.

*Contract reference: [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Concurrency, [`CONTRACT_VOLUME.md`](docs/contracts/CONTRACT_VOLUME.md) §Concurrency, [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §ConditionalWriter Capability*

### Performance

CAS replaces Delete+Put (2 store calls) with a single `CompareAndSwap`,
reducing both latency and allocation count. Stores without `ConditionalWriter`
retain the existing Delete+Put path with no change.

Benchmarks on the in-memory adapter (20 sequential writes per iteration,
AMD Ryzen 9 5900XT):

| Metric | Delete+Put | CAS (ConditionalWriter) | Delta |
|--------|------------|-------------------------|-------|
| ns/op  | 161,603    | 136,200                 | −16%  |
| B/op   | 110,587    | 98,710                  | −11%  |
| allocs | 740        | 720                     | −3%   |

### Large Upload Guarantees

For uploads exceeding the storage adapter's atomic threshold (e.g., 5GB for S3),
the S3 adapter uses conditional completion (`If-None-Match` on `CompleteMultipartUpload`)
to provide the same atomic no-overwrite guarantee as small uploads.

Both upload paths now provide atomic no-overwrite guarantees:
- **Small uploads** (≤ 5GB): Atomic via `PutObject` with `If-None-Match`
- **Large uploads** (> 5GB): Atomic via `CompleteMultipartUpload` with `If-None-Match`

Preflight existence checks are retained as a fail-fast optimization to avoid
uploading parts for objects that already exist.

**Backend Compatibility Caveat:**

The atomic guarantee for large uploads (>5GB) depends on backend support for
`If-None-Match` on `CompleteMultipartUpload`:

| Backend | Conditional Multipart | Status |
|---------|----------------------|--------|
| Cloudflare R2 | Supported | ✅ Production verified |
| AWS S3 | Supported | ✅ Expected compatible |
| MinIO | Unknown | ⚠️ Test backend only |
| LocalStack | Unknown | ⚠️ Test backend only |

For backends marked ⚠️: these are used for integration testing only. If deploying
against a backend not listed as verified, test with large uploads (>5GB) before
relying on the multipart path, or ensure single-writer semantics at the application level.

*Contract reference: [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §Put Upload Paths, [`CONTRACT_WRITE_API.md`](docs/contracts/CONTRACT_WRITE_API.md) §Storage-Level Concurrency*

### Cleanup Behavior

**Cleanup of partial objects is best-effort, not guaranteed.**

On abort or error before commit:
- No manifest is written (the snapshot does not exist).
- A failed `Put` leaves nothing behind (`CONTRACT_STORAGE.md` §Put is all-or-nothing); the writer never deletes after a failed `Put`.
- A data object that was fully written but whose commit failed is deleted best-effort and may remain in storage.
- Callers should not rely on automatic cleanup of partial objects.
- Cleanup uses an independent context to maximize success even if the caller's
  context was canceled.

Failure to delete a partial object does not create a snapshot.

*Contract reference: [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md) §Streaming API Errors, [`CONTRACT_STORAGE.md`](docs/contracts/CONTRACT_STORAGE.md) §Streaming Write Atomicity*

---

## Retention

Lode never removes anything on its own. `Dataset.Expire` removes snapshots
the caller selects; which snapshots to keep, and for how long, is the
caller's policy (`CONTRACT_RETENTION.md`).

<!-- illustrative -->
```go
// Preview, then remove, one run of a Hive dataset.
req := lode.ExpireRequest{Partition: "source=s/category=c/day=2026-10-19/run_id=r1", DryRun: true}
preview, err := ds.Expire(ctx, req)
// preview.Expired, preview.Objects, preview.Bytes

req.DryRun = false
report, err := ds.Expire(ctx, req)
if report.RetainedHead != "" {
    // The newest snapshot is the head and is kept; expire it after a newer commit.
}
```

`ExpireRequest` fields intersect; at least one of them must be set:

| Field | Selects |
|-------|---------|
| `IDs` | These snapshots |
| `Partition` | Snapshots in this partition or beneath it, by whole segment (`run_id=r1` never matches `run_id=r10`). Hive layouts only |
| `Before` | Snapshots whose write began before this instant (read from the snapshot ID; no manifest is loaded to select) |

- **The head is never expired.** When the selection includes it, it is kept
  and reported in `ExpireReport.RetainedHead`; that is not an error.
- **Mixed snapshots are kept.** A snapshot selected by `Partition` that also
  has files outside it is reported in `Mixed`. Name it in `IDs` to expire it.
- **Expiry is recorded.** `Snapshot` and `Read` on an expired ID return
  `ErrSnapshotExpired`, which also matches `ErrNotFound`.
- **Expiry is resumable.** The report is non-nil on error and lists the
  work done; running the same request again completes the rest while any
  of a snapshot's manifests remain. Data objects left after its last
  manifest is deleted are no longer described by any snapshot.
- **Lode deletes only what it wrote.** Objects a caller put next to the
  dataset through `Store.Put` (sidecar files) are never deleted by
  `Expire`; delete them after their snapshots are expired.
- **Readers mid-read may fail.** A `Read` that loaded a manifest before its
  expiry began can see `ErrNotFound` on a data file. Lode has no read lease.

---

## Errors

Errors are returned for invalid configuration, storage failures, or
missing objects. Error semantics are stable and documented.

### Sentinel Errors

Use `errors.Is()` to check for sentinel errors:

<!-- illustrative -->
```go
snap, err := ds.Latest(ctx)
if errors.Is(err, lode.ErrNoSnapshots) {
    // Dataset exists but has no committed snapshots
}
```

| Sentinel | Meaning | Typical Source |
|----------|---------|----------------|
| `ErrNotFound` | Object/path does not exist | Storage, DatasetReader |
| `ErrNoSnapshots` | Dataset or Volume has no committed snapshots | Dataset, Volume |
| `ErrNoManifests` | Storage has objects but no valid manifests | DatasetReader |
| `ErrPathExists` | Write to existing path (immutability violation) | Storage |
| `ErrInvalidPath` | Key escapes root or is empty; prefix not canonical; bad `ReadRange` bounds | Storage, DatasetReader |
| `ErrInvalidArgument` | Caller-supplied value cannot be used (nil factory, bad offset, blob write shape, …) | Dataset, DatasetReader, Volume |
| `ErrDatasetsNotModeled` | Layout doesn't support dataset enumeration | DatasetReader |
| `ErrPartitionPrefixNotSupported` | `Prefix` set on a layout without partitions | DatasetReader |
| `ErrManifestInvalid` | Manifest fails validation | DatasetReader |
| `ErrCodecConfigured` | StreamWrite called with codec configured | Dataset |
| `ErrCodecNotStreamable` | StreamWriteRecords with non-streaming codec | Dataset |
| `ErrNilIterator` | Nil iterator passed to StreamWriteRecords | Dataset |
| `ErrOptionNotValidForDataset` | Option not applicable to Dataset | Dataset |
| `ErrOptionNotValidForDatasetReader` | Option not applicable to DatasetReader | DatasetReader |
| `ErrOptionNotValidForVolume` | Option not applicable to Volume | Volume |
| `ErrPartitioningNotSupported` | StreamWriteRecords with partitioning | Dataset |
| `ErrRangeReadNotSupported` | Store doesn't support range reads | Storage |
| `ErrRangeMissing` | Volume ReadAt range not fully committed | Volume |
| `ErrOverlappingBlocks` | Committed blocks overlap in cumulative manifest | Volume |
| `ErrSnapshotConflict` | Another writer committed since parent was resolved (CAS) | Dataset, Volume |
| `ErrSnapshotExpired` | Snapshot was removed by `Expire`; also matches `ErrNotFound` | Dataset |
| `ErrHeadUncommitted` | Latest pointer names a snapshot whose manifest is not written yet | Dataset.Expire |
| `ErrSchemaViolation` | Record doesn't conform to Parquet schema | Parquet Codec |
| `ErrInvalidFormat` | Malformed or corrupted Parquet file | Parquet Codec |

### Error Handling Guidelines

**Retry-safe:**
- Storage I/O errors (network, timeout) — may retry
- `ErrNotFound` during race — may retry if expecting eventual consistency
- `ErrSnapshotConflict` — re-read `Latest()`, merge state, re-commit (or use `WithRetryCount` for automatic retry)
- `ErrHeadUncommitted` — retry `Expire` once the in-flight commit completes

**Non-retry (configuration/logic error):**
- `ErrDatasetsNotModeled` — reconfigure with different layout
- `ErrInvalidArgument` — fix the call; the message names the value
- `ErrManifestInvalid` — data corruption, investigate source
- `ErrPathExists` — logic error (double-write attempt)
- Component mismatch — reconfigure dataset or use matching snapshot

*Contract reference: [`CONTRACT_ERRORS.md`](docs/contracts/CONTRACT_ERRORS.md)*

---

## S3 Storage Adapter

The `lode/s3` package provides an S3-compatible storage adapter.

**Public API:**
- `s3.New(client, config)` - Create store from AWS SDK client
- `s3.Config{Bucket, Prefix}` - Store configuration

Client construction uses the AWS SDK directly. This keeps Lode's API surface
minimal while giving you full control over credentials, endpoints, and options.

### AWS S3

<!-- illustrative -->
```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

    "github.com/pithecene-io/lode/lode/s3"
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

    "github.com/pithecene-io/lode/lode/s3"
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

**Bucket requirement:** The bucket must exist before use. Lode does not create buckets.

**Consistency:** AWS S3 provides strong read-after-write consistency.
Other backends may differ — consult their documentation.

---

## Examples

Examples under `examples/` use the public API only and demonstrate the
default bundle, explicit configuration, and required metadata.

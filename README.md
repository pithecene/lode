<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/lode-splash-dark.png">
  <img alt="Lode — structured persistence for object storage" src="docs/assets/lode-splash-light.png" width="100%">
</picture>

# Lode

**Structured persistence for object storage.**

[![Go Reference](https://pkg.go.dev/badge/github.com/pithecene-io/lode.svg)](https://pkg.go.dev/github.com/pithecene-io/lode)
[![Release](https://img.shields.io/github/v/release/pithecene/lode?sort=semver)](https://github.com/pithecene/lode/releases/latest)
[![CI](https://github.com/pithecene/lode/actions/workflows/ci.yml/badge.svg)](https://github.com/pithecene/lode/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/pithecene/lode)](LICENSE)

Lode is an embeddable Go persistence framework that brings snapshots,
metadata, and safe write semantics to object storage—without running a
database. It offers two coequal persistence paradigms: **Datasets** for
structured collections of named objects, and **Volumes** for sparse,
resumable byte-range storage.

---

## What Lode is (and is not)

**Lode is:**
- An embeddable Go library
- A persistence framework for structured data and sparse byte ranges
- Snapshots, atomic commits, and metadata guarantees you'd otherwise hand-roll on raw storage APIs
- Explicit and predictable by design

**Lode is not:**
- A database or storage engine
- A query planner or execution runtime
- A distributed system or background service

Lode owns *structure and lifecycle*, not execution.

---

## Why Lode exists

Many systems already use object storage as their primary persistence layer:

- Events written to S3
- Parquet files on a filesystem
- Analytics data dumped “by convention”

What’s usually missing is discipline:
- No clear snapshots or versions
- No metadata guarantees
- Accidental overwrites
- Ad-hoc directory layouts
- Unclear read/write safety

Lode formalizes the parts that should be formalized and refuses to do the rest.

---

## Core ideas

Lode is built around a small set of invariants:

**Dataset** (structured collections):
- **Datasets are immutable** — writes produce snapshots
- **Snapshots reference manifests** that describe data files and metadata
- **Storage, format, compression, and partitioning are orthogonal**

**Volume** (sparse byte ranges):
- **Volumes are sparse, resumable byte address spaces**
- **Volume commits track which byte ranges exist** — gaps are explicit, never zero-filled
- **Each commit produces a cumulative manifest** covering all committed blocks

**Shared invariants:**
- **Manifest presence is the commit signal** — a snapshot is visible only after its manifest is persisted
- **Metadata is explicit, persisted, and self-describing (never inferred)**
- **Safe concurrent writes** — CAS-capable stores detect conflicts automatically; others require single-writer discipline

If you know the snapshot ID, you know exactly what data you are reading.

---

## Quick Start (5 minutes)

### Install

<!-- runnable -->
```bash
go get github.com/pithecene-io/lode
```

### Write and read a blob

<!-- illustrative: shows API pattern; see examples/ for runnable code -->
```go
package main

import (
    "context"
    "fmt"
    "github.com/pithecene-io/lode/lode"
)

func main() {
    ctx := context.Background()

    // Create dataset with filesystem storage
    ds, _ := lode.NewDataset("mydata", lode.NewFSFactory("/tmp/lode-demo"))

    // Write a blob (default: raw bytes, no codec)
    snap, _ := ds.Write(ctx, []any{[]byte("hello world")}, lode.Metadata{"source": "demo"})
    fmt.Println("Created snapshot:", snap.ID)

    // Read it back
    data, _ := ds.Read(ctx, snap.ID)
    fmt.Println("Data:", string(data[0].([]byte)))
}
```

### Write structured records

<!-- illustrative: shows API pattern; see examples/ for runnable code -->
```go
// With a codec, Write accepts structured data
ds, _ := lode.NewDataset("events",
    lode.NewFSFactory("/tmp/lode-demo"),
    lode.WithCodec(lode.NewJSONLCodec()),
)

records := lode.R(
    lode.D{"id": 1, "event": "signup"},
    lode.D{"id": 2, "event": "login"},
)
snap, _ := ds.Write(ctx, records, lode.Metadata{"batch": "1"})
```

### Stream large files

<!-- illustrative: shows API pattern; see examples/ for runnable code -->
```go
// StreamWrite is for large binary payloads (no codec)
ds, _ := lode.NewDataset("backups", lode.NewFSFactory("/tmp/lode-demo"))

sw, _ := ds.StreamWrite(ctx, lode.Metadata{"type": "backup"})
sw.Write([]byte("... large data ..."))
snap, _ := sw.Commit(ctx)
```

### Write sparse byte ranges (Volume)

<!-- illustrative: shows API pattern; see examples/ for runnable code -->
```go
// Volume: sparse, resumable byte-range persistence
vol, _ := lode.NewVolume("disk-image", lode.NewFSFactory("/tmp/lode-demo"), 1024)

// Stage a byte range and commit
blk, _ := vol.StageWriteAt(ctx, 0, bytes.NewReader(data))
snap, _ := vol.Commit(ctx, []lode.BlockRef{blk}, lode.Metadata{"step": "boot"})

// Read it back
result, _ := vol.ReadAt(ctx, snap.ID, 0, len(data))
```

See [`examples/`](#examples) for complete runnable code.

---

## Which Write API?

| Paradigm | API | Use Case | Codec | Partitioning |
|----------|-----|----------|-------|--------------|
| Dataset | `Write` | In-memory data, small batches | ✅ Optional | ✅ Supported |
| Dataset | `StreamWrite` | Large binary payloads (GB+) | ❌ Raw only | ❌ Not supported |
| Dataset | `StreamWriteRecords` | Large record streams | ✅ Required (streaming) | ❌ Not supported |
| Volume | `StageWriteAt` + `Commit` | Sparse byte ranges, resumable | ❌ Raw only | ❌ Not applicable |

**Decision flow:**
1. Is the data already in memory? → Use `Write`
2. Is it a large binary blob (no structure)? → Use `StreamWrite`
3. Is it a large stream of records? → Use `StreamWriteRecords` with a streaming codec
4. Is it a sparse/resumable byte address space? → Use `Volume`

---

## Guarantees

What Lode commits to:

| Guarantee | Detail |
|-----------|--------|
| **Immutable snapshots** | Once written, data files and manifests never change |
| **Atomic commits** | Manifest presence is the commit signal; no partial visibility |
| **Explicit metadata** | Every snapshot has caller-supplied metadata (never inferred) |
| **Safe writes** | Overwrites are prevented; atomic for standard writes on all built-in adapters. Large uploads (>5GB) are atomic when the backend supports conditional multipart completion, best-effort otherwise ([details](PUBLIC_API.md#large-upload-guarantees)). |
| **Backend-agnostic** | Same semantics on filesystem, memory, or S3 |

What Lode explicitly does NOT provide:

| Non-goal | Why |
|----------|-----|
| Distributed coordination | Lode detects conflicts (CAS) but does not provide locks, consensus, or leader election |
| Query execution | Lode structures data; query engines consume it |
| Background compaction | No implicit mutations; callers control lifecycle |
| Automatic cleanup | Partial objects from failed writes may remain |

For full contract details: [`docs/contracts/`](docs/contracts/)

---

## Gotchas

Common pitfalls when using Lode:

- **Metadata defaults to empty** — `nil` metadata is coalesced to `Metadata{}`; pass `nil` or `Metadata{}` for empty metadata.
- **Raw mode expects `[]byte`** — Without a codec, `Write` expects exactly one `[]byte` element.
- **Concurrent writes need CAS-capable stores** — Without `ConditionalWriter`, concurrent writers may corrupt history. Built-in Memory and S3 adapters support CAS; FS supports CAS on Unix only (non-Unix FS falls back to single-writer Delete+Put).
- **Cleanup is best-effort** — Failed streams may leave partial objects in storage.
- **StreamWriteRecords requires streaming codec** — Not all codecs support streaming.

See [`PUBLIC_API.md`](PUBLIC_API.md) for complete usage guidance.

---

## Example: event storage

A canonical Lode workflow looks like this:

- Dataset: `events`
- Rows: timestamped events
- Partitioning: Hive-style by day (`dt=YYYY-MM-DD`)
- Format: JSON Lines
- Compression: gzip
- Backend: file system or S3

Each write produces a new snapshot.
Reads always target a snapshot explicitly.

---

## Supported Backends

- **Filesystem** — Local storage via `NewFSFactory`
- **In-memory** — Testing via `NewMemoryFactory`
- **S3** — AWS S3, MinIO, LocalStack, R2 via `lode/s3`

---

## Storage Prerequisites

**Ensure storage exists before constructing a dataset, volume, or reader.**

Lode does not create storage infrastructure. This is intentional:
- No hidden side effects in constructors
- Explicit provisioning keeps control with the caller
- Same pattern across all backends

### Filesystem

```bash
# Create directory before use
mkdir -p /data/lode
```

<!-- illustrative -->
```go
// Then construct dataset
ds, err := lode.NewDataset("events", lode.NewFSFactory("/data/lode"))
```

### S3

```bash
# Create bucket before use (via AWS CLI, console, or IaC)
aws s3 mb s3://my-bucket
```

<!-- illustrative -->
```go
// Then construct dataset (wrap Store in factory)
store, err := s3.New(client, s3.Config{Bucket: "my-bucket"})
factory := func() (lode.Store, error) { return store, nil }
ds, err := lode.NewDataset("events", factory)
```

### Bootstrap Helpers

If you need provisioning helpers, implement them outside core APIs:

<!-- illustrative -->
```go
// Example: ensure directory exists before constructing dataset
func EnsureFSDataset(id, root string, opts ...lode.Option) (lode.Dataset, error) {
    if err := os.MkdirAll(root, 0755); err != nil {
        return nil, fmt.Errorf("create storage root: %w", err)
    }
    return lode.NewDataset(id, lode.NewFSFactory(root), opts...)
}
```

This keeps Lode's core APIs explicit and predictable.

---

## Examples

| Example | Purpose | Run |
|---------|---------|-----|
| [`default_layout`](examples/default_layout) | Write → list → read with default layout | `go run ./examples/default_layout` |
| [`hive_layout`](examples/hive_layout) | Partition-first layout with Hive partitioner | `go run ./examples/hive_layout` |
| [`blob_upload`](examples/blob_upload) | Raw blob write/read (no codec, default bundle) | `go run ./examples/blob_upload` |
| [`manifest_driven`](examples/manifest_driven) | Demonstrates manifest-as-commit-signal | `go run ./examples/manifest_driven` |
| [`stream_write_records`](examples/stream_write_records) | Streaming record writes with iterator | `go run ./examples/stream_write_records` |
| [`parquet`](examples/parquet) | Parquet codec with schema-typed fields | `go run ./examples/parquet` |
| [`volume_sparse`](examples/volume_sparse) | Sparse Volume: stage, commit, read with gaps | `go run ./examples/volume_sparse` |
| [`vector_artifacts`](examples/vector_artifacts) | Vector artifact pipeline: embeddings, indices, pointers | `go run ./examples/vector_artifacts` |
| [`optimistic_concurrency`](examples/optimistic_concurrency) | CAS conflict detection and retry pattern | `go run ./examples/optimistic_concurrency` |
| [`s3`](examples/s3) | S3 adapter with LocalStack/MinIO | `go run ./examples/s3` |

Each example is self-contained and runnable. See the example source for detailed comments.

---

## Status

Lode is at **v0.10.0** and under active development.
APIs are stabilizing; the breaking changes still under consideration for v1.0 are
listed once, with their blast radius, in [`docs/V1_API_DECISIONS.md`](docs/V1_API_DECISIONS.md).

v0.10.0 adds nightly mutation-testing gates and the fixes they surfaced (all-or-nothing `Put`,
stream writes that fail fast, bounded `ReadRange`), completes the listing API with cursors
and deterministic order, classifies every refused argument under `ErrInvalidArgument`, and
restructures the internals around one head-pointer protocol, one path grammar and one
write pipeline — with no change on disk and benchmarks at parity. CAS-capable stores
(Memory, S3, FS on Unix) detect concurrent writers; others require single-writer discipline.

If you are evaluating Lode, focus on:
- snapshot semantics (Dataset and Volume)
- metadata visibility
- API clarity

Usage overview: [`PUBLIC_API.md`](PUBLIC_API.md)
Concrete usage: [`examples/`](examples/)
Benchmarks: [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md)
Implementation milestones: [`docs/IMPLEMENTATION_PLAN.md`](docs/IMPLEMENTATION_PLAN.md)

---

## License
Apache License 2.0. Copyright © 2026 Andrew Hu <me@andrewhu.nyc>.

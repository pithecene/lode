<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/lode-splash-dark.jpg">
  <img alt="Lode — an illustrated geological section: folded, faulted strata under a hill, a gold vein along the crest of the fold, and a mine shaft running down to it from a headframe" src="docs/assets/lode-splash-light.jpg" width="100%">
</picture>

# Lode

**Structured persistence for object storage.**

[![Go Reference](https://pkg.go.dev/badge/github.com/pithecene/lode.svg)](https://pkg.go.dev/github.com/pithecene/lode)
[![Release](https://img.shields.io/github/v/release/pithecene/lode?sort=semver)](https://github.com/pithecene/lode/releases/latest)
[![CI](https://github.com/pithecene/lode/actions/workflows/ci.yml/badge.svg)](https://github.com/pithecene/lode/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/pithecene/lode)](LICENSE)

Lode is an embeddable Go persistence framework that provides snapshots,
metadata, and safe write semantics on object storage, without requiring a
database. It offers **Datasets** for structured collections of named
objects and, in an experimental package (`lode/volume`), **Volumes** for
sparse, resumable storage of byte ranges.

---

## Quick Start

### Install

<!-- runnable -->
```bash
go get github.com/pithecene/lode
```

### Writing and Reading a Blob

<!-- illustrative: shows API pattern; see examples/ for runnable code -->
```go
package main

import (
    "context"
    "fmt"
    "github.com/pithecene/lode/lode"
)

func main() {
    ctx := context.Background()

    // Create a dataset on filesystem storage.
    ds, _ := lode.NewDataset("mydata", lode.NewFSFactory("/tmp/lode-demo"))

    // Write a blob (by default, raw bytes without a codec).
    snap, _ := ds.Write(ctx, []any{[]byte("hello world")}, lode.Metadata{"source": "demo"})
    fmt.Println("Created snapshot:", snap.ID)

    // Read the blob back.
    data, _ := ds.Read(ctx, snap.ID)
    fmt.Println("Data:", string(data[0].([]byte)))
}
```

### Writing Structured Records

<!-- illustrative: shows API pattern; see examples/ for runnable code -->
```go
// With a codec, Write accepts structured records.
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

Lode stores data on the filesystem (`NewFSFactory`), in memory
(`NewMemoryFactory`), or on S3-compatible storage (`lode/s3`). The storage
location must exist before a dataset is constructed (see [Storage
Prerequisites](PUBLIC_API.md#storage-prerequisites)). Streaming writes and
the experimental Volume API are described in [`PUBLIC_API.md`](PUBLIC_API.md),
and complete programs are listed in [`examples/`](examples/).

---

## Core Ideas

**Dataset** (structured collections):
- Each write produces a new, immutable snapshot.
- Each snapshot references a manifest that describes its data files and
  metadata.
- Storage, format, compression, and partitioning are independent.

**Volume** (sparse byte ranges; experimental package `lode/volume`):
- A volume is a sparse, resumable byte address space.
- Commits record which ranges exist; gaps are never filled with zeros.
- Each commit produces a cumulative manifest.

**Both:**
- A snapshot is visible only once its manifest is persisted.
- Metadata is explicit and persisted, never inferred.
- Stores that support CAS detect concurrent writers.

---

## Guarantees

- **Immutable snapshots**: data files and manifests never change once
  written.
- **Atomic commits**: a snapshot is visible only once its manifest exists.
- **Explicit metadata**: every snapshot carries metadata supplied by the
  caller, never inferred.
- **Safe writes**: existing objects are never overwritten, and stores that
  support CAS detect concurrent writers.

Lode does not coordinate distributed writers, execute queries, compact
data, or remove anything without being asked; [the concepts
guide](docs/guides/concepts.md#what-lode-guarantees) gives the details.

---

## Documentation

| Topic | Where |
|-------|-------|
| Storage model, commits, and choosing a write API | [Concepts guide](docs/guides/concepts.md) |
| Complete API usage, options, and common pitfalls | [`PUBLIC_API.md`](PUBLIC_API.md) |
| Expiring snapshots and reclaiming residual data | [Retention guide](docs/guides/retention.md) |
| Snapshot ID format and ordering | [Snapshot ID guide](docs/guides/snapshot-ids.md) |
| Normative behavior | [`docs/contracts/`](docs/contracts/) |
| Runnable programs | [`examples/`](examples/) |
| Performance | [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) |

---

## Status

Lode is at **v0.10.0** and under active development. Its APIs are
stabilizing; the breaking changes still under consideration for v1.0 are
listed in [`docs/V1_API_DECISIONS.md`](docs/V1_API_DECISIONS.md), and each
release is described in [`CHANGELOG.md`](CHANGELOG.md).

---

## License

Apache License 2.0 (see [`LICENSE`](LICENSE)). Copyright © 2026 Pithecene LLC.

Contact: contact@pithecene.sh. To report a vulnerability, follow
[`SECURITY.md`](SECURITY.md).

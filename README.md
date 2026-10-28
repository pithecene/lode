<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/lode-banner-dark.png">
  <img alt="Lode: structured persistence for object storage. An engraved section of folded, faulted strata, hand-colored in tarnished metals, cut by a gold-bearing lode" src="docs/assets/lode-banner-light.png" width="100%">
</picture>

# Lode

**Structured persistence for object storage.**

[![Go Reference](https://pkg.go.dev/badge/github.com/pithecene/lode.svg)](https://pkg.go.dev/github.com/pithecene/lode)
[![Release](https://img.shields.io/github/v/release/pithecene/lode?sort=semver)](https://github.com/pithecene/lode/releases/latest)
[![CI](https://github.com/pithecene/lode/actions/workflows/ci.yml/badge.svg)](https://github.com/pithecene/lode/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/pithecene/lode)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-pithecene.sh-1b1a17)](https://pithecene.sh/docs/lode)

Lode is an embeddable Go persistence framework that provides snapshots,
metadata, and safe write semantics on object storage, without requiring a
database. It offers **Datasets**, which hold structured collections of
named objects, and **Volumes**, which store sparse, resumable byte ranges
and reside in an experimental package (`lode/volume`).

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

    // Open a root over filesystem storage, and a blob dataset in it.
    blobs, _ := lode.Open(lode.FS("/tmp/lode-demo")).Blobs(ctx, "mydata")

    // Write a blob: one []byte per snapshot.
    snap, _ := blobs.Write(ctx, []byte("hello world"), lode.D{"source": "demo"})
    fmt.Println("Created snapshot:", snap.ID)

    // Read the blob back.
    data, _ := blobs.Read(ctx, snap.ID)
    fmt.Println("Data:", string(data))
}
```

### Writing Structured Records

<!-- illustrative: shows API pattern; see examples/ for runnable code -->
```go
// The codec fixes the record type: here Event, encoded as JSON Lines.
type Event struct {
    ID    int    `json:"id"`
    Event string `json:"event"`
    Day   string `json:"day"`
}

// Batch is the snapshot's metadata.
type Batch struct {
    Number int `json:"number"`
}

root := lode.Open(lode.FS("/tmp/lode-demo"))
events, _ := root.Records(ctx, "events", lode.JSONL[Event](),
    lode.WithLayout(lode.Hive("day").By(func(e Event) lode.Partition {
        return lode.Partition{"day": e.Day}
    })))

snap, _ := events.Write(ctx, []Event{
    {ID: 1, Event: "signup", Day: "2026-10-22"},
    {ID: 2, Event: "login", Day: "2026-10-22"},
}, Batch{Number: 1})
got, _ := events.Read(ctx, snap.ID) // []Event

var batch Batch
_ = snap.Metadata.Decode(&batch)

// A reader opens a view: it takes the layout from the manifests.
view, _ := lode.OpenReadOnly(lode.FS("/tmp/lode-demo")).Records(ctx, "events", lode.JSONL[Event]())
```

Lode stores data on the filesystem (`lode.FS`), in memory (`lode.Memory`),
or on S3-compatible storage (`s3.Factory`). The storage location must exist
before the caller opens a dataset (see [Storage
Prerequisites](PUBLIC_API.md#storage-prerequisites)).
[`PUBLIC_API.md`](PUBLIC_API.md) describes streaming writes and the
experimental Volume API, and [`examples/`](examples/) contains complete
programs.

---

## Core Ideas

A **Dataset**, which holds structured collections, follows these rules:
- Each write produces a new, immutable snapshot.
- Each snapshot references a manifest that describes its data files and
  metadata.
- A caller chooses storage, format, compression, and partitioning
  independently of one another.

A **Volume**, which stores sparse byte ranges and resides in the
experimental package `lode/volume`, follows these rules:
- A Volume is a sparse, resumable byte address space.
- Commits record which ranges exist, and Lode never fills a gap with zeros.
- Each commit produces a cumulative manifest.

Datasets and Volumes share these rules:
- A snapshot becomes visible only after Lode persists its manifest.
- Lode persists the metadata that the caller supplies and never infers
  metadata.
- Every store detects concurrent writers through compare-and-swap (CAS).

---

## Guarantees

- **Immutable snapshots**: Lode never modifies a data file or manifest
  after it persists the object.
- **Atomic commits**: a snapshot is visible only once its manifest exists.
- **Explicit metadata**: every snapshot carries metadata that the caller
  supplies, and Lode never infers metadata.
- **Safe writes**: Lode never overwrites an existing object, and every store
  detects concurrent writers through CAS.

Lode does not coordinate distributed writers, execute queries, compact
data, or delete objects unless the caller requests deletion. [The concepts
guide](docs/guides/concepts.md#guarantees-and-non-goals) defines each guarantee
and each exclusion.

Lode has two limitations by design. A backend that lacks conditional writes
supports writers in one process only, through `SingleProcess`, so writers
in separate processes require external coordination ([Concurrency and
CAS](PUBLIC_API.md#concurrency-and-cas)). On an S3-compatible service other
than AWS S3, the guarantees depend on conditional writes and consistency
that Lode does not verify ([Other S3-Compatible
Services](PUBLIC_API.md#other-s3-compatible-services)).

---

## Documentation

Pithecene publishes the guides, the public API reference, and the contracts
at [pithecene.sh/docs/lode](https://pithecene.sh/docs/lode). The repository
holds the same documents:

| Topic | Where |
|-------|-------|
| Storage model, commits, and write API selection | [Concepts guide](docs/guides/concepts.md) |
| Rules of the API that most often surprise a caller | [Usage pitfalls guide](docs/guides/pitfalls.md) |
| Complete API usage and options | [`PUBLIC_API.md`](PUBLIC_API.md) |
| Expiring snapshots and reclaiming residual data | [Retention guide](docs/guides/retention.md) |
| Snapshot ID format and ordering | [Snapshot ID guide](docs/guides/snapshot-ids.md) |
| Normative behavior | [`docs/contracts/`](docs/contracts/) |
| Runnable programs | [`examples/`](examples/) |
| Performance | [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) |
| Documentation conventions, for maintainers | [`docs/maintainers/`](docs/maintainers/) |

---

## Status

The current release of Lode is **v0.12.2**. [`docs/plans/V1_API_DECISIONS.md`](docs/plans/V1_API_DECISIONS.md)
lists the breaking changes still under consideration for v1.0, and
[`CHANGELOG.md`](CHANGELOG.md) describes each release.

---

## License

Pithecene LLC distributes Lode under the Apache License 2.0 (see
[`LICENSE`](LICENSE)). Copyright © 2026 Pithecene LLC.

Pithecene answers inquiries at contact@pithecene.sh.
[`SECURITY.md`](SECURITY.md) describes the procedure for reporting a
vulnerability.

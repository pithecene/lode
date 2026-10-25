# Lode — Implementation Plan (Contract-First)

This plan reflects Lode’s clarified scope: **persistence structure only**.
It introduces an explicit **contract-freezing phase** before any implementation
details harden.

Lode’s core principle:

> **Lode defines structure, not execution.**  
> It stores facts, not interpretations.

---

## Conceptual Stack

```
Data Producer (any language / system)
        ↓
Lode Write API (structure + metadata)
        ↓
Storage Adapter (FS, S3, etc.)
        ↓
Immutable Objects + Manifests
        ↓
External Readers (query engines, tools)
```

---

## Phase 0 — Foundations & Guardrails

### Goal
Establish repo discipline and enforce boundary constraints before adding behavior.

### Deliverables
- Repo skeleton with `lode/`, `internal/`, `docs/`, `examples/`
- `AGENTS.md` constraints applied
- Taskfile scaffolding (even if minimal)

### Mini-milestones
- [x] Guardrails exist and are visible
- [x] Task targets are present (build / test / lint)
- [x] No execution logic introduced

---

## Phase 0.5 — Contract Definitions (NO CODE)

### Goal
Freeze **interfaces and invariants** required for parallel implementation,
without committing to internal details.

This phase produces **authoritative documents**, not code.

### Deliverables

#### 0.5.1 Core Model Contract (`docs/contracts/CONTRACT_CORE.md`)
Defines:
- Dataset and snapshot identity
- Manifest self-description requirements
- Metadata explicitness and persistence
- Immutability rules and linear history

#### 0.5.2 Write API Contract (`docs/contracts/CONTRACT_WRITE_API.md`)
Defines:
- `Dataset.Write` semantics and error behavior
- Snapshot creation and parent linkage
- Empty dataset behavior
- Prohibited behaviors (no in-place mutation)

#### 0.5.3 Storage Adapter Contract (`docs/contracts/CONTRACT_STORAGE.md`)
Defines:
- Adapter obligations for `Put/Get/List/Exists/Delete`
- Safe write semantics (no overwrite)
- Atomic commit expectations (manifest presence as commit signal)

#### 0.5.4 Layout & Component Contract (`docs/contracts/CONTRACT_LAYOUT.md`)
Defines:
- Logical key layout ownership
- Partitioner / compressor / codec roles
- NoOp components and explicit recording
- Optional codec semantics for raw-byte data
- Canonical object key persistence

#### 0.5.5 Iterator Contract (`docs/contracts/CONTRACT_ITERATION.md`)
Defines:
- Object iterator lifecycle semantics
- Close/Err/Next behavior guarantees

#### 0.5.6 Read API Contract (Stretch) (`docs/contracts/CONTRACT_READ_API.md`)
Defines:
- Adapter-agnostic read surface
- Range-read requirements
- Manifest-driven discovery

#### 0.5.7 Public API Spec (`PUBLIC_API.md`)
Defines:
- Public entrypoints and defaults
- Option-based configuration shape
- Default bundle behavior

### Exit criteria
- All contract documents exist
- Ambiguity is resolved by contract references
- Phase 0 plan assertions are captured in contracts
- Public API spec exists and documents defaults and options

---

## Phase 1 — Core Persistence Skeleton

### Goal
Implement the minimal, end-to-end write path with immutable snapshots.

### Deliverables
- Dataset abstraction
- Manifest schema and persistence
- Snapshot creation with explicit metadata
- Safe write semantics enforced

### Mini-milestones
- [x] Write → manifest → snapshot works
- [x] No metadata inference or defaults
- [x] Empty dataset behavior matches contracts

---

## Phase 2 — Adapters & Components

### Goal
Make storage and format choices pluggable without changing semantics.

### Deliverables
- Filesystem and in-memory adapters
- JSONL codec
- Gzip compression
- Hive-style partitioner
- NoOp components for partitioning/compression
- **Unified Layout abstraction** (publicly single, internally composable)
- **Default layout** (dataset-modeled, segment-anchored, flat)
- Curated layout implementations (default + hive-style + flat)

### Mini-milestones
- [x] Manifests record component names
- [x] No nil component handling
- [x] Layout is portable across adapters

---

## Phase 3 — Read Surface (Optional)

### Goal
Provide a minimal read facade for external consumers.

### Deliverables
- Dataset / partition / segment listing
- Manifest access
- Object open and random access via adapter
- Layout-aware discovery and path parsing
- Layout-specific dataset errors (when datasets are not modeled)

### Mini-milestones
- [x] Range reads are true range reads
- [x] Manifests are sufficient for discovery
- [x] No planning or query logic added

---

## Phase 4 — Hardening

### Goal
Stabilize behavior and validate invariants with tests and examples.

### Deliverables
- Error taxonomy and test coverage
- Example: write → list → read
- Example: single-object blob upload (public API only)
- Example: manifest-driven discovery (pattern, not a layout)
- Explicit metadata visibility on disk
- Examples for default layout and hive-style layout

### Mini-milestones
- [x] Tests enforce immutability
- [x] Examples align with contracts
- [x] Single-object blob example uses default bundle and explicit metadata
- [x] No hidden state detected

---

## Phase 5 — Expansion Experiments (Optional)

### Goal
Explore new adapters or codecs without expanding the public API.

### Deliverables
- S3 adapter (if needed)
- Parquet codec
- Manifest stats (optional, additive)

### Mini-milestones
- [x] Minimal API surface (only `s3.New` and `s3.Config` exported)
- [x] No backend-specific conditionals
- [x] CONTRACT_STORAGE.md compliance verified
- [x] Zstd compressor added as an additive compression option
- [x] Parquet codec implemented
- [x] Manifest stats extensions finalized (additive)

### S3 Adapter

**Status**: Promoted to public API (`lode/s3`)

The S3 adapter is a stable public component supporting AWS S3, MinIO,
LocalStack, Cloudflare R2, and other S3-compatible object stores.
Client construction uses AWS SDK directly (see PUBLIC_API.md for examples).

**Location**: `lode/s3/`

**Consistency notes**:
- S3 provides strong read-after-write consistency (since December 2020)
- List operations are also strongly consistent
- Commit semantics rely on manifest presence: write data objects before manifest

**Integration testing**:
- Gated behind `LODE_S3_TESTS=1` environment variable
- Requires Docker Compose for LocalStack/MinIO services
- Run: `task s3:up && task s3:test && task s3:down`

**Usage**:
```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

    "github.com/pithecene/lode/lode/s3"
)

// AWS S3 (use standard AWS SDK config)
cfg, _ := config.LoadDefaultConfig(ctx)
client := awss3.NewFromConfig(cfg)
store, _ := s3.New(client, s3.Config{Bucket: "my-bucket", Prefix: "datasets/"})
```

---

## Contract Change Protocol

Any change that affects contract behavior must:
1. Update the relevant `docs/contracts/CONTRACT_*.md` file.
2. Include a compatibility note (breaking vs additive).
3. Avoid expanding public API without explicit justification.

---

## Post-v0.4 Roadmap (Planned)

### Priority Track A — Storage Safety Hardening

- [x] Evaluate and adopt stronger S3 conditional-write guarantees for multipart paths
- [x] Update storage contract/docs with backend-specific guarantee notes where needed
- [x] Add integration coverage for large-upload overwrite prevention paths

### Priority Track B — Format and Ecosystem

- [x] Prioritize Parquet codec delivery
- [x] Define additive manifest stats needed for Parquet-oriented pruning workflows
- [x] Add/refresh examples for columnar and streaming workflows

### Priority Track C — Zarr/Xarray Direction

- [ ] Design reference-based path for GRIB2/NetCDF-to-Zarr workflows within Lode boundaries
- [ ] Define docs/examples for chunk/region safety expectations
- [ ] Evaluate native Zarr encoding path after reference-based workflow is validated

### Priority Track D — Vector Artifact Storage

**Status**: Validated — no new API surface required.

Lode's existing persistence primitives cover the full vector artifact lifecycle:
- Embedding batches as raw blob snapshots (`Write`)
- Serialized indices via `StreamWrite` (large binary payloads)
- `Latest()` as an atomic active-index pointer
- Snapshot history for rollback and version progression
- Explicit metadata for provenance and rebuild contracts

**Scope boundary**: Lode is durability infrastructure, not a vector database.
It stores embedding batches and serialized indices as opaque blobs.
Similarity search, index construction, and query execution remain the caller's responsibility.

**Deliverables**:
- [x] `examples/vector_artifacts/` — end-to-end pipeline example

**Deferred**:
- Custom codec or format work for vector-specific encodings (not needed; raw blobs suffice)
- ANN index introspection or validation (execution concern, out of scope)

---

## Phase 6 — Dual Persistence Paradigms (v0.6+)

### Goal
Introduce a second first-class persistence paradigm, `Volume`, alongside `Dataset`,
without blurring Lode's scope boundary.

### Framing Principle

**Dataset streaming builds complete objects before committing truth; Volume streaming commits truth incrementally.**

### Product Direction

- `Dataset` remains complete, structured, object-centric persistence
- `Volume` adds sparse, resumable, range-addressable byte-space persistence
- Both abstractions are coequal and explicit; neither is implemented as a special case of the other
- Runtime concerns (torrent peers, scheduling, networking, execution policy) remain outside Lode

### Phase-0 Volume Deliverables

- [x] Public `Volume` abstraction and constructor shape documented
- [x] Volume snapshot manifest model documented (`volume_id`, `total_length`, committed blocks)
- [x] Explicit range-read semantics documented (missing ranges are errors, no zero-fill inference)
- [x] Filesystem and memory validation examples for sparse/resumable flows
- [x] Restart/resume correctness tests for committed blocks

### Dataset Streaming Boundary (Retained)

- [x] Clarify in docs and API guidance that Dataset streaming is atomic object construction
- [x] Preserve guarantee: object is either fully committed in snapshot or absent
- [x] Explicitly route sparse/resumable/range-verified workflows to Volume

### Phase-0 Exclusions

- [x] No torrent protocol concepts (pieces, peers, trackers)
- [x] No networking, scheduling, or runtime orchestration
- [x] No hash policy ownership in Lode core
- [x] No deployables/daemons added to Lode

---

## v0.6 Execution Plan (Volume Implementation)

### Design Decisions (Resolved)

| ID | Decision | Resolution |
|----|----------|------------|
| DD-1 | Naming disambiguation | `Reader` → `DatasetReader`. `SegmentRef` → `ManifestRef` (DatasetReader), `BlockRef` (Volume). `ListSegments` → `ListManifests`. Volume uses composed `VolumeWriter` + `VolumeReader` interface. |
| DD-2 | Manifest model | Cumulative. Each snapshot is self-contained. |
| DD-3 | Storage layout | Fixed internal layout. Layouts are Dataset-specific. |
| DD-4 | Staged data lifecycle | Direct to final path. Manifest = visibility. No Abort API. |
| DD-5 | Resume semantics | Explicit/caller-driven. No uncommitted data discovery. |
| DD-6 | VolumeOption | `WithVolumeChecksum` only for v0.6. |
| DD-7 | Type naming | `Snapshot` → `DatasetSnapshot`, `SnapshotID` → `DatasetSnapshotID`. Volume gets symmetric types. |
| DD-8 | Overlap validation | Strict on full cumulative manifest. Supersession explored later. |

Additional resolved items:
- Empty commits (no new blocks) are invalid
- No block count limit for v0.6
- `ErrOverlappingBlocks` sentinel added
- S3 validation deferred to post-v0.6
- Unix nanosecond IDs (consistent with Dataset)
- Future roadmap: CAS (multi-process) + parallel staging (single-process)

### PR1 — Contract Finalization and Design Decisions
- [x] Finalize `CONTRACT_VOLUME.md` as authoritative (remove draft, apply DD-1–DD-8)
- [x] Add concurrency matrices to `CONTRACT_WRITE_API.md` and `CONTRACT_VOLUME.md`
- [x] Add `ErrOverlappingBlocks` to `CONTRACT_ERRORS.md`
- [x] Add Layout scope note to `CONTRACT_LAYOUT.md` (Dataset-specific)
- [x] Update `PUBLIC_API.md` Volume section with finalized API surface
- [x] Update `IMPLEMENTATION_PLAN.md` with resolved decisions

### PR2 — Volume Types + Dataset Rename
- [x] Rename `Snapshot` → `DatasetSnapshot`, `SnapshotID` → `DatasetSnapshotID` in `api.go`
- [x] Rename `Reader` → `DatasetReader`, `NewReader` → `NewDatasetReader`
- [x] Rename `SegmentRef` → `ManifestRef`, `ListSegments` → `ListManifests` in DatasetReader API
- [x] Add Volume types: `VolumeID`, `VolumeSnapshotID`, `BlockRef`, `VolumeManifest`, `VolumeSnapshot`
- [x] Add sentinels: `ErrRangeMissing`, `ErrOverlappingBlocks`
- [x] Add `VolumeOption` type with `WithVolumeChecksum`
- [x] Cascade renames through all code, tests, and examples

### PR3 — Core Volume Implementation (Stage + Commit + ReadAt)
- [x] `NewVolume` constructor with validation
- [x] `StageWriteAt`: write data to final path, return `BlockRef`
- [x] `Commit`: validate, build cumulative manifest, write to store
- [x] `ReadAt`: resolve range from manifest, read from store
- [x] `Latest` / `Snapshots` / `Snapshot` for history access
- [x] Fixed layout: `volumes/<id>/snapshots/<snap>/manifest.json`, `volumes/<id>/data/<offset>-<length>.bin`
- [x] Overlap validation (sort + sweep on full cumulative block set)
- [x] Works with fsStore and memoryStore

### PR4 — Comprehensive Test Suite
- [x] Sparse write patterns, multi-snapshot progression
- [x] Resume pattern: new Volume instance, load latest, continue staging
- [x] Edge cases: zero-length read, boundary reads, offset validation
- [x] Missing range errors, overlap rejection
- [x] Checksum validation when configured
- [x] FS and memory store coverage
- [x] Manifest round-trip (serialize → deserialize → validate)

### PR5 — Example: Sparse Volume Ranges
- [x] Runnable `examples/volume_sparse/` demonstrating stage → commit → read
- [x] Demonstrates `ErrRangeMissing` for uncommitted ranges
- [x] Demonstrates incremental commits and completeness
- [x] Follows `CONTRACT_EXAMPLES.md` conventions

### PR6 — Documentation Finalization + Release Prep
- [x] Remove all "planned"/"draft" language from shipped Volume features
- [x] Update README with Volume overview
- [x] Update `docs/maintainers/TEST_MATRIX.md` with Volume coverage
- [x] Final cross-reference validation across all docs

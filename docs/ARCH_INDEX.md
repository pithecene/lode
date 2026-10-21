# ARCH_INDEX.md — Lode Architecture Index

This file is a **navigation map** of Lode’s subsystems.
It summarizes *what exists and where*, not how things are implemented.

Normative behavior is defined in `docs/contracts/CONTRACT_*.md`.
Public API shape and defaults are defined in `PUBLIC_API.md`.

Maintenance rule:
Update this file only when a new subsystem boundary becomes real.
Do not update for internal refactors.

---

## Root

- `AGENTS.md` — development guardrails and agent constraints
- `CHANGELOG.md` — release history
- `CLAUDE.md` — repository constitution and AI governance
- `Taskfile.yaml` — task orchestration and developer workflows
- `go.mod` — Go module definition
- `mise.toml` — toolchain and task runner configuration
- `PUBLIC_API.md` — developer-facing public API spec (product contract)
- `README.md` — project overview and goals
- `CONTRIBUTING.md` — contribution guidelines
- `SECURITY.md` — supported versions and vulnerability reporting

---

## docs/

Plans, contracts, and research notes. Contracts define **system behavior**.

- `IMPLEMENTATION_PLAN.md` — contract-first, phased implementation plan
- `V1_READINESS.md` — v1.0 release criteria and dogfooding evidence
- `dogfood_audit_2026-10.md` — full-history production dataset audit (evidence for `V1_READINESS.md`)
- `V1_API_DECISIONS.md` — breaking API changes held for v1.0, decided once
- `contracts/CONTRACT_CORE.md` — core model invariants and immutability
- `contracts/CONTRACT_WRITE_API.md` — write API semantics and errors
- `contracts/CONTRACT_STORAGE.md` — storage adapter obligations
- `contracts/CONTRACT_LAYOUT.md` — layout, partitioning, and component rules
- `contracts/CONTRACT_ITERATION.md` — object iterator lifecycle semantics
- `contracts/CONTRACT_COMPOSITION.md` — store composition determinism
- `contracts/CONTRACT_PARQUET.md` — Parquet codec semantics and schema rules
- `contracts/CONTRACT_READ_API.md` — read API contract and adapter obligations
- `contracts/CONTRACT_ERRORS.md` — error taxonomy and handling guidelines
- `contracts/CONTRACT_TEST_MATRIX.md` — contract-to-test traceability and coverage gaps
- `contracts/CONTRACT_EXAMPLES.md` — example and callsite conventions
- `contracts/CONTRACT_VOLUME.md` — contract for Volume persistence model
- `contracts/CONTRACT_COMPLEXITY.md` — complexity bounds for all public operations
- `contracts/CONTRACT_RETENTION.md` — snapshot expiry and unreferenced-object sweep
- `BENCHMARKS.md` — benchmark results and performance baselines
- `DESIGN_SCOPED_CONCURRENCY.md` — scoped concurrency design for non-overlapping writers
- `SNIPPET_POLICY.md` — markdown code fence conventions
- `guides/` — explanatory guides (non-normative): `concepts.md` (storage model, commits, choosing a write API), `retention.md` (expiry, tombstones, sweep), `snapshot-ids.md` (ID format, ordering, uniqueness)
- `assets/` — README splash (light and dark JPEG); `assets/source/` holds the illustrated section and `render.sh` that regenerate it

Contracts are authoritative over code.

---

## lode/

Public API surface.

- `api.go` — core public interfaces, types, and error sentinels
- `s3/` — S3-compatible storage adapter
- `volume/` — Volume: sparse byte-range persistence (experimental; outside the v1.0 promise, `V1_API_DECISIONS.md` D9)

---

## internal/

Internal implementations.

- `testutil/` — internal test helpers, including the fault-injection store
- `storepath/` — key, prefix, and range rules shared by every Store adapter
- `history/` — snapshot-history core (snapshot chain, latest pointer, CAS commit loop) shared by every container
- `retry/` — CAS-conflict retry configuration and backoff shared by every container
- `validate/` — argument and manifest validation errors shared by every package (owns the sentinels `lode` re-exports as `ErrInvalidArgument`, `ErrManifestInvalid`)

---

## examples/

Example usage and integration references; `README.md` indexes them.

- `default_layout/` — write → list → read with DefaultLayout
- `hive_layout/` — partition-first layout with Hive partitioner
- `manifest_driven/` — manifest-driven discovery demonstration
- `blob_upload/` — raw blob write/read with default bundle
- `stream_write_records/` — streaming record writes with iterator
- `parquet/` — Parquet codec with schema-typed fields
- `volume_sparse/` — sparse Volume: stage, commit, read with gaps
- `optimistic_concurrency/` — CAS conflict detection and retry pattern
- `vector_artifacts/` — vector artifact pipeline: embeddings, indices, active pointers
- `retention/` — expire runs by partition with a dry-run preview, then sweep
- `s3/` — S3 adapter example

---

## scripts/

Build and maintenance scripts.

- `verify-snippets.sh` — validates runnable code fences in documentation
- `fuzz.sh` — runs every Go fuzz target for a fixed duration (`task fuzz`)

---

## ai/

AI governance: repo-local skill definitions.

- `skills/repo-convention-enforcer/v1/` — structural convention validator (SKILL.md format)

---

## .github/

CI/CD workflows (GitHub Actions).

- `workflows/ci.yml` — continuous integration
- `workflows/nightly.yml` — nightly checks
- `workflows/release.yml` — release automation

---

## Architectural Notes

- Lode defines persistence structure: datasets, snapshots, manifests, metadata
- Execution concerns (planning, scheduling, optimization) are out of scope
- Metadata is explicit and persisted; snapshots and data files are immutable
- Layout and semantics must remain backend-agnostic

This index is intentionally stable and low-detail.

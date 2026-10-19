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

---

## docs/

Plans, contracts, and research notes. Contracts define **system behavior**.

- `IMPLEMENTATION_PLAN.md` — contract-first, phased implementation plan
- `V1_READINESS.md` — v1.0 release criteria and dogfooding evidence
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
- `BENCHMARKS.md` — benchmark results and performance baselines
- `DESIGN_SCOPED_CONCURRENCY.md` — scoped concurrency design for non-overlapping writers
- `SNIPPET_POLICY.md` — markdown code fence conventions

Contracts are authoritative over code.

---

## lode/

Public API surface.

- `api.go` — core public interfaces, types, and error sentinels
- `s3/` — S3-compatible storage adapter

---

## internal/

Internal implementations.

- `testutil/` — internal test helpers
- `storepath/` — key, prefix, and range rules shared by every Store adapter

---

## examples/

Example usage and integration references.

- `default_layout/` — write → list → read with DefaultLayout
- `hive_layout/` — partition-first layout with Hive partitioner
- `manifest_driven/` — manifest-driven discovery demonstration
- `blob_upload/` — raw blob write/read with default bundle
- `stream_write_records/` — streaming record writes with iterator
- `parquet/` — Parquet codec with schema-typed fields
- `volume_sparse/` — sparse Volume: stage, commit, read with gaps
- `optimistic_concurrency/` — CAS conflict detection and retry pattern
- `vector_artifacts/` — vector artifact pipeline: embeddings, indices, active pointers
- `s3/` — S3 adapter example

---

## scripts/

Build and maintenance scripts.

- `verify-snippets.sh` — validates runnable code fences in documentation

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

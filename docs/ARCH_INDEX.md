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

Documentation, grouped by audience. Contracts define **system behavior**.

Readers:

- `guides/` — explanatory guides (non-normative): `concepts.md` (storage model, commits, choosing a write API), `pitfalls.md` (rules of the API that most often surprise a caller), `retention.md` (expiry, tombstones, sweep), `snapshot-ids.md` (ID format, ordering, uniqueness)
- `BENCHMARKS.md` — benchmark results and performance baselines

Contracts (`contracts/`), by subject:

- Data model: `CONTRACT_CORE.md` (core model invariants and immutability), `CONTRACT_VOLUME.md` (Volume persistence model)
- Writing and reading: `CONTRACT_WRITE_API.md` (write semantics), `CONTRACT_READ_API.md` (read semantics and adapter obligations), `CONTRACT_ITERATION.md` (object iterator lifecycle), `CONTRACT_RETENTION.md` (snapshot expiry and unreferenced-object sweep)
- Storage and formats: `CONTRACT_STORAGE.md` (storage adapter obligations), `CONTRACT_COMPOSITION.md` (store composition determinism), `CONTRACT_LAYOUT.md` (layout, partitioning, and component rules), `CONTRACT_PARQUET.md` (Parquet codec semantics and schema rules)
- Errors and bounds: `CONTRACT_ERRORS.md` (error taxonomy), `CONTRACT_COMPLEXITY.md` (complexity bounds for all public operations)
- Repository conventions: `CONTRACT_EXAMPLES.md` (example and callsite conventions)

Maintainers (`maintainers/`):

- `SNIPPET_POLICY.md` — markdown code fence conventions
- `DOCS_SITE.md` — the documents that pithecene.sh publishes, and how a renamed document reaches the site

Plans and records (`plans/`):

- `IMPLEMENTATION_PLAN.md` — contract-first, phased implementation plan
- `V1_READINESS.md` — v1.0 release criteria and dogfooding evidence
- `V1_API_DECISIONS.md` — breaking API changes held for v1.0, decided once
- `dogfood_audit_2026-10.md` — full-history production dataset audit (evidence for `V1_READINESS.md`)
- `DESIGN_SCOPED_CONCURRENCY.md` — scoped concurrency design for non-overlapping writers

Assets:

- `assets/` — README banner (light and dark PNG); `assets/source/` holds `plate.py` (the engraved section), `banner.html`, and `render.sh` that regenerate it

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
- `chaos/` — randomized concurrency harness with invariant checks, run on demand (`task chaos`, Chaos workflow)
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
- `release-notes.sh` — extracts the release notes from the message of an annotated tag
- `fuzz.sh` — runs every Go fuzz target for a fixed duration (`task fuzz`)

---

## ai/

AI governance: repo-local skill definitions and agent bookkeeping.

- `skills/repo-convention-enforcer/v1/` — structural convention validator (SKILL.md format)
- `TEST_MATRIX.md` — contract-to-test traceability that agents maintain with each contract or test change

---

## .github/

CI/CD workflows (GitHub Actions).

- `workflows/ci.yml` — continuous integration
- `workflows/nightly.yml` — nightly checks (run on demand)
- `workflows/chaos.yml` — chaos harness (run on demand)
- `workflows/release.yml` — release automation
- `actions/release-check/` — installs the internal release check at its pinned commit

---

## Architectural Notes

- Lode defines persistence structure: datasets, snapshots, manifests, metadata
- Execution concerns (planning, scheduling, optimization) are out of scope
- Metadata is explicit and persisted; snapshots and data files are immutable
- Layout and semantics must remain backend-agnostic

This index is intentionally stable and low-detail.

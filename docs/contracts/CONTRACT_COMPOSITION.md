# Lode Store Composition — Contract

This document defines the deterministic behavior of store composition.

---

## Goals

1. Deterministic behavior derived from explicit configuration.
2. No concealed ordering and no flags specific to a backend.

---

## Required Semantics

- Exactly one base store is required.
- The order in which wrappers are applied is **defined by the user** and
  must be respected.
- Reordering wrappers MUST NOT affect the correctness of reads or writes.
- The order of wrappers MAY affect observability (metrics, caching) but
  not results.

---

## Prohibited Behaviors

- Implicit sorting or reordering of wrappers
- Conditional behavior specific to a backend

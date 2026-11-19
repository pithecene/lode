# Lode Store Composition Contract

This document defines the deterministic behavior of store composition.

---

## Goals

1. Deterministic behavior derived from explicit configuration.
2. No concealed ordering and no flags specific to a backend.

---

## Required Semantics

- A composition requires exactly one base store.
- The composition applies wrappers in the explicit order that the **user
  defines**.
- Purely observational wrappers (such as logging or metrics) MUST NOT alter
  the results of reads or writes when reordered.
- Semantic wrappers (such as `Prefixed`, which scopes keys, or
  `SingleProcess`, which serializes in-process conditional writes over an
  `ObjectStore`) compose strictly from inside out in the caller's order and
  MUST preserve the store capabilities of the wrapped store where applicable.

---

## Prohibited Behaviors

- Implicit sorting or reordering of wrappers
- Conditional behavior specific to a backend

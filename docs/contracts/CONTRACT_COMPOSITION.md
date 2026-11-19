# Lode Store Composition Contract

This document defines the deterministic behavior of store composition.

---

## Goals

1. Deterministic behavior derived from explicit configuration.
2. No concealed ordering and no flags specific to a backend.

---

## Required Semantics

- A composition requires exactly one base store.
- The composition applies wrappers in the explicit order that the **caller
  defines**.
- Purely observational wrappers (such as logging or metrics) MUST NOT alter
  the results of reads or writes when reordered.
- Semantic wrappers (such as `Prefixed`, which scopes keys beneath a prefix,
  `SingleProcess`, which serializes in-process conditional writes over an
  `ObjectStore`, and the read-only store wrapper of `OpenReadOnly`) compose
  strictly from inside out in the caller's order and MUST preserve optional
  store capabilities (`RangeReader`, `PrefixLister`) whenever the wrapped
  store implements them.

---

## Prohibited Behaviors

- Implicit sorting or reordering of wrappers
- Conditional behavior specific to a backend

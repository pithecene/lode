# Lode Store Composition Contract

This document defines the deterministic behavior of store composition.

---

## Goals

1. Deterministic behavior derived from explicit configuration.
2. No concealed ordering and no flags specific to a backend.

---

## Required Semantics

- A composition requires exactly one base store.
- The **user defines** the order in which wrappers apply, and the
  composition applies them in that order.
- Reordering wrappers MUST NOT affect the correctness of reads or writes.
- Wrapper order MAY affect observability, such as recorded metrics, and
  performance, such as cache hit rate, but not results.

---

## Prohibited Behaviors

- Implicit sorting or reordering of wrappers
- Conditional behavior specific to a backend

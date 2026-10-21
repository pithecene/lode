# Lode Object Iteration — Contract

This document defines the lifecycle semantics for object iteration.

---

## Goals

1. Predictable behavior over the iterator's lifecycle.
2. Safe handling of resources and idempotent closing.

---

## Interface Shape (Conceptual)

```
type ObjectIterator interface {
    Next() bool
    Ref() ObjectRef
    Err() error
    Close() error
}
```

---

## Required Semantics

- The order of results is unspecified.
- Pagination is unspecified.
- `Next()` MUST return false once the iterator is exhausted or after
  `Close()` is called.
- `Close()` MUST be idempotent.
- `Err()` MAY be called after the iterator is exhausted or closed.
- Implementations MUST release resources on `Close()` or on exhaustion.

---

## Prohibited Behaviors

- Implicit guarantees of order
- Concealed buffering that alters visibility semantics

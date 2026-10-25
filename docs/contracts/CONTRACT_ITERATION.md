# Lode Object Iteration Contract

This document defines the lifecycle semantics for object iteration.

---

## Goals

1. Predictable behavior over the iterator's lifecycle.
2. Safe resource handling and idempotent closing.

---

## Conceptual Iterator Interface

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

- Result order is unspecified.
- Pagination is unspecified.
- `Next()` MUST return false once the iterator is exhausted or once a
  caller has invoked `Close()`.
- `Close()` MUST be idempotent.
- A caller MAY invoke `Err()` after the iterator is exhausted or closed.
- Implementations MUST release resources when a caller invokes `Close()` or
  when the iterator reaches exhaustion.

---

## Prohibited Behaviors

- Implicit ordering guarantees
- Concealed buffering that alters visibility semantics

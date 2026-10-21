# CONTRACT_EXAMPLES.md — Example and Callsite Conventions

This contract defines normative conventions for examples and API call sites
in Lode.

---

## Authority

- `PUBLIC_API.md` is canonical for call-site conventions and API
  signatures.
- Example code must not diverge from `PUBLIC_API.md`.
- If an example and `PUBLIC_API.md` conflict, `PUBLIC_API.md` is
  authoritative.

---

## Record Construction Conventions

### Preferred: `lode.R(...)`

Use `lode.R(...)` in `Write` examples with inline record literals:

```go
records := lode.R(
    lode.D{"id": "1", "name": "alice"},
    lode.D{"id": "2", "name": "bob"},
)
snapshot, err := ds.Write(ctx, records, lode.Metadata{})
```

**Rationale:** `R(...)` is concise, reads naturally, and constructs `[]any`
directly.

### Allowed: `[]lode.D` for Iterator Backing

Use `[]lode.D` only when the slice backs a `RecordIterator`:

```go
// []lode.D is appropriate here: it backs the iterator
records := []lode.D{
    {"id": "1", "name": "alice"},
    {"id": "2", "name": "bob"},
}
iter := NewSliceIterator(records)
snapshot, err := ds.StreamWriteRecords(ctx, iter, lode.Metadata{})
```

When using `[]lode.D`, add a brief comment explaining that the slice backs
the iterator.

### Raw Blobs

For raw blob writes (without a codec), use `[]any{blobData}`:

```go
snapshot, err := ds.Write(ctx, []any{blobData}, lode.Metadata{})
```

---

## Variable Naming Conventions

| Variable | Purpose |
|----------|---------|
| `records` | A slice of records for `Write` or for backing an iterator |
| `iter` | A `RecordIterator` |
| `metadata` | A metadata map (`lode.Metadata{}`) |
| `snapshot` | The snapshot returned by a write operation |
| `ds` | A Dataset |
| `ctx` | A context |

---

## Pre-v1 Ergonomics Changes

Changes to ergonomics before v1 (for example, reordering parameters) are
permitted only when:

1. the contract is updated in the same PR as the behavior change;
2. `PUBLIC_API.md` is updated;
3. all tests are updated;
4. all examples are updated; and
5. the CHANGELOG includes a migration note.

After the change, no call site may retain the former form.

---

## Agent Behavior

Agents must:
- stop and ask if an example diverges from the conventions of
  `PUBLIC_API.md`;
- stop and ask before introducing a call-site pattern not covered here; and
- prefer editing existing examples to creating new ones.

---

## References

- [`PUBLIC_API.md`](../../PUBLIC_API.md): canonical API documentation
- [`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md): write API semantics

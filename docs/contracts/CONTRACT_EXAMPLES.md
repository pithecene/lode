# Lode Examples and Call Sites — Contract

This contract defines normative conventions for examples and API call sites
in Lode.

---

## Authority

- [`PUBLIC_API.md`](../../PUBLIC_API.md) is canonical for call-site conventions and API
  signatures.
- Example code must not diverge from [`PUBLIC_API.md`](../../PUBLIC_API.md).
- If an example and [`PUBLIC_API.md`](../../PUBLIC_API.md) conflict, [`PUBLIC_API.md`](../../PUBLIC_API.md) is
  authoritative.

---

## Record Construction Conventions

### Preferred: Typed Records

When a record has a known shape, define a struct and let the codec fix the
handle's record type:

```go
events, err := root.Records(ctx, "events", lode.JSONL[Event]())
snapshot, err := events.Write(ctx, []Event{{...}, {...}}, RunInfo{RunID: id})
```

Partition typed records with `lode.Hive(keys...).By(func(Event) lode.Partition)`.

### Map Records: `lode.R(...)`

Use `lode.R(...)` with `lode.JSONL[lode.D]()` for inline map record literals:

```go
records := lode.R(
    lode.D{"id": "1", "name": "alice"},
    lode.D{"id": "2", "name": "bob"},
)
snapshot, err := ds.Write(ctx, records, nil)
```

**Rationale:** `R(...)` is concise, reads naturally, and constructs the
`[]lode.D` that `Records[lode.D].Write` takes.

### Allowed: `[]lode.D` for Iterator Backing

Use a `[]lode.D` literal when the slice backs an iterator for
`Records.StreamWrite`:

```go
// []lode.D is appropriate here: it backs the iterator
records := []lode.D{
    {"id": "1", "name": "alice"},
    {"id": "2", "name": "bob"},
}
snapshot, err := ds.StreamWrite(ctx, each(records), nil)
```

When using `[]lode.D`, add a brief comment explaining that the slice backs
the iterator.

### Blobs

For blobs, open a blob dataset and pass the bytes:

```go
blobs, err := root.Blobs(ctx, "artifacts")
snapshot, err := blobs.Write(ctx, blobData, nil)
```

### Metadata

Pass metadata as a struct when its shape is known, and decode it with
`snapshot.Metadata.Decode(&v)`; pass a `lode.D` for ad hoc keys, or `nil`
for none.

---

## Variable Naming Conventions

| Variable | Purpose |
|----------|---------|
| `root` | A `*lode.Root` (or `readRoot` for a `*lode.ReadRoot`) |
| `records` | A slice of records for `Write` or for backing an iterator |
| `metadata` | A metadata value (a struct, a `lode.D`, or `nil`) |
| `snapshot` | The snapshot returned by a write operation |
| `ds` | A dataset handle (`Records[T]` or `Blobs`) |
| `view` | A read-only view (`RecordView[T]` or `BlobView`) |
| `ctx` | A context |

---

## Pre-v1 Ergonomics Changes

Changes to ergonomics before v1 (for example, reordering parameters) are
permitted only when:

1. the contract is updated in the same PR as the behavior change;
2. [`PUBLIC_API.md`](../../PUBLIC_API.md) is updated;
3. all tests are updated;
4. all examples are updated; and
5. the CHANGELOG includes a migration note.

After the change, no call site may retain the former form.

---

## Agent Behavior

Agents must:
- stop and ask if an example diverges from the conventions of
  [`PUBLIC_API.md`](../../PUBLIC_API.md);
- stop and ask before introducing a call-site pattern not covered here; and
- prefer editing existing examples to creating new ones.

---

## References

- [`PUBLIC_API.md`](../../PUBLIC_API.md): canonical API documentation
- [`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md): write API semantics

# Lode Examples and Call Sites Contract

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

### Typed Records as the Preferred Form

When a record has a known shape, define a struct and let the codec set the
handle's record type:

```go
events, err := root.Records(ctx, "events", lode.JSONL[Event]())
snapshot, err := events.Write(ctx, []Event{{...}, {...}}, RunInfo{RunID: id})
```

Partition typed records with `lode.Hive(keys...).By(func(Event) lode.Partition)`.

### Map Records with `lode.R(...)`

Use `lode.R(...)` with `lode.JSONL[lode.D]()` for inline map record literals:

```go
records := lode.R(
    lode.D{"id": "1", "name": "alice"},
    lode.D{"id": "2", "name": "bob"},
)
snapshot, err := ds.Write(ctx, records, nil)
```

Examples use `R(...)` because it states records concisely and constructs
the `[]lode.D` that `Records[lode.D].Write` takes.

### `[]lode.D` as Permitted Iterator Backing

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

For blobs, open a blob dataset and pass the bytes to its `Write` method:

```go
blobs, err := root.Blobs(ctx, "artifacts")
snapshot, err := blobs.Write(ctx, blobData, nil)
```

### Metadata

Pass metadata as a struct when the caller knows its shape, and decode it
with `snapshot.Metadata.Decode(&v)`. For ad hoc keys, use a `lode.D`, and
when a write carries no metadata, pass `nil`.

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

The project permits an ergonomic change before v1, such as reordering
parameters, only when:

1. the PR that changes the behavior also updates the contract,
2. the same PR updates [`PUBLIC_API.md`](../../PUBLIC_API.md),
3. the same PR updates all tests,
4. the same PR updates all examples, and
5. the CHANGELOG includes a migration note.

After the change, no call site may retain the former form.

---

## Agent Behavior

Agents must:
- stop and ask the maintainer if an example diverges from the conventions
  of [`PUBLIC_API.md`](../../PUBLIC_API.md)
- stop and ask the maintainer before introducing a call-site pattern that
  this contract does not address
- prefer editing existing examples to creating new ones

---

## References

- [`PUBLIC_API.md`](../../PUBLIC_API.md): canonical API documentation
- [`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md): write API semantics

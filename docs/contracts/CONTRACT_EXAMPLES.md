# Lode Examples and Call Sites Contract

This contract defines normative conventions for examples and API call sites
in Lode.

---

## Authority

- [`PUBLIC_API.md`](../../PUBLIC_API.md) is canonical for call-site
  conventions and API signatures.
- Example code MUST NOT diverge from [`PUBLIC_API.md`](../../PUBLIC_API.md).
- If an example and [`PUBLIC_API.md`](../../PUBLIC_API.md) conflict,
  [`PUBLIC_API.md`](../../PUBLIC_API.md) is authoritative.

---

## Record and Blob Construction Conventions

### Typed Records as the Preferred Form

When a record has a known shape, define a Go struct and let the codec infer
the handle's record type (for both `JSONL[T]()` and `Parquet[T](schema)`):

```go
events, err := root.Records(ctx, "events", lode.JSONL[Event]())
snapshot, err := events.Write(ctx, []Event{{...}, {...}}, RunInfo{RunID: id})
```

Partition typed records with `lode.Hive(keys...).By(func(Event) lode.Partition)`.

### Map Records with `lode.R(...)`

Use `lode.R(...)` with `lode.JSONL[lode.D]()` or `lode.Parquet[lode.D](schema)`
for inline map record literals passed to `Write`:

```go
records := lode.R(
    lode.D{"id": "1", "name": "alice"},
    lode.D{"id": "2", "name": "bob"},
)
snapshot, err := ds.Write(ctx, records, nil)
```

### `[]lode.D` as Permitted Iterator Backing

Use a `[]lode.D` literal only when the slice backs an `iter.Seq2[lode.D, error]`
iterator for `Records.StreamWrite`, accompanied by a brief explanatory
comment:

```go
// []lode.D is appropriate here because the slice backs the iterator.
records := []lode.D{
    {"id": "1", "name": "alice"},
    {"id": "2", "name": "bob"},
}
snapshot, err := ds.StreamWrite(ctx, each(records), nil)
```

### Blobs

Open a `*lode.Blobs` handle and pass `[]byte` to `Write` or an `io.Reader` to
`StreamWrite`:

```go
blobs, err := root.Blobs(ctx, "artifacts", lode.WithChecksum(lode.SHA256()))
snapshot, err := blobs.Write(ctx, blobData, nil)
streamSnap, err := blobs.StreamWrite(ctx, reader, lode.D{"filename": "archive.tar"})
```

### Metadata

Pass metadata as a typed struct when the caller knows its shape and decode it
with `snapshot.Metadata.Decode(&v)`. Use `lode.D` for ad hoc keys, and pass
`nil` when a write carries no metadata.

---

## Variable Naming Conventions

| Variable | Purpose |
|----------|---------|
| `root` | A `*lode.Root` (or `readRoot` for a `*lode.ReadRoot`) |
| `records` | A slice of records passed to `Write` or backing an iterator |
| `metadata` | A metadata value (a struct, a `lode.D`, or `nil`) |
| `snapshot` | A `*lode.Snapshot` (or `*volume.Snapshot`) returned by a write or commit |
| `ds` | A dataset writer handle (`*lode.Records[T]` or `*lode.Blobs`) |
| `view` | A read-only view (`*lode.RecordView[T]` or `*lode.BlobView`) |
| `ctx` | A `context.Context` |

---

## Ergonomics and API Changes

Any change to public call-site ergonomics requires updating together in a
single change:
1. every contract under `docs/contracts/` that describes the call,
2. [`PUBLIC_API.md`](../../PUBLIC_API.md) and [`README.md`](../../README.md),
3. all tests,
4. all runnable programs under `examples/`, and
5. [`CHANGELOG.md`](../../CHANGELOG.md).

---

## Agent Behavior

Agents MUST:
- stop and ask the maintainer when an example diverges from
  [`PUBLIC_API.md`](../../PUBLIC_API.md),
- stop and ask the maintainer before introducing a call-site pattern that
  this contract does not address, and
- prefer editing existing examples to creating new ones.

---

## References

- [`PUBLIC_API.md`](../../PUBLIC_API.md): canonical API documentation
- [`CONTRACT_WRITE_API.md`](CONTRACT_WRITE_API.md): write API semantics

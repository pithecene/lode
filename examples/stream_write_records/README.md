# Records.StreamWrite — Streaming Record Writes

This example demonstrates `Records.StreamWrite`, which streams large sets
of records without loading all of the data into memory.

## What This Demonstrates

1. **Iterator-driven streaming**: records come from an
   `iter.Seq2[T, error]`, one at a time.
2. **Memory efficiency**: only one record is in memory at a time (the
   iterator determines the pace).
3. **Atomic completion**: either all records are written and the manifest
   is created, or nothing is committed.
4. **Explicit metadata**: `nil` metadata persists as `{}`.

Note: commit visibility (a snapshot is invisible until its manifest exists)
is documented behavior but is not demonstrated here. See the
`manifest_driven` example for visibility semantics.

## When to Use Records.StreamWrite

Use `Records.StreamWrite` when:
- You have large streams of records that should not be buffered in memory
- Records come from a database cursor, a file reader, or a channel
- You are using a codec that streams (for example, JSONL)

Do not use `Records.StreamWrite` when:
- You need partitioning (single-pass streaming cannot partition records)
- Your codec does not stream
- The data is already in memory (use `Write` instead)

## Failure Modes

`Records.StreamWrite` validates its configuration and input before
streaming begins.

**Sentinel errors** (use `errors.Is()` to check):

| Error | Cause | Remedy |
|-------|-------|-----|
| `ErrNilIterator` | `nil` was passed as the iterator | Supply an iterator |
| `ErrCodecNotStreamable` | The codec is not a `StreamingCodec` | Use a streaming codec (for example, JSONL) or use `Write` |
| `ErrPartitioningNotSupported` | The dataset's layout is partitioned | Use `Write` for partitioned data |

### Iterator Errors

If the iterator yields a non-nil error during streaming:
- Streaming stops immediately.
- No manifest is written, so no snapshot is created.
- Partial data may remain in storage (its removal is best-effort, and
  `Sweep` reclaims it later).

### Abort Semantics

`Records.StreamWrite` is atomic: either all records are written and the
manifest is created, or nothing is committed. There is no explicit
`Abort()`; an error from the iterator aborts the write.

*Contract references: [`CONTRACT_ERRORS.md`](../../docs/contracts/CONTRACT_ERRORS.md), [`CONTRACT_WRITE_API.md`](../../docs/contracts/CONTRACT_WRITE_API.md)*

## The Iterator

`Records.StreamWrite` takes a range-over-func iterator:

```go
func each(records []lode.D) iter.Seq2[lode.D, error] {
    return func(yield func(lode.D, error) bool) {
        for _, r := range records {
            if !yield(r, nil) {
                return
            }
        }
    }
}
```

Yield a non-nil error to stop the write; stop yielding when `yield`
returns false.

## Run

```bash
go run ./examples/stream_write_records
```

## Expected Output

```
Storage root: /tmp/lode-stream-records-...

=== SETUP ===
Created dataset with JSONL codec (streaming-capable)

=== STREAM WRITE ===
Streaming 5 records...
Created snapshot: <id>
Row count: 5
Codec: jsonl
Files:
  - datasets/events/snapshots/<id>/data/data (...)

=== VERIFY ===
Read back 5 records:
  map[event:signup id:1 user:alice]
  ...

=== KEY POINTS ===
1. StreamWrite requires a streaming-capable codec (e.g., JSONL)
2. Records are consumed from the iterator one at a time (memory efficient)
3. Manifest is written only after successful completion
4. If the iterator yields an error, no manifest is written (no snapshot)
5. nil metadata persists as {}
6. Partitioning is NOT supported (single-pass streaming cannot partition)

=== SUCCESS ===
StreamWrite demonstration complete!
```

## Related

- [`blob_upload`](../blob_upload) — Blob streaming with `Blobs.StreamWrite`
- [`default_layout`](../default_layout) — In-memory `Write` with a codec
- [PUBLIC_API.md](../../PUBLIC_API.md) — Streaming API constraints

# Records.StreamWrite — Streaming Record Writes

This example demonstrates `Records.StreamWrite`, which streams large sets of
records without loading the entire dataset into memory.

## What This Demonstrates

1. **Iterator-driven streaming**: records come from an `iter.Seq2[T, error]`,
   one at a time.
2. **Memory efficiency**: only one record is held in memory at a time, at the
   pace of the iterator.
3. **Atomic completion**: either all records are encoded and the manifest is
   persisted, or no snapshot is committed.
4. **Explicit metadata**: `nil` metadata persists as `{}`.

Note: commit visibility (a snapshot is invisible until its canonical manifest
exists) is demonstrated separately in the `manifest_driven` example.

## When to Use Records.StreamWrite

Use `Records.StreamWrite` when:
- You have large streams of records that should not be buffered in memory.
- Records arrive from a database cursor, a file reader, or a channel.
- You configure a codec that implements `StreamingCodec[T]` (such as
  `JSONL[T]()`).

Do not use `Records.StreamWrite` when:
- You require Hive partitioning (single-pass streaming cannot group records
  across partition objects).
- Your codec does not stream (such as `Parquet[T]`, which requires a file
  footer).
- Your records are already in memory (use `Records.Write` instead).

## Failure Modes

`Records.StreamWrite` validates its configuration and input before streaming
begins.

**Sentinel errors** (check with `errors.Is()`):

| Error | Cause | Remedy |
|-------|-------|--------|
| `ErrInvalidArgument` | `nil` iterator or metadata that does not encode as a JSON object | Supply a non-nil iterator and object-shaped metadata (or `nil`) |
| `ErrCodecNotStreamable` | The codec does not implement `StreamingCodec[T]` | Use a streaming codec (`JSONL[T]()`) or call `Records.Write` |
| `ErrPartitioningNotSupported` | The dataset's layout is partitioned | Use `Records.Write` for partitioned data |

### Iterator Errors and Abort Semantics

If the iterator yields a non-nil error during streaming:
- Streaming stops immediately.
- Lode writes no manifest, so no snapshot is created.
- Partial data may remain in storage (best-effort cleanup runs when `Store.Put`
  succeeded before manifest persistence, and `Records.Sweep` reclaims any
  residual objects after its grace period).

`Records.StreamWrite` has no separate `Abort` handle: yielding an error from
the iterator or canceling `ctx` aborts the write before any manifest is
committed.

*Contract references: [`CONTRACT_ERRORS.md`](../../docs/contracts/CONTRACT_ERRORS.md), [`CONTRACT_WRITE_API.md`](../../docs/contracts/CONTRACT_WRITE_API.md)*

## The Iterator

`Records.StreamWrite` accepts a standard Go range-over-func iterator:

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

Yield a non-nil error to abort the write, and return immediately whenever
`yield` returns `false`.

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
Objects:
  - datasets/events/snapshots/<id>/data/data (... bytes)

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

- [`blob_upload`](../blob_upload): single-call blob streaming with `Blobs.StreamWrite`
- [`default_layout`](../default_layout): in-memory `Records.Write` with a codec
- [`PUBLIC_API.md`](../../PUBLIC_API.md): streaming API constraints

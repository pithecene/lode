# StreamWriteRecords — Streaming Record Writes

This example demonstrates `StreamWriteRecords`, which streams large sets of
records without loading all of the data into memory.

## What This Demonstrates

1. **Pull-based streaming**: records are retrieved from an iterator one at a
   time.
2. **Memory efficiency**: only one record is in memory at a time (the
   iterator determines the pace).
3. **Atomic completion**: either all records are written and the manifest
   is created, or nothing is committed.
4. **Explicit metadata**: `nil` metadata is converted to `Metadata{}` (pass
   `nil` or `{}` for empty metadata).

Note: commit visibility (a snapshot is invisible until its manifest exists)
is documented behavior but is not demonstrated here. See the
`manifest_driven` example for visibility semantics.

## When to Use StreamWriteRecords

Use `StreamWriteRecords` when:
- You have large streams of records that should not be buffered in memory
- Records come from a database cursor, a file reader, or a channel
- You are using a codec that supports streaming (for example, JSONL)

Do not use `StreamWriteRecords` when:
- You need partitioning (single-pass streaming cannot partition records)
- Your codec does not support streaming
- The data is already in memory (use `Write` instead)

## Failure Modes

`StreamWriteRecords` validates its configuration and input before streaming
begins.

**Sentinel errors** (use `errors.Is()` to check):

| Error | Cause | Remedy |
|-------|-------|-----|
| `ErrNilIterator` | `nil` was passed as the iterator | Supply a valid `RecordIterator` |
| `ErrCodecNotStreamable` | The codec does not support streaming | Use a streaming codec (for example, JSONL) or use `Write` |
| `ErrPartitioningNotSupported` | The dataset has partitioning configured | Use `Write` for partitioned data |

### Iterator Errors

If `iterator.Err()` returns a non-nil error during streaming:
- Streaming stops immediately.
- No manifest is written, so no snapshot is created.
- Partial data may remain in storage (its removal is best-effort, and
  `Dataset.Sweep` reclaims it later).

### Abort Semantics

`StreamWriteRecords` is atomic: either all records are written and the
manifest is created, or nothing is committed. There is no explicit
`Abort()`; an error during iteration aborts the write implicitly.

*Contract references: [`CONTRACT_ERRORS.md`](../../docs/contracts/CONTRACT_ERRORS.md), [`CONTRACT_WRITE_API.md`](../../docs/contracts/CONTRACT_WRITE_API.md)*

## RecordIterator Interface

```go
type RecordIterator interface {
    Next() bool  // Advances to next record. Returns false when exhausted.
    Record() any // Returns current record. Only valid after Next returns true.
    Err() error  // Returns any error encountered during iteration.
}
```

Implement this interface to stream from any data source.

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
1. StreamWriteRecords requires a streaming-capable codec (e.g., JSONL)
2. Records are pulled from iterator one at a time (memory efficient)
3. Manifest is written only after successful completion
4. If iterator returns error, no manifest is written (no snapshot)
5. nil metadata is coalesced to Metadata{} (pass nil or {} for empty)
6. Partitioning is NOT supported (single-pass streaming cannot partition)

=== SUCCESS ===
StreamWriteRecords demonstration complete!
```

## Related

- [`blob_upload`](../blob_upload) — Raw blob streaming with `StreamWrite`
- [`default_layout`](../default_layout) — In-memory `Write` with codec
- [PUBLIC_API.md](../../PUBLIC_API.md) — Streaming API constraints
